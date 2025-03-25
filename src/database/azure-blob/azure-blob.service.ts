import { Injectable } from '@nestjs/common';
import { BlobServiceClient } from '@azure/storage-blob';
import * as sql from 'mssql';
import * as fs from 'fs';
import * as path from 'path';
import * as stream from 'stream';
import * as dotenv from 'dotenv';
dotenv.config({ path: './.env' });

@Injectable()
export class AzureBlobService {
    private blobServiceClient: BlobServiceClient;
    private containerClient;
    destinationDBConfig = {
        user: 'sa',
        password: '1234',
        server: 'DESKTOP-6G7AMFT',
        database: 'balkeappv1',
        options: {
            trustServerCertificate: true,
        },
    }
    constructor() {
        const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
        const containerName = process.env.AZURE_STORAGE_CONTAINER_NAME;

        if (!connectionString || !containerName) {
            throw new Error('Azure storage connection string or container name is missing.');
        }

        this.blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
        this.containerClient = this.blobServiceClient.getContainerClient(containerName);
    }

    // Upload File
    async uploadFile(file: any): Promise<string> {
        const blobName = `${Date.now()}-${file.originalname}`;
        const blockBlobClient = this.containerClient.getBlockBlobClient(blobName);
        await blockBlobClient.uploadData(file.buffer);
        return blobName;
    }

    // Get Blob URL
    getBlobUrl(blobName: string): string {
        return `${this.containerClient.url}/${blobName}`;
    }

    // Download File
    async downloadFile(blobName: string): Promise<Buffer> {
        const blockBlobClient = this.containerClient.getBlockBlobClient(blobName);
        const downloadBlockBlobResponse = await blockBlobClient.download();
        return this.streamToBuffer(downloadBlockBlobResponse.readableStreamBody);
    }

    // Helper function to convert stream to buffer
    private async streamToBuffer(readableStream: stream.Readable): Promise<Buffer> {
        return new Promise((resolve, reject) => {
            const chunks: Buffer[] = [];
            readableStream.on('data', (chunk) => chunks.push(chunk));
            readableStream.on('end', () => resolve(Buffer.concat(chunks)));
            readableStream.on('error', reject);
        });
    }

    async saveFileData(blobName: string): Promise<{ data: Buffer; contentType: string }> {
        const blobClient = this.containerClient.getBlobClient(blobName);
        const downloadBlockBlobResponse = await blobClient.download();

        if (!downloadBlockBlobResponse.readableStreamBody) {
            throw new Error('Blob not found or empty');
        }

        // Convert stream to Buffer
        const chunks: Buffer[] = [];
        for await (const chunk of downloadBlockBlobResponse.readableStreamBody) {
            chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)); // Convert to Buffer if needed
        }

        const fileBuffer = Buffer.concat(chunks);

        const contentType = downloadBlockBlobResponse.contentType || 'application/octet-stream';

        let destinationPool = new sql.ConnectionPool(this.destinationDBConfig);
        await destinationPool.connect();
        const transaction = new sql.Transaction(destinationPool);
        await transaction.begin();

        try {
            let tableName = blobName.split('.')[0];
            tableName = tableName?.toLowerCase();
            const createTable = this.generateCreateTableQuery(tableName, JSON.parse(fileBuffer.toString()));

            const data = await destinationPool.request().query(createTable);
            const tableInfo = { TABLE_SCHEMA: 'dbo', TABLE_NAME: tableName };
            await this.recordInsert(tableInfo, destinationPool, JSON.parse(fileBuffer.toString()));
            await transaction.commit();
            await transaction.rollback();

        } catch (error) {

        }
        finally {
            await destinationPool.close();
        }

        return { data: fileBuffer, contentType };
    }

    async recordInsert(row, destinationPool, data) {
        const BATCH_SIZE = 1000;
        const { TABLE_SCHEMA, TABLE_NAME } = row;
        console.log(`Transferring data for table: ${TABLE_SCHEMA}.${TABLE_NAME}`);


        const columns = Object.keys(data[0]).map(col => `[${col}]`).join(', ');

        let valueRows: string[] = [];

        for (let i = 0; i < data.length; i++) {
            const record = data[i];

            const values = Object.values(record)
                .map((v) => {
                    if (v instanceof Date) {
                        return `'${v.toISOString().slice(0, 19).replace('T', ' ')}'`;
                    } else if (typeof v === 'string') {
                        return `'${v.replace(/'/g, "''")}'`;
                    } else if (typeof v === 'boolean') {
                        return v ? 1 : 0;
                    } else if (v === null || v === undefined) {
                        return 'NULL';
                    } else if (typeof v === 'number') {
                        return v;
                    } else if (Buffer.isBuffer(v)) {
                        return `0x${v.toString('hex')}`;
                    } else {
                        return `'${String(v).replace(/'/g, "''")}'`;
                    }
                })
                .join(', ');

            valueRows.push(`(${values})`);

            // **Execute batch insert when we reach BATCH_SIZE**
            if (valueRows.length === BATCH_SIZE || i === data.length - 1) {
                const insertQuery = ` INSERT INTO ${TABLE_SCHEMA}.${TABLE_NAME} (${columns})  VALUES ${valueRows.join(',\n')} `;

                console.log(`Executing batch insert (${valueRows.length} rows) for: ${TABLE_SCHEMA}.${TABLE_NAME}`);
                await destinationPool.request().query(insertQuery);

                // **Reset the valueRows array for the next batch**
                valueRows = [];
            }
        }
    }
    generateCreateTableQuery(tableName, jsonArray) {
        if (!Array.isArray(jsonArray) || jsonArray.length === 0) {
            throw new Error("Invalid JSON data: Must be a non-empty array.");
        }
    
        let columns = new Map(); // Using Map to store column types
        let hasIdColumn = false;
    
        for (const obj of jsonArray) {
            for (const [key, value] of Object.entries(obj)) {
                let columnType;
    
                if (key.toLowerCase() === "id") {
                    hasIdColumn = true;
                    columnType = "BIGINT IDENTITY(1,1) PRIMARY KEY"; // Auto-incremented primary key
                } else if (typeof value === "string") {
                    columnType = "NVARCHAR(255)"; // For text data
                } else if (typeof value === "number") {
                    columnType = Number.isInteger(value) ? "INT" : "DECIMAL(18,2)"; // INT for whole numbers, DECIMAL for float
                } else if (typeof value === "boolean") {
                    columnType = "BIT"; // For boolean values
                } else {
                    columnType = "NVARCHAR(MAX)"; // Default to text if unknown type
                }
    
                // Ensure column type consistency (e.g., avoid conflicts if the same column appears with different types)
                if (!columns.has(key)) {
                    columns.set(key, columnType);
                }
            }
        }
    
        // If no 'id' column is found, add it as BIGINT AUTO_INCREMENT PRIMARY KEY
        if (!hasIdColumn) {
            columns.set("id", "BIGINT IDENTITY(1,1) PRIMARY KEY");
        }
    
        const columnDefinitions = Array.from(columns.entries()).map(([key, type]) => `[${key}] ${type}`);
        const query = `CREATE TABLE [${tableName}] (\n    ${columnDefinitions.join(",\n    ")}\n);`;
        return query;
    }
    
}
