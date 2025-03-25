import { Injectable, Logger } from '@nestjs/common';
import { exec } from 'child_process';
import * as fs from 'fs-extra';
import * as path from 'path';
import { Client } from 'pg';

@Injectable()
export class DatabaseService {
    private readonly logger = new Logger(DatabaseService.name);

    private readonly SOURCE_DB = {
        user: 'aiappupdef',
        host: 'localhost',
        port: 5432,
        database: 'aiappupdef',
        password: '1234',
    };

    private readonly DESTINATION_DB = {
        user: 'postgres',
        host: 'localhost',
        port: 5433,
        database: 'postgres',
        password: '1234',
    };
    sourceDBConfig = {
        user: 'postgres',
        host: 'localhost',
        port: 5432,
        database: 'aiappupdef',
        password: '1234',
    };

    destinationDBConfig = {
        user: 'user2',
        host: 'localhost',
        port: 5433,
        database: 'postgres',
        password: '1234',
    };

    private readonly POSTGRES_BIN_PATH = `"C:\\Program Files\\PostgreSQL\\17\\bin\\"`; // PostgreSQL bin folder
    private readonly BACKUP_FILE = path.join(__dirname, '../backups/remote_backup.dump'); // Store in project

    constructor() {
        // 🔹 Ensure "backups" folder exists
        fs.ensureDirSync(path.join(__dirname, '../backups'));
    }

    // 🔹 Backup Remote Database to Local
    async backupRemoteDatabase(): Promise<void> {
        const command = `SET PGPASSWORD=${this.SOURCE_DB.password} && ${this.POSTGRES_BIN_PATH}pg_dump.exe -U ${this.SOURCE_DB.user} -h ${this.SOURCE_DB.host} -p ${this.SOURCE_DB.port} -d ${this.SOURCE_DB.database} -F c -f "${this.BACKUP_FILE}"`;

        this.logger.log(`Backing up remote database to: ${this.BACKUP_FILE}`);
        exec(command, (error, stdout, stderr) => {
            if (error) {
                this.logger.error('Backup failed:', stderr);
            } else {
                this.logger.log('Remote database backup successful!');
            }
        });
    }

    // 🔹 Restore Backup to Local or Another Server
    async restoreDatabase(): Promise<void> {
        if (!fs.existsSync(this.BACKUP_FILE)) {
            this.logger.error('Backup file not found! Run backup first.');
            return;
        }

        const command = `SET PGPASSWORD=${this.DESTINATION_DB.password} && ${this.POSTGRES_BIN_PATH}pg_restore.exe -U ${this.DESTINATION_DB.user} -h ${this.DESTINATION_DB.host} -p ${this.DESTINATION_DB.port} -d ${this.DESTINATION_DB.database} -c "${this.BACKUP_FILE}"`;

        this.logger.log(`Restoring database from: ${this.BACKUP_FILE}`);
        exec(command, (error, stdout, stderr) => {
            if (error) {
                this.logger.error('Restore failed:', stderr);
            } else {
                this.logger.log('Database restored successfully!');
            }
        });
    }
    async transferDatabase() {
    const sourceDB = new Client(this.sourceDBConfig);
    const destinationDB = new Client({ ...this.destinationDBConfig, database: 'postgres' }); // Connect to default DB to check/create DB

    try {
        await sourceDB.connect();
        await destinationDB.connect();

        // Step 1: Check if the destination database exists; if not, create it
        const dbName = this.sourceDBConfig.database;
        const checkDbQuery = `SELECT 1 FROM pg_database WHERE datname = '${dbName}'`;
        const dbExists = (await destinationDB.query(checkDbQuery)).rowCount > 0;

        if (!dbExists) {
            console.log(`Creating database: ${dbName}`);
            await destinationDB.query(`CREATE DATABASE ${dbName}`);
        }

        // Reconnect to the destination database
        await destinationDB.end();
        const destinationDBWithDb = new Client({ ...this.destinationDBConfig, database: dbName });
        await destinationDBWithDb.connect();

        // Step 2: Transfer the schema (tables, indexes, constraints, etc.)
        console.log('Transferring schema...');
        const schemaQuery = await sourceDB.query(`
            SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          `);

        // Group columns by schema and table
        const schemaMap = new Map<string, Map<string, any[]>>();
        for (const row of schemaQuery.rows) {
            const { table_schema, table_name, column_name, data_type, is_nullable, column_default } = row;

            if (!schemaMap.has(table_schema)) {
                schemaMap.set(table_schema, new Map());
            }

            const tableMap = schemaMap.get(table_schema)!;
            if (!tableMap.has(table_name)) {
                tableMap.set(table_name, []);
            }

            tableMap.get(table_name)!.push({ column_name, data_type, is_nullable, column_default });
        }

        // Create schemas and tables in the destination database
        for (const [schemaName, tableMap] of schemaMap.entries()) {
            // Create the schema if it doesn't exist
            await destinationDBWithDb.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`);

            for (const [tableName, columns] of tableMap.entries()) {
                const columnDefinitions = columns
                    .map(
                        (col) =>
                            `${col.column_name} ${col.data_type} ${col.is_nullable === 'NO' ? 'NOT NULL' : ''} ${col.column_default ? `DEFAULT ${col.column_default}` : ''
                            }`,
                    )
                    .join(', ');

                const createTableQuery = `
                CREATE TABLE IF NOT EXISTS ${schemaName}.${tableName} (
                  ${columnDefinitions}
                )
              `;
                await destinationDBWithDb.query(createTableQuery);
            }
        }

        // Step 3: Transfer the data (rows in each table)
        console.log('Transferring data...');
        const tablesQuery = await sourceDB.query(`
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          `);

        for (const row of tablesQuery.rows) {
            const { table_schema, table_name } = row;
            console.log(`Transferring data for table: ${table_schema}.${table_name}`);

            // Fetch data from the source table
            const data = await sourceDB.query(`SELECT * FROM ${table_schema}.${table_name}`);

            // Insert data into the destination table
            for (const record of data.rows) {
                const columns = Object.keys(record).join(', ');
                const values = Object.values(record)
                    .map((v) => (typeof v === 'string' ? `'${v}'` : v))
                    .join(', ');

                await destinationDBWithDb.query(`
                INSERT INTO ${table_schema}.${table_name} (${columns})
                VALUES (${values})
              `);
            }
        }

        console.log('Database transfer completed!');
    } catch (error) {
        console.error('Error during database transfer:', error);
    } finally {
        await sourceDB.end();
        await destinationDB.end();
    }
}
}
