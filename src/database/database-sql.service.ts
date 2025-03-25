import { Injectable } from '@nestjs/common';
import * as sql from 'mssql';
import { ConnectionPool } from 'mssql';

@Injectable()
export class DatabaseSqlService {
    sourceDBConfig = {
        user: 'sa',
        password: 'Fazi123@',
        server: '208.109.188.83',
        port: 1432,
        database: 'balkeappv1',
        options: {
            encrypt: true,
            trustServerCertificate: true,
        },
    };

    destinationDBConfig = {
        user: 'sa',
        password: '1234',
        server: 'DESKTOP-6G7AMFT',
        database: 'master',
        options: {
            trustServerCertificate: true,
        },
    };

    constructor() { }

    async transferDatabase() {
        const sourcePool = new sql.ConnectionPool(this.sourceDBConfig);
        let destinationPool = new sql.ConnectionPool(this.destinationDBConfig);

        try {
            await sourcePool.connect();
            await destinationPool.connect();
            const dbName = this.sourceDBConfig.database;
            const checkDbQuery = `SELECT 1 FROM sys.databases WHERE name = '${dbName}'`;
            const dbExists = (await destinationPool.request().query(checkDbQuery)).recordset.length > 0;

            if (!dbExists) {
                console.log(`Creating database: ${dbName}`);
                await destinationPool.request().query(`CREATE DATABASE ${dbName}`);
            }

            // Close and reconnect to the newly created database
            await destinationPool.close();
            destinationPool = new sql.ConnectionPool({ ...this.destinationDBConfig, database: dbName });
            await destinationPool.connect();

            // Start a transaction
            const transaction = new sql.Transaction(destinationPool);
            await transaction.begin();

            console.log('Transferring schema...');
            try {
                const schemaQuery = `
                SELECT 
                    c.TABLE_SCHEMA, 
                    c.TABLE_NAME, 
                    c.COLUMN_NAME, 
                    c.DATA_TYPE, 
                    c.IS_NULLABLE, 
                    c.COLUMN_DEFAULT, 
                    c.CHARACTER_MAXIMUM_LENGTH,
                    pk.CONSTRAINT_NAME AS PRIMARY_KEY,
                    fk.FOREIGN_KEY,
                    fk.REFERENCED_TABLE_SCHEMA,
                    fk.REFERENCED_TABLE_NAME,
                    fk.REFERENCED_COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS c
                LEFT JOIN (
                    SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME, tc.CONSTRAINT_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
                        ON ku.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                    WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA AND c.TABLE_NAME = pk.TABLE_NAME AND c.COLUMN_NAME = pk.COLUMN_NAME
                LEFT JOIN (
                    SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME, 
                        rc.UNIQUE_CONSTRAINT_SCHEMA AS REFERENCED_TABLE_SCHEMA, 
                        rc.UNIQUE_CONSTRAINT_NAME AS FOREIGN_KEY,
                        ku2.TABLE_NAME AS REFERENCED_TABLE_NAME,
                        ku2.COLUMN_NAME AS REFERENCED_COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
                        ON ku.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku2 
                        ON rc.UNIQUE_CONSTRAINT_NAME = ku2.CONSTRAINT_NAME
                ) fk ON c.TABLE_SCHEMA = fk.TABLE_SCHEMA AND c.TABLE_NAME = fk.TABLE_NAME AND c.COLUMN_NAME = fk.COLUMN_NAME
                WHERE c.TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
                `;

                const schemaResult = await sourcePool.request().query(schemaQuery);
                const schemaMap = new Map<string, Map<string, any[]>>();
                const relationships = new Map<string, any[]>(); // To store relationships

                for (const row of schemaResult.recordset) {
                    const {
                        TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH,
                        PRIMARY_KEY, FOREIGN_KEY, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                    } = row;

                    // Store table schema
                    if (!schemaMap.has(TABLE_SCHEMA)) {
                        schemaMap.set(TABLE_SCHEMA, new Map());
                    }

                    const tableMap = schemaMap.get(TABLE_SCHEMA)!;
                    if (!tableMap.has(TABLE_NAME)) {
                        tableMap.set(TABLE_NAME, []);
                    }

                    tableMap.get(TABLE_NAME)!.push({ COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH, PRIMARY_KEY, FOREIGN_KEY });

                    // Store relationships
                    if (FOREIGN_KEY) {
                        if (!relationships.has(TABLE_NAME)) {
                            relationships.set(TABLE_NAME, []);
                        }
                        relationships.get(TABLE_NAME)!.push({
                            column: COLUMN_NAME,
                            referencedTable: REFERENCED_TABLE_NAME,
                            referencedColumn: REFERENCED_COLUMN_NAME
                        });
                    }
                }
                const insertOrder = [];
                const visited = new Set<string>();

                function resolveDependencies(tableName: string) {
                    if (visited.has(tableName)) return;
                    visited.add(tableName);

                    if (relationships.has(tableName)) {
                        for (const relation of relationships.get(tableName)!) {
                            resolveDependencies(relation.referencedTable); // Process parent first
                        }
                    }

                    insertOrder.push(tableName); // Add table after its dependencies
                }

                // Compute insert order for all tables
                for (const [schema, tables] of schemaMap.entries()) {
                    for (const table of tables.keys()) {
                        resolveDependencies(table);
                    }
                }

                console.log("Table Insert Order:", insertOrder);

                for (const [schemaName, tableMap] of schemaMap.entries()) {
                    await destinationPool.request().query(
                        `IF SCHEMA_ID('${schemaName}') IS NULL EXEC('CREATE SCHEMA ${schemaName}')`
                    );

                    for (const item of insertOrder) {
                        // Get primary keys and foreign keys for this table
                        const tableMapCheck = schemaMap.get(schemaName)
                        let columns = tableMapCheck.get(item);
                        const tableName = item
                        const createTableQuery = await this.fetchTableQuery(sourcePool, schemaName, tableName, columns);

                        await destinationPool.request().query(createTableQuery);
                    }
                }



                console.log('Transferring data...');
                const tablesQuery = `
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
            `;

                const tablesResult = await sourcePool.request().query(tablesQuery);

                const tablesArray = tablesResult.recordsets[0];

                // Sort based on insertOrder
                const sortedTables = tablesArray.sort(
                    (a, b) => insertOrder.indexOf(a.TABLE_NAME) - insertOrder.indexOf(b.TABLE_NAME)
                );

                console.log(sortedTables);
                for (const row of sortedTables) {
                    await this.recordInsert(row, sourcePool, destinationPool);
                }
                await transaction.commit();
            } catch (error) {
                await transaction.rollback();
                console.error('Error during database transfer:', error);
            }
            console.log('Database transfer completed!');
        } catch (error) {
            console.error('Error during database transfer:', error);
        } finally {
            await sourcePool.close();
            await destinationPool.close();
        }
    }

    async fetchTableQuery(sourcePool, schemaName, tableName, columns) {
        const primaryKeys = await sourcePool.request().query(`
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = '${schemaName}'
            AND TABLE_NAME = '${tableName}'
            AND CONSTRAINT_NAME IN (
                SELECT CONSTRAINT_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
                WHERE TABLE_SCHEMA = '${schemaName}'
                AND TABLE_NAME = '${tableName}'
                AND CONSTRAINT_TYPE = 'PRIMARY KEY'
            )
        `);

        const foreignKeys = await sourcePool.request().query(`
            SELECT 
                fk.COLUMN_NAME, 
                rc.UNIQUE_CONSTRAINT_SCHEMA AS REFERENCED_TABLE_SCHEMA, 
                ku.TABLE_NAME AS REFERENCED_TABLE_NAME, 
                ku.COLUMN_NAME AS REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk
            JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
                ON fk.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku 
                ON rc.UNIQUE_CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                                        AND fk.TABLE_SCHEMA = '${schemaName}'
                                        AND fk.TABLE_NAME = '${tableName}'
        `);

        // Define columns
        const columnDefinitions = columns
            .map((col: any) => {
                let columnName = `[${col.COLUMN_NAME}]`; // Wrap column names in square brackets
                let dataType = col.DATA_TYPE;

                if (dataType === 'nvarchar' || dataType === 'varchar') {
                    dataType += col.CHARACTER_MAXIMUM_LENGTH === -1 ? '(MAX)' : `(${col.CHARACTER_MAXIMUM_LENGTH})`;
                }

                return `${columnName} ${dataType} ${col.IS_NULLABLE === 'NO' ? 'NOT NULL' : ''} ${col.COLUMN_DEFAULT ? `DEFAULT ${col.COLUMN_DEFAULT}` : ''
                    }`;
            })
            .join(', ');

        // Define Primary Key Constraint
        const primaryKeyColumns = primaryKeys.recordset.map((pk) => `[${pk.COLUMN_NAME}]`).join(', ');
        const primaryKeyConstraint = primaryKeyColumns ? `, CONSTRAINT PK_${tableName} PRIMARY KEY (${primaryKeyColumns})` : '';

        // Define Foreign Key Constraints
        const foreignKeyConstraints = foreignKeys.recordset
            .map(
                (fk) => `, CONSTRAINT FK_${tableName}_${fk.COLUMN_NAME} FOREIGN KEY ([${fk.COLUMN_NAME}]) REFERENCES ${fk.REFERENCED_TABLE_SCHEMA}.${fk.REFERENCED_TABLE_NAME} ([${fk.REFERENCED_COLUMN_NAME}])`
            )
            .join(' ');

        // Final CREATE TABLE query
        const createTableQuery = `
            IF OBJECT_ID('${schemaName}.${tableName}', 'U') IS NULL
            CREATE TABLE ${schemaName}.${tableName} (
                ${columnDefinitions}
                ${primaryKeyConstraint}
                ${foreignKeyConstraints}
            )
        `;
        return createTableQuery;
        console.log(createTableQuery);
    }
    async recordInsert(row, sourcePool, destinationPool) {
        const BATCH_SIZE = 1000;
        const { TABLE_SCHEMA, TABLE_NAME } = row;
        console.log(`Transferring data for table: ${TABLE_SCHEMA}.${TABLE_NAME}`);

        // Fetch data from the source database
        const data = await sourcePool.request().query(`SELECT * FROM ${TABLE_SCHEMA}.${TABLE_NAME}`);

        if (data.recordset.length === 0) {
            console.log(`Skipping empty table: ${TABLE_SCHEMA}.${TABLE_NAME}`);
            return;
        }

        const columns = Object.keys(data.recordset[0]).map(col => `[${col}]`).join(', ');

        let valueRows: string[] = [];

        for (let i = 0; i < data.recordset.length; i++) {
            const record = data.recordset[i];

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
            if (valueRows.length === BATCH_SIZE || i === data.recordset.length - 1) {
                const insertQuery = ` INSERT INTO ${TABLE_SCHEMA}.${TABLE_NAME} (${columns})  VALUES ${valueRows.join(',\n')} `;

                console.log(`Executing batch insert (${valueRows.length} rows) for: ${TABLE_SCHEMA}.${TABLE_NAME}`);
                await destinationPool.request().query(insertQuery);

                // **Reset the valueRows array for the next batch**
                valueRows = [];
            }
        }
    }
    async fetchTableRecord() {
        const tableInfo = { TABLE_SCHEMA: 'dbo', TABLE_NAME: 'Transactions' };
        const { TABLE_SCHEMA, TABLE_NAME } = tableInfo;
        const query = ` SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '${TABLE_SCHEMA}' AND TABLE_NAME = '${TABLE_NAME}'`;

        const sourcePool = new sql.ConnectionPool(this.sourceDBConfig);
        this.destinationDBConfig.database = 'balkeappv1';
        let destinationPool = new sql.ConnectionPool(this.destinationDBConfig);

        try {
            await sourcePool.connect();
            await destinationPool.connect();
            const columns = await sourcePool.request().query(query);
            const createTableQuery = await this.fetchTableQuery(sourcePool, TABLE_SCHEMA, TABLE_NAME, columns.recordset);
            await destinationPool.request().query(createTableQuery);
            await this.recordInsert(tableInfo, sourcePool, destinationPool);

        } catch (error) {
            console.error('Error during database transfer:', error);
        }
    }
}
