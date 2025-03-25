import { Injectable } from '@nestjs/common';
import { Pool } from 'pg';

@Injectable()
export class GetDbService {


    constructor() {
        // **Source Database Connection**


        // **Destination Database Connection**
    }

    // **Fetch records from source DB**
    async fetchRecords(): Promise<any[]> {
        try {
            const sourceDbPool = new Pool({

                connectionString: 'postgres://exampigejs:4VZXA2j7oV1xaO93WRBkVcst@192.168.194.181:6432/exampigejs',
                idleTimeoutMillis: 25000,
                connectionTimeoutMillis: 15000,
                query_timeout: 1500000,
            });
            const query = 'SELECT * FROM ecomm.option'; // Replace with your actual table
            const result = await sourceDbPool.query(query);
            sourceDbPool.end();
            return result.rows;
        } catch (error) {
            console.error('Error fetching records:', error);
            throw error;
        }
    }

    // **Insert records into destination DB**
    async insertRecords(tableName: string, records: any[]): Promise<void> {
        if (records.length === 0) {
            console.log('No records to insert.');
            return;
        }

        try {
            const destinationDbPool = new Pool({
                connectionString: 'postgres://exampigejs:gvF90tud9HTU3ar2lewdCB5P@localhost:5433/exampigejs',
                idleTimeoutMillis: 25000,
                connectionTimeoutMillis: 15000,
                query_timeout: 1500000,
            });
            const client = await destinationDbPool.connect();
            try {
                // **Extract Columns from First Record**
                const keys = Object.keys(records[0]);
                const queryList = [];

                for (let values of records) {
                    const namedParameters = this.convertObjectToStringInsert(values);
                    const query = `INSERT INTO ${tableName} (${keys.join(', ')})  VALUES (${namedParameters}) RETURNING *;`;
                    queryList.push(query);
                }



                await client.query(queryList.join(' '));
                console.log(`${records.length} records inserted into ${tableName}!`);
            } finally {
                client.end();
            }
        } catch (error) {
            console.error('Error inserting records:', error);
            throw error;
        }
    }

    convertObjectToStringInsert(obj) {
        const values = Object.values(obj).map(value => {
            if (value === '' || value === null || value === undefined) {
                // Handle null, undefined, or empty string
                return 'null';
            } else if (Array.isArray(value)) {
                // Handle arrays
                let arrayAsString = JSON.stringify(value).replace(/'/g, "'");
                arrayAsString = arrayAsString.replace(/'/g, "''")
                arrayAsString = `'${arrayAsString}'`;
                return arrayAsString;

            } else if (typeof value === 'string') {
                value = value.replace(/'/g, "''")
                return `'${value}'`;
            }
            else if (typeof value === 'object') {
                // Handle objects
                let objectAsString = JSON.stringify(value);
                return `'${objectAsString}'`;
            }
            else if (typeof value === 'number' || typeof value === 'boolean') {
                return `${value}`;
            } else {
                // Handle other data types as needed
                return `${value}`;
            }
        });

        return values.join(', ');
    }

    // **Main function to transfer data**
    async transferData() {
        try {
            console.log('Fetching records from source DB...');
            const records = await this.fetchRecords();

            if (records.length > 0) {
                console.log(`Fetched ${records.length} records. Inserting into destination DB...`);
                await this.insertRecords('ecomm.option', records);
            } else {
                console.log('No records found to transfer.');
            }
        } catch (error) {
            console.error('Data transfer failed:', error);
        }
    }
}
