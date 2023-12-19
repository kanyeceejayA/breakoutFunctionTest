require('dotenv').config();
const mysql = require('mysql2/promise');
const zlib = require('zlib');
const util = require('util');
const zlibUnzipAsync = util.promisify(zlib.unzip);

async function processDatabaseRecords() {
    let sourceConnection, destConnection;
    try {
        // Establish connections
        sourceConnection = await mysql.createConnection({
            host: process.env.DB_HOST,
            user: process.env.DB_USER,
            password: process.env.DB_PASSWORD,
            database: process.env.DB_NAME,
        });
        console.log('Connected to the source database');

        destConnection = await mysql.createConnection({
            host: process.env.DB_HOST_DEST,
            user: process.env.DB_USER_DEST,
            password: process.env.DB_PASSWORD_DEST,
            database: process.env.DB_NAME_DEST,
        });
        console.log('Connected to the destination database');

        // Query the source database
        const [rows] = await sourceConnection.query(`SELECT * FROM ${process.env.DB_TABLE} LIMIT 1`);

        // Process each row
        for (const row of rows) {
            const questionnaireBuffer = row.questionnaire;
            const uncompressedData = await zlibUnzipAsync(questionnaireBuffer.slice(4));
            const data = JSON.parse(uncompressedData.toString('utf8'));

            for (const record of data.POPULATION_RECORD) {
                const filteredRecord = {
                    HH_KEEP_ROW: record.HH_KEEP_ROW,
                    HH_P2: record.HH_P2,
                    HH_P3: record.HH_P3,
                    HH_P4: record.HH_P4,
                    HH_P5: record.HH_P5,
                    HH_P6: record.HH_P6,
                    HH_P7: record.HH_P7,
                    HH_P8: record.HH_P8,
                    HH_P10: record.HH_P10
                };
                await destConnection.query('INSERT INTO population_record SET ?', filteredRecord, (err, result) => {
                    if (err) {
                        console.error('Error inserting data into destination database:', err);
                        return;
                    }
                    console.log('Inserted record into destination database:', result.insertId);
                });
                console.log('Processed record:', filteredRecord);
            }
        }
    } catch (err) {
        console.error('Error:', err);
    } finally {
        // Close connections
        if (sourceConnection) {
            await sourceConnection.end();
            console.log('Source connection closed');
        }
        if (destConnection) {
            await destConnection.end();
            console.log('Destination connection closed');
        }
    }
}

processDatabaseRecords();