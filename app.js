require('dotenv').config(); // Load environment variables from .env

const mysql = require('mysql2');
const zlib = require('zlib'); // Import the zlib module

// Create a connection to the MySQL database using environment variables
const connection = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    table: process.env.DB_TABLE
});

console.log('username is ' + process.env.DB_USER)

// Connect to the database
connection.connect((err) => {
    if (err) {
        console.error('Error connecting to the database: ' + err.stack);
        return;
    }
    console.log('Connected to the database');
});

// Perform a query to read records from a table
connection.query(`SELECT * FROM ${process.env.DB_TABLE} limit 1`, (err, results, fields) => {
    if (err) {
        console.error('Error executing query: ' + err.stack);
        return;
    }

    // Process each row
    results.forEach((row) => {
        const questionnaireBuffer = row.questionnaire;

        // Unzip and process the data
        zlib.unzip(questionnaireBuffer.slice(4), (err, uncompressedData) => {
            if (err) {
                console.error('Error unzipping data:', err);
                return;
            }

            // Handle the uncompressed data here (e.g., log it)
            const uncompressedString = uncompressedData.toString('utf8');
            //   console.log('Uncompressed Data:', uncompressedString);
            const data = JSON.parse(uncompressedString);
            console.log('Data:', data);
            data.POPULATION_RECORDS.forEach((record) => {
                console.log('Record:', record);
            }
            );
        });
    });

    

    // Close the database connection
    connection.end((err) => {
        if (err) {
            console.error('Error closing the database connection: ' + err.stack);
            return;
        }
        console.log('Connection closed');
    });
});
