import express from 'express';
import multer from 'multer';
import dotenv from 'dotenv';
import bodyParser from 'body-parser';
import fs from 'fs';
import fastCsv from 'fast-csv';
import pkg from 'pg';

const { Pool } = pkg;

// Load environment variables
dotenv.config();

const pool = new Pool({
    user: process.env.DBUSER,
    host: process.env.DBHOST,
    database: process.env.DBNAME,
    password: process.env.DBPASSWORD,
    port: process.env.DBPORT || 5432,
});

const app = express();
const PORT = process.env.PORT || 3000;
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.json());

// Simple hello world route
app.get('/', (req, res) => {
    res.send('Hello, World!');
});

// Ensure uploads folder exists
if (!fs.existsSync("uploads")) {
    fs.mkdirSync("uploads");
}

// Multer for file upload
const storage = multer.diskStorage({
    destination: (req, file, cb) => cb(null, "uploads/"),
    filename: (req, file, cb) => cb(null, `${Date.now()}-${file.originalname}`),
});
const upload = multer({ storage });

// Create table from CSV headers
const createTableFromCsv = async (filePath, tableName) => {
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(filePath);
        const headers = [];

        const csvStream = fastCsv
            .parse({ headers: true })
            .on("headers", async (cols) => {
                headers.push(...cols.map(col => col.replace(/\s+/g, "_").toLowerCase()));
                csvStream.pause();
                
                const client = await pool.connect();
                try {
                    await client.query(`DROP TABLE IF EXISTS ${tableName}`);
                    const columns = headers.map(header => `${header} TEXT`).join(", ");
                    const createTableQuery = `CREATE TABLE ${tableName} (id SERIAL PRIMARY KEY, ${columns})`;
                    await client.query(createTableQuery);
                    console.log(`✅ Table '${tableName}' created.`);
                    resolve(headers);
                } catch (error) {
                    reject(error);
                } finally {
                    client.release();
                    csvStream.resume();
                }
            })
            .on("error", (error) => reject(error))
            .on("end", () => console.log("CSV Headers Processed"));

        stream.pipe(csvStream);
    });
};

// Import data into table
const importCsvToDb = async (filePath, tableName, headers) => {
    return new Promise((resolve, reject) => {
        const stream = fs.createReadStream(filePath);
        const csvData = [];

        const csvStream = fastCsv
            .parse({ headers: true })
            .on("data", (row) => csvData.push(Object.values(row)))
            .on("end", async () => {
                if (csvData.length === 0) {
                    return reject(new Error("No data found in CSV"));
                }
                const client = await pool.connect();
                try {
                    await client.query("BEGIN");
                    const columns = headers.join(", ");
                    const placeholders = headers.map((_, i) => `$${i + 1}`).join(", ");
                    const insertQuery = `INSERT INTO ${tableName} (${columns}) VALUES (${placeholders})`;

                    for (const row of csvData) {
                        await client.query(insertQuery, row);
                    }
                    await client.query("COMMIT");
                    console.log(`✅ Data imported to '${tableName}'`);
                    resolve();
                } catch (err) {
                    await client.query("ROLLBACK");
                    reject(err);
                } finally {
                    client.release();
                    fs.unlinkSync(filePath);
                }
            })
            .on("error", (error) => reject(error));

        stream.pipe(csvStream);
    });
};

// Upload route
app.post("/upload", upload.array("files", 10), async (req, res) => {
    if (!req.files || req.files.length === 0) {
        return res.status(400).json({ message: "No files uploaded" });
    }

    console.log("Uploaded Files:", req.files); // Debugging

    try {
        for (const file of req.files) {
            const tableName = file.originalname.split(".")[0].replace(/\s+/g, "_").toLowerCase();
            const headers = await createTableFromCsv(file.path, tableName);
            await importCsvToDb(file.path, tableName, headers);
        }
        res.json({ message: "All CSV files imported successfully" });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Start server
app.listen(PORT, () => {
    console.log(`🚀 Server is running on http://localhost:${PORT}`);
});
