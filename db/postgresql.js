const { Pool } = require("pg");
require("dotenv").config();

const pool = new Pool({
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: process.env.PG_PORT,
  connectionTimeoutMillis: 10000,
});

async function connectPostgres() {
  try {
    await pool.connect();
    console.log("Connected to PostgreSQL database");
    const res = await pool.query("SELECT NOW()");
    console.log("Database time:", res.rows[0]);
  } catch (err) {
    console.error("Error connecting to PostgreSQL database:", err);
    throw err;
  }
}

// connectPostgres();

module.exports = pool;
