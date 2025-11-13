const mysql = require("mysql2/promise");
require("dotenv").config();

const pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  port: process.env.MYSQL_PORT,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  connectTimeout: 10000,
});

async function connectMySQL() {
  try {
    const connection = await pool.getConnection();

    console.log("✅ MySQL connected");

    const [rows] = await connection.execute("SELECT NOW() AS now");
    console.log("Database time:", rows[0]);

    connection.release();
  } catch (err) {
    console.error("❌ MySQL connection error:", err.message);
  }
}

// connectMySQL();
module.exports = pool;
