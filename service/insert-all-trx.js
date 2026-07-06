const poolPg = require("../db/postgresql");
const poolMy = require("../db/mysql");
const moment = require("moment");
const cron = require("node-cron");

// ============================================================
// CONFIGURATION
// ============================================================
const MAX_RETRIES = 3;
const BASE_DELAY_MS = 5000; // 5s -> 10s -> 20s (exponential)
const TRANSIENT_ERROR_CODES = [
  "ETIMEDOUT",
  "ECONNRESET",
  "ECONNREFUSED",
  "PROTOCOL_CONNECTION_LOST",
  "EPIPE",
  "EAI_AGAIN",
];

// ============================================================
// UTILITY: Retry with Exponential Backoff
// ============================================================
async function withRetry(fn, fnName, maxRetries = MAX_RETRIES, baseDelayMs = BASE_DELAY_MS) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      const isTransient = TRANSIENT_ERROR_CODES.includes(err.code);

      if (isTransient && attempt < maxRetries) {
        const delay = baseDelayMs * Math.pow(2, attempt - 1);
        console.warn(
          `[RETRY] ${fnName} failed (attempt ${attempt}/${maxRetries}): ${err.code}. Retrying in ${delay / 1000}s...`,
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
      } else {
        console.error(
          `[FAILED] ${fnName} failed after ${attempt} attempt(s):`,
          err.message || err,
        );
        throw err;
      }
    }
  }
}

// ============================================================
// UTILITY: Sleep
// ============================================================
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================================
// STEP 1: Fetch dates from MySQL
// ============================================================
async function getDatesFromMySQL() {
  let conn;
  try {
    conn = await poolMy.getConnection();
    const queryGetDate = `SELECT tanggal FROM transaksi t GROUP BY TANGGAL;`;
    const [rows] = await conn.query(queryGetDate);
    if (!rows.length) return [];
    return rows.map((r) => moment(r.tanggal).format("YYYY-MM-DD"));
  } catch (err) {
    console.error("Error getting dates from MySQL:", err.message || err);
    throw err;
  } finally {
    if (conn) conn.release();
  }
}

// ============================================================
// STEP 2: Fetch recap data from MySQL
// ============================================================
async function getDataFromMySQL() {
  let conn;
  try {
    conn = await poolMy.getConnection();
    const query = `
    SELECT
    x.jam_group,
    x.tanggal,
    x.NAMAPRODUK,
    x.NamaReseller,
    x.KodeProduk,
    x.status_transaksi,
    x.total_trx,
    x.amount,
    ROUND(y.sukses_trx / y.total_trx * 100, 2) AS success_rate
FROM
(
    SELECT
        CONCAT(
            LPAD(FLOOR(HOUR(JAM)/3)*3, 2, '0'), ':00 - ',
            LPAD(FLOOR(HOUR(JAM)/3)*3 + 2, 2, '0'), ':59'
        ) AS jam_group,
        tanggal,
        NAMAPRODUK,
        NamaReseller,
        t.KodeProduk,
        CASE
            WHEN statustransaksi = 1 THEN 'SUKSES'
            WHEN statustransaksi = 2 THEN 'GAGAL'
            ELSE 'PENDING'
        END AS status_transaksi,
        COUNT(*) AS total_trx,
        SUM(CASE WHEN statustransaksi = 1 THEN HARGAJUAL ELSE 0 END) AS amount
    FROM transaksi t
    JOIN produk p
        ON p.KodeProduk = t.KodeProduk
    WHERE jenistransaksi IN ('1','6','8')
    GROUP BY
        NamaReseller,
        t.KodeProduk,
        NAMAPRODUK,
        tanggal,
        jam_group,
        statustransaksi
) x
JOIN
(
    SELECT
        NamaReseller,
        KodeProduk,
        tanggal,
        COUNT(*) AS total_trx,
        SUM(CASE WHEN statustransaksi = 1 THEN 1 ELSE 0 END) AS sukses_trx
    FROM transaksi
    WHERE jenistransaksi IN ('1','6','8')
    GROUP BY
        NamaReseller,
        KodeProduk,
        tanggal
    ) y
    ON  x.NamaReseller = y.NamaReseller
    AND x.KodeProduk   = y.KodeProduk
    AND x.tanggal      = y.tanggal;
    `;
    const { date, startTime, endTime } = getPrevious3HourWindow();

    const [rows] = await conn.query(query, [date, startTime, endTime]);
    console.log(rows.length, "rows found");
    return rows;
  } catch (err) {
    console.error("Error getting MySQL connection:", err.message || err);
    throw err;
  } finally {
    if (conn) conn.release();
  }
}

// ============================================================
// STEP 3: Atomic DELETE + INSERT in PostgreSQL Transaction
// ============================================================
async function syncToPostgres(dates, datas) {
  if (datas.length === 0) {
    console.log("No data to insert, skipping sync");
    return;
  }

  const client = await poolPg.connect();
  try {
    // --- BEGIN TRANSACTION ---
    await client.query("BEGIN");

    if (dates.length > 0) {
      await client.query(
        `DELETE FROM summary_transaction WHERE tanggal = ANY($1::text[])`,
        [dates],
      );
      console.log("Old data deleted successfully (within transaction)");
    }

    const mappedDatas = datas.map((data) => ({
      tanggal: moment(data.tanggal).format("YYYY-MM-DD"),
      range_hour: data.jam_group,
      product_name: data.NAMAPRODUK,
      product_code: data.KodeProduk,
      status: data.status_transaksi,
      client_name: data.NamaReseller,
      total_transaction: data.total_trx,
      total_amount: data.amount,
      success_rate: data.success_rate,
    }));

    const cols = [
      "tanggal",
      "range_hour",
      "product_name",
      "product_code",
      "status",
      "client_name",
      "total_transaction",
      "total_amount",
      "success_rate",
    ];

    const CHUNK_SIZE = 500;
    let insertedCount = 0;

    for (let i = 0; i < mappedDatas.length; i += CHUNK_SIZE) {
      const chunk = mappedDatas.slice(i, i + CHUNK_SIZE);

      const values = chunk
        .map(
          (_, idx) =>
            `(${cols.map((_, j) => `$${idx * cols.length + j + 1}`).join(",")})`,
        )
        .join(",");

      const flatValues = chunk.flatMap((obj) => cols.map((c) => obj[c]));

      const query = `INSERT INTO summary_transaction (${cols.join(
        ",",
      )}) VALUES ${values}`;

      await client.query(query, flatValues);
      insertedCount += chunk.length;
    }

    // --- COMMIT TRANSACTION ---
    await client.query("COMMIT");
    console.log(
      `Data inserted successfully — ${insertedCount} rows (transaction committed)`,
    );
  } catch (err) {
    // --- ROLLBACK on any failure ---
    try {
      await client.query("ROLLBACK");
      console.error(
        "[ROLLBACK] Transaction rolled back. Data lama tetap utuh di PostgreSQL.",
      );
    } catch (rollbackErr) {
      console.error("[ROLLBACK ERROR]", rollbackErr.message || rollbackErr);
    }
    throw err;
  } finally {
    client.release();
  }
}

// ============================================================
// HELPER: Get previous 3-hour window
// ============================================================
function getPrevious3HourWindow() {
  const now = moment();

  const end = moment(now)
    .minute(0)
    .second(0)
    .millisecond(0)
    .subtract(now.hour() % 3, "hours");

  const start = moment(end).subtract(3, "hours");
  return {
    date: start.format("YYYY-MM-DD"),
    startTime: start.format("HH:mm:ss"),
    endTime: end.subtract(1, "seconds").format("HH:mm:ss"),
  };
}

async function runSync() {
  console.log(
    "Starting data insertion task...",
    moment().format("YYYY-MM-DD HH:mm:ss"),
  );

  // STEP 1: Fetch dates from MySQL (with retry)
  const dates = await withRetry(
    () => getDatesFromMySQL(),
    "getDatesFromMySQL",
  );

  if (dates.length === 0) {
    console.log("No dates found in MySQL, skipping sync.");
    return;
  }
  console.log(dates);

  // STEP 2: Fetch recap data from MySQL (with retry)
  const dataFromMySQL = await withRetry(
    () => getDataFromMySQL(),
    "getDataFromMySQL",
  );

  if (dataFromMySQL.length === 0) {
    console.log("No recap data found in MySQL, skipping sync.");
    return;
  }

  // STEP 3: Atomic DELETE + INSERT in PostgreSQL (with retry)
  await withRetry(
    () => syncToPostgres(dates, dataFromMySQL),
    "syncToPostgres",
  );

  console.log(
    "Data insertion task completed.",
    moment().format("YYYY-MM-DD HH:mm:ss"),
  );
}

// ============================================================
// CRON SCHEDULE: Every 3 hours
// ============================================================
cron.schedule("0 0 */3 * * *", async () => {
  try {
    await runSync();
  } catch (err) {
    console.error(
      "[CRON] Sync FAILED after all retries:",
      moment().format("YYYY-MM-DD HH:mm:ss"),
      err.message || err,
    );
  }
});

// ============================================================
// MANUAL RUN (uncomment to test)
// ============================================================
// (async () => {
//   try {
//     await runSync();
//   } catch (err) {
//     console.error("Manual run failed:", err);
//   }
// })();
