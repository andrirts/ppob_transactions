const poolPg = require("../db/postgresql");
const poolMy = require("../db/mysql");
const moment = require("moment");
const cron = require("node-cron");

// function getPrevious3Hours() {
//   const threeHoursAgo = moment().subtract(3, "hours").format("HH:mm:ss");
//   const now = moment().format("HH:mm:ss");
//   const getDateThreeHoursAgo = moment()
//     .subtract(3, "hours")
//     .format("YYYY-MM-DD");
//   console.log(threeHoursAgo, getDateThreeHoursAgo, now);
//   return {
//     now,
//     threeHoursAgo,
//     getDateThreeHoursAgo,
//   };
// }

function getPrevious3HourWindow() {
  const now = moment();

  // Align to the nearest 3-hour boundary
  const end = moment(now)
    .minute(0)
    .second(0)
    .millisecond(0)
    .subtract(now.hour() % 3, "hours");

  const start = moment(end).subtract(3, "hours");
  return {
    date: start.format("YYYY-MM-DD"),
    startTime: start.format("HH:mm:ss"),
    endTime: end.subtract(1, "seconds").format("HH:mm:ss"), // to make it inclusive of the last second
  };
}

async function getDataFromMySQL() {
  let conn;
  try {
    conn = await poolMy.getConnection();
    const query = `
    select CONCAT(
        LPAD(FLOOR(HOUR(JAM)/3)*3, 2, '0'), ':00 - ',
        LPAD(FLOOR(HOUR(JAM)/3)*3 + 2, 2, '0'), ':59'
    ) AS jam_group,
    tanggal,
    NAMAPRODUK,
    NamaReseller,
    t.KodeProduk,
    case
    	when statustransaksi = 1 then 'SUKSES'
    	when statustransaksi = 2 then 'GAGAL'
        else 'PENDING'
    end as status_transaksi
    ,
    count(*) as total_trx,sum(case when statustransaksi = 1 then HARGAJUAL else 0 END) as amount
    from transaksi t
    join produk p
    on p.KodeProduk = t.KodeProduk
    where jenistransaksi in ('1','6')
    group by NamaReseller, statustransaksi, KodeProduk, jam_group, tanggal;
    `;
    const { date, startTime, endTime } = getPrevious3HourWindow();

    const [rows] = await conn.query(query, [date, startTime, endTime]);
    console.log(rows.length, "rows found");
    return rows;
  } catch (err) {
    console.error("Error getting MySQL connection:", err);
    throw err;
  } finally {
    if (conn) conn.release();
  }
}

async function insertDataToPostgres(datas) {
  let client;
  try {
    client = await poolPg.connect();
    if (datas.length === 0) {
      console.log("No data to insert");
      return;
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
    ];

    const values = mappedDatas
      .map(
        (_, i) =>
          `(${cols.map((_, j) => `$${i * cols.length + j + 1}`).join(",")})`,
      )
      .join(",");

    const flatValues = mappedDatas.flatMap((obj) => cols.map((c) => obj[c]));

    const query = `INSERT INTO summary_transaction (${cols.join(
      ",",
    )}) VALUES ${values}`;

    await client.query(query, flatValues);
    console.log("Data inserted successfully");
  } catch (err) {
    throw err;
  } finally {
    if (client) {
      client.release();
    }
  }
}

async function deleteDataFromPostgres() {
  let client;
  let conn;
  try {
    conn = await poolMy.getConnection();
    client = await poolPg.connect();
    const queryGetDate = `select tanggal from transaksi t 
group by TANGGAL;`;
    const [rows] = await conn.query(queryGetDate);
    if (!rows.length) return;

    const dates = rows.map((r) => moment(r.tanggal).format("YYYY-MM-DD"));
    console.log(dates);

    await client.query(
      `
  DELETE FROM summary_transaction
  WHERE tanggal = ANY($1::text[])
  `,
      [dates],
    );
    console.log("Old data deleted successfully");
  } catch (err) {
    console.log(err);
    throw err;
  } finally {
    if (client) client.release();
    if (conn) conn.release();
  }
}

cron.schedule("0 0 */3 * * *", async () => {
  try {
    console.log(
      "Starting data insertion task...",
      moment().format("YYYY-MM-DD HH:mm:ss"),
    );
    await deleteDataFromPostgres();
    const dataFromMySQL = await getDataFromMySQL();
    await insertDataToPostgres(dataFromMySQL);
    console.log(
      "Data insertion task completed.",
      moment().format("YYYY-MM-DD HH:mm:ss"),
    );
  } catch (err) {
    console.error("Error:", err);
  }
});

// (async () => {
//   try {
//     console.log(
//       "Starting data insertion task...",
//       moment().format("YYYY-MM-DD HH:mm:ss"),
//     );
//     await deleteDataFromPostgres();
//     const dataFromMySQL = await getDataFromMySQL();
//     await insertDataToPostgres(dataFromMySQL);
//     console.log(
//       "Data insertion task completed.",
//       moment().format("YYYY-MM-DD HH:mm:ss"),
//     );
//   } catch (err) {
//     console.error("Error:", err);
//   }
// })();
