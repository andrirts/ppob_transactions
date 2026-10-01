const poolPg = require("../db/postgresql");
const poolMy = require("../db/mysql");
const moment = require("moment");
const cron = require("node-cron");
const { findStringBetween } = require("../utils/utils");

async function getDataFromMySQL(targetDate) {
  let client;
  try {
    client = await poolMy.getConnection();
    const query = `SELECT th.idtransaksi, th.tanggal, th.NamaReseller, p.NAMAPRODUK, th.HargaJual, th.keterangan
    FROM transaksi th
    JOIN produk p on th.KodeProduk = p.KodeProduk
    WHERE th.namaterminal = ?
    AND th.NamaReseller NOT REGEXP ? 
    AND DATE(th.tanggal) = ?
    ORDER BY th.idtransaksi ASC
    `;

    const namaterminal = "INDICO";
    const namaReseller = "TEST|DEV|RTS";

    const [rows] = await client.query(query, [
      namaterminal,
      namaReseller,
      targetDate,
    ]);

    console.log(rows.length, "rows found");

    const groupedDatas = [];
    for (let i = 0; i < rows.length; i++) {
      const data = rows[i];
      const keteranganRaw = data["keterangan"] || "";
      
      let extractedStatus = null;
      let extractedMsg = null;
      
      const statusMatch = keteranganRaw.match(/STATUS:\s*([^,]+)/);
      if (statusMatch) extractedStatus = statusMatch[1].trim();
      
      const msgMatch = keteranganRaw.match(/MSG:\s*([^}]+)/);
      if (msgMatch) extractedMsg = msgMatch[1].trim();

      let information = extractedMsg ? extractedMsg : "No Respon From INDICO";
      let resultCode = extractedStatus ? `STATUS:${extractedStatus}` : "NULL";
      
      const status = extractedStatus === "SUKSES" ? "Success" : "Failed";
      const product = data["NAMAPRODUK"];
      const sellPrice = data["HargaJual"] ? data["HargaJual"] : 0;
      
      let sourceOfAlerts = status === "Success" ? "Partner" : "INDICO";
      const dateTransaction = moment(data["tanggal"]).format("YYYY-MM-DD");

      const findIfExists = groupedDatas.findIndex((item) => {
        return (
          item["tanggal"] === dateTransaction &&
          item["mitra"] === data["NamaReseller"] &&
          item["response"] === resultCode &&
          item["produk"] === product
        );
      });
      if (findIfExists !== -1) {
        groupedDatas[findIfExists]["total_harga"] += sellPrice;
        groupedDatas[findIfExists]["total_transaction"] += 1;
        continue;
      }
      groupedDatas.push({
        tanggal: dateTransaction,
        mitra: data["NamaReseller"],
        response: resultCode,
        keterangan: information,
        status: status,
        produk: product,
        harga: sellPrice,
        source_of_alert: sourceOfAlerts,
        total_transaction: 1,
        total_harga: sellPrice,
      });
    }

    return groupedDatas;
  } catch (err) {
    throw err;
  } finally {
    if (client) {
      client.release();
    }
  }
}

async function checkDataExists(datas) {
  let client;
  try {
    client = await poolPg.connect();
    const existDatas = [];
    const newDatas = [];
    const query = `SELECT * FROM indico WHERE tanggal = $1 AND mitra = $2 AND response = $3 AND produk = $4`;
    for (const data of datas) {
      const values = [data.tanggal, data.mitra, data.response, data.produk];
      const result = await client.query(query, values);
      if (result.rows.length > 0) {
        existDatas.push(data);
      } else {
        newDatas.push(data);
      }
    }
    return { existDatas, newDatas };
  } catch (err) {
    throw err;
  } finally {
    client.release();
  }
}

async function insertOrUpdateDataToPostgres(datas, objMappedDatas) {
  let client;
  try {
    client = await poolPg.connect();
    if (datas.length === 0) {
      console.log("No new data to insert");
      return;
    }
    const cols = [
      "tanggal",
      "mitra",
      "response",
      "keterangan",
      "status",
      "produk",
      "harga",
      "source_of_alert",
      "total_transaction",
      "total_harga",
    ];

    if (objMappedDatas.existDatas.length !== 0) {
      for (const data of objMappedDatas.existDatas) {
        const query = `UPDATE indico SET
            total_transaction = $1,
            total_harga = $2
            WHERE tanggal = $3 AND mitra = $4 AND response = $5 AND produk = $6
            `;
        const values = [
          data.total_transaction,
          data.total_harga,
          data.tanggal,
          data.mitra,
          data.response,
          data.produk,
        ];
        await client.query(query, values);
      }
    }

    if (objMappedDatas.newDatas.length !== 0) {
      const values = objMappedDatas.newDatas
        .map(
          (_, i) =>
            `(${cols.map((_, j) => `$${i * cols.length + j + 1}`).join(",")})`
        )
        .join(",");

      const flatValues = objMappedDatas.newDatas.flatMap((obj) =>
        cols.map((c) => obj[c])
      );

      const query = `INSERT INTO indico (${cols.join(",")}) VALUES ${values}`;

      await client.query(query, flatValues);
    }

    console.log("Data inserted successfully");
  } catch (err) {
    throw err;
  } finally {
    client.release();
  }
}

async function deleteOldData() {
  const client = await poolPg.connect();
  try {
    // Delete yesterday data
    const yesterday = moment().subtract(1, "days").format("YYYY-MM-DD");
    const query = `DELETE FROM indico WHERE tanggal = '${yesterday}'`;
    await client.query(query);
    console.log("Old data deleted successfully");
  } catch (err) {
    throw err;
  } finally {
    client.release();
  }
}

// Create the same function as above but execute every 10 minutes using interval
async function runTask() {
  try {
    const today = moment().format("YYYY-MM-DD");
    console.log(
      `Fetching data from MySQL for ${today}...`,
      moment().format("YYYY-MM-DD HH:mm:ss")
    );
    const data = await getDataFromMySQL(today);
    const { existDatas, newDatas } = await checkDataExists(data);
    await insertOrUpdateDataToPostgres(data, { existDatas, newDatas });

    console.log("Process completed.", moment().format("YYYY-MM-DD HH:mm:ss"));
  } catch (error) {
    console.error("Error:", error);
  } finally {
    // Schedule the next run 20 minutes later
    setTimeout(runTask, 20 * 60 * 1000);
  }
}

// Run immediately
runTask();

cron.schedule("0 5 * * *", async () => {
  try {
    console.log(
      "Deleting old data from PostgreSQL...",
      moment().format("YYYY-MM-DD HH:mm:ss")
    );
    await deleteOldData();

    // Re-sync final yesterday data
    const yesterday = moment().subtract(1, "days").format("YYYY-MM-DD");
    console.log(
      `Syncing final data from MySQL for ${yesterday}...`,
      moment().format("YYYY-MM-DD HH:mm:ss")
    );
    const data = await getDataFromMySQL(yesterday);
    const { existDatas, newDatas } = await checkDataExists(data);
    await insertOrUpdateDataToPostgres(data, { existDatas, newDatas });
    console.log("Final sync completed.", moment().format("YYYY-MM-DD HH:mm:ss"));
  } catch (error) {
    console.error("Error:", error);
  }
});
