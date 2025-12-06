const poolPg = require("../db/postgresql");
const poolMy = require("../db/mysql");
const moment = require("moment");
const cron = require("node-cron");
const { findStringBetween } = require("../utils/utils");

let retryCount = 0;
const maxRetries = 5;
const retryDelay = 5000; // 5 seconds

async function getMappingErrorMessage(str) {
  if (str.includes("MSISDN IS NOT FOUND PLEASE VERIFY THE MSISDN")) {
    return "MSISDN IS NOT FOUND, PLEASE VERIFY THE MSISDN YOUVE ENTERED";
  } else if (str.includes("SUBSCRIBER NOT ELIGIBLE DUE TO BYU BRAND")) {
    return "SUBSCRIBER NOT ELIGIBLE DUE TO BYU BRAND";
  } else if (str.includes("PRODUCT NOT ELIGIBLE DUE TO SUBSCRIBER LOCATION")) {
    return "PRODUCT NOT ELIGIBLE DUE TO SUBSCRIBER LOCATION";
  } else if (str.includes("TARGET MSISDN BLOCK 1 PLEASE CHECK MSIDN")) {
    return "TARGET MSISDN BLOCK 1, PLEASE CHECK MSIDN";
  } else if (str.includes("TARGET MSISDN PENDING PLEASE CHECK MSIDN")) {
    return "TARGET MSISDN PENDING, PLEASE CHECK MSIDN";
  } else if (
    str.includes("SERVICE PROVIDER ERROR: DSP - INTERNAL SERVER ERROR")
  )
    return "SERVICE PROVIDER ERROR: DSP - INTERNAL SERVER ERROR";
  else if (str.includes("READ TIMED OUT")) return "READ TIMED OUT";
  else if (
    str.includes(
      "THE APPLICATION IS CURRENTLY UNDER MAINTENANCE. PLEASE TRY AGAIN LATER"
    )
  )
    return "THE APPLICATION IS CURRENTLY UNDER MAINTENANCE. PLEASE TRY AGAIN LATER";
  else {
    return "Failed";
  }
}

async function getDataFromMySQL() {
  let conn;
  try {
    conn = await poolMy.getConnection();
    const query = `
    SELECT th.idtransaksi, th.tanggal, th.NamaReseller, p.NAMAPRODUK, th.HargaJual, th.keterangan
    FROM transaksi th
    JOIN produk p on th.KodeProduk = p.KodeProduk
    WHERE th.namaterminal = ?
    AND th.NamaReseller NOT REGEXP ?
    ORDER BY th.idtransaksi ASC
    `;
    // const tanggal = moment().subtract(1, "days").format("YYYY-MM-DD");
    // const startDate = "2025-09-28";
    // const endDate = "2025-09-28";
    const namaterminal = "MEPRO";
    const namaReseller = "TEST|DEV|RTS";

    const [rows] = await conn.query(query, [
      namaterminal,
      namaReseller,
      //   startDate,
      //   endDate,
    ]);

    console.log(rows.length, "rows found");

    const groupedDatas = [];
    for (let i = 0; i < rows.length; i++) {
      const data = rows[i];
      let rc;
      let message;
      let status;
      let sourceOfAlerts;
      data["keterangan"] = data["keterangan"]
        .replaceAll(",", " ")
        .replaceAll("  ", " ");
      const findSn = await findStringBetween(
        data["keterangan"],
        "REQUESTID:",
        "}"
      );

      if (data["keterangan"].includes("DATA:{STATUS:SUCCESS")) {
        if (findSn) {
          rc = "00";
          message = "Success";
          sourceOfAlerts = "Partner";
          status = "Success";
        } else {
          rc = "68";
          message = "Suspect";
          sourceOfAlerts = "MEPRO";
          status = "Suspect";
        }
      } else if (
        data["keterangan"].includes("ERRORCODE:5001") ||
        data["keterangan"].includes("MESSAGE=FAILED") ||
        data["keterangan"].includes("STATUS:FALSE ERRORCODE:4009") ||
        data["keterangan"].includes("STATUS:FALSE ERRORCODE:4005") ||
        data["keterangan"].includes("TRANSACTIONSTATUS:FAILED") ||
        data["keterangan"].includes("READ TIMED OUT") ||
        data["keterangan"].includes("ERROR:SERVICE UNAVAILABLE")
      ) {
        rc =
          (await findStringBetween(data["keterangan"], "RC:", " SN")) || "02";
        message = await getMappingErrorMessage(data["keterangan"]);
        status = "Failed";
        sourceOfAlerts = "MEPRO";

        if (
          [
            "MSISDN IS NOT FOUND, PLEASE VERIFY THE MSISDN YOUVE ENTERED",
            "SUBSCRIBER NOT ELIGIBLE DUE TO BYU BRAND",
            "PRODUCT NOT ELIGIBLE DUE TO SUBSCRIBER LOCATION",
            "TARGET MSISDN BLOCK 1, PLEASE CHECK MSIDN",
            "TARGET MSISDN PENDING, PLEASE CHECK MSIDN",
          ].includes(message)
        ) {
          sourceOfAlerts = "Partner";
        }
      } else if (data["keterangan"].includes("TRANSAKSI SEDANG DIPROSES")) {
        rc = "68";
        message = "Suspect";
        sourceOfAlerts = "MEPRO";
        status = "Suspect";
      } else {
        rc = null;
        message = "No Respon From MEPRO";
        sourceOfAlerts = "MEPRO";
        status = "Failed";
      }

      let resultCode = rc != null ? `resultCode:${rc}` : "NULL";
      const product = data["NAMAPRODUK"];
      const sellPrice = data["HargaJual"] ? data["HargaJual"] : 0;
      const dateTransaction = moment(data["tanggal"]).format("YYYY-MM-DD");

      const findIfExists = groupedDatas.findIndex((item) => {
        return (
          item["tanggal"] === dateTransaction &&
          item["mitra"] === data["NamaReseller"] &&
          item["response"] === resultCode &&
          item["produk"] === product &&
          item["keterangan"] === message
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
        keterangan: message,
        status: status,
        produk: product,
        harga: sellPrice,
        source_of_alert: sourceOfAlerts,
        total_transaction: 1,
        total_harga: sellPrice,
      });
    }

    // console.log(groupedDatas);
    retryCount = 0;
    return groupedDatas;
  } catch (err) {
    console.log(err);
    if (err.code === "ETIMEDOUT") {
      if (retryCount < maxRetries) {
        retryCount++;
        console.log(
          `MySQL connection timed out. Retrying ${retryCount}/${maxRetries} in ${
            retryDelay / 1000
          } seconds...`
        );
        await new Promise((resolve) => setTimeout(resolve, retryDelay));
        return getDataFromMySQL();
      } else {
        console.error("Max retries reached. Could not connect to MySQL.");
        throw err;
      }
    }
  } finally {
    if (conn) {
      conn.release();
    }
  }
}

async function checkDataExists(datas) {
  let client;
  try {
    client = await poolPg.connect();
    const existDatas = [];
    const newDatas = [];
    const query = `SELECT * FROM mepro WHERE tanggal = $1 AND mitra = $2 AND response = $3 AND produk = $4 AND keterangan = $5`;
    for (const data of datas) {
      const values = [
        data.tanggal,
        data.mitra,
        data.response,
        data.produk,
        data.keterangan,
      ];
      const result = await client.query(query, values);
      if (result.rows.length > 0) {
        existDatas.push(data);
      } else {
        newDatas.push(data);
      }
    }
    return { existDatas, newDatas };
  } catch (err) {
    if (err.code === "ETIMEDOUT") {
      if (retryCount < maxRetries) {
        retryCount++;
        console.log(
          `PostgreSQL connection timed out. Retrying ${retryCount}/${maxRetries} in ${
            retryDelay / 1000
          } seconds...`
        );
        await new Promise((resolve) => setTimeout(resolve, retryDelay));
        return checkDataExists(datas);
      } else {
        console.error("Max retries reached. Could not connect to PostgreSQL.");
        throw err;
      }
    }
    throw err;
  } finally {
    if (client) {
      client.release();
    }
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
        const query = `UPDATE mepro SET
            total_transaction = $1,
            total_harga = $2
            WHERE tanggal = $3 AND mitra = $4 AND response = $5 AND produk = $6 AND keterangan = $7
            `;
        const values = [
          data.total_transaction,
          data.total_harga,
          data.tanggal,
          data.mitra,
          data.response,
          data.produk,
          data.keterangan,
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

      const query = `INSERT INTO mepro (${cols.join(",")}) VALUES ${values}`;
      await client.query(query, flatValues);
    }
    console.log("Data inserted successfully");
  } catch (err) {
    if (err.code === "ETIMEDOUT") {
      if (retryCount < maxRetries) {
        retryCount++;
        console.log(
          `PostgreSQL connection timed out. Retrying ${retryCount}/${maxRetries} in ${
            retryDelay / 1000
          } seconds...`
        );
        await new Promise((resolve) => setTimeout(resolve, retryDelay));
        return insertOrUpdateDataToPostgres(datas, objMappedDatas);
      } else {
        console.error("Max retries reached. Could not connect to PostgreSQL.");
        throw err;
      }
    }
    console.log(err);
  } finally {
    if (client) {
      client.release();
    }
  }
}

async function deleteOldData() {
  let client;
  try {
    client = await poolPg.connect();
    // Delete yesterday data
    const yesterday = moment().subtract(1, "days").format("YYYY-MM-DD");
    const query = `DELETE FROM mepro WHERE tanggal = '${yesterday}'`;
    await client.query(query);
    console.log("Old data deleted successfully");
  } catch (err) {
    throw err;
  } finally {
    if (client) {
      client.release();
    }
  }
}

async function runTask() {
  try {
    console.log(
      "Fetching data from MySQL...",
      moment().format("YYYY-MM-DD HH:mm:ss")
    );
    const data = await getDataFromMySQL();
    const { existDatas, newDatas } = await checkDataExists(data);
    await insertOrUpdateDataToPostgres(data, { existDatas, newDatas });

    console.log("Process completed.", moment().format("YYYY-MM-DD HH:mm:ss"));
  } catch (error) {
    console.error("Error:", error);
  } finally {
    // Schedule the next run 10 minutes later
    setTimeout(runTask, 20 * 60 * 1000);
  }
}

// Run immediately
// runTask();
console.log("MEPRO service started...");

cron.schedule("0 5 * * *", async () => {
  try {
    console.log(
      "Deleting old data from PostgreSQL...",
      moment().format("YYYY-MM-DD HH:mm:ss")
    );
    await deleteOldData();
  } catch (error) {
    console.error("Error:", error);
  }
});

cron.schedule("*/20 * * * * ", async () => {
  try {
    console.log(
      "Fetching data from MySQL...",
      moment().format("YYYY-MM-DD HH:mm:ss")
    );

    const data = await getDataFromMySQL();
    const { existDatas, newDatas } = await checkDataExists(data);
    await insertOrUpdateDataToPostgres(data, { existDatas, newDatas });

    console.log("Process completed.", moment().format("YYYY-MM-DD HH:mm:ss"));
  } catch (error) {
    console.error("Error:", error);
  }
});
