const poolPg = require("../db/postgresql");
const poolMy = require("../db/mysql");
const moment = require("moment");
const cron = require("node-cron");
const { findStringBetween } = require("../utils/utils");

async function getDataFromMySQL() {
  try {
    await poolMy.getConnection();
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
    const namaterminal = "CNET";
    const namaReseller = "TEST|DEV|RTS";

    const [rows] = await poolMy.query(query, [
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

      const findSn = await findStringBetween(data["keterangan"], "SN: ", " =");

      if (data["keterangan"].includes("STATUSCODE=1")) {
        if (findSn) {
          rc = "00";
          message = "Success";
          sourceOfAlerts = "Partner";
          status = "Success";
        } else {
          rc = "68";
          message = "Suspect";
          sourceOfAlerts = "CNET";
          status = "Suspect";
        }
      } else if (
        data["keterangan"].includes("STATUSCODE=2") ||
        data["keterangan"].includes("GAGAL")
      ) {
        rc = "02";
        message = "Failed";
        sourceOfAlerts = "CNET";
        status = "Failed";
      } else if (data["keterangan"].includes("RC:")) {
        if (data["keterangan"].includes("RC:0068")) {
          rc = "68";
          message = "Suspect";
          sourceOfAlerts = "CNET";
          status = "Suspect";
        } else {
          rc = await findStringBetween(data["keterangan"], "RC:", ",MSG:");
          message = await findStringBetween(data["keterangan"], "MSG:", "}");
          sourceOfAlerts = "CNET";
          status = "Failed";
        }
      } else {
        rc = null;
        message = "No Respon From CNET";
        sourceOfAlerts = "CNET";
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
          item["produk"] === product
        );
      });
      if (findIfExists !== -1) {
        groupedDatas[findIfExists]["total_harga"] += sellPrice;
        groupedDatas[findIfExists]["total_transaction"] += 1;
        continue;
      }
      if (message == null) {
        console.log(rc);
        console.log(message);
        console.log(status);
        console.log(data);
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

    return groupedDatas;
  } catch (err) {
    throw err;
  }
}

async function checkDataExists(datas) {
  const client = await poolPg.connect();
  try {
    const existDatas = [];
    const newDatas = [];
    const query = `SELECT * FROM cnet WHERE tanggal = $1 AND mitra = $2 AND response = $3 AND produk = $4`;
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
  const client = await poolPg.connect();
  try {
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
        const query = `UPDATE cnet SET
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

      const query = `INSERT INTO cnet (${cols.join(",")}) VALUES ${values}`;

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
    const query = `DELETE FROM cnet WHERE tanggal = '${yesterday}'`;
    await client.query(query);
    console.log("Old data deleted successfully");
  } catch (err) {
    throw err;
  } finally {
    client.release();
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
runTask();

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
