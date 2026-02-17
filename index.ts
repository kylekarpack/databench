import pl from "nodejs-polars";
import { DuckDBConnection } from "@duckdb/node-api";
import { readFileSync } from "fs";
import { readCSV } from "danfojs-node";

const csvPath = "./people.csv";
const csv = readFileSync(csvPath).toString();

console.time("read-csv-js");

const values = csv
	.split("\n")
	.map((line) => {
		const parts = line.split(",");
		return isNaN(parseInt(parts[0])) ? 0 : parseInt(parts[0]);
	});
console.timeEnd("read-csv-js");

console.time("mean-js");
console.log(values.reduce((a, b) => a + b) / values.length);
console.timeEnd("mean-js");

console.log("----------------------------------------------");
console.time("polars-readcsv");
const df = pl.readCSV(csv, { hasHeader: true, tryParseDates: true });
console.timeEnd("polars-readcsv");

const dateCol = df.getColumn("Date of birth");
console.time("mean-polars (pure computation)");
const rawMean = dateCol.mean();
console.timeEnd("mean-polars (pure computation)");
console.log("Raw Mean (days since epoch):", rawMean);

// Formatting is done as a selection in the engine
console.time("mean-polars (formatting in engine)");
const meanExpr = pl.col("Date of birth").mean().cast(pl.Date);
const resultFrame = df.select(meanExpr).toObject()["Date of birth"][0];
console.log(resultFrame)
console.timeEnd("mean-polars (formatting in engine)");

console.log("----------------------------------------------");

(async () => {
	console.time("duckdb-csv-init");
	const connection = await DuckDBConnection.create();
	await connection.run(
		`create table people as select * from read_csv('${csvPath}')`,
	);
	console.timeEnd("duckdb-csv-init");

	console.time("mean-duckdb");
	const result = await connection.run(
		`SELECT mean("Date of birth") as mean FROM people`,
	);
	const rows = await result.getRowObjectsJS();
	console.log(rows[0].mean);
	console.timeEnd("mean-duckdb");
	console.log("----------------------------------------------");
})();
