import { DuckDBConnection } from "@duckdb/node-api";
import { SessionContext } from '@napi-rs/datafusion';
import * as aq from "arquero";
import { readCSV } from "danfojs-node";
import { failure } from "modestbench";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import pl from "nodejs-polars";
import { parse } from "papaparse";


const dataDir = join(process.cwd(), "data");
const csvFiles = readdirSync(dataDir).filter((file) => file.endsWith(".csv"));

const suites: Record<string, any> = {};

for (const file of csvFiles) {
	const csvPath = join(dataDir, file);
	const suiteName = `Parse ${file}`;
	let csvContent = "";

	suites[suiteName] = {
		tags: [file.includes("1000000") ? "large" : "small"],
		setup() {
			csvContent = readFileSync(csvPath, "utf-8");
		},
		benchmarks: {
			"js (basic)": () => {
				return csvContent.split("\n").map((line) => line.split(","));
			},

			papaparse: () => {
				return parse(csvContent, { header: true, dynamicTyping: true });
			},

			arquero: () => {
				if (csvPath.includes("1000000")) {
					throw failure("Arquero cannot handle large files")
				}
				return aq.loadCSV(csvPath, { header: true, autoType: true });
			},

			"polars (string)": () => {
				return pl.readCSV(csvContent, { hasHeader: true, tryParseDates: true });
			},

			"polars (file)": () => {
				return pl.readCSV(csvPath, { hasHeader: true, tryParseDates: true });
			},

			datafusion: async () => {
				const ctx = new SessionContext()
				return await ctx.readCsv(csvPath)
			},

			danfo: async () => {
				if (csvPath.includes("1000000")) {
					throw failure("Danfo cannot handle this file")
				}
				return await readCSV(csvPath, {
					header: true,
					dynamicTyping: true,
					quoteChar: '"',
					fastMode: false,
				});
			},

			duckdb: async () => {
				const connection = await DuckDBConnection.create();
				await connection.run(
					`create table if not exists people as select * from read_csv('${csvPath}')`,
				);
			},
		},
	};
}

export default { suites };
