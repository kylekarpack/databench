import pl, { DataFrame } from "nodejs-polars";
import { DuckDBConnection } from "@duckdb/node-api";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { parse } from "papaparse";
import * as aq from "arquero";
import { col, SessionContext } from "@napi-rs/datafusion";
import {
	DataFrame as DataFusionDataFrame,
	avg,
	min,
	max,
	approxMedian,
} from "@napi-rs/datafusion";

const dataDir = join(process.cwd(), "data");
const csvFiles = readdirSync(dataDir).filter((file) => file.endsWith(".csv"));

/**
 * Calculates basic statistics for an array of numbers.
 */
function calculateStats(values: number[]) {
	const n = values.length;
	if (n === 0) return null;

	// Sort for median and mode
	const sorted = [...values].sort((a, b) => a - b);
	const min = sorted[0];
	const max = sorted[n - 1];
	const sum = values.reduce((a, b) => a + b, 0);
	const mean = sum / n;

	// Median
	const mid = Math.floor(n / 2);
	const median =
		n % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];

	// Mode
	const counts = new Map<number, number>();
	let maxCount = 0;
	let mode = sorted[0];
	for (const v of values) {
		const count = (counts.get(v) || 0) + 1;
		counts.set(v, count);
		if (count > maxCount) {
			maxCount = count;
			mode = v;
		}
	}

	// Variance and Standard Deviation
	const variance = values.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / n;
	const stdDev = Math.sqrt(variance);

	return { min, max, mean, median, mode, variance, stdDev };
}

const suites: Record<string, any> = {};

for (const file of csvFiles) {
	const csvPath = join(dataDir, file);
	const suiteName = `Full Stats ${file}`;
	let papaparsed: { data: any[] };
	let polarsDf: DataFrame;
	let arqueroDf: aq.ColumnTable;
	let connection: DuckDBConnection;
	let jsValues: number[] = [];
	let datafusionDf: DataFusionDataFrame;

	suites[suiteName] = {
		tags: [file.includes("1000000") ? "large" : "small"],
		async setup() {
			const csvContent = readFileSync(csvPath, "utf-8");
			papaparsed = parse(csvContent, {
				header: true,
				dynamicTyping: true,
			}) as any;
			polarsDf = pl.readCSV(csvContent, {
				hasHeader: true,
				tryParseDates: true,
			});
			arqueroDf = aq.fromCSV(csvContent);

			connection = await DuckDBConnection.create();
			await connection.run(
				`create table if not exists people as select * from read_csv('${csvPath}')`,
			);

			const ctx = new SessionContext();
			datafusionDf = await ctx.readCsv(csvPath);

			// Pre-parse values for JS benchmarks to measure computation performance
			jsValues = papaparsed.data
				.map((row: any) => {
					const val = row["Date of birth"];
					if (typeof val === "number") return val;
					if (val instanceof Date) return val.getTime();
					return Date.parse(val);
				})
				.filter((v: number) => !isNaN(v));
		},
		benchmarks: {
			"js (basic)": () => {
				return calculateStats(jsValues);
			},

			"polars (Series)": () => {
				const col = polarsDf.getColumn("Date of birth").cast(pl.Float64);
				return {
					min: col.min(),
					max: col.max(),
					mean: col.mean(),
					median: col.median(),
					mode: col.mode().get(0),
				};
			},

			"polars (Engine)": () => {
				return polarsDf
					.select(
						pl.col("Date of birth").min().alias("min"),
						pl.col("Date of birth").max().alias("max"),
						pl.col("Date of birth").mean().alias("mean"),
						pl.col("Date of birth").median().alias("median"),
						pl.col("Date of birth").cast(pl.Float64).std().alias("stdDev"),
						pl.col("Date of birth").cast(pl.Float64).var().alias("variance"),
						pl.col("Date of birth").mode().first().alias("mode"),
					)
					.toObject();
			},

			datafusion: async () => {
				const aggregatedDf = datafusionDf.clone().aggregate(
					[],
					[
						avg(col("Index")).alias("avg_index"),
						min(col("Index")).alias("min_index"),
						max(col("Index")).alias("max_index"),
						approxMedian(col("Index")).alias("median_index"),
					],
				);

				// The result is an Arrow Table
				return aggregatedDf;
			},

			arquero: () => {
				return arqueroDf
					.rollup({
						min: aq.op.min("Date of birth"),
						max: aq.op.max("Date of birth"),
						mean: aq.op.mean("Date of birth"),
						median: aq.op.median("Date of birth"),
						mode: aq.op.mode("Date of birth"),
						variance: aq.op.variance("Date of birth"),
						stdDev: aq.op.stdev("Date of birth"),
					})
					.objects()[0];
			},

			duckdb: async () => {
				const result = await connection.run(
					`SELECT 
						min("Date of birth") as min, 
						max("Date of birth") as max, 
						avg("Date of birth") as mean, 
						median("Date of birth") as median, 
						stddev(epoch("Date of birth")) as stdDev, 
						variance(epoch("Date of birth")) as variance, 
						mode("Date of birth") as mode 
					FROM people`,
				);
				const rows = await result.getRowObjectsJS();
				return rows[0];
			},
		},
	};
}

export default { suites };
