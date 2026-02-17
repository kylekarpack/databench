import {
	approxMedian,
	avg,
	col,
	max,
	min,
	SessionContext,
} from "@napi-rs/datafusion";

(async () => {
	const ctx = new SessionContext();
	const datafusionDf = await ctx.readCsv("./data/people-10.csv");
	const aggregatedDf = datafusionDf.clone().aggregate([], [
		avg(col("Index")).alias("avg_index"),
		min(col("Index")).alias("min_index"),
		max(col("Index")).alias("max_index"),
		approxMedian(col("Index")).alias("median_index"),
	]);
	await aggregatedDf.show();

	const df2 = datafusionDf.clone().aggregate([], [
		avg(col("Index")).alias("avg_index"),
		min(col("Index")).alias("min_index"),
		max(col("Index")).alias("max_index"),
		approxMedian(col("Index")).alias("median_index"),
	]);
	await df2.show();
})();
