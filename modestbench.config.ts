import type { ModestBenchConfig } from "modestbench";

const config: Partial<ModestBenchConfig> = {
	iterations: 2,
	outputDir: "./.modestbench",
	pattern: "bench/**/*.bench.{js,ts}",
	reporters: ["human", "json"],
	time: 10000,
	warmup: 1,
};

export default config;
