# Benchmark Project

This project uses [ModestBench](https://modestbench.dev) for performance testing.

## Getting Started

Run all benchmarks:
```bash
modestbench run
```

Run specific benchmarks:
```bash
modestbench run "bench/array-*.bench.js"
```

View benchmark history:
```bash
modestbench history list
```

## Configuration

See `modestbench.config.*` for benchmark configuration options.

## Writing Benchmarks

Create new benchmark files in the `bench/` directory. See the examples for the expected format.
