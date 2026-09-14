using BenchmarkDotNet.Running;
using ToyStream.Protocol.Benchmark;

// Pass args through so --filter / --job Dry etc. work from the command line.
BenchmarkRunner.Run<Benchmarks>(args: args);