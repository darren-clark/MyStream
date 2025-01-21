// See https://aka.ms/new-console-template for more information

using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using ToyStream.Protocol.Benchmark;


var summary = BenchmarkRunner.Run<Benchmarks>();
// var bench = new Benchmarks();
// bench.MessageRuns = 1000;
// bench.SetupFakeServer();
// await bench.SimpleMessageFlow();
// await bench.Cleanup();