// See https://aka.ms/new-console-template for more information

using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using ToyStream.Protocol.Benchmark;


var summary = BenchmarkRunner.Run<Benchmarks>();
// var bench = new Benchmarks
// {
//     MessageRuns = 10000
// };
// await bench.SimpleMessageFlow();
// bench.SetupFakeServer();
// await bench.SimpleMessageFlow();
// await bench.Cleanup();