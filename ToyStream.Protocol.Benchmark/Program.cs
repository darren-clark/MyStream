using BenchmarkDotNet.Running;
using ToyStream.Protocol.Benchmark;

if (args is ["jitdump", ..])
{
    if (args is [_, "direct", ..])
    {
        await JitDumpDirect.RunAsync();
    }
    else
    {
        await JitDump.RunAsync();
    }

    return;
}

// Switcher so a single class can be selected with --filter; args pass through for --job Long etc.
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);

public partial class Program;