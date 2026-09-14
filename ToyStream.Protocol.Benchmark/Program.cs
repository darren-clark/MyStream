using BenchmarkDotNet.Running;

// Switcher so a single class can be selected with --filter; args pass through for --job Long etc.
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);

public partial class Program;