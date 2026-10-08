# Generates the switch-scale connections: one server with a 30 arm switch, one with a single arm.
# Both are Direct shaped (synchronous decode in the arm, direct non-virtual handler call).
param([int]$Count = 30, [int]$FirstTypeByte = 10)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$outputPath = Join-Path $repoRoot 'ToyStream.Protocol.Benchmark\SwitchScale\ScaleDirectServers.g.cs'
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $outputPath) | Out-Null

function Write-Arm($sb, $index, $typeByte) {
    $n = '{0:D2}' -f $index
    [void]$sb.AppendLine("                    case ${typeByte}:")
    [void]$sb.AppendLine('                    {')
    [void]$sb.AppendLine('                        var primitiveReader = new PrimitiveReader(body);')
    [void]$sb.AppendLine("                        if (!TryDecode<ScaleMessage$n>(ref primitiveReader, out var message))")
    [void]$sb.AppendLine('                        {')
    [void]$sb.AppendLine('                            buffer = await this.RefillAsync(buffer).ConfigureAwait(false);')
    [void]$sb.AppendLine('                            continue;')
    [void]$sb.AppendLine('                        }')
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine('                        body = primitiveReader.Remaining;')
    [void]$sb.AppendLine("                        pending = this.Handle$n(in message);")
    [void]$sb.AppendLine('                        break;')
    [void]$sb.AppendLine('                    }')
    [void]$sb.AppendLine('')
}

function Write-Handler($sb, $index) {
    $n = '{0:D2}' -f $index
    [void]$sb.AppendLine("    private ValueTask Handle$n(ref readonly ScaleMessage$n message)")
    [void]$sb.AppendLine('    {')
    [void]$sb.AppendLine('        this.Total += message.Value;')
    [void]$sb.AppendLine('        return this.Write(new AcknowledgeMessage());')
    [void]$sb.AppendLine('    }')
    [void]$sb.AppendLine('')
}

function Write-Server($sb, $className, $armIndexes) {
    [void]$sb.AppendLine("public sealed class $className : DirectConnection")
    [void]$sb.AppendLine('{')
    [void]$sb.AppendLine('    public long Total;')
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine("    public $className(PipeReader reader, PipeWriter writer, CancellationToken cancellationToken)")
    [void]$sb.AppendLine('        : base(reader, writer, cancellationToken)')
    [void]$sb.AppendLine('        => this.StartReading();')
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine('    protected override async Task Read()')
    [void]$sb.AppendLine('    {')
    [void]$sb.AppendLine('        await Task.Yield();')
    [void]$sb.AppendLine('        var buffer = ReadOnlySequence<byte>.Empty;')
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine('        try')
    [void]$sb.AppendLine('        {')
    [void]$sb.AppendLine('            while (!this.CancellationToken.IsCancellationRequested)')
    [void]$sb.AppendLine('            {')
    [void]$sb.AppendLine('                if (buffer.Length < sizeof(MessageType))')
    [void]$sb.AppendLine('                {')
    [void]$sb.AppendLine('                    var result = await this.Reader.ReadAsync(this.CancellationToken).ConfigureAwait(false);')
    [void]$sb.AppendLine('                    buffer = result.Buffer;')
    [void]$sb.AppendLine('                }')
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine('                var typeByte = MemoryMarshal.Read<byte>(buffer.Slice(0, 1).FirstSpan);')
    [void]$sb.AppendLine('                var body = buffer.Slice(1);')
    [void]$sb.AppendLine('                ValueTask pending;')
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine('                switch (typeByte)')
    [void]$sb.AppendLine('                {')

    foreach ($i in $armIndexes) { Write-Arm $sb $i ($i + $FirstTypeByte) }

    [void]$sb.AppendLine('                    default:')
    [void]$sb.AppendLine('                        throw new InvalidOperationException($"Unexpected message type {typeByte}.");')
    [void]$sb.AppendLine('                }')
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine('                buffer = body;')
    [void]$sb.AppendLine('                this.Reader.AdvanceTo(buffer.Start);')
    [void]$sb.AppendLine('')
    [void]$sb.AppendLine('                if (!pending.IsCompleted)')
    [void]$sb.AppendLine('                {')
    [void]$sb.AppendLine('                    await pending.ConfigureAwait(false);')
    [void]$sb.AppendLine('                }')
    [void]$sb.AppendLine('            }')
    [void]$sb.AppendLine('        }')
    [void]$sb.AppendLine('        catch (OperationCanceledException)')
    [void]$sb.AppendLine('        {')
    [void]$sb.AppendLine('        }')
    [void]$sb.AppendLine('    }')
    [void]$sb.AppendLine('')

    foreach ($i in $armIndexes) { Write-Handler $sb $i }

    [void]$sb.AppendLine('}')
    [void]$sb.AppendLine('')
}

$sb = [System.Text.StringBuilder]::new()
[void]$sb.AppendLine('// Generated by scripts/gen-scale-direct.ps1. Do not edit.')
[void]$sb.AppendLine('namespace ToyStream.Protocol.Benchmark.SwitchScale;')
[void]$sb.AppendLine('')
[void]$sb.AppendLine('using System.Buffers;')
[void]$sb.AppendLine('using System.IO.Pipelines;')
[void]$sb.AppendLine('using System.Runtime.InteropServices;')
[void]$sb.AppendLine('using Messages;')
[void]$sb.AppendLine('using Protocol.Dispatch.Direct;')
[void]$sb.AppendLine('using Scale;')
[void]$sb.AppendLine('')

Write-Server $sb 'ScaleDirectServerWide' (0..($Count - 1))
Write-Server $sb 'ScaleDirectServerNarrow' @(0)

Set-Content -Path $outputPath -Value $sb.ToString() -Encoding UTF8
Write-Output "Wrote wide ($Count arms) and narrow (1 arm) servers to $outputPath"
