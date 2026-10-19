namespace ToyStream.Protocol.Test.Primitives;

using System.Buffers;
using System.Text;
using Shared;

/// <summary>
/// The same text must hash and compare identically however the pipe happened to split it, otherwise
/// dictionary lookups succeed or fail based on socket read boundaries.
/// </summary>
public class RefUtf8StringTests
{
    // Deliberately not a multiple of four: HashCode folds a sub-block remainder in differently.
    private const string Name = "my-topic-name";

    private static readonly string LongName = new('t', 400);

    [Test]
    public void GetHashCode_IsIndependentOfSegmentation([Values] bool longText)
    {
        var text = longText ? LongName : Name;
        var encoded = Encoding.UTF8.GetBytes(text);
        var expected = new RefUtf8String(encoded.AsSpan()).GetHashCode();

        for (var split = 1; split < encoded.Length; split++)
        {
            var actual = new RefUtf8String(Segmented(encoded, split)).GetHashCode();
            Assert.That(actual, Is.EqualTo(expected), $"hash differed when split at {split}");
        }
    }

    [Test]
    public void GetHashCode_IsIndependentOfRepresentation([Values] bool longText)
    {
        var text = longText ? LongName : Name;
        var encoded = Encoding.UTF8.GetBytes(text);

        var fromUtf8 = new RefUtf8String(encoded.AsSpan()).GetHashCode();
        var fromUtf16 = new RefUtf8String(text.AsSpan()).GetHashCode();
        var fromSequence = new RefUtf8String(Segmented(encoded, encoded.Length / 3)).GetHashCode();

        Assert.That(fromUtf16, Is.EqualTo(fromUtf8));
        Assert.That(fromSequence, Is.EqualTo(fromUtf8));
    }

    [Test]
    public void Equals_IsIndependentOfSegmentation([Values] bool longText)
    {
        var text = longText ? LongName : Name;
        var encoded = Encoding.UTF8.GetBytes(text);
        var other = new Utf8String(text);

        for (var split = 1; split < encoded.Length; split++)
        {
            Assert.That(new RefUtf8String(Segmented(encoded, split)).Equals(other), Is.True,
                $"compare failed when split at {split}");
        }
    }

    [Test]
    public void Equals_IsFalseWhenOnlyALaterSegmentDiffers()
    {
        var encoded = Encoding.UTF8.GetBytes(Name);
        var differing = Encoding.UTF8.GetBytes(Name[..^1] + "X");
        var other = new Utf8String(Name);

        // Split inside the shared prefix, so only the trailing segment distinguishes the two.
        Assert.That(new RefUtf8String(Segmented(differing, 3)).Equals(other), Is.False);
        Assert.That(new RefUtf8String(Segmented(encoded, 3)).Equals(other), Is.True);
    }

    [Test]
    public void Equals_IsFalseWhenSegmentsRepeatTheSamePrefix()
    {
        // "abcabc" vs "abcxyz": a compare that failed to advance the other side would see the
        // second segment match the start of the other string and wrongly report equality.
        var candidate = Encoding.UTF8.GetBytes("abcabc");
        var other = new Utf8String("abcxyz");

        Assert.That(new RefUtf8String(Segmented(candidate, 3)).Equals(other), Is.False);
    }

    private static ReadOnlySequence<byte> Segmented(ReadOnlySpan<byte> value, int splitAt)
    {
        var first = new Segment(value[..splitAt].ToArray());
        var last = first.Append(value[splitAt..].ToArray());
        return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
    }

    private sealed class Segment : ReadOnlySequenceSegment<byte>
    {
        public Segment(ReadOnlyMemory<byte> memory) => Memory = memory;

        public Segment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new Segment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }
}
