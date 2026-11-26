namespace MyStream;

public static class SpanExtensions
{
    public static Span<T> RemoveAt<T>(this Span<T> span, int index)
    {
        if (index < 0 || index >= span.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        if (index == 0)
        {
            return span[1..];
        }
        if (index < span.Length - 1)
        {
            span[(index + 1)..].CopyTo(span[index..]);
        }

        return span[..^1];
    }
}