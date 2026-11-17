using MyStream.Protocol.Primitives;

namespace MyStream.Protocol;

internal class Utf8AlternateEqualityComparer :
    IEqualityComparer<Utf8String>,
    IAlternateEqualityComparer<RefUtf8String, Utf8String>
{
    public static Utf8AlternateEqualityComparer Instance { get; } = new();

    public bool Equals(RefUtf8String alternate, Utf8String other) => alternate.Equals(other);

    public int GetHashCode(RefUtf8String alternate) => alternate.GetHashCode();

    public Utf8String Create(RefUtf8String alternate) => (Utf8String)alternate;

    public bool Equals(Utf8String x, Utf8String y) => x.Equals(y);

    public int GetHashCode(Utf8String obj) => obj.GetHashCode();
}