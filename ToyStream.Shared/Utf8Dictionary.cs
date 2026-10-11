namespace ToyStream.Shared;

public class Utf8Dictionary<T>: Dictionary<Utf8String, T>
{
    private readonly AlternateLookup<RefUtf8String> alternateLookup;

    public Utf8Dictionary():base(AlternateEqualityComparer.Instance)
    {
        this.alternateLookup = this.GetAlternateLookup<RefUtf8String>();

    }

    private class AlternateEqualityComparer :
       IEqualityComparer<Utf8String>,
       IAlternateEqualityComparer<RefUtf8String, Utf8String>
    {
        public static AlternateEqualityComparer Instance { get; } = new();
        
        public bool Equals(RefUtf8String alternate, Utf8String other) => alternate.Equals(other);

        public int GetHashCode(RefUtf8String alternate) => alternate.GetHashCode();

        public Utf8String Create(RefUtf8String alternate) => (Utf8String) alternate;

        public bool Equals(Utf8String? x, Utf8String? y) => (x, y) switch
        {
            (null, null) => true,
            (not null, not null) => x.Equals(y),
            _ => false
        };

        public int GetHashCode(Utf8String obj) => obj.GetHashCode();
    }
}