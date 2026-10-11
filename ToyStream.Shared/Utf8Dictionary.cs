using System.Diagnostics.CodeAnalysis;

namespace ToyStream.Shared;

public class Utf8Dictionary<TValue>: Dictionary<Utf8String, TValue>
{
    private readonly AlternateLookup<RefUtf8String> alternateLookup;

    public Utf8Dictionary():base(Utf8AlternateEqualityComparer.Instance)
    {
        this.alternateLookup = this.GetAlternateLookup<RefUtf8String>();
    }

    public bool TryGetValue(RefUtf8String key, [MaybeNullWhen(false)] out TValue value) =>
        alternateLookup.TryGetValue(key, out value);

    public TValue this[RefUtf8String key] => alternateLookup[key];
    public bool ContainsKey(RefUtf8String key) => alternateLookup.ContainsKey(key);
    
    public bool Remove(RefUtf8String key) => alternateLookup.Remove(key);
    public bool TryAdd(RefUtf8String key, TValue value)  => alternateLookup.TryAdd(key, value);
}