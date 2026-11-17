using System.Diagnostics.CodeAnalysis;

namespace MyStream;

public class AlternateDictionary<TKey, TAlternateKey, TComparer, TValue> : Dictionary<TKey, TValue>
    where TKey : notnull
    where TComparer : IEqualityComparer<TKey>, IAlternateEqualityComparer<TAlternateKey, TKey>
    where TAlternateKey : notnull
{
    private readonly AlternateLookup<TAlternateKey> alternateLookup;

    public AlternateDictionary(TComparer comparer) : base(comparer)
    {
        alternateLookup = this.GetAlternateLookup<TAlternateKey>();
    }

    public bool TryGetValue(TAlternateKey key, [MaybeNullWhen(false)] out TValue value) =>
        alternateLookup.TryGetValue(key, out value);

    public TValue this[TAlternateKey key] => alternateLookup[key];
    public bool ContainsKey(TAlternateKey key) => alternateLookup.ContainsKey(key);

    public bool Remove(TAlternateKey key) => alternateLookup.Remove(key);
    public bool TryAdd(TAlternateKey key, TValue value) => alternateLookup.TryAdd(key, value);
}