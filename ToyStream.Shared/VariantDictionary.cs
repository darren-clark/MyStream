namespace ToyStream.Shared;

using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

public interface ICopyableTo<in TIn, out TOut> where TIn : ICopyableTo<TIn, TOut>,allows ref struct
{
    
    // Operator because multiple implementations will overlap.
    // Explicit because it may allocate
    public static abstract explicit operator TOut(TIn value);
}

/// <summary>
/// Dictionary that allows lookups from equivalent types.
///
/// Note that types used to lookup must implement <see cref="IEquatable{TKey}"/>
/// but ALSO must have an equivalent implementation of <see cref="Object.GetHashCode()"/>.
///
/// This is primarily to do non-allocating lookups of Utf-8 strings.
/// </summary>
/// <remarks>
/// This is basically a copy of <see cref="Dictionary{TKey,TValue}"/>.
///
/// But with a modification for the find.
///
/// This means you also cannot provide you're own <see cref="IEqualityComparer{TKey}"/>.
/// since it will be comparing different types.
/// </remarks>
[DebuggerDisplay("Count = {Count}")]
[Serializable]
public class VariantDictionary<TKey, TValue> :
    IDictionary<TKey, TValue>,
    IReadOnlyDictionary<TKey, TValue> where TKey : IEquatable<TKey>
{
    private int[]? _buckets;
    private Entry[]? _entries;
#if TARGET_64BIT
        private ulong _fastModMultiplier;
#endif
    private int _count;
    private int _freeList;
    private int _freeCount;
    private int _version;
    private KeyCollection? _keys;
    private ValueCollection? _values;
    private const int StartOfFreeList = -3;

    public VariantDictionary() : this(0)
    {
    }

    public VariantDictionary(int capacity)
    {
        if (capacity < 0)
        {
            ThrowHelper.ThrowArgumentOutOfRangeException(nameof(capacity));
        }

        if (capacity > 0)
        {
            Initialize(capacity);
        }
    }

    public VariantDictionary(IDictionary<TKey, TValue> dictionary) :
        this(dictionary?.Count ?? 0)
    {
        ArgumentNullException.ThrowIfNull(dictionary);

        AddRange(dictionary);
    }

    public VariantDictionary(IEnumerable<KeyValuePair<TKey, TValue>> collection) :
        this((collection as ICollection<KeyValuePair<TKey, TValue>>)?.Count ?? 0)
    {
        ArgumentNullException.ThrowIfNull(collection);

        AddRange(collection);
    }

    private void AddRange(IEnumerable<KeyValuePair<TKey, TValue>> enumerable)
    {
        // It is likely that the passed-in enumerable is Dictionary<TKey,TValue>. When this is the case,
        // avoid the enumerator allocation and overhead by looping through the entries array directly.
        // We only do this when dictionary is Dictionary<TKey,TValue> and not a subclass, to maintain
        // back-compat with subclasses that may have overridden the enumerator behavior.
        if (enumerable.GetType() == typeof(VariantDictionary<TKey, TValue>))
        {
            VariantDictionary<TKey, TValue> source = (VariantDictionary<TKey, TValue>)enumerable;

            if (source.Count == 0)
            {
                // Nothing to copy, all done
                return;
            }

            // This is not currently a true .AddRange as it needs to be an initialized dictionary
            // of the correct size, and also an empty dictionary with no current entities (and no argument checks).
            Debug.Assert(source._entries is not null);
            Debug.Assert(_entries is not null);
            Debug.Assert(_entries.Length >= source.Count);
            Debug.Assert(_count == 0);

            Entry[] oldEntries = source._entries;

            // Comparers differ need to rehash all the entries via Add
            int count = source._count;
            for (int i = 0; i < count; i++)
            {
                // Only copy if an entry
                if (oldEntries[i].next >= -1)
                {
                    Add(oldEntries[i].key, oldEntries[i].value);
                }
            }

            return;
        }

        // We similarly special-case KVP<>[] and List<KVP<>>, as they're commonly used to seed dictionaries, and
        // we want to avoid the enumerator costs (e.g. allocation) for them as well. Extract a span if possible.
        ReadOnlySpan<KeyValuePair<TKey, TValue>> span;
        if (enumerable is KeyValuePair<TKey, TValue>[] array)
        {
            span = array;
        }
        else if (enumerable.GetType() == typeof(List<KeyValuePair<TKey, TValue>>))
        {
            span = CollectionsMarshal.AsSpan((List<KeyValuePair<TKey, TValue>>)enumerable);
        }
        else
        {
            // Fallback path for all other enumerables
            foreach (KeyValuePair<TKey, TValue> pair in enumerable)
            {
                Add(pair.Key, pair.Value);
            }

            return;
        }

        // We got a span. Add the elements to the dictionary.
        foreach (KeyValuePair<TKey, TValue> pair in span)
        {
            Add(pair.Key, pair.Value);
        }
    }

    public int Count => _count - _freeCount;

    public KeyCollection Keys => _keys ??= new KeyCollection(this);

    ICollection<TKey> IDictionary<TKey, TValue>.Keys => Keys;

    IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys => Keys;

    public ValueCollection Values => _values ??= new ValueCollection(this);

    ICollection<TValue> IDictionary<TKey, TValue>.Values => Values;

    IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values => Values;

    public TValue this[TKey key]
    {
        get
        {
            ref TValue value = ref FindValue(key);
            if (!Unsafe.IsNullRef(ref value))
            {
                return value;
            }

            ThrowHelper.ThrowKeyNotFoundException(key);
            return default!;
        }
        set
        {
            bool modified = TryInsert(key, value, InsertionBehavior.OverwriteExisting);
            Debug.Assert(modified);
        }
    }

    public void Add(TKey key, TValue value)
    {
        bool modified = TryInsert(key, value, InsertionBehavior.ThrowOnExisting);
        Debug.Assert(modified); // If there was an existing key and the Add failed, an exception will already have been thrown.
    }

    public void Add<T>(T key, TValue value) where T : ICopyableTo<T, TKey>
    {
        Add((TKey)key, value);
    }

    void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> keyValuePair) =>
        Add(keyValuePair.Key, keyValuePair.Value);

    bool ICollection<KeyValuePair<TKey, TValue>>.Contains(KeyValuePair<TKey, TValue> keyValuePair)
    {
        ref TValue value = ref FindValue(keyValuePair.Key);
        if (!Unsafe.IsNullRef(ref value) && EqualityComparer<TValue>.Default.Equals(value, keyValuePair.Value))
        {
            return true;
        }

        return false;
    }

    bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> keyValuePair)
    {
        ref TValue value = ref FindValue(keyValuePair.Key);
        if (!Unsafe.IsNullRef(ref value) && EqualityComparer<TValue>.Default.Equals(value, keyValuePair.Value))
        {
            Remove(keyValuePair.Key);
            return true;
        }

        return false;
    }

    public void Clear()
    {
        int count = _count;
        if (count > 0)
        {
            Debug.Assert(_buckets != null, "_buckets should be non-null");
            Debug.Assert(_entries != null, "_entries should be non-null");

            Array.Clear(_buckets);

            _count = 0;
            _freeList = -1;
            _freeCount = 0;
            Array.Clear(_entries, 0, count);
        }
    }

    public bool ContainsKey<T>(T key) where T : IEquatable<TKey> => !Unsafe.IsNullRef(ref FindValue(key));

    public bool ContainsKey(TKey key) =>
        !Unsafe.IsNullRef(ref FindValue(key));

    public bool ContainsValue(TValue value)
    {
        Entry[]? entries = _entries;
        if (value == null)
        {
            for (int i = 0; i < _count; i++)
            {
                if (entries![i].next >= -1 && entries[i].value == null)
                {
                    return true;
                }
            }
        }
        else if (typeof(TValue).IsValueType)
        {
            // ValueType: Devirtualize with EqualityComparer<TValue>.Default intrinsic
            for (int i = 0; i < _count; i++)
            {
                if (entries![i].next >= -1 && EqualityComparer<TValue>.Default.Equals(entries[i].value, value))
                {
                    return true;
                }
            }
        }
        else
        {
            // Object type: Shared Generic, EqualityComparer<TValue>.Default won't devirtualize
            // https://github.com/dotnet/runtime/issues/10050
            // So cache in a local rather than get EqualityComparer per loop iteration
            EqualityComparer<TValue> defaultComparer = EqualityComparer<TValue>.Default;
            for (int i = 0; i < _count; i++)
            {
                if (entries![i].next >= -1 && defaultComparer.Equals(entries[i].value, value))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private void CopyTo(KeyValuePair<TKey, TValue>[] array, int index)
    {
        ArgumentNullException.ThrowIfNull(array);

        ArgumentOutOfRangeException.ThrowIfGreaterThan(index, array.Length);

        if (array.Length - index < Count)
        {
            ThrowHelper.ThrowArgumentException("Array size plus offset too small");
        }

        int count = _count;
        Entry[]? entries = _entries;
        for (int i = 0; i < count; i++)
        {
            if (entries![i].next >= -1)
            {
                array[index++] = new KeyValuePair<TKey, TValue>(entries[i].key, entries[i].value);
            }
        }
    }

    public Enumerator GetEnumerator() => new Enumerator(this, Enumerator.KeyValuePair);

    IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator() =>
        Count == 0 ? GenericEmptyEnumerator<KeyValuePair<TKey, TValue>>.Instance : GetEnumerator();

    internal ref TValue FindValue<T>(T key) where T : IEquatable<TKey>
    {
        ArgumentNullException.ThrowIfNull(key);

        ref Entry entry = ref Unsafe.NullRef<Entry>();
        if (_buckets != null)
        {
            Debug.Assert(_entries != null, "expected entries to be != null");
            uint hashCode = (uint)key.GetHashCode();
            int i = GetBucket(hashCode);
            Entry[]? entries = _entries;
            uint collisionCount = 0;

            // ValueType: Devirtualize with EqualityComparer<TKey>.Default intrinsic
            i--; // Value in _buckets is 1-based; subtract 1 from i. We do it here so it fuses with the following conditional.
            do
            {
                // Should be a while loop https://github.com/dotnet/runtime/issues/9422
                // Test in if to drop range check for following array access
                if ((uint)i >= (uint)entries.Length)
                {
                    goto ReturnNotFound;
                }

                entry = ref entries[i];
                if (entry.hashCode == hashCode && key.Equals(entry.key))
                {
                    goto ReturnFound;
                }

                i = entry.next;

                collisionCount++;
            } while (collisionCount <= (uint)entries.Length);

            // The chain of entries forms a loop; which means a concurrent update has happened.
            // Break out of the loop and throw, rather than looping forever.
            ThrowHelper.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
        }

        goto ReturnNotFound;

        ReturnFound:
        ref TValue value = ref entry.value;
        Return:
        return ref value;
        ReturnNotFound:
        value = ref Unsafe.NullRef<TValue>();
        goto Return;
    }

    private int Initialize(int capacity)
    {
        int size = HashHelpers.GetPrime(capacity);
        int[] buckets = new int[size];
        Entry[] entries = new Entry[size];

        // Assign member variables after both arrays allocated to guard against corruption from OOM if second fails
        _freeList = -1;
#if TARGET_64BIT
            _fastModMultiplier = HashHelpers.GetFastModMultiplier((uint)size);
#endif
        _buckets = buckets;
        _entries = entries;

        return size;
    }

    private bool TryInsert(TKey key, TValue value, InsertionBehavior behavior)
    {
        // NOTE: this method is mirrored in CollectionsMarshal.GetValueRefOrAddDefault below.
        // If you make any changes here, make sure to keep that version in sync as well.

        ArgumentNullException.ThrowIfNull(key);

        if (_buckets == null)
        {
            Initialize(0);
        }

        Debug.Assert(_buckets != null);

        Entry[]? entries = _entries;
        Debug.Assert(entries != null, "expected entries to be non-null");

        uint hashCode = (uint)key.GetHashCode();


        uint collisionCount = 0;
        ref int bucket = ref GetBucket(hashCode);
        int i = bucket - 1; // Value in _buckets is 1-based
        // ValueType: Devirtualize with EqualityComparer<TKey>.Default intrinsic
        while (true)
        {
            // Should be a while loop https://github.com/dotnet/runtime/issues/9422
            // Test uint in if rather than loop condition to drop range check for following array access
            if ((uint)i >= (uint)entries.Length)
            {
                break;
            }

            if (entries[i].hashCode == hashCode && EqualityComparer<TKey>.Default.Equals(entries[i].key, key))
            {
                if (behavior == InsertionBehavior.OverwriteExisting)
                {
                    entries[i].value = value;
                    return true;
                }

                if (behavior == InsertionBehavior.ThrowOnExisting)
                {
                    ThrowHelper.ThrowAddingDuplicateWithKeyArgumentException(key);
                }

                return false;
            }

            i = entries[i].next;

            collisionCount++;
            if (collisionCount > (uint)entries.Length)
            {
                // The chain of entries forms a loop; which means a concurrent update has happened.
                // Break out of the loop and throw, rather than looping forever.
                ThrowHelper.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
            }
        }

        int index;
        if (_freeCount > 0)
        {
            index = _freeList;
            Debug.Assert((StartOfFreeList - entries[_freeList].next) >= -1,
                "shouldn't overflow because `next` cannot underflow");
            _freeList = StartOfFreeList - entries[_freeList].next;
            _freeCount--;
        }
        else
        {
            int count = _count;
            if (count == entries.Length)
            {
                Resize();
                bucket = ref GetBucket(hashCode);
            }

            index = count;
            _count = count + 1;
            entries = _entries;
        }

        ref Entry entry = ref entries![index];
        entry.hashCode = hashCode;
        entry.next = bucket - 1; // Value in _buckets is 1-based
        entry.key = key;
        entry.value = value;
        bucket = index + 1; // Value in _buckets is 1-based
        _version++;

        return true;
    }

    /// <summary>
    /// A helper class containing APIs exposed through <see cref="CollectionsMarshal"/>.
    /// These methods are relatively niche and only used in specific scenarios, so adding them in a separate type avoids
    /// the additional overhead on each <see cref="VariantDictionary{TKey,TValue}"/> instantiation, especially in AOT scenarios.
    /// </summary>
    internal static class CollectionsMarshalHelper
    {
        /// <inheritdoc cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey, TValue}(VariantDictionary{TKey,TValue}, TKey, out bool)"/>
        public static ref TValue? GetValueRefOrAddDefault(VariantDictionary<TKey, TValue> dictionary, TKey key,
            out bool exists)
        {
            // NOTE: this method is mirrored by Dictionary<TKey, TValue>.TryInsert above.
            // If you make any changes here, make sure to keep that version in sync as well.

            ArgumentNullException.ThrowIfNull(key);

            if (dictionary._buckets == null)
            {
                dictionary.Initialize(0);
            }

            Debug.Assert(dictionary._buckets != null);

            Entry[]? entries = dictionary._entries;
            Debug.Assert(entries != null, "expected entries to be non-null");

            uint hashCode = (uint) key.GetHashCode();

            uint collisionCount = 0;
            ref int bucket = ref dictionary.GetBucket(hashCode);
            int i = bucket - 1; // Value in _buckets is 1-based

            // ValueType: Devirtualize with EqualityComparer<TKey>.Default intrinsic
            while (true)
            {
                // Should be a while loop https://github.com/dotnet/runtime/issues/9422
                // Test uint in if rather than loop condition to drop range check for following array access
                if ((uint)i >= (uint)entries.Length)
                {
                    break;
                }

                if (entries[i].hashCode == hashCode && EqualityComparer<TKey>.Default.Equals(entries[i].key, key))
                {
                    exists = true;

                    return ref entries[i].value!;
                }

                i = entries[i].next;

                collisionCount++;
                if (collisionCount > (uint)entries.Length)
                {
                    // The chain of entries forms a loop; which means a concurrent update has happened.
                    // Break out of the loop and throw, rather than looping forever.
                    ThrowHelper.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
                }
            }

            int index;
            if (dictionary._freeCount > 0)
            {
                index = dictionary._freeList;
                Debug.Assert((StartOfFreeList - entries[dictionary._freeList].next) >= -1,
                    "shouldn't overflow because `next` cannot underflow");
                dictionary._freeList = StartOfFreeList - entries[dictionary._freeList].next;
                dictionary._freeCount--;
            }
            else
            {
                int count = dictionary._count;
                if (count == entries.Length)
                {
                    dictionary.Resize();
                    bucket = ref dictionary.GetBucket(hashCode);
                }

                index = count;
                dictionary._count = count + 1;
                entries = dictionary._entries;
            }

            ref Entry entry = ref entries![index];
            entry.hashCode = hashCode;
            entry.next = bucket - 1; // Value in _buckets is 1-based
            entry.key = key;
            entry.value = default!;
            bucket = index + 1; // Value in _buckets is 1-based
            dictionary._version++;

            exists = false;

            return ref entry.value!;
        }
    }

    private void Resize() => Resize(HashHelpers.ExpandPrime(_count));

    private void Resize(int newSize)
    {
        // Value types never rehash
        Debug.Assert(_entries != null, "_entries should be non-null");
        Debug.Assert(newSize >= _entries.Length);

        Entry[] entries = new Entry[newSize];

        int count = _count;
        Array.Copy(_entries, entries, count);

        // Assign member variables after both arrays allocated to guard against corruption from OOM if second fails
        _buckets = new int[newSize];
#if TARGET_64BIT
            _fastModMultiplier = HashHelpers.GetFastModMultiplier((uint)newSize);
#endif
        for (int i = 0; i < count; i++)
        {
            if (entries[i].next >= -1)
            {
                ref int bucket = ref GetBucket(entries[i].hashCode);
                entries[i].next = bucket - 1; // Value in _buckets is 1-based
                bucket = i + 1;
            }
        }

        _entries = entries;
    }

    bool IDictionary<TKey, TValue>.Remove(TKey key) => Remove(key);
    
    public bool Remove<T>(T key) where T: IEquatable<TKey>
    {
        // The overload Remove(TKey key, out TValue value) is a copy of this method with one additional
        // statement to copy the value for entry being removed into the output parameter.
        // Code has been intentionally duplicated for performance reasons.

        ArgumentNullException.ThrowIfNull(key);

        if (_buckets != null)
        {
            Debug.Assert(_entries != null, "entries should be non-null");
            uint collisionCount = 0;

            uint hashCode = (uint) key.GetHashCode();

            ref int bucket = ref GetBucket(hashCode);
            Entry[]? entries = _entries;
            int last = -1;
            int i = bucket - 1; // Value in buckets is 1-based
            while (i >= 0)
            {
                ref Entry entry = ref entries[i];

                if (entry.hashCode == hashCode && key.Equals(entry.key))
                {
                    if (last < 0)
                    {
                        bucket = entry.next + 1; // Value in buckets is 1-based
                    }
                    else
                    {
                        entries[last].next = entry.next;
                    }

                    Debug.Assert((StartOfFreeList - _freeList) < 0,
                        "shouldn't underflow because max hashtable length is MaxPrimeArrayLength = 0x7FEFFFFD(2146435069) _freelist underflow threshold 2147483646");
                    entry.next = StartOfFreeList - _freeList;

                    if (RuntimeHelpers.IsReferenceOrContainsReferences<TKey>())
                    {
                        entry.key = default!;
                    }

                    if (RuntimeHelpers.IsReferenceOrContainsReferences<TValue>())
                    {
                        entry.value = default!;
                    }

                    _freeList = i;
                    _freeCount++;
                    return true;
                }

                last = i;
                i = entry.next;

                collisionCount++;
                if (collisionCount > (uint)entries.Length)
                {
                    // The chain of entries forms a loop; which means a concurrent update has happened.
                    // Break out of the loop and throw, rather than looping forever.
                    ThrowHelper.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
                }
            }
        }

        return false;
    }

    public bool Remove<T>(T key, [MaybeNullWhen(false)] out TValue value) where T: IEquatable<TKey>
    {
        // This overload is a copy of the overload Remove(TKey key) with one additional
        // statement to copy the value for entry being removed into the output parameter.
        // Code has been intentionally duplicated for performance reasons.

        ArgumentNullException.ThrowIfNull(key);

        if (_buckets != null)
        {
            Debug.Assert(_entries != null, "entries should be non-null");
            uint collisionCount = 0;

            uint hashCode = (uint) key.GetHashCode();

            ref int bucket = ref GetBucket(hashCode);
            Entry[]? entries = _entries;
            int last = -1;
            int i = bucket - 1; // Value in buckets is 1-based
            while (i >= 0)
            {
                ref Entry entry = ref entries[i];

                if (entry.hashCode == hashCode && key.Equals(entry.key))
                {
                    if (last < 0)
                    {
                        bucket = entry.next + 1; // Value in buckets is 1-based
                    }
                    else
                    {
                        entries[last].next = entry.next;
                    }

                    value = entry.value;

                    Debug.Assert((StartOfFreeList - _freeList) < 0,
                        "shouldn't underflow because max hashtable length is MaxPrimeArrayLength = 0x7FEFFFFD(2146435069) _freelist underflow threshold 2147483646");
                    entry.next = StartOfFreeList - _freeList;

                    if (RuntimeHelpers.IsReferenceOrContainsReferences<TKey>())
                    {
                        entry.key = default!;
                    }

                    if (RuntimeHelpers.IsReferenceOrContainsReferences<TValue>())
                    {
                        entry.value = default!;
                    }

                    _freeList = i;
                    _freeCount++;
                    return true;
                }

                last = i;
                i = entry.next;

                collisionCount++;
                if (collisionCount > (uint)entries.Length)
                {
                    // The chain of entries forms a loop; which means a concurrent update has happened.
                    // Break out of the loop and throw, rather than looping forever.
                    ThrowHelper.ThrowInvalidOperationException_ConcurrentOperationsNotSupported();
                }
            }
        }

        value = default;
        return false;
    }

    bool IDictionary<TKey, TValue>.TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value) =>
        TryGetValue(key, out value);

    bool IReadOnlyDictionary<TKey, TValue>.TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value) =>
        TryGetValue(key, out value);

    public bool TryGetValue<T>(T key, [MaybeNullWhen(false)] out TValue value) where T: IEquatable<TKey>
    {
        ref TValue valRef = ref FindValue(key);
        if (!Unsafe.IsNullRef(ref valRef))
        {
            value = valRef;
            return true;
        }

        value = default;
        return false;
    }

    public bool TryAdd(TKey key, TValue value) =>
        TryInsert(key, value, InsertionBehavior.None);

    public bool TryAdd<T>(T key, TValue value) where T : ICopyableTo<T,TKey> =>
        TryInsert((TKey) key, value, InsertionBehavior.None);

    bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly => false;

    void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int index) =>
        CopyTo(array, index);

   IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<KeyValuePair<TKey, TValue>>)this).GetEnumerator();

    /// <summary>
    /// Ensures that the dictionary can hold up to 'capacity' entries without any further expansion of its backing storage
    /// </summary>
    public int EnsureCapacity(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);

        int currentCapacity = _entries == null ? 0 : _entries.Length;
        if (currentCapacity >= capacity)
        {
            return currentCapacity;
        }

        _version++;

        if (_buckets == null)
        {
            return Initialize(capacity);
        }

        int newSize = HashHelpers.GetPrime(capacity);
        Resize(newSize);
        return newSize;
    }

    /// <summary>
    /// Sets the capacity of this dictionary to what it would be if it had been originally initialized with all its entries
    /// </summary>
    /// <remarks>
    /// This method can be used to minimize the memory overhead
    /// once it is known that no new elements will be added.
    ///
    /// To allocate minimum size storage array, execute the following statements:
    ///
    /// dictionary.Clear();
    /// dictionary.TrimExcess();
    /// </remarks>
    public void TrimExcess() => TrimExcess(Count);

    /// <summary>
    /// Sets the capacity of this dictionary to hold up 'capacity' entries without any further expansion of its backing storage
    /// </summary>
    /// <remarks>
    /// This method can be used to minimize the memory overhead
    /// once it is known that no new elements will be added.
    /// </remarks>
    public void TrimExcess(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, Count);

        int newSize = HashHelpers.GetPrime(capacity);
        Entry[]? oldEntries = _entries;
        int currentCapacity = oldEntries == null ? 0 : oldEntries.Length;
        if (newSize >= currentCapacity)
        {
            return;
        }

        int oldCount = _count;
        _version++;
        Initialize(newSize);

        Debug.Assert(oldEntries is not null);

        CopyEntries(oldEntries, oldCount);
    }

    private void CopyEntries(Entry[] entries, int count)
    {
        Debug.Assert(_entries is not null);

        Entry[] newEntries = _entries;
        int newCount = 0;
        for (int i = 0; i < count; i++)
        {
            uint hashCode = entries[i].hashCode;
            if (entries[i].next >= -1)
            {
                ref Entry entry = ref newEntries[newCount];
                entry = entries[i];
                ref int bucket = ref GetBucket(hashCode);
                entry.next = bucket - 1; // Value in _buckets is 1-based
                bucket = newCount + 1;
                newCount++;
            }
        }

        _count = newCount;
        _freeCount = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref int GetBucket(uint hashCode)
    {
        int[] buckets = _buckets!;
#if TARGET_64BIT
            return ref buckets[HashHelpers.FastMod(hashCode, (uint)buckets.Length, _fastModMultiplier)];
#else
        return ref buckets[(uint)hashCode % buckets.Length];
#endif
    }

    private struct Entry
    {
        public uint hashCode;

        /// <summary>
        /// 0-based index of next entry in chain: -1 means end of chain
        /// also encodes whether this entry _itself_ is part of the free list by changing sign and subtracting 3,
        /// so -2 means end of free list, -3 means index 0 but on free list, -4 means index 1 but on free list, etc.
        /// </summary>
        public int next;

        public TKey key; // Key of entry
        public TValue value; // Value of entry
    }

    public struct Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>, IDictionaryEnumerator
    {
        private readonly VariantDictionary<TKey, TValue> _dictionary;
        private readonly int _version;
        private int _index;
        private KeyValuePair<TKey, TValue> _current;
        private readonly int _getEnumeratorRetType; // What should Enumerator.Current return?

        internal const int DictEntry = 1;
        internal const int KeyValuePair = 2;

        internal Enumerator(VariantDictionary<TKey, TValue> dictionary, int getEnumeratorRetType)
        {
            _dictionary = dictionary;
            _version = dictionary._version;
            _index = 0;
            _getEnumeratorRetType = getEnumeratorRetType;
            _current = default;
        }

        public bool MoveNext()
        {
            if (_version != _dictionary._version)
            {
                ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion();
            }

            // Use unsigned comparison since we set index to dictionary.count+1 when the enumeration ends.
            // dictionary.count+1 could be negative if dictionary.count is int.MaxValue
            while ((uint)_index < (uint)_dictionary._count)
            {
                ref Entry entry = ref _dictionary._entries![_index++];

                if (entry.next >= -1)
                {
                    _current = new KeyValuePair<TKey, TValue>(entry.key, entry.value);
                    return true;
                }
            }

            _index = _dictionary._count + 1;
            _current = default;
            return false;
        }

        public KeyValuePair<TKey, TValue> Current => _current;

        public void Dispose()
        {
        }

        object? IEnumerator.Current
        {
            get
            {
                if (_index == 0 || (_index == _dictionary._count + 1))
                {
                    ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                }

                if (_getEnumeratorRetType == DictEntry)
                {
                    return new DictionaryEntry(_current.Key, _current.Value);
                }

                return new KeyValuePair<TKey, TValue>(_current.Key, _current.Value);
            }
        }

        void IEnumerator.Reset()
        {
            if (_version != _dictionary._version)
            {
                ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion();
            }

            _index = 0;
            _current = default;
        }

        DictionaryEntry IDictionaryEnumerator.Entry
        {
            get
            {
                if (_index == 0 || (_index == _dictionary._count + 1))
                {
                    ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                }

                return new DictionaryEntry(_current.Key, _current.Value);
            }
        }

        object IDictionaryEnumerator.Key
        {
            get
            {
                if (_index == 0 || (_index == _dictionary._count + 1))
                {
                    ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                }

                return _current.Key;
            }
        }

        object? IDictionaryEnumerator.Value
        {
            get
            {
                if (_index == 0 || (_index == _dictionary._count + 1))
                {
                    ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                }

                return _current.Value;
            }
        }
    }

    [DebuggerDisplay("Count = {Count}")]
    public sealed class KeyCollection : ICollection<TKey>, ICollection, IReadOnlyCollection<TKey>
    {
        private readonly VariantDictionary<TKey, TValue> _dictionary;

        public KeyCollection(VariantDictionary<TKey, TValue> dictionary)
        {
            ArgumentNullException.ThrowIfNull(dictionary);

            _dictionary = dictionary;
        }

        public Enumerator GetEnumerator() => new Enumerator(_dictionary);

        public void CopyTo(TKey[] array, int index)
        {
            ArgumentNullException.ThrowIfNull(array);

            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(index, array.Length);

            if (array.Length - index < _dictionary.Count)
            {
                ThrowHelper.ThrowArgumentException("Array size plus offset too small");
            }

            int count = _dictionary._count;
            Entry[]? entries = _dictionary._entries;
            for (int i = 0; i < count; i++)
            {
                if (entries![i].next >= -1) array[index++] = entries[i].key;
            }
        }

        public int Count => _dictionary.Count;

        bool ICollection<TKey>.IsReadOnly => true;

        void ICollection<TKey>.Add(TKey item) =>
            ThrowHelper.ThrowNotSupportedException("Directly manipulating Keys collection is not supported");


        void ICollection<TKey>.Clear() =>
            ThrowHelper.ThrowNotSupportedException("Directly manipulating Keys collection is not supported");

        public bool Contains(TKey item) =>
            _dictionary.ContainsKey(item);

        bool ICollection<TKey>.Remove(TKey item)
        {
            ThrowHelper.ThrowNotSupportedException("Directly manipulating Keys collection is not supported");
            return false;
        }

        IEnumerator<TKey> IEnumerable<TKey>.GetEnumerator() =>
            Count == 0 ? GenericEmptyEnumerator<TKey>.Instance : GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<TKey>)this).GetEnumerator();

        void ICollection.CopyTo(Array array, int index)
        {
            ArgumentNullException.ThrowIfNull(array);

            ArgumentOutOfRangeException.ThrowIfNotEqual(array.Rank, 1);
            ArgumentOutOfRangeException.ThrowIfNotEqual(array.GetLowerBound(0),0);

            ArgumentOutOfRangeException.ThrowIfGreaterThan(index, array.Length);

            if (array.Length - index < _dictionary.Count)
            {
                ThrowHelper.ThrowArgumentException("Array size plus offset too small");
            }

            if (array is TKey[] keys)
            {
                CopyTo(keys, index);
            }
            else
            {
                object[]? objects = array as object[];
                if (objects == null)
                {
                    ThrowHelper.ThrowArgumentException_Argument_IncompatibleArrayType();
                    return;
                }

                int count = _dictionary._count;
                Entry[]? entries = _dictionary._entries;
                try
                {
                    for (int i = 0; i < count; i++)
                    {
                        if (entries![i].next >= -1) objects[index++] = entries[i].key;
                    }
                }
                catch (ArrayTypeMismatchException)
                {
                    ThrowHelper.ThrowArgumentException_Argument_IncompatibleArrayType();
                }
            }
        }

        bool ICollection.IsSynchronized => false;

        object ICollection.SyncRoot => ((ICollection)_dictionary).SyncRoot;

        public struct Enumerator : IEnumerator<TKey>, IEnumerator
        {
            private readonly VariantDictionary<TKey, TValue> _dictionary;
            private int _index;
            private readonly int _version;
            private TKey? _currentKey;

            internal Enumerator(VariantDictionary<TKey, TValue> dictionary)
            {
                _dictionary = dictionary;
                _version = dictionary._version;
                _index = 0;
                _currentKey = default;
            }

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                if (_version != _dictionary._version)
                {
                    ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion();
                }

                while ((uint)_index < (uint)_dictionary._count)
                {
                    ref Entry entry = ref _dictionary._entries![_index++];

                    if (entry.next >= -1)
                    {
                        _currentKey = entry.key;
                        return true;
                    }
                }

                _index = _dictionary._count + 1;
                _currentKey = default;
                return false;
            }

            public TKey Current => _currentKey!;

            object? IEnumerator.Current
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary._count + 1))
                    {
                        ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                    }

                    return _currentKey;
                }
            }

            void IEnumerator.Reset()
            {
                if (_version != _dictionary._version)
                {
                    ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion();
                }

                _index = 0;
                _currentKey = default;
            }
        }
    }

    [DebuggerDisplay("Count = {Count}")]
    public sealed class ValueCollection : ICollection<TValue>, ICollection, IReadOnlyCollection<TValue>
    {
        private readonly VariantDictionary<TKey, TValue> _dictionary;

        public ValueCollection(VariantDictionary<TKey, TValue> dictionary)
        {
            ArgumentNullException.ThrowIfNull(dictionary);

            _dictionary = dictionary;
        }

        public Enumerator GetEnumerator() => new(_dictionary);

        public void CopyTo(TValue[] array, int index)
        {
            ArgumentNullException.ThrowIfNull(array);

            ArgumentOutOfRangeException.ThrowIfGreaterThan(index, array.Length);

            if (array.Length - index < _dictionary.Count)
            {
                ThrowHelper.ThrowArgumentException("Array plus offset is too small");
            }

            int count = _dictionary._count;
            Entry[]? entries = _dictionary._entries;
            for (int i = 0; i < count; i++)
            {
                if (entries![i].next >= -1) array[index++] = entries[i].value;
            }
        }

        public int Count => _dictionary.Count;

        bool ICollection<TValue>.IsReadOnly => true;

        void ICollection<TValue>.Add(TValue item) =>
            ThrowHelper.ThrowNotSupportedException("Directly manipulating Values collection is not supported");

        bool ICollection<TValue>.Remove(TValue item)
        {
            ThrowHelper.ThrowNotSupportedException("Directly manipulating Values collection is not supported");
            return false;
        }

        void ICollection<TValue>.Clear() =>
            ThrowHelper.ThrowNotSupportedException("Directly manipulating Values collection is not supported");

        bool ICollection<TValue>.Contains(TValue item) => _dictionary.ContainsValue(item);

        IEnumerator<TValue> IEnumerable<TValue>.GetEnumerator() =>
            Count == 0 ? GenericEmptyEnumerator<TValue>.Instance : GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<TValue>)this).GetEnumerator();

        void ICollection.CopyTo(Array array, int index)
        {
            ArgumentNullException.ThrowIfNull(array);

            ArgumentOutOfRangeException.ThrowIfNotEqual(array.Rank, 1);
            ArgumentOutOfRangeException.ThrowIfNotEqual(array.GetLowerBound(0),0);
        
            ArgumentOutOfRangeException.ThrowIfGreaterThan(index, array.Length);

            if (array.Length - index < _dictionary.Count)
            {
                ThrowHelper.ThrowArgumentException("Array plus offset is too small");
            }

            if (array is TValue[] values)
            {
                CopyTo(values, index);
            }
            else
            {
                object[]? objects = array as object[];
                if (objects == null)
                {
                    ThrowHelper.ThrowArgumentException_Argument_IncompatibleArrayType();
                    return;
                }

                int count = _dictionary._count;
                Entry[]? entries = _dictionary._entries;
                try
                {
                    for (int i = 0; i < count; i++)
                    {
                        if (entries![i].next >= -1) objects[index++] = entries[i].value!;
                    }
                }
                catch (ArrayTypeMismatchException)
                {
                    ThrowHelper.ThrowArgumentException_Argument_IncompatibleArrayType();
                }
            }
        }

        bool ICollection.IsSynchronized => false;

        object ICollection.SyncRoot => ((ICollection)_dictionary).SyncRoot;

        public struct Enumerator : IEnumerator<TValue>, IEnumerator
        {
            private readonly VariantDictionary<TKey, TValue> _dictionary;
            private int _index;
            private readonly int _version;
            private TValue? _currentValue;

            internal Enumerator(VariantDictionary<TKey, TValue> dictionary)
            {
                _dictionary = dictionary;
                _version = dictionary._version;
                _index = 0;
                _currentValue = default;
            }

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                if (_version != _dictionary._version)
                {
                    ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion();
                }

                while ((uint)_index < (uint)_dictionary._count)
                {
                    ref Entry entry = ref _dictionary._entries![_index++];

                    if (entry.next >= -1)
                    {
                        _currentValue = entry.value;
                        return true;
                    }
                }

                _index = _dictionary._count + 1;
                _currentValue = default;
                return false;
            }

            public TValue Current => _currentValue!;

            object? IEnumerator.Current
            {
                get
                {
                    if (_index == 0 || (_index == _dictionary._count + 1))
                    {
                        ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen();
                    }

                    return _currentValue;
                }
            }

            void IEnumerator.Reset()
            {
                if (_version != _dictionary._version)
                {
                    ThrowHelper.ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion();
                }

                _index = 0;
                _currentValue = default;
            }
        }
    }

    private enum InsertionBehavior : byte
    {
        None,
        OverwriteExisting,
        ThrowOnExisting,
    }
}

file static class ThrowHelper
{
    public static void ThrowArgumentOutOfRangeException(string argument) =>
        throw new ArgumentOutOfRangeException(argument);

    public static void ThrowKeyNotFoundException<TKey>(TKey key) where TKey : notnull =>
        throw new KeyNotFoundException($"Key ${key} not found");

    public static void ThrowInvalidOperationException_ConcurrentOperationsNotSupported() =>
        throw new InvalidOperationException("Concurrent options are not supported");

    public static void ThrowArgumentException(string message) => throw new ArgumentException(message: message);

    public static void ThrowInvalidOperationException_EnumCurrent(int i)
    {
        throw new NotImplementedException();
    }

    public static void ThrowInvalidOperationException_InvalidOperation_EnumFailedVersion()
    {
        throw new NotImplementedException();
    }

    public static void ThrowInvalidOperationException_InvalidOperation_EnumOpCantHappen() =>
        throw new NotImplementedException();

    public static void ThrowNotSupportedException(string message) => throw new NotSupportedException(message);

    public static void ThrowArgumentException_Argument_IncompatibleArrayType() =>
        throw new ArgumentException("Incompatible array type");

    public static void ThrowAddingDuplicateWithKeyArgumentException<TKey>(TKey key) where TKey : IEquatable<TKey>
    {
        throw new NotImplementedException();
    }
}

internal abstract class GenericEmptyEnumeratorBase : IDisposable, IEnumerator
{
#pragma warning disable CA1822 // https://github.com/dotnet/roslyn-analyzers/issues/5911
    public bool MoveNext() => false;

    public object Current
    {
        get
        {
            ThrowHelper.ThrowInvalidOperationException_EnumCurrent(-1);
            return default!;
        }
    }

    public void Reset()
    {
    }

    public void Dispose()
    {
    }
#pragma warning restore CA1822
}

/// <summary>Provides an empty enumerator singleton.</summary>
/// <remarks>
/// If the consumer is using SZGenericArrayEnumerator elsewhere or is otherwise likely
/// to be using T[] elsewhere, SZGenericArrayEnumerator's singleton should be used.  Otherwise,
/// this singleton should be used, as it doesn't reference T[] in order to reduce footprint.
/// </remarks>
file sealed class GenericEmptyEnumerator<T> : GenericEmptyEnumeratorBase, IEnumerator<T>
{
    public static readonly GenericEmptyEnumerator<T> Instance = new();

    private GenericEmptyEnumerator()
    {
    }

    public new T Current
    {
        get
        {
            ThrowHelper.ThrowInvalidOperationException_EnumCurrent(-1);
            return default!;
        }
    }
}

file static partial class HashHelpers
{
    public const uint HashCollisionThreshold = 100;

    // This is the maximum prime smaller than Array.MaxLength.
    public const int MaxPrimeArrayLength = 0x7FFFFFC3;

    public const int HashPrime = 101;

    // Table of prime numbers to use as hash table sizes.
    // A typical resize algorithm would pick the smallest prime number in this array
    // that is larger than twice the previous capacity.
    // Suppose our Hashtable currently has capacity x and enough elements are added
    // such that a resize needs to occur. Resizing first computes 2x then finds the
    // first prime in the table greater than 2x, i.e. if primes are ordered
    // p_1, p_2, ..., p_i, ..., it finds p_n such that p_n-1 < 2x < p_n.
    // Doubling is important for preserving the asymptotic complexity of the
    // hashtable operations such as add.  Having a prime guarantees that double
    // hashing does not lead to infinite loops.  IE, your hash function will be
    // h1(key) + i*h2(key), 0 <= i < size.  h2 and the size must be relatively prime.
    // We prefer the low computation costs of higher prime numbers over the increased
    // memory allocation of a fixed prime number i.e. when right sizing a HashSet.
    internal static ReadOnlySpan<int> Primes => new int[]
    {
        3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
        1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
        17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
        187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
        1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369
    };

    public static bool IsPrime(int candidate)
    {
        if ((candidate & 1) != 0)
        {
            int limit = (int)Math.Sqrt(candidate);
            for (int divisor = 3; divisor <= limit; divisor += 2)
            {
                if ((candidate % divisor) == 0)
                    return false;
            }

            return true;
        }

        return candidate == 2;
    }

    public static int GetPrime(int min)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(min);

        foreach (int prime in Primes)
        {
            if (prime >= min)
                return prime;
        }

        // Outside of our predefined table. Compute the hard way.
        for (int i = (min | 1); i < int.MaxValue; i += 2)
        {
            if (IsPrime(i) && ((i - 1) % HashPrime != 0))
                return i;
        }

        return min;
    }

    // Returns size of hashtable to grow to.
    public static int ExpandPrime(int oldSize)
    {
        int newSize = 2 * oldSize;

        // Allow the hashtables to grow to maximum possible size (~2G elements) before encountering capacity overflow.
        // Note that this check works even when _items.Length overflowed thanks to the (uint) cast
        if ((uint)newSize > MaxPrimeArrayLength && MaxPrimeArrayLength > oldSize)
        {
            Debug.Assert(MaxPrimeArrayLength == GetPrime(MaxPrimeArrayLength), "Invalid MaxPrimeArrayLength");
            return MaxPrimeArrayLength;
        }

        return GetPrime(newSize);
    }

    /// <summary>Returns approximate reciprocal of the divisor: ceil(2**64 / divisor).</summary>
    /// <remarks>This should only be used on 64-bit.</remarks>
    public static ulong GetFastModMultiplier(uint divisor) =>
        ulong.MaxValue / divisor + 1;

    /// <summary>Performs a mod operation using the multiplier pre-computed with <see cref="GetFastModMultiplier"/>.</summary>
    /// <remarks>This should only be used on 64-bit.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint FastMod(uint value, uint divisor, ulong multiplier)
    {
        // We use modified Daniel Lemire's fastmod algorithm (https://github.com/dotnet/runtime/pull/406),
        // which allows to avoid the long multiplication if the divisor is less than 2**31.
        Debug.Assert(divisor <= int.MaxValue);

        // This is equivalent of (uint)Math.BigMul(multiplier * value, divisor, out _). This version
        // is faster than BigMul currently because we only need the high bits.
        uint highbits = (uint)(((((multiplier * value) >> 32) + 1) * divisor) >> 32);

        Debug.Assert(highbits == value % divisor);
        return highbits;
    }
}