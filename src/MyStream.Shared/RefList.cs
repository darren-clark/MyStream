using System.Buffers;

namespace MyStream;

public readonly ref struct RefList<T>: IRefSerializable<RefList<T>> where T: struct, IRefSerializable<T>, allows ref struct
{
    private readonly ReadOnlySequence<byte> items;
    
    public RefList(ReadOnlySequence<byte> items)
    {
        this.items = items;
    }

    public RefListEnumerator GetEnumerator() => new RefListEnumerator(new SequenceReader<byte>(items));
    
    public ref struct RefListEnumerator
    {
        private SequenceReader<byte> reader;
        private T current;

        public RefListEnumerator(SequenceReader<byte> reader)
        {
            this.reader = reader;
        }

        public bool MoveNext()
        {
            // Can't pass a ref reader, so we'll return how many bytes to advance.
            // Give them a new sequence reader to make it easy to get the bytes read.
            if (!T.TryRead(new SequenceReader<byte>(reader.UnreadSequence), out T value, out var read, out _))
            {
                return false;
            }
            
            reader.Advance(read);
            current = value;
            return true;
        }

        public T Current => current;
    }

    public static bool TryRead(SequenceReader<byte> reader, out RefList<T> value, out long read, out long bytesNeeded)
    {
        if (!reader.TryReadBigEndian(out short length))
        {
            value = default;
            read = 0;
            bytesNeeded =  sizeof(short) - reader.UnreadSequence.Length;
            return false;
        }

        // Save where we're starting reading the list.
        var itemStart = reader.UnreadSequence;
        long itemsLength = 0;
        for (var i = 0; i < length; i++)
        {
            // We're not actually generating a list, just walking the buffer to see how
            // big the contiguous items are.
            if (!T.TryRead(reader, out _, out var itemLength, out bytesNeeded))
            {
                value = default;
                read = 0;
                return false;
            }
            itemsLength += itemLength;
        }
        value = new RefList<T>(itemStart.Slice(0, itemsLength));
        read = sizeof(short) + itemsLength;
        bytesNeeded = 0;
        return true;
    }
}