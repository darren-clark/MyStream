using System.Buffers;

namespace MyStream.Protocol.Primitives;

public readonly ref struct PrimitiveList<T> : 
    IProtocolPrimitive<PrimitiveList<T>> where T: struct, IProtocolPrimitive<T>
{
    private readonly ReadOnlySpan<T> itemSpan;
    private readonly ReadOnlySequence<byte> itemBuffer;

    public PrimitiveList(ushort count, ReadOnlySequence<byte> items)
    {
        this.Count = count;
        this.itemBuffer = items;
    }

    public PrimitiveList(ReadOnlySpan<T> items)
    {
        this.Count = (ushort) items.Length;
        this.itemSpan = items;
    }

    public void CopyTo(Span<T> destination)
    {
        if (destination.Length != this.Count)
        {
            ThrowHelpers.ThrowIncorrectSize(this.Count, destination.Length);
        }

        if (this.itemSpan.IsEmpty)
        {
            var reader = new PrimitiveReader(this.itemBuffer);
            for (int i = 0; i < this.Count; i++)
            {
                var read = T.TryRead(ref reader, out var item, out _);
                if (!read)
                {
                    ThrowHelpers.ThrowIncorrectSize(this.Count, i);
                }
                destination[i] = item;
            }
        }
        else
        {
            this.itemSpan.CopyTo(destination);
        }
    }
    public ushort Count { get; }

    public Enumerator GetEnumerator() => new Enumerator(this);

    public ref struct Enumerator
    {
        private readonly PrimitiveList<T> list;
        private PrimitiveReader reader;
        private short currentIndex = -1;
        private T current;

        public Enumerator(PrimitiveList<T> list)
        {
            this.list = list;
            this.reader = new PrimitiveReader(list.itemBuffer);
        }
        
        public bool MoveNext()
        {
            if (list.itemSpan.IsEmpty)
            {
                return T.TryRead(ref reader, out current, out _);
            }

            if (++currentIndex >= list.itemSpan.Length)
            {
                return false;
            }

            current = list.itemSpan[currentIndex];
            return true;
        }

        public T Current => current;        
    }

    public static bool TryRead<TReader>(scoped ref TReader reader, out PrimitiveList<T> value, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (!reader.TryReadUInt16(out var count, out bytesToRead))
        {
            value = default;
            return false;
        }

        //Nested reader to get list length
        var listReader = new PrimitiveReader(reader.Remaining);
        for (var i = 0; i < count; i++)
        {
            if (!listReader.TryReadPrimitive<T>(out var item, out bytesToRead))
            {
                value = default;
                return false;
            }
        }

        var items = listReader.GetRead();
        reader.Advance(listReader.Consumed);
        
        value = new PrimitiveList<T>(count, items);
        return true;
    }
    
    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(this.Count);
        if (this.itemSpan.IsEmpty)
        {
            writer.Write(this.itemBuffer);
        }
        else
        {
            foreach (var item in itemSpan)
            {
                writer.Write(item);
            }
        }
    }
}

public readonly ref struct PrimitiveList<TNonRef, TRef> : IProtocolPrimitive<PrimitiveList<TNonRef, TRef>>
    where TRef : struct, IProtocolPrimitive<TRef>,allows ref struct
    where TNonRef : struct, IRefConvertible<TNonRef, TRef>
{
    private readonly ReadOnlySpan<TNonRef> itemSpan;
    private readonly ReadOnlySequence<byte> itemBuffer;

    public PrimitiveList(ushort count, ReadOnlySequence<byte> items)
    {
        this.Count = count;
        this.itemBuffer = items;
    }

    public PrimitiveList(ReadOnlySpan<TNonRef> items)
    {
        this.Count = (ushort) items.Length;
        this.itemSpan = items;
    }

    public ushort Count { get; }

    public Enumerator GetEnumerator() => new Enumerator(this);
    

    public ref struct Enumerator
    {
        private readonly PrimitiveList<TNonRef, TRef> list;
        private PrimitiveReader reader;
        private short currentIndex = -1;
        private TRef current;

        internal Enumerator(PrimitiveList<TNonRef, TRef> list)
        {
            this.list = list;
            this.reader = new PrimitiveReader(list.itemBuffer);
        }

        public bool MoveNext()
        {
            if (list.itemSpan.IsEmpty)
            {
                return TRef.TryRead(ref reader, out current, out _);
            }

            if (++currentIndex >= list.itemSpan.Length)
            {
                return false;
            }

            current = list.itemSpan[currentIndex];
            return true;
        }

        public TRef Current => current;
    }

    public static bool TryRead<TReader>(scoped ref TReader reader, out PrimitiveList<TNonRef, TRef> value, out int bytesToRead) where TReader : IPrimitiveReader, allows ref struct
    {
        if (!reader.TryReadUInt16(out var count, out bytesToRead))
        {
            value = default;
            return false;
        }

        //Nested reader to get list length
        var listReader = new PrimitiveReader(reader.Remaining);
        for (var i = 0; i < count; i++)
        {
            if (!listReader.TryReadPrimitive<TRef>(out var item, out bytesToRead))
            {
                value = default;
                return false;
            }
        }

        var items = listReader.GetRead();
        reader.Advance(listReader.Consumed);
        
        value = new PrimitiveList<TNonRef, TRef>(count, items);
        return true;
    }
    
    public void Write<TWriter>(TWriter writer) where TWriter : struct, IPrimitiveWriter, allows ref struct
    {
        writer.Write(this.Count);
        if (this.itemSpan.IsEmpty)
        {
            writer.Write(this.itemBuffer);
        }
        else
        {
            foreach (var item in itemSpan)
            {
                TRef asRef = item;
                writer.Write(asRef);
            }
        }
    }
}