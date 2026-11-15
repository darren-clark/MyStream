// using System.Buffers;
// using System.Diagnostics;
//
// namespace MyStream;
//
// public ref struct RefUtf8StringList 
// {
//     private short count;
//     private ReadOnlySequence<byte> buffer;
//
//     public RefUtf8StringList(short count, ReadOnlySequence<byte> buffer)
//     {
//         this.count = count;
//         this.buffer = buffer;
//     }
//
//     public short Count => count;
//     
//     public RefUtfStringListEnumerator GetEnumerator()
//     {
//         return new RefUtfStringListEnumerator(count, new SequenceReader<byte>(buffer));
//     }
//
//     public ref struct RefUtfStringListEnumerator
//     {
//         private SequenceReader<byte> reader;
//         private RefUtf8String current = default;
//
//         public RefUtfStringListEnumerator(short count, SequenceReader<byte> reader)
//         {
//             this.reader = reader;
//         }
//         
//         public void Dispose()
//         {
//         }
//
//         public bool MoveNext()
//         {
//             if (!reader.TryReadBigEndian(out short size))
//             {
//                 return false;
//             }
//
//             var read = reader.TryReadExact(size, out var buffer);
//             Debug.Assert(read);
//             current = new RefUtf8String(buffer);
//             return true;
//         }
//
//         public void Reset()
//         {
//             throw new NotImplementedException();
//         }
//
//         RefUtf8String Current => current;
//     }
// }