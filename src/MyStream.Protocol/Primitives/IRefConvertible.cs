namespace MyStream.Protocol.Primitives;

public interface IRefConvertible<in TStruct, out TRefStruct>
    where TStruct: IRefConvertible<TStruct, TRefStruct>
    where TRefStruct: struct, allows ref struct
{
    public static abstract implicit operator TRefStruct(TStruct value);
}