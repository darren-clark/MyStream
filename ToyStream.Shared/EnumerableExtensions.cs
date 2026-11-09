namespace ToyStream.Shared;

public static class EnumerableExtensions
{
    public static IEnumerable<T> WhereNotNull<T>(this IEnumerable<T?> source)
        => source.Where(item => item is not null)!;

}