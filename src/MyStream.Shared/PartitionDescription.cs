namespace MyStream;

/// <summary>
/// Offsets are a half open range, so an empty partition is simply Start == End. Shared rather than
/// protocol owned: it is storage data that happens to also go on the wire.
/// </summary>
public readonly record struct PartitionDescription(short Partition, long StartOffset, long EndOffset);
