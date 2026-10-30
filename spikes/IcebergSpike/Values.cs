using System.Buffers.Binary;
using System.Numerics;
using System.Text;

namespace IcebergSpike;

// Iceberg values in one CLR shape per type, so bounds, partition values and Parquet data compare directly:
// long (int widened), string, decimal, DateTimeOffset (timestamptz, UTC), DateTime (timestamp), DateOnly, bool, double.
public static class Values
{
    private static readonly DateTime Epoch = DateTime.UnixEpoch;

    // Appendix D, binary single-value serialization (lower_bounds, upper_bounds, field summaries).
    public static object Decode(IcebergType type, byte[] bytes) => type.Name switch
    {
        "boolean" => bytes[0] != 0,
        "int" => (long)BinaryPrimitives.ReadInt32LittleEndian(bytes),
        "long" => BinaryPrimitives.ReadInt64LittleEndian(bytes),
        "float" => (double)BinaryPrimitives.ReadSingleLittleEndian(bytes),
        "double" => BinaryPrimitives.ReadDoubleLittleEndian(bytes),
        "date" => DateOnly.FromDayNumber(DateOnly.FromDateTime(Epoch).DayNumber + BinaryPrimitives.ReadInt32LittleEndian(bytes)),
        "timestamptz" => new DateTimeOffset(Epoch.AddTicks(BinaryPrimitives.ReadInt64LittleEndian(bytes) * 10), TimeSpan.Zero),
        "timestamp" => DateTime.SpecifyKind(Epoch.AddTicks(BinaryPrimitives.ReadInt64LittleEndian(bytes) * 10), DateTimeKind.Unspecified),
        "string" => Encoding.UTF8.GetString(bytes),
        "decimal" => (decimal)new BigInteger(bytes, isUnsigned: false, isBigEndian: true) / Pow10(type.Scale),
        _ => throw new NotSupportedException($"Bounds of type {type.Name}"),
    };

    // Values coming from Parquet.Net or AvroConvert into the shapes above.
    public static object? Normalize(IcebergType type, object? value) => value switch
    {
        null => null,
        int i when type.Name is "int" or "long" => (long)i,
        float f => (double)f,
        ReadOnlyMemory<char> m => m.ToString(),
        DateTime dt when type.Name == "timestamptz" => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
        DateTime dt when type.Name == "date" => DateOnly.FromDateTime(dt),
        _ => value,
    };

    public static int Compare(object a, object b) => (a, b) switch
    {
        (long x, long y) => x.CompareTo(y),
        (string x, string y) => string.CompareOrdinal(x, y),
        (decimal x, decimal y) => x.CompareTo(y),
        (DateTimeOffset x, DateTimeOffset y) => x.CompareTo(y),
        (DateTime x, DateTime y) => x.CompareTo(y),
        (DateOnly x, DateOnly y) => x.CompareTo(y),
        (bool x, bool y) => x.CompareTo(y),
        (double x, double y) => x.CompareTo(y),
        _ => throw new NotSupportedException($"Compare {a.GetType().Name} with {b.GetType().Name}"),
    };

    private static decimal Pow10(int scale)
    {
        decimal result = 1;
        for (var i = 0; i < scale; i++) result *= 10;
        return result;
    }
}
