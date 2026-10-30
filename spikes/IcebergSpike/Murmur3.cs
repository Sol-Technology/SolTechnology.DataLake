using System.Buffers.Binary;
using System.Numerics;
using System.Text;

namespace IcebergSpike;

// Appendix B: 32-bit Murmur3, x86 variant, seed 0.
public static class Murmur3
{
    public static int Hash(ReadOnlySpan<byte> data)
    {
        const uint c1 = 0xcc9e2d51, c2 = 0x1b873593;
        uint h = 0;
        var blocks = data.Length / 4;
        for (var i = 0; i < blocks; i++)
        {
            var k = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(i * 4, 4));
            k *= c1; k = BitOperations.RotateLeft(k, 15); k *= c2;
            h ^= k; h = BitOperations.RotateLeft(h, 13); h = h * 5 + 0xe6546b64;
        }

        uint tail = 0;
        var rest = data[(blocks * 4)..];
        switch (rest.Length)
        {
            case 3: tail ^= (uint)rest[2] << 16; goto case 2;
            case 2: tail ^= (uint)rest[1] << 8; goto case 1;
            case 1:
                tail ^= rest[0];
                tail *= c1; tail = BitOperations.RotateLeft(tail, 15); tail *= c2; h ^= tail;
                break;
        }

        h ^= (uint)data.Length;
        h ^= h >> 16; h *= 0x85ebca6b; h ^= h >> 13; h *= 0xc2b2ae35; h ^= h >> 16;
        return (int)h;
    }

    public static int HashLong(long value)
    {
        Span<byte> bytes = stackalloc byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return Hash(bytes);
    }

    public static int HashValue(object value) => value switch
    {
        int i => HashLong(i),
        long l => HashLong(l),
        DateOnly d => HashLong(d.DayNumber - DateOnly.FromDateTime(DateTime.UnixEpoch).DayNumber),
        DateTimeOffset t => HashLong((t.UtcTicks - DateTime.UnixEpoch.Ticks) / 10),
        DateTime t => HashLong((t.Ticks - DateTime.UnixEpoch.Ticks) / 10),
        TimeOnly t => HashLong(t.Ticks / 10),
        string s => Hash(Encoding.UTF8.GetBytes(s)),
        Guid g => Hash(g.ToByteArray(bigEndian: true)),
        byte[] b => Hash(b),
        decimal m => Hash(MinBigEndian(m)),
        _ => throw new NotSupportedException($"Hash of {value.GetType().Name}"),
    };

    private static byte[] MinBigEndian(decimal value)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        var magnitude = ((BigInteger)(uint)bits[2] << 64) | ((BigInteger)(uint)bits[1] << 32) | (uint)bits[0];
        return (value < 0 ? -magnitude : magnitude).ToByteArray(isUnsigned: false, isBigEndian: true);
    }
}

public static class Transforms
{
    public static object? Apply(string transform, object? value)
    {
        if (value is null) return null;
        if (transform == "identity") return value;
        if (transform == "day") return value switch
        {
            DateTimeOffset t => DateOnly.FromDateTime(t.UtcDateTime),
            DateTime t => DateOnly.FromDateTime(t),
            DateOnly d => d,
            _ => throw new NotSupportedException($"day({value.GetType().Name})"),
        };
        if (transform.StartsWith("bucket[", StringComparison.Ordinal))
        {
            var n = int.Parse(transform["bucket[".Length..^1]);
            return (long)((Murmur3.HashValue(value) & int.MaxValue) % n);
        }
        throw new NotSupportedException($"Transform {transform}");
    }
}
