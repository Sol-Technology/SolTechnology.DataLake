using System.Buffers.Binary;

namespace SolTechnology.DataLake.Writing;

/// <summary>
/// Just enough of the Thrift compact protocol to read a Parquet footer into a generic tree, change a few fields and
/// write it back. Field ids and types are kept as read, so fields this code knows nothing about survive unchanged.
/// </summary>
internal static class ThriftCompact
{
    public const byte True = 1, False = 2, Byte = 3, I16 = 4, I32 = 5, I64 = 6, Double = 7, Binary = 8, List = 9, Set = 10, Map = 11, Struct = 12;

    public sealed class ThriftStruct
    {
        public List<ThriftField> Fields { get; } = [];

        public ThriftField? Find(short id) => Fields.FirstOrDefault(f => f.Id == id);

        public void Set(short id, byte type, object value)
        {
            var index = Fields.FindIndex(f => f.Id == id);
            var field = new ThriftField(id, type, value);
            if (index >= 0) Fields[index] = field;
            else Fields.Add(field);
            Fields.Sort((a, b) => a.Id.CompareTo(b.Id));
        }
    }

    // Booleans are stored with type True and a bool value.
    public sealed record ThriftField(short Id, byte Type, object Value);

    public sealed record ThriftList(byte ElementType, List<object> Items);

    public sealed record ThriftMap(byte KeyType, byte ValueType, List<(object Key, object Value)> Entries);

    public static ThriftStruct Read(ReadOnlySpan<byte> bytes) => Read(bytes, out _);

    /// <summary>Reads one struct from the start of <paramref name="bytes"/>; <paramref name="length"/> is how many bytes it took.</summary>
    public static ThriftStruct Read(ReadOnlySpan<byte> bytes, out int length)
    {
        var reader = new Reader(bytes);
        var result = reader.ReadStruct();
        length = reader.Position;
        return result;
    }

    public static byte[] Write(ThriftStruct value)
    {
        var writer = new Writer();
        writer.WriteStruct(value);
        return writer.ToArray();
    }

    private ref struct Reader(ReadOnlySpan<byte> bytes)
    {
        private readonly ReadOnlySpan<byte> _bytes = bytes;
        private int _position;

        public readonly int Position => _position;

        public ThriftStruct ReadStruct()
        {
            var result = new ThriftStruct();
            short last = 0;
            while (true)
            {
                var header = _bytes[_position++];
                if (header == 0) return result;
                var type = (byte)(header & 0x0F);
                var delta = header >> 4;
                var id = delta == 0 ? (short)ZigZag(ReadVarint()) : (short)(last + delta);
                last = id;
                // A boolean field carries its value in the type nibble.
                result.Fields.Add(type is True or False
                    ? new ThriftField(id, True, type == True)
                    : new ThriftField(id, type, ReadValue(type)));
            }
        }

        private object ReadValue(byte type) => type switch
        {
            True or False => _bytes[_position++] == True,
            Byte => (long)(sbyte)_bytes[_position++],
            I16 or I32 or I64 => ZigZag(ReadVarint()),
            Double => ReadDouble(),
            Binary => ReadBinary(),
            List or Set => ReadList(),
            Map => ReadMap(),
            Struct => ReadStruct(),
            _ => throw new InvalidDataException($"Unknown Thrift compact type {type} in the Parquet footer."),
        };

        private double ReadDouble()
        {
            var value = BinaryPrimitives.ReadDoubleLittleEndian(_bytes[_position..]);
            _position += 8;
            return value;
        }

        private byte[] ReadBinary()
        {
            var length = (int)ReadVarint();
            var value = _bytes.Slice(_position, length).ToArray();
            _position += length;
            return value;
        }

        private ThriftList ReadList()
        {
            var header = _bytes[_position++];
            var size = header >> 4;
            if (size == 15) size = (int)ReadVarint();
            var elementType = (byte)(header & 0x0F);
            var items = new List<object>(size);
            for (var i = 0; i < size; i++) items.Add(ReadValue(elementType));
            return new ThriftList(elementType, items);
        }

        private ThriftMap ReadMap()
        {
            var size = (int)ReadVarint();
            if (size == 0) return new ThriftMap(0, 0, []);
            var types = _bytes[_position++];
            var keyType = (byte)(types >> 4);
            var valueType = (byte)(types & 0x0F);
            var entries = new List<(object, object)>(size);
            for (var i = 0; i < size; i++) entries.Add((ReadValue(keyType), ReadValue(valueType)));
            return new ThriftMap(keyType, valueType, entries);
        }

        private ulong ReadVarint()
        {
            ulong result = 0;
            var shift = 0;
            while (true)
            {
                var b = _bytes[_position++];
                result |= (ulong)(b & 0x7F) << shift;
                if ((b & 0x80) == 0) return result;
                shift += 7;
            }
        }

        private static long ZigZag(ulong n) => (long)(n >> 1) ^ -(long)(n & 1);
    }

    private sealed class Writer
    {
        private readonly MemoryStream _out = new();

        public byte[] ToArray() => _out.ToArray();

        public void WriteStruct(ThriftStruct value)
        {
            short last = 0;
            foreach (var field in value.Fields)
            {
                var type = field.Type == True ? (bool)field.Value ? True : False : field.Type;
                var delta = field.Id - last;
                if (delta is > 0 and <= 15)
                {
                    _out.WriteByte((byte)((delta << 4) | type));
                }
                else
                {
                    _out.WriteByte(type);
                    WriteVarint(ZigZag(field.Id));
                }
                last = field.Id;
                if (field.Type != True) WriteValue(field.Type, field.Value);
            }
            _out.WriteByte(0);
        }

        private void WriteValue(byte type, object value)
        {
            switch (type)
            {
                case True or False:
                    _out.WriteByte((bool)value ? True : False);
                    break;
                case Byte:
                    _out.WriteByte((byte)(sbyte)(long)value);
                    break;
                case I16 or I32 or I64:
                    WriteVarint(ZigZag((long)value));
                    break;
                case Double:
                    Span<byte> bytes = stackalloc byte[8];
                    BinaryPrimitives.WriteDoubleLittleEndian(bytes, (double)value);
                    _out.Write(bytes);
                    break;
                case Binary:
                    var binary = (byte[])value;
                    WriteVarint((ulong)binary.Length);
                    _out.Write(binary);
                    break;
                case List or Set:
                    var list = (ThriftList)value;
                    if (list.Items.Count < 15)
                    {
                        _out.WriteByte((byte)((list.Items.Count << 4) | list.ElementType));
                    }
                    else
                    {
                        _out.WriteByte((byte)(0xF0 | list.ElementType));
                        WriteVarint((ulong)list.Items.Count);
                    }
                    foreach (var item in list.Items) WriteValue(list.ElementType, item);
                    break;
                case Map:
                    var map = (ThriftMap)value;
                    WriteVarint((ulong)map.Entries.Count);
                    if (map.Entries.Count == 0) break;
                    _out.WriteByte((byte)((map.KeyType << 4) | map.ValueType));
                    foreach (var (k, v) in map.Entries)
                    {
                        WriteValue(map.KeyType, k);
                        WriteValue(map.ValueType, v);
                    }
                    break;
                case Struct:
                    WriteStruct((ThriftStruct)value);
                    break;
                default:
                    throw new InvalidOperationException($"Unknown Thrift compact type {type}.");
            }
        }

        private void WriteVarint(ulong n)
        {
            while (n >= 0x80)
            {
                _out.WriteByte((byte)(n | 0x80));
                n >>= 7;
            }
            _out.WriteByte((byte)n);
        }

        private static ulong ZigZag(long n) => (ulong)((n << 1) ^ (n >> 63));
    }
}
