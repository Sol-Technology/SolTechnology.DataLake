using System.Dynamic;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.Formats;

/// <summary>
/// A file format inside a folder dataset (<c>UseFiles</c>, <c>Dataset.Open</c>): which files it reads, how it reads them
/// into a class, and how tools read them without one. Parquet is built in; other formats come as packages and are added
/// with an extension method on <see cref="FilesOptions"/>.
/// </summary>
internal interface ILakeFileFormat
{
    /// <summary>Lower case, for <c>datalake ls</c> and messages: <c>parquet</c>, <c>avro</c>, <c>arrow</c>.</summary>
    string Name { get; }

    /// <summary>File name endings with the dot, matched case-insensitively.</summary>
    IReadOnlyList<string> Extensions { get; }

    /// <summary>Rows of one file into <typeparamref name="T"/>, with the mapping, partitions, filter and metrics of <paramref name="scan"/>.</summary>
    IAsyncEnumerable<T> ReadAsync<T>(Stream stream, string location, FileScan<T> scan, CancellationToken ct) where T : class;

    /// <summary>Rows of one file as dictionaries: column name to value, structs as dictionaries, lists as lists.</summary>
    IAsyncEnumerable<Dictionary<string, object?>> ReadUntypedAsync(Stream stream, string location, CancellationToken ct);

    /// <summary>Rows in one file. Reads every row unless the format knows better.</summary>
    async Task<long> CountAsync(Stream stream, string location, CancellationToken ct)
    {
        long count = 0;
        await foreach (var _ in ReadUntypedAsync(stream, location, ct).ConfigureAwait(false)) count++;
        return count;
    }

    /// <summary>The schema of one file as text lines, for <c>datalake schema</c>.</summary>
    Task<IReadOnlyList<string>> DescribeSchemaAsync(Stream stream, string location, CancellationToken ct);
}

/// <summary>
/// The formats one dataset reads, found by file extension. Files of a format that comes in a separate package fail when
/// the package is not added, instead of being left out; other unknown files are left out.
/// </summary>
internal sealed class FileFormats
{
    private static readonly (string Extension, string Kind, string Package, string Method)[] PlugIns =
    [
        (".avro", "an Avro file", "SolTechnology.DataLake.Avro", "Avro()"),
        (".arrow", "an Arrow IPC file", "SolTechnology.DataLake.Arrow", "Arrow()"),
        (".feather", "an Arrow IPC file", "SolTechnology.DataLake.Arrow", "Arrow()"),
        (".ipc", "an Arrow IPC file", "SolTechnology.DataLake.Arrow", "Arrow()"),
        (".arrows", "an Arrow IPC file", "SolTechnology.DataLake.Arrow", "Arrow()"),
    ];

    private readonly ILakeFileFormat[] _formats;
    private readonly Func<string, string> _howToAdd;

    /// <param name="formats">The formats, first match wins.</param>
    /// <param name="howToAdd">Where to call the method that adds a format, e.g. <c>Avro()</c> into "call f.Avro() in UseFiles".</param>
    public FileFormats(IEnumerable<ILakeFileFormat> formats, Func<string, string> howToAdd)
    {
        _formats = formats.ToArray();
        _howToAdd = howToAdd;
    }

    public IReadOnlyList<ILakeFileFormat> All => _formats;

    /// <summary>The format of a data file, or null for a file no format knows.</summary>
    /// <exception cref="NotSupportedException">The file belongs to a format whose package is not added.</exception>
    public ILakeFileFormat? Of(LakeFile file)
    {
        foreach (var format in _formats)
            foreach (var extension in format.Extensions)
                if (file.RelativePath.EndsWith(extension, StringComparison.OrdinalIgnoreCase)) return format;

        foreach (var (extension, kind, package, method) in PlugIns)
            if (file.RelativePath.EndsWith(extension, StringComparison.OrdinalIgnoreCase))
                throw new NotSupportedException($"{file.Location} is {kind}. Add the {package} package and {_howToAdd(method)}.");
        return null;
    }

    /// <summary>Values from formats that decode generically (ExpandoObject records, key-value lists) as plain dictionaries and lists.</summary>
    public static object? Plain(object? value) => value switch
    {
        ExpandoObject record => ((IDictionary<string, object?>)record).ToDictionary(kv => kv.Key, kv => Plain(kv.Value), StringComparer.OrdinalIgnoreCase),
        IDictionary<string, object?> map => map.ToDictionary(kv => kv.Key, kv => Plain(kv.Value)),
        List<KeyValuePair<object?, object?>> pairs => pairs.ToDictionary(kv => kv.Key!, kv => Plain(kv.Value)),
        System.Collections.IList list when value is not byte[] => list.Cast<object?>().Select(Plain).ToList(),
        _ => value,
    };
}
