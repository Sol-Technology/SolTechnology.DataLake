using SolTechnology.DataLake.Formats;

namespace SolTechnology.DataLake;

/// <summary>
/// Settings of a folder of files (<see cref="LakeContextOptionsBuilder.UseFiles"/>, <see cref="LakeContextOptionsBuilder.UseInMemory"/>,
/// <see cref="DataLakeOptions.Files"/>): how Parquet files are written, and which formats are read. Parquet is built in;
/// <c>Avro()</c> comes with SolTechnology.DataLake.Avro and <c>Arrow()</c> with SolTechnology.DataLake.Arrow.
/// </summary>
/// <example>
/// <code>
/// o.UseFiles("s3://shop/", s3, f => f.Parquet(p => p.Compression(ParquetCompression.Zstd)).Avro());
/// </code>
/// </example>
public sealed class FilesOptions
{
    private readonly List<ILakeFileFormat> _formats = [ParquetFileFormat.Instance];

    internal FilesOptions() { }

    internal ParquetWriteOptions ParquetSettings { get; } = new();

    /// <summary>How Parquet files are written. Writes are always Parquet.</summary>
    public FilesOptions Parquet(Action<ParquetWriteOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(ParquetSettings);
        return this;
    }

    /// <summary>Reads files of <paramref name="format"/> too. Adding a format twice changes nothing.</summary>
    internal FilesOptions Add(ILakeFileFormat format)
    {
        ArgumentNullException.ThrowIfNull(format);
        if (_formats.All(f => f.Name != format.Name)) _formats.Add(format);
        return this;
    }

    /// <param name="howToAdd">For the message about a format whose package is missing: the method, e.g. <c>Avro()</c>, into where to call it.</param>
    internal FileFormats Formats(Func<string, string> howToAdd) => new(_formats, howToAdd);

    internal static FilesOptions From(Action<FilesOptions>? configure)
    {
        var options = new FilesOptions();
        configure?.Invoke(options);
        return options;
    }
}

/// <summary>How Parquet files are written, set through <see cref="FilesOptions.Parquet"/>.</summary>
public sealed class ParquetWriteOptions
{
    internal ParquetCompression CompressionValue { get; private set; } = ParquetCompression.Snappy;
    internal int RowGroupSizeValue { get; private set; } = 128 * 1024;
    internal long TargetFileSizeValue { get; private set; } = 512L << 20;

    /// <summary>Compression of the column pages. Snappy by default, like Spark and pyarrow.</summary>
    public ParquetWriteOptions Compression(ParquetCompression compression)
    {
        CompressionValue = compression;
        return this;
    }

    /// <summary>Rows per row group, 131 072 by default. Each partition being written keeps this many rows in memory.</summary>
    public ParquetWriteOptions RowGroupSize(int rows)
    {
        RowGroupSizeValue = rows > 0 ? rows : throw new ArgumentOutOfRangeException(nameof(rows), "Must be positive.");
        return this;
    }

    /// <summary>A file is closed and a new one started after the row group that crosses this size, 512 MiB by default.</summary>
    public ParquetWriteOptions TargetFileSize(long bytes)
    {
        TargetFileSizeValue = bytes > 0 ? bytes : throw new ArgumentOutOfRangeException(nameof(bytes), "Must be positive.");
        return this;
    }
}

/// <summary>Compression of Parquet column pages.</summary>
public enum ParquetCompression
{
    /// <summary>Fast, moderate ratio. The default of Spark and pyarrow.</summary>
    Snappy,

    /// <summary>Better ratio at similar speed; read by every current engine.</summary>
    Zstd,

    /// <summary>For old readers that know nothing newer.</summary>
    Gzip,

    /// <summary>No compression.</summary>
    None,
}
