using System.Runtime.CompilerServices;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace SolTechnology.DataLake.Arrow;

/// <summary>
/// Apache Arrow to your classes and back: <see cref="RecordBatch"/>, an <see cref="IArrowArrayStream"/> (what ADBC drivers
/// for Databricks, Snowflake, BigQuery and Flight SQL return), and Arrow IPC files and streams. Columns map to properties
/// by name with the same rules and type conversions as Parquet datasets.
/// </summary>
/// <example>
/// <code>
/// using var statement = connection.CreateStatement();
/// statement.SqlQuery = "SELECT id, amount, created_at FROM sales.orders";
/// var result = await statement.ExecuteQueryAsync();
/// await foreach (var order in ArrowConvert.DeserializeAsync&lt;Order&gt;(result.Stream!)) { }
/// </code>
/// </example>
public static class ArrowConvert
{
    /// <summary>The rows of one record batch.</summary>
    public static IReadOnlyList<T> Deserialize<T>(RecordBatch batch, ArrowOptions? options = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(batch);
        return BatchReader<T>.Read(batch, options?.ColumnNaming);
    }

    /// <summary>Every row of every batch in <paramref name="stream"/>. Each batch is disposed once its rows are out.</summary>
    public static async IAsyncEnumerable<T> DeserializeAsync<T>(IArrowArrayStream stream, ArrowOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(stream);
        while (await stream.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false) is { } batch)
        {
            IReadOnlyList<T> rows;
            using (batch) rows = BatchReader<T>.Read(batch, options?.ColumnNaming);
            foreach (var row in rows) yield return row;
        }
    }

    /// <summary>One record batch with all <paramref name="rows"/>.</summary>
    public static RecordBatch Serialize<T>(IEnumerable<T> rows, ArrowOptions? options = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(rows);
        return BatchWriter<T>.For(options?.ColumnNaming).Write(rows as IReadOnlyList<T> ?? rows.ToList());
    }

    /// <summary>The Arrow schema <see cref="Serialize{T}"/> writes for <typeparamref name="T"/>.</summary>
    public static Schema SchemaOf<T>(ArrowOptions? options = null) where T : class => BatchWriter<T>.For(options?.ColumnNaming).Schema;

    /// <summary>Rows of an Arrow IPC file (Feather v2, <c>.arrow</c>) or stream (<c>.arrows</c>); the format is detected.</summary>
    public static async IAsyncEnumerable<T> ReadIpcAsync<T>(Stream stream, ArrowOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(stream);
        using var reader = await OpenIpcAsync(stream, cancellationToken).ConfigureAwait(false);
        await foreach (var row in DeserializeAsync<T>(reader, options, cancellationToken).ConfigureAwait(false))
            yield return row;
    }

    /// <summary>Writes <paramref name="rows"/> as an Arrow IPC file or stream, <see cref="ArrowOptions.BatchSize"/> rows per batch.</summary>
    public static async Task WriteIpcAsync<T>(Stream stream, IAsyncEnumerable<T> rows, ArrowIpcFormat format = ArrowIpcFormat.File,
        ArrowOptions? options = null, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(rows);
        var batchSize = options?.BatchSize ?? ArrowOptions.DefaultBatchSize;
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(options), "BatchSize must be positive.");
        var writer = BatchWriter<T>.For(options?.ColumnNaming);

        using var ipc = format == ArrowIpcFormat.File
            ? new ArrowFileWriter(stream, writer.Schema, leaveOpen: true)
            : new ArrowStreamWriter(stream, writer.Schema, leaveOpen: true);
        await ipc.WriteStartAsync(cancellationToken).ConfigureAwait(false);
        var buffer = new List<T>(batchSize);
        await foreach (var row in rows.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            buffer.Add(row);
            if (buffer.Count < batchSize) continue;
            await WriteBatchAsync(ipc, writer, buffer, cancellationToken).ConfigureAwait(false);
            buffer.Clear();
        }
        if (buffer.Count > 0) await WriteBatchAsync(ipc, writer, buffer, cancellationToken).ConfigureAwait(false);
        await ipc.WriteEndAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="WriteIpcAsync{T}(Stream, IAsyncEnumerable{T}, ArrowIpcFormat, ArrowOptions?, CancellationToken)"/>
    public static Task WriteIpcAsync<T>(Stream stream, IEnumerable<T> rows, ArrowIpcFormat format = ArrowIpcFormat.File,
        ArrowOptions? options = null, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(rows);
        return WriteIpcAsync(stream, rows.ToAsyncEnumerable(), format, options, cancellationToken);
    }

    private static async Task WriteBatchAsync<T>(ArrowStreamWriter ipc, BatchWriter<T> writer, List<T> rows, CancellationToken ct) where T : class
    {
        using var batch = writer.Write(rows);
        await ipc.WriteRecordBatchAsync(batch, ct).ConfigureAwait(false);
    }

    private static readonly byte[] FileMagic = "ARROW1"u8.ToArray();

    internal static async Task<ArrowStreamReader> OpenIpcAsync(Stream stream, CancellationToken ct)
    {
        if (!stream.CanSeek)
        {
            // Without seeking the file footer can't be read, but the file format starts with a stream after 8 bytes.
            var head = new byte[8];
            var read = await stream.ReadAtLeastAsync(head, head.Length, throwOnEndOfStream: false, ct).ConfigureAwait(false);
            if (read >= 6 && head.AsSpan(0, 6).SequenceEqual(FileMagic)) return new ArrowStreamReader(stream, leaveOpen: true);
            return new ArrowStreamReader(new PrefixedStream(head.AsMemory(0, read), stream), leaveOpen: true);
        }

        var start = stream.Position;
        var magic = new byte[6];
        var count = await stream.ReadAtLeastAsync(magic, magic.Length, throwOnEndOfStream: false, ct).ConfigureAwait(false);
        stream.Position = start;
        return count == 6 && magic.AsSpan().SequenceEqual(FileMagic)
            ? new ArrowFileReader(stream, leaveOpen: true)
            : new ArrowStreamReader(stream, leaveOpen: true);
    }

    /// <summary>Bytes already read, then the rest of the stream.</summary>
    private sealed class PrefixedStream(ReadOnlyMemory<byte> prefix, Stream rest) : Stream
    {
        private ReadOnlyMemory<byte> _prefix = prefix;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            if (_prefix.IsEmpty) return rest.Read(buffer);
            var n = Math.Min(buffer.Length, _prefix.Length);
            _prefix.Span[..n].CopyTo(buffer);
            _prefix = _prefix[n..];
            return n;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_prefix.IsEmpty) return await rest.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            var n = Math.Min(buffer.Length, _prefix.Length);
            _prefix[..n].CopyTo(buffer);
            _prefix = _prefix[n..];
            return n;
        }

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}

/// <summary>Options for <see cref="ArrowConvert"/>.</summary>
public sealed class ArrowOptions
{
    internal const int DefaultBatchSize = 64 * 1024;

    /// <summary>How property names become column names without <c>[DataMember(Name = ...)]</c>, e.g. <see cref="JsonNamingPolicy.SnakeCaseLower"/>.</summary>
    public JsonNamingPolicy? ColumnNaming { get; init; }

    /// <summary>Rows per record batch when writing IPC, 65 536 by default.</summary>
    public int BatchSize { get; init; } = DefaultBatchSize;
}

/// <summary>The two Arrow IPC formats.</summary>
public enum ArrowIpcFormat
{
    /// <summary>Random-access file with a footer (Feather v2, <c>.arrow</c>).</summary>
    File,

    /// <summary>A stream of batches without a footer (<c>.arrows</c>), for pipes and sockets.</summary>
    Stream,
}
