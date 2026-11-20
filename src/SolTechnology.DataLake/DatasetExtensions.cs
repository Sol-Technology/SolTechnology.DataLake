using System.Linq.Expressions;

namespace SolTechnology.DataLake;

/// <summary>
/// Shorter calls on <see cref="IDatasetQuery{T}"/> and <see cref="IDataset{T}"/>. They live here, not on the interfaces,
/// so a mock only has to set up one method per operation.
/// </summary>
public static class DatasetExtensions
{
    /// <summary>The rows, streamed.</summary>
    public static IAsyncEnumerable<T> ReadAsync<T>(this IDatasetQuery<T> query) where T : class =>
        query.ReadAsync(CancellationToken.None);

    /// <summary>The rows, streamed, adding what was skipped and read to <paramref name="metrics"/>.</summary>
    /// <remarks>A query that does not measure (a mock) reads without filling <paramref name="metrics"/>.</remarks>
    public static IAsyncEnumerable<T> ReadAsync<T>(this IDatasetQuery<T> query, ScanMetrics metrics, CancellationToken cancellationToken = default) where T : class
    {
        ArgumentNullException.ThrowIfNull(metrics);
        return query is IMeasuredQuery<T> measured ? measured.ReadAsync(metrics, cancellationToken) : query.ReadAsync(cancellationToken);
    }

    /// <summary>Number of rows.</summary>
    public static Task<long> CountAsync<T>(this IDatasetQuery<T> query) where T : class => query.CountAsync(CancellationToken.None);

    /// <inheritdoc cref="IDataset{T}.AppendAsync"/>
    public static Task<LakeWriteResult> AppendAsync<T>(this IDataset<T> dataset, IAsyncEnumerable<T> rows) where T : class =>
        dataset.AppendAsync(rows, CancellationToken.None);

    /// <inheritdoc cref="IDataset{T}.AppendAsync"/>
    public static Task<LakeWriteResult> AppendAsync<T>(this IDataset<T> dataset, IEnumerable<T> rows, CancellationToken cancellationToken = default) where T : class =>
        dataset.AppendAsync(Stream(rows), cancellationToken);

    /// <inheritdoc cref="IDataset{T}.OverwriteAsync(IAsyncEnumerable{T}, CancellationToken)"/>
    public static Task<LakeWriteResult> OverwriteAsync<T>(this IDataset<T> dataset, IAsyncEnumerable<T> rows) where T : class =>
        dataset.OverwriteAsync(rows, CancellationToken.None);

    /// <inheritdoc cref="IDataset{T}.OverwriteAsync(IAsyncEnumerable{T}, CancellationToken)"/>
    public static Task<LakeWriteResult> OverwriteAsync<T>(this IDataset<T> dataset, IEnumerable<T> rows, CancellationToken cancellationToken = default) where T : class =>
        dataset.OverwriteAsync(Stream(rows), cancellationToken);

    /// <inheritdoc cref="IDataset{T}.OverwriteAsync(IAsyncEnumerable{T}, Expression{Func{T, bool}}, CancellationToken)"/>
    public static Task<LakeWriteResult> OverwriteAsync<T>(this IDataset<T> dataset, IAsyncEnumerable<T> rows, Expression<Func<T, bool>> where) where T : class =>
        dataset.OverwriteAsync(rows, where, CancellationToken.None);

    /// <inheritdoc cref="IDataset{T}.OverwriteAsync(IAsyncEnumerable{T}, Expression{Func{T, bool}}, CancellationToken)"/>
    public static Task<LakeWriteResult> OverwriteAsync<T>(this IDataset<T> dataset, IEnumerable<T> rows, Expression<Func<T, bool>> where,
        CancellationToken cancellationToken = default) where T : class =>
        dataset.OverwriteAsync(Stream(rows), where, cancellationToken);

    /// <inheritdoc cref="IDataset{T}.ReplacePartitionsAsync"/>
    public static Task<LakeWriteResult> ReplacePartitionsAsync<T>(this IDataset<T> dataset, IAsyncEnumerable<T> rows) where T : class =>
        dataset.ReplacePartitionsAsync(rows, CancellationToken.None);

    /// <inheritdoc cref="IDataset{T}.ReplacePartitionsAsync"/>
    public static Task<LakeWriteResult> ReplacePartitionsAsync<T>(this IDataset<T> dataset, IEnumerable<T> rows, CancellationToken cancellationToken = default) where T : class =>
        dataset.ReplacePartitionsAsync(Stream(rows), cancellationToken);

    /// <inheritdoc cref="IDataset{T}.DeleteAsync"/>
    public static Task<LakeWriteResult> DeleteAsync<T>(this IDataset<T> dataset, Expression<Func<T, bool>> where) where T : class =>
        dataset.DeleteAsync(where, CancellationToken.None);

    private static IAsyncEnumerable<T> Stream<T>(IEnumerable<T> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);
        return rows.ToAsyncEnumerable();
    }
}

/// <summary>A query that can fill <see cref="ScanMetrics"/>; the providers' queries implement it.</summary>
internal interface IMeasuredQuery<T> where T : class
{
    IAsyncEnumerable<T> ReadAsync(ScanMetrics metrics, CancellationToken cancellationToken);
}
