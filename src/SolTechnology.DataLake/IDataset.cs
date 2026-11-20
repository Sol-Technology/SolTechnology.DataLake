using System.Linq.Expressions;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake;

/// <summary>
/// A set of datasets with one storage technology behind them. <see cref="LakeContext"/> implements it; inject your own
/// context or an interface derived from this one, and mock it in unit tests.
/// </summary>
public interface IDataLake
{
    /// <summary>
    /// The dataset called <paramref name="name"/>. Datasets declared in the model can be read and written; others can
    /// only be read, so a typo never creates or overwrites data.
    /// </summary>
    IDataset<T> Dataset<T>(string name) where T : class;
}

/// <summary>Rows of a dataset, maybe filtered. Reading only: writes go to the <see cref="IDataset{T}"/> itself.</summary>
public interface IDatasetQuery<T> where T : class
{
    /// <summary>
    /// Keeps rows matching <paramref name="predicate"/>, combined with earlier filters by <c>&amp;&amp;</c>. Comparisons of
    /// properties with values also skip whole files (partitions) and row groups (statistics) before reading them.
    /// </summary>
    IDatasetQuery<T> Where(Expression<Func<T, bool>> predicate);

    /// <summary>The rows, streamed. Pass <see cref="ScanMetrics"/> through <c>ReadAsync(metrics)</c> to see what was skipped.</summary>
    IAsyncEnumerable<T> ReadAsync(CancellationToken cancellationToken);

    /// <summary>Number of rows. What it costs depends on the provider: see its documentation.</summary>
    Task<long> CountAsync(CancellationToken cancellationToken);
}

/// <summary>
/// A dataset: rows of <typeparamref name="T"/> stored together, as files in a folder or as a table (Iceberg). The same
/// calls work on every provider; what a provider can't do throws <see cref="NotSupportedException"/>.
/// </summary>
public interface IDataset<T> : IDatasetQuery<T> where T : class
{
    /// <summary>The name the dataset was asked for.</summary>
    string Name { get; }

    /// <summary>Adds <paramref name="rows"/>.</summary>
    Task<LakeWriteResult> AppendAsync(IAsyncEnumerable<T> rows, CancellationToken cancellationToken);

    /// <summary>Replaces the whole dataset with <paramref name="rows"/>. No rows leaves it empty.</summary>
    Task<LakeWriteResult> OverwriteAsync(IAsyncEnumerable<T> rows, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes what <paramref name="where"/> matches and adds <paramref name="rows"/>, each of which has to match it too
    /// (like Delta's <c>replaceWhere</c>).
    /// </summary>
    Task<LakeWriteResult> OverwriteAsync(IAsyncEnumerable<T> rows, Expression<Func<T, bool>> where, CancellationToken cancellationToken);

    /// <summary>
    /// Replaces only the partitions <paramref name="rows"/> fall into; other partitions stay (Spark's dynamic partition
    /// overwrite). No rows changes nothing. A dataset without partitions throws: use <see cref="OverwriteAsync(IAsyncEnumerable{T}, CancellationToken)"/>.
    /// </summary>
    Task<LakeWriteResult> ReplacePartitionsAsync(IAsyncEnumerable<T> rows, CancellationToken cancellationToken);

    /// <summary>Deletes what <paramref name="where"/> matches.</summary>
    Task<LakeWriteResult> DeleteAsync(Expression<Func<T, bool>> where, CancellationToken cancellationToken);
}

/// <summary>What a write did.</summary>
public sealed class LakeWriteResult
{
    /// <summary>Files written, in the order they were finished.</summary>
    public IReadOnlyList<LakeFile> Written { get; init; } = [];

    /// <summary>Files removed by an overwrite, a partition replacement or a delete.</summary>
    public IReadOnlyList<LakeFile> Removed { get; init; } = [];

    /// <summary>Rows written.</summary>
    public long Rows { get; init; }
}
