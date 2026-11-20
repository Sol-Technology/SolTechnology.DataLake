using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace SolTechnology.DataLake.Storage;

/// <summary>
/// Files in memory, for tests: <c>UseInMemory()</c> runs the Parquet provider on it. Locations start with
/// <c>memory://</c>. Thread-safe; a written file is never changed in place, so readers in progress keep their bytes.
/// </summary>
public sealed class MemoryStorage : ILakeStorage
{
    internal const string Root = "memory://lake";

    private readonly ConcurrentDictionary<string, byte[]> _files = new(StringComparer.Ordinal);

    /// <summary>Locations of every file, sorted.</summary>
    public IReadOnlyList<string> Locations => _files.Keys.Order(StringComparer.Ordinal).ToList();

    /// <inheritdoc />
    public bool CanHandle(string location) => location.StartsWith("memory://", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    /// <remarks>A location with nothing under it lists nothing, like S3 and Azure.</remarks>
    public async IAsyncEnumerable<LakeFile> ListAsync(string location, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var key = Normalize(location);
        var prefix = key + "/";
        var found = false;
        foreach (var (path, bytes) in _files.OrderBy(f => f.Key, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!path.StartsWith(prefix, StringComparison.Ordinal)) continue;
            found = true;
            yield return new LakeFile(path, bytes.Length, path[prefix.Length..]);
        }
        if (!found && _files.TryGetValue(key, out var single))
            yield return new LakeFile(key, single.Length, key[(key.LastIndexOf('/') + 1)..]);
        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask<int> ReadAsync(LakeFile file, long offset, Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (!_files.TryGetValue(Normalize(file.Location), out var bytes)) throw new FileNotFoundException($"'{file.Location}' does not exist.", file.Location);
        if (offset >= bytes.Length) return ValueTask.FromResult(0);
        var count = (int)Math.Min(buffer.Length, bytes.Length - offset);
        bytes.AsMemory((int)offset, count).CopyTo(buffer);
        return ValueTask.FromResult(count);
    }

    /// <inheritdoc />
    public async ValueTask<LakeFile> WriteAsync(string location, Stream content, WriteMode mode = WriteMode.Overwrite, CancellationToken cancellationToken = default)
    {
        var key = Normalize(location);
        using var copy = new MemoryStream();
        await content.CopyToAsync(copy, cancellationToken).ConfigureAwait(false);
        var bytes = copy.ToArray();
        if (mode == WriteMode.CreateNew)
        {
            if (!_files.TryAdd(key, bytes)) throw new LakeFileExistsException(location);
        }
        else
        {
            _files[key] = bytes;
        }
        return new LakeFile(key, bytes.Length, key[(key.LastIndexOf('/') + 1)..]);
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(string location, CancellationToken cancellationToken = default)
    {
        _files.TryRemove(Normalize(location), out _);
        return ValueTask.CompletedTask;
    }

    private string Normalize(string location)
    {
        if (!CanHandle(location)) throw new ArgumentException($"'{location}' is not a memory:// location.", nameof(location));
        return "memory://" + location["memory://".Length..].Trim('/');
    }
}
