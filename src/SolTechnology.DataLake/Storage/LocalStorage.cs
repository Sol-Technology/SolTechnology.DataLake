using System.Runtime.CompilerServices;

namespace SolTechnology.DataLake.Storage;

/// <summary>Files on a local or mounted disk. Locations are paths or <c>file://</c> URIs.</summary>
public sealed class LocalStorage : ILakeStorage
{
    /// <summary>The shared instance; the class has no state.</summary>
    public static LocalStorage Instance { get; } = new();

    /// <inheritdoc />
    public bool CanHandle(string location) =>
        location.StartsWith("file://", StringComparison.OrdinalIgnoreCase) || !location.Contains("://", StringComparison.Ordinal);

    /// <inheritdoc />
    public async IAsyncEnumerable<LakeFile> ListAsync(string location, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var root = ToPath(location);
        if (!Directory.Exists(root)) throw new DirectoryNotFoundException($"Dataset location '{location}' does not exist.");

        foreach (var path in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new LakeFile(path, new FileInfo(path).Length, Path.GetRelativePath(root, path).Replace('\\', '/'));
        }
        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<int> ReadAsync(LakeFile file, long offset, Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        using var handle = File.OpenHandle(ToPath(file.Location), FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.Asynchronous);
        var total = 0;
        while (total < buffer.Length)
        {
            var read = await RandomAccess.ReadAsync(handle, buffer[total..], offset + total, cancellationToken).ConfigureAwait(false);
            if (read == 0) break;
            total += read;
        }
        return total;
    }

    internal static string ToPath(string location) =>
        location.StartsWith("file://", StringComparison.OrdinalIgnoreCase) ? new Uri(location).LocalPath : Path.GetFullPath(location);
}
