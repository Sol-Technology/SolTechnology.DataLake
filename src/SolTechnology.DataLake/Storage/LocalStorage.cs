using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace SolTechnology.DataLake.Storage;

/// <summary>
/// Files on a local or mounted disk. Locations are paths or <c>file://</c> URIs. Writes go to a hidden temporary file
/// next to the target and are renamed into place, so readers never see a half-written file.
/// </summary>
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
        if (File.Exists(root))
        {
            yield return new LakeFile(root, new FileInfo(root).Length, Path.GetFileName(root));
            yield break;
        }
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

    /// <inheritdoc />
    public async ValueTask<LakeFile> WriteAsync(string location, Stream content, WriteMode mode = WriteMode.Overwrite, CancellationToken cancellationToken = default)
    {
        var path = ToPath(location);
        var directory = Path.GetDirectoryName(path)!;
        Directory.CreateDirectory(directory);
        // Hidden name next to the target: datasets skip it, and the rename below stays on one file system.
        var temp = Path.Combine(directory, $".{Path.GetFileName(path)}.{Guid.NewGuid():N}.tmp");
        try
        {
            long length;
            await using (var file = new FileStream(temp, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1 << 16, FileOptions.Asynchronous))
            {
                await content.CopyToAsync(file, cancellationToken).ConfigureAwait(false);
                length = file.Length;
                file.Flush(flushToDisk: true);
            }

            if (mode == WriteMode.CreateNew) MoveIfAbsent(temp, path, location);
            else File.Move(temp, path, overwrite: true);
            return new LakeFile(path, length, Path.GetFileName(path));
        }
        finally
        {
            File.Delete(temp);
        }
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(string location, CancellationToken cancellationToken = default)
    {
        try
        {
            File.Delete(ToPath(location));
        }
        catch (DirectoryNotFoundException)
        {
        }
        return ValueTask.CompletedTask;
    }

    // File.Move without overwrite checks and renames in two steps on Unix, so two writers can both "win".
    // link() fails with EEXIST in one step; Windows MoveFileEx without REPLACE_EXISTING is already atomic.
    private static void MoveIfAbsent(string temp, string path, string location)
    {
        if (!OperatingSystem.IsWindows())
        {
            if (link(temp, path) == 0) return;
            if (Marshal.GetLastPInvokeError() == EEXIST) throw new LakeFileExistsException(location);
            // No hard links on this file system (FAT, some network shares): fall back to the two-step move.
        }

        try
        {
            File.Move(temp, path, overwrite: false);
        }
        catch (IOException) when (File.Exists(path))
        {
            throw new LakeFileExistsException(location);
        }
    }

    private const int EEXIST = 17; // the same on Linux and macOS

    [DllImport("libc", SetLastError = true)]
    private static extern int link(string existing, string created);

    internal static string ToPath(string location) =>
        location.StartsWith("file://", StringComparison.OrdinalIgnoreCase) ? new Uri(location).LocalPath : Path.GetFullPath(location);
}
