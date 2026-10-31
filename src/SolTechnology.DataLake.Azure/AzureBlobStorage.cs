using System.Runtime.CompilerServices;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.Azure;

/// <summary>
/// Azure Blob Storage, ADLS Gen2 and Microsoft Fabric OneLake, through the Blob API (which ADLS Gen2 and OneLake
/// also serve). Files are read with ranged downloads: the Parquet footer and the column chunks a query needs.
/// </summary>
/// <remarks>
/// Locations: <c>abfss://container@account.dfs.core.windows.net/path</c>, <c>wasbs://container@account.blob.core.windows.net/path</c>,
/// <c>https://account.blob.core.windows.net/container/path</c>, OneLake <c>abfss://workspace@onelake.dfs.fabric.microsoft.com/item/Files/path</c>,
/// and <c>az://container/path</c> for the account the client points to. The <see cref="BlobServiceClient"/> must point to
/// the same account: <c>https://account.blob.core.windows.net</c>, or <c>https://onelake.blob.fabric.microsoft.com</c> for OneLake.
/// </remarks>
/// <example>
/// <code>
/// var blobs = new BlobServiceClient(new Uri("https://shop.blob.core.windows.net"), new DefaultAzureCredential());
/// var orders = DataLake.Open&lt;Order&gt;("abfss://lake@shop.dfs.core.windows.net/orders/", new DataLakeOptions { Storage = new AzureBlobStorage(blobs) });
/// </code>
/// </example>
public sealed class AzureBlobStorage(BlobServiceClient service) : ILakeStorage
{
    private static readonly string[] Schemes = ["abfss", "abfs", "wasbs", "wasb", "az"];
    private static readonly string[] HttpsHosts = [".blob.core.windows.net", ".dfs.core.windows.net", ".fabric.microsoft.com"];

    /// <inheritdoc />
    public bool CanHandle(string location)
    {
        var scheme = Scheme(location);
        return Schemes.Contains(scheme, StringComparer.OrdinalIgnoreCase)
               || scheme.Equals("https", StringComparison.OrdinalIgnoreCase) && HttpsHosts.Any(h => Host(location).EndsWith(h, StringComparison.OrdinalIgnoreCase));
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<LakeFile> ListAsync(string location, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var (container, prefix) = Parse(location);
        // "orders" must not match "orders_old/": list the folder, not a name prefix.
        if (prefix.Length > 0 && !prefix.EndsWith('/')) prefix += "/";

        var client = service.GetBlobContainerClient(container);
        await foreach (var blob in client.GetBlobsAsync(BlobTraits.Metadata, BlobStates.None, prefix, cancellationToken).ConfigureAwait(false))
        {
            // ADLS Gen2 and OneLake list directories as empty blobs marked hdi_isfolder.
            if (blob.Metadata?.TryGetValue("hdi_isfolder", out var folder) == true && folder == "true") continue;
            if (blob.Name.EndsWith('/')) continue;
            yield return new LakeFile(Location(location, container, blob.Name), blob.Properties.ContentLength ?? 0, blob.Name[prefix.Length..]);
        }
    }

    /// <inheritdoc />
    public async ValueTask<int> ReadAsync(LakeFile file, long offset, Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.Length == 0 || offset >= file.Length) return 0;
        var (container, name) = Parse(file.Location);
        var length = Math.Min(buffer.Length, file.Length - offset);

        var response = await service.GetBlobContainerClient(container).GetBlobClient(name)
            .DownloadStreamingAsync(new BlobDownloadOptions { Range = new HttpRange(offset, length) }, cancellationToken).ConfigureAwait(false);
        await using var content = response.Value.Content;

        var total = 0;
        while (total < length)
        {
            var read = await content.ReadAsync(buffer[total..(int)length], cancellationToken).ConfigureAwait(false);
            if (read == 0) break;
            total += read;
        }
        return total;
    }

    // Not System.Uri for the path: blob names may contain '#', '%' or spaces.
    private (string Container, string Path) Parse(string location)
    {
        var scheme = Scheme(location);
        var rest = location[(scheme.Length + 3)..];
        var slash = rest.IndexOf('/');
        var authority = slash < 0 ? rest : rest[..slash];
        var path = slash < 0 ? "" : rest[(slash + 1)..];

        if (scheme.Equals("https", StringComparison.OrdinalIgnoreCase))
        {
            CheckAccount(authority, location);
            var next = path.IndexOf('/');
            return next < 0 ? (path, "") : (path[..next], path[(next + 1)..]);
        }

        var at = authority.IndexOf('@');
        if (at >= 0) CheckAccount(authority[(at + 1)..], location);
        return (at < 0 ? authority : authority[..at], path);
    }

    private void CheckAccount(string host, string location)
    {
        var expected = service.Uri.Host;
        // Emulators (Azurite) and custom endpoints: nothing to compare.
        if (!HttpsHosts.Any(h => expected.EndsWith(h, StringComparison.OrdinalIgnoreCase))) return;
        if (!string.Equals(Account(host), Account(expected), StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException($"'{location}' is in account '{Account(host)}', but the BlobServiceClient points to '{expected}'.");
    }

    // shop.dfs.core.windows.net and shop.blob.core.windows.net are the same account.
    private static string Account(string host) => host.Split('.')[0];

    private static string Location(string listed, string container, string name)
    {
        var scheme = Scheme(listed);
        var rest = listed[(scheme.Length + 3)..];
        var slash = rest.IndexOf('/');
        var authority = slash < 0 ? rest : rest[..slash];
        return scheme.Equals("https", StringComparison.OrdinalIgnoreCase)
            ? $"{scheme}://{authority}/{container}/{name}"
            : $"{scheme}://{authority}/{name}";
    }

    private static string Scheme(string location)
    {
        var end = location.IndexOf("://", StringComparison.Ordinal);
        return end < 0 ? "" : location[..end];
    }

    private static string Host(string location)
    {
        var rest = location[(Scheme(location).Length + 3)..];
        var slash = rest.IndexOf('/');
        return slash < 0 ? rest : rest[..slash];
    }
}
