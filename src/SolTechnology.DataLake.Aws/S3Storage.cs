using System.Runtime.CompilerServices;
using Amazon.S3;
using Amazon.S3.Model;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.Aws;

/// <summary>
/// Amazon S3 and S3-compatible storage (MinIO, Cloudflare R2, Ceph) for <c>s3://bucket/prefix/</c> locations.
/// Files are read with ranged GETs: the Parquet footer and the column chunks a query needs, not whole objects.
/// </summary>
/// <example>
/// <code>
/// var s3 = new AmazonS3Client(); // credentials and region from the default AWS chain
/// var orders = DataLake.Open&lt;Order&gt;("s3://shop/orders/", new DataLakeOptions { Storage = new S3Storage(s3) });
/// </code>
/// </example>
public sealed class S3Storage(IAmazonS3 client) : ILakeStorage
{
    /// <inheritdoc />
    public bool CanHandle(string location) =>
        location.StartsWith("s3://", StringComparison.OrdinalIgnoreCase) || location.StartsWith("s3a://", StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public async IAsyncEnumerable<LakeFile> ListAsync(string location, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var (scheme, bucket, prefix) = Parse(location);
        // "orders" must not match "orders_old/": list the folder, not a key prefix.
        if (prefix.Length > 0 && !prefix.EndsWith('/')) prefix += "/";

        var request = new ListObjectsV2Request { BucketName = bucket, Prefix = prefix };
        do
        {
            var page = await client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
            foreach (var item in page.S3Objects ?? [])
            {
                if (item.Key.EndsWith('/')) continue;
                yield return new LakeFile($"{scheme}://{bucket}/{item.Key}", item.Size ?? 0, item.Key[prefix.Length..]);
            }
            request.ContinuationToken = page.IsTruncated == true ? page.NextContinuationToken : null;
        }
        while (request.ContinuationToken is not null);
    }

    /// <inheritdoc />
    public async ValueTask<int> ReadAsync(LakeFile file, long offset, Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (buffer.Length == 0 || offset >= file.Length) return 0;
        var (_, bucket, key) = Parse(file.Location);
        var end = Math.Min(offset + buffer.Length, file.Length) - 1;

        using var response = await client.GetObjectAsync(new GetObjectRequest
        {
            BucketName = bucket,
            Key = key,
            ByteRange = new ByteRange(offset, end),
        }, cancellationToken).ConfigureAwait(false);

        var total = 0;
        while (total < buffer.Length)
        {
            var read = await response.ResponseStream.ReadAsync(buffer[total..], cancellationToken).ConfigureAwait(false);
            if (read == 0) break;
            total += read;
        }
        return total;
    }

    // Not System.Uri: S3 keys may contain '#', '%' or spaces, which a URI would treat as syntax.
    private static (string Scheme, string Bucket, string Key) Parse(string location)
    {
        var schemeEnd = location.IndexOf("://", StringComparison.Ordinal);
        var rest = location[(schemeEnd + 3)..];
        var slash = rest.IndexOf('/');
        return slash < 0
            ? (location[..schemeEnd], rest, "")
            : (location[..schemeEnd], rest[..slash], rest[(slash + 1)..]);
    }
}
