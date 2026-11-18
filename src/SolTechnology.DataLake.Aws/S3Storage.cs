using System.Buffers;
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
        var (scheme, bucket, key) = Parse(location);
        // "orders" must not match "orders_old/": list the folder, not a key prefix.
        var prefix = key.Length > 0 && !key.EndsWith('/') ? key + "/" : key;

        var found = false;
        var request = new ListObjectsV2Request { BucketName = bucket, Prefix = prefix };
        do
        {
            var page = await client.ListObjectsV2Async(request, cancellationToken).ConfigureAwait(false);
            foreach (var item in page.S3Objects ?? [])
            {
                if (item.Key.EndsWith('/')) continue;
                found = true;
                yield return new LakeFile($"{scheme}://{bucket}/{item.Key}", item.Size ?? 0, item.Key[prefix.Length..]);
            }
            request.ContinuationToken = page.IsTruncated == true ? page.NextContinuationToken : null;
        }
        while (request.ContinuationToken is not null);

        // Not a folder: maybe the location is one object. Spark writes folders named "x.parquet", so the name alone can't tell.
        if (!found && prefix != key && await SizeOf(bucket, key, cancellationToken).ConfigureAwait(false) is { } size)
            yield return new LakeFile($"{scheme}://{bucket}/{key}", size, key[(key.LastIndexOf('/') + 1)..]);
    }

    private async Task<long?> SizeOf(string bucket, string key, CancellationToken cancellationToken)
    {
        try
        {
            var metadata = await client.GetObjectMetadataAsync(bucket, key, cancellationToken).ConfigureAwait(false);
            return metadata.ContentLength;
        }
        catch (AmazonS3Exception e) when (e.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
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

    /// <inheritdoc />
    /// <remarks>
    /// Up to <see cref="PartSize"/> bytes go in one PUT; more go as a multipart upload in parts of that size, so the stream is
    /// never buffered whole and may be up to 80 GB. <see cref="WriteMode.CreateNew"/> sends <c>If-None-Match: *</c>, which
    /// Amazon S3, MinIO and Cloudflare R2 support; an S3-compatible store that ignores it overwrites.
    /// </remarks>
    public async ValueTask<LakeFile> WriteAsync(string location, Stream content, WriteMode mode = WriteMode.Overwrite, CancellationToken cancellationToken = default)
    {
        var (scheme, bucket, key) = Parse(location);
        var ifNoneMatch = mode == WriteMode.CreateNew ? "*" : null;
        var buffer = ArrayPool<byte>.Shared.Rent(PartSize);
        try
        {
            var filled = await FillAsync(content, buffer, cancellationToken).ConfigureAwait(false);
            var length = filled < PartSize
                ? await PutAsync(bucket, key, buffer, filled, ifNoneMatch, location, cancellationToken).ConfigureAwait(false)
                : await UploadPartsAsync(bucket, key, content, buffer, ifNoneMatch, location, cancellationToken).ConfigureAwait(false);
            var name = key[(key.LastIndexOf('/') + 1)..];
            return new LakeFile($"{scheme}://{bucket}/{key}", length, name);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <inheritdoc />
    public async ValueTask DeleteAsync(string location, CancellationToken cancellationToken = default)
    {
        var (_, bucket, key) = Parse(location);
        await client.DeleteObjectAsync(bucket, key, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Size of one PUT and of each part of a multipart upload: 8 MiB.</summary>
    public const int PartSize = 8 << 20;

    private async Task<long> PutAsync(string bucket, string key, byte[] buffer, int count, string? ifNoneMatch, string location, CancellationToken ct)
    {
        try
        {
            await client.PutObjectAsync(new PutObjectRequest
            {
                BucketName = bucket,
                Key = key,
                InputStream = new MemoryStream(buffer, 0, count, writable: false),
                IfNoneMatch = ifNoneMatch,
            }, ct).ConfigureAwait(false);
            return count;
        }
        catch (AmazonS3Exception e) when (ifNoneMatch is not null && IsConflict(e))
        {
            throw new LakeFileExistsException(location);
        }
    }

    private async Task<long> UploadPartsAsync(string bucket, string key, Stream content, byte[] buffer, string? ifNoneMatch, string location, CancellationToken ct)
    {
        var upload = await client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest { BucketName = bucket, Key = key }, ct).ConfigureAwait(false);
        try
        {
            var parts = new List<PartETag>();
            var length = 0L;
            var filled = PartSize;
            while (filled > 0)
            {
                var number = parts.Count + 1;
                var part = await client.UploadPartAsync(new UploadPartRequest
                {
                    BucketName = bucket,
                    Key = key,
                    UploadId = upload.UploadId,
                    PartNumber = number,
                    PartSize = filled,
                    InputStream = new MemoryStream(buffer, 0, filled, writable: false),
                }, ct).ConfigureAwait(false);
                parts.Add(new PartETag(number, part.ETag));
                length += filled;
                filled = await FillAsync(content, buffer, ct).ConfigureAwait(false);
            }

            await client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
            {
                BucketName = bucket,
                Key = key,
                UploadId = upload.UploadId,
                PartETags = parts,
                IfNoneMatch = ifNoneMatch,
            }, ct).ConfigureAwait(false);
            return length;
        }
        catch (Exception e)
        {
            // Parts of an unfinished upload are stored, and billed, until aborted.
            await client.AbortMultipartUploadAsync(bucket, key, upload.UploadId, CancellationToken.None).ConfigureAwait(false);
            if (ifNoneMatch is not null && e is AmazonS3Exception s3 && IsConflict(s3)) throw new LakeFileExistsException(location);
            throw;
        }
    }

    // 412: the object exists. 409: another conditional write to the same key is in progress.
    private static bool IsConflict(AmazonS3Exception e) =>
        e.StatusCode is System.Net.HttpStatusCode.PreconditionFailed or System.Net.HttpStatusCode.Conflict;

    private static async Task<int> FillAsync(Stream content, byte[] buffer, CancellationToken ct)
    {
        var total = 0;
        while (total < PartSize)
        {
            var read = await content.ReadAsync(buffer.AsMemory(total, PartSize - total), ct).ConfigureAwait(false);
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
