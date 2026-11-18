using System.Security.Cryptography;
using SolTechnology.DataLake.Aws;
using SolTechnology.DataLake.Azure;
using SolTechnology.DataLake.IntegrationTests.Containers;
using SolTechnology.DataLake.Storage;
using static SolTechnology.DataLake.IntegrationTests.IntegrationTestsFixture;

namespace SolTechnology.DataLake.IntegrationTests;

// The same writes against every cloud storage: S3 (Silo) and Azure Blob (Azurite).
public abstract class StorageWriteTests
{
    private ILakeStorage _storage = null!;
    private string _root = null!;

    protected abstract Task<(ILakeStorage Storage, string Root)> CreateAsync();

    [SetUp]
    public async Task Create() => (_storage, _root) = await CreateAsync();

    [TearDown]
    public void Dispose() => (_storage as IDisposable)?.Dispose();

    [Test]
    public async Task Writes_a_file_that_is_listed_and_read_back()
    {
        var bytes = RandomNumberGenerator.GetBytes(100_000);

        var file = await _storage.WriteAsync($"{_root}/date=2026-10-01/part-0.bin", new MemoryStream(bytes));

        file.Should().Be(new LakeFile($"{_root}/date=2026-10-01/part-0.bin", 100_000, "part-0.bin"));
        (await _storage.ListAsync(_root).ToListAsync()).Should().Equal(new LakeFile(file.Location, 100_000, "date=2026-10-01/part-0.bin"));
        (await ReadAll(file)).Should().Equal(bytes);
    }

    [Test]
    public async Task Writes_a_large_stream_that_cannot_seek()
    {
        // Two full parts and a bit for S3's multipart upload.
        var bytes = RandomNumberGenerator.GetBytes(2 * S3Storage.PartSize + 12_345);

        var file = await _storage.WriteAsync($"{_root}/big.bin", new ForwardOnlyStream(bytes));

        file.Length.Should().Be(bytes.Length);
        (await _storage.ListAsync(file.Location).SingleAsync()).Length.Should().Be(bytes.Length);
        (await ReadAll(file)).Should().Equal(bytes);
    }

    [Test]
    public async Task Overwrite_replaces_the_file()
    {
        await _storage.WriteAsync($"{_root}/part-0.bin", new MemoryStream([1, 2, 3]));

        var file = await _storage.WriteAsync($"{_root}/part-0.bin", new MemoryStream([9]));

        (await ReadAll(file)).Should().Equal(9);
    }

    [Test]
    public async Task CreateNew_does_not_touch_an_existing_file()
    {
        var location = $"{_root}/v1.metadata.json";
        var file = await _storage.WriteAsync(location, new MemoryStream([1, 2, 3]), WriteMode.CreateNew);

        var write = () => _storage.WriteAsync(location, new MemoryStream([9]), WriteMode.CreateNew).AsTask();

        (await write.Should().ThrowAsync<LakeFileExistsException>()).Which.Location.Should().Be(location);
        (await ReadAll(file)).Should().Equal(1, 2, 3);
    }

    [Test]
    public async Task CreateNew_of_a_large_stream_does_not_touch_an_existing_file()
    {
        var location = $"{_root}/big.bin";
        var file = await _storage.WriteAsync(location, new MemoryStream([1, 2, 3]));

        var write = () => _storage.WriteAsync(location, new ForwardOnlyStream(new byte[S3Storage.PartSize + 1]), WriteMode.CreateNew).AsTask();

        await write.Should().ThrowAsync<LakeFileExistsException>();
        (await ReadAll(file)).Should().Equal(1, 2, 3);
    }

    [Test]
    public async Task Of_concurrent_CreateNew_writes_exactly_one_wins()
    {
        var location = $"{_root}/v2.metadata.json";

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(async i =>
        {
            try
            {
                return ((LakeFile?)await _storage.WriteAsync(location, new MemoryStream([(byte)i]), WriteMode.CreateNew), (byte?)i);
            }
            catch (LakeFileExistsException)
            {
                return (null, null);
            }
        }));

        var (file, winner) = results.Should().ContainSingle(r => r.Item2 != null).Which;
        (await ReadAll(file!)).Should().Equal(winner!.Value);
    }

    [Test]
    public async Task Delete_removes_the_file_and_a_missing_file_is_not_an_error()
    {
        var file = await _storage.WriteAsync($"{_root}/part-0.bin", new MemoryStream([1]));

        await _storage.DeleteAsync(file.Location);
        await _storage.DeleteAsync(file.Location);

        (await _storage.ListAsync(_root).ToListAsync()).Should().BeEmpty();
    }

    private async Task<byte[]> ReadAll(LakeFile file)
    {
        var bytes = new byte[file.Length];
        (await _storage.ReadAsync(file, 0, bytes)).Should().Be(bytes.Length);
        return bytes;
    }

    public sealed class OnS3 : StorageWriteTests
    {
        protected override Task<(ILakeStorage, string)> CreateAsync() =>
            Task.FromResult<(ILakeStorage, string)>((new DisposingS3Storage(Silo.CreateClient()), $"s3://{SiloFixture.Bucket}/writes/{Guid.NewGuid():N}"));
    }

    public sealed class OnAzure : StorageWriteTests
    {
        protected override async Task<(ILakeStorage, string)> CreateAsync()
        {
            var blobs = Azurite.CreateClient();
            await blobs.GetBlobContainerClient("lake").CreateIfNotExistsAsync();
            return (new AzureBlobStorage(blobs), $"az://lake/writes/{Guid.NewGuid():N}");
        }
    }

    private sealed class DisposingS3Storage(Amazon.S3.AmazonS3Client client) : ILakeStorage, IDisposable
    {
        private readonly S3Storage _inner = new(client);
        public bool CanHandle(string location) => _inner.CanHandle(location);
        public IAsyncEnumerable<LakeFile> ListAsync(string location, CancellationToken ct = default) => _inner.ListAsync(location, ct);
        public ValueTask<int> ReadAsync(LakeFile file, long offset, Memory<byte> buffer, CancellationToken ct = default) => _inner.ReadAsync(file, offset, buffer, ct);
        public ValueTask<LakeFile> WriteAsync(string location, Stream content, WriteMode mode = WriteMode.Overwrite, CancellationToken ct = default) => _inner.WriteAsync(location, content, mode, ct);
        public ValueTask DeleteAsync(string location, CancellationToken ct = default) => _inner.DeleteAsync(location, ct);
        public void Dispose() => client.Dispose();
    }
}

/// <summary>A stream that can only be read forward, like a network or compression stream.</summary>
internal sealed class ForwardOnlyStream(byte[] bytes) : Stream
{
    private readonly MemoryStream _inner = new(bytes);

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
