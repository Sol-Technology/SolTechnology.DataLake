using System.Security.Cryptography;
using Parquet.Schema;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.UnitTests;

public class LocalStorageWriteTests
{
    private readonly LocalStorage _storage = LocalStorage.Instance;
    private TempDataset _lake = null!;

    [SetUp]
    public void CreateLake() => _lake = new TempDataset();

    [TearDown]
    public void DeleteLake() => _lake.Dispose();

    [Test]
    public async Task Writes_a_file_in_new_folders()
    {
        var bytes = RandomNumberGenerator.GetBytes(100_000);

        var file = await _storage.WriteAsync(_lake.PathOf("date=2026-10-01/part-0.bin"), new ForwardOnlyStream(bytes));

        file.Should().Be(new LakeFile(_lake.PathOf("date=2026-10-01/part-0.bin"), 100_000, "part-0.bin"));
        (await File.ReadAllBytesAsync(file.Location)).Should().Equal(bytes);
        Directory.EnumerateFiles(_lake.Root, "*", SearchOption.AllDirectories).Should().ContainSingle("the temporary file is renamed, not left behind");
    }

    [Test]
    public async Task Writes_from_the_current_position_of_the_stream()
    {
        var stream = new MemoryStream([1, 2, 3, 4, 5]) { Position = 2 };

        var file = await _storage.WriteAsync(new Uri(_lake.PathOf("tail.bin")).AbsoluteUri, stream);

        file.Length.Should().Be(3);
        (await File.ReadAllBytesAsync(_lake.PathOf("tail.bin"))).Should().Equal(3, 4, 5);
    }

    [Test]
    public async Task Overwrite_replaces_the_file()
    {
        var path = _lake.PathOf("part-0.bin");
        await _storage.WriteAsync(path, new MemoryStream([1, 2, 3]));

        await _storage.WriteAsync(path, new MemoryStream([9]));

        (await File.ReadAllBytesAsync(path)).Should().Equal(9);
    }

    [Test]
    public async Task CreateNew_writes_when_the_file_is_missing()
    {
        var file = await _storage.WriteAsync(_lake.PathOf("v1.metadata.json"), new MemoryStream([1]), WriteMode.CreateNew);

        file.Length.Should().Be(1);
    }

    [Test]
    public async Task CreateNew_does_not_touch_an_existing_file()
    {
        var path = _lake.PathOf("v1.metadata.json");
        await _storage.WriteAsync(path, new MemoryStream([1, 2, 3]));

        var write = () => _storage.WriteAsync(path, new MemoryStream([9]), WriteMode.CreateNew).AsTask();

        (await write.Should().ThrowAsync<LakeFileExistsException>()).Which.Location.Should().Be(path);
        (await File.ReadAllBytesAsync(path)).Should().Equal(1, 2, 3);
        Directory.EnumerateFiles(_lake.Root).Should().ContainSingle();
    }

    [Test]
    public async Task Of_concurrent_CreateNew_writes_exactly_one_wins()
    {
        var path = _lake.PathOf("v2.metadata.json");

        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(i => Task.Run(async () =>
        {
            try
            {
                await _storage.WriteAsync(path, new MemoryStream([(byte)i]), WriteMode.CreateNew);
                return (byte?)i;
            }
            catch (LakeFileExistsException)
            {
                return null;
            }
        })));

        var winner = results.Should().ContainSingle(r => r != null).Which;
        (await File.ReadAllBytesAsync(path)).Should().Equal(winner!.Value);
    }

    [Test]
    public async Task Delete_removes_the_file_and_a_missing_file_is_not_an_error()
    {
        var path = _lake.PathOf("date=2026-10-01/part-0.bin");
        await _storage.WriteAsync(path, new MemoryStream([1]));

        await _storage.DeleteAsync(path);
        await _storage.DeleteAsync(path);
        await _storage.DeleteAsync(_lake.PathOf("no-such-folder/part-0.bin"));

        File.Exists(path).Should().BeFalse();
    }

    [Test]
    public async Task A_written_Parquet_file_is_read_as_a_dataset()
    {
        var source = new TempDataset();
        await source.WriteAsync("part-0.parquet", [new DataField<long>("Id")], [new long[] { 1, 2, 3 }]);
        await using (var parquet = File.OpenRead(source.PathOf("part-0.parquet")))
            await _storage.WriteAsync(_lake.PathOf("orders/date=2026-10-01/part-0.parquet"), parquet);
        source.Dispose();

        var rows = await Dataset.Open<Order>(_lake.PathOf("orders")).ReadAsync().ToListAsync();

        rows.Select(r => (r.Id, r.Date)).Should().Equal((1, new DateOnly(2026, 10, 1)), (2, new DateOnly(2026, 10, 1)), (3, new DateOnly(2026, 10, 1)));
    }

    [Test]
    public async Task A_read_only_storage_says_so()
    {
        ILakeStorage storage = new ReadOnlyStorage();

        var write = () => storage.WriteAsync("x", new MemoryStream()).AsTask();
        var delete = () => storage.DeleteAsync("x").AsTask();

        (await write.Should().ThrowAsync<NotSupportedException>()).WithMessage("ReadOnlyStorage is read-only.");
        await delete.Should().ThrowAsync<NotSupportedException>();
    }

    public sealed class Order
    {
        public long Id { get; set; }
        public DateOnly Date { get; set; }
    }

    private sealed class ReadOnlyStorage : ILakeStorage
    {
        public bool CanHandle(string location) => true;
        public IAsyncEnumerable<LakeFile> ListAsync(string location, CancellationToken cancellationToken = default) => AsyncEnumerable.Empty<LakeFile>();
        public ValueTask<int> ReadAsync(LakeFile file, long offset, Memory<byte> buffer, CancellationToken cancellationToken = default) => ValueTask.FromResult(0);
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
