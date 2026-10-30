namespace SolTechnology.DataLake.Storage;

/// <summary>
/// Seekable read-only stream over <see cref="ILakeStorage.ReadAsync"/>. Parquet readers seek to the footer and then
/// to column chunks; each miss fetches one block of at least <see cref="BlockSize"/> bytes, so small reads next to
/// each other cost one request.
/// </summary>
internal sealed class RangeStream(ILakeStorage storage, LakeFile file, Action<long>? onFetched = null) : Stream
{
    public const int BlockSize = 1 << 20;

    private byte[] _block = [];
    private long _blockStart = -1;
    private int _blockLength;
    private long _position;

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;
    public override long Length => file.Length;

    public override long Position
    {
        get => _position;
        set => _position = value is >= 0 ? value : throw new ArgumentOutOfRangeException(nameof(value));
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var total = 0;
        while (total < buffer.Length && _position < file.Length)
        {
            if (!InBlock(_position)) await FetchAsync(_position, buffer.Length - total, cancellationToken).ConfigureAwait(false);
            var offset = (int)(_position - _blockStart);
            var count = Math.Min(buffer.Length - total, _blockLength - offset);
            _block.AsMemory(offset, count).CopyTo(buffer[total..]);
            total += count;
            _position += count;
        }
        return total;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override int Read(byte[] buffer, int offset, int count) => ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

    public override long Seek(long offset, SeekOrigin origin) => Position = origin switch
    {
        SeekOrigin.Begin => offset,
        SeekOrigin.Current => _position + offset,
        SeekOrigin.End => file.Length + offset,
        _ => throw new ArgumentOutOfRangeException(nameof(origin)),
    };

    private bool InBlock(long position) => _blockStart >= 0 && position >= _blockStart && position < _blockStart + _blockLength;

    private async Task FetchAsync(long position, int wanted, CancellationToken cancellationToken)
    {
        var size = (int)Math.Min(Math.Max(wanted, BlockSize), file.Length - position);
        if (_block.Length < size) _block = new byte[size];
        _blockLength = await storage.ReadAsync(file, position, _block.AsMemory(0, size), cancellationToken).ConfigureAwait(false);
        _blockStart = position;
        onFetched?.Invoke(_blockLength);
        if (_blockLength == 0) throw new EndOfStreamException($"{file.Location} ended at {position}, expected {file.Length} bytes.");
    }

    public override void Flush() { }
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
