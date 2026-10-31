using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.CompatibilityTests;

/// <summary>
/// parquet-testing/bad_data: files crafted to crash readers (fuzzer finds from Arrow, arrow-rs, parquet-mr). Reading them
/// must end with an <see cref="InvalidDataException"/> that names the file, not an IndexOutOfRangeException from deep inside.
/// </summary>
public class BadDataTests
{
    private static readonly string Folder = Path.Combine(AppContext.BaseDirectory, "data", "parquet-testing", "bad_data");

    public static IEnumerable<TestCaseData> Files() =>
        Directory.EnumerateFiles(Folder, "*.parquet").Order(StringComparer.Ordinal)
            .Select(path => new TestCaseData(Path.GetFileName(path)).SetArgDisplayNames(Path.GetFileName(path)));

    // Not corrupt for a lenient reader: RLE with bit width 0 means every index is 0 (parquet-mr writes it for a one-value dictionary).
    private static readonly Dictionary<string, string> Readable = new(StringComparer.Ordinal)
    {
        ["ARROW-GH-43605.parquet"] = "dictionary indexes with RLE bit width 0",
    };

    [TestCaseSource(nameof(Files))]
    [CancelAfter(30_000)]
    public async Task Fails_with_the_file_name_or_reads(string file)
    {
        var read = async () =>
        {
            await foreach (var _ in UntypedReader.ReadAsync(LocalStorage.Instance, Path.Combine(Folder, file))) { }
        };

        if (Readable.ContainsKey(file))
        {
            await read.Should().NotThrowAsync();
            return;
        }
        (await read.Should().ThrowAsync<InvalidDataException>()).WithMessage($"*{file}*");
    }
}
