using System.Collections;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace SolTechnology.DataLake.UnitTests;

// Fixtures/nested.parquet is written by pyarrow (Fixtures/generate.py); nested.json is what pyarrow reads back.
public class NestedColumnTests
{
    private static string Fixtures => Path.Combine(AppContext.BaseDirectory, "Fixtures");

    public sealed class Address
    {
        public string? City { get; set; }
        public string? Zip { get; set; }
    }

    public sealed class Line
    {
        public string? Sku { get; set; }
        public int? Qty { get; set; }
        public decimal? Price { get; set; }
    }

    public sealed class Customer
    {
        public string? Name { get; set; }
        public List<string?>? Emails { get; set; }
    }

    public sealed class Row
    {
        public long Id { get; set; }
        public Address? Address { get; set; }
        public List<string?>? Tags { get; set; }
        public int[]? Scores { get; set; }
        public IReadOnlyList<Line?>? Lines { get; set; }
        public Dictionary<string, long?>? Attributes { get; set; }
        public List<List<int?>?>? Matrix { get; set; }
        public Customer? Customer { get; set; }
        public TimeOnly? At { get; set; }
        public Guid? Uid { get; set; }
    }

    [Test]
    public async Task Structs_lists_maps_time_and_uuid_read_like_pyarrow()
    {
        var expected = JsonNode.Parse(await File.ReadAllTextAsync(Path.Combine(Fixtures, "nested.json")))!.AsArray();
        var root = CopyFixture();

        var rows = await DataLake.Open<Row>(root).ReadAsync().ToListAsync();

        rows.Should().HaveCount(expected.Count);
        for (var i = 0; i < rows.Count; i++)
            JsonNode.DeepEquals(ToJson(rows[i]), expected[i]).Should().BeTrue($"row {i}:\nactual   {ToJson(rows[i])!.ToJsonString()}\nexpected {expected[i]!.ToJsonString()}");
    }

    public sealed class UntypedRow
    {
        public long Id { get; set; }
        public Dictionary<string, object?>? Address { get; set; }
        public object? Lines { get; set; }
        public object? Attributes { get; set; }
    }

    [Test]
    public async Task Nested_columns_into_dictionaries_and_object()
    {
        var root = CopyFixture();

        var rows = await DataLake.Open<UntypedRow>(root).ReadAsync().ToListAsync();

        rows[0].Address.Should().BeEquivalentTo(new Dictionary<string, object?> { ["city"] = "Wroclaw", ["zip"] = "50-001" });
        ((IList)rows[0].Lines!).Count.Should().Be(2);
        rows[0].Attributes.Should().BeEquivalentTo(new Dictionary<object, object?> { ["color"] = 1L, ["size"] = 2L });
        rows[1].Address.Should().BeNull();
    }

    public sealed class WrongStruct
    {
        public string? Address { get; set; }
    }

    public sealed class WrongList
    {
        public string? Tags { get; set; }
    }

    public sealed class NullElementIntoNonNullable
    {
        public List<string>? Tags { get; set; }
        public List<int>? Scores { get; set; }
        public List<List<int>?>? Matrix { get; set; }
    }

    [Test]
    public async Task Clear_errors_for_wrong_shapes_and_nulls()
    {
        var root = CopyFixture();

        var structIntoString = () => DataLake.Open<WrongStruct>(root).ReadAsync().ToListAsync().AsTask();
        var listIntoString = () => DataLake.Open<WrongList>(root).ReadAsync().ToListAsync().AsTask();
        var nullElement = () => DataLake.Open<NullElementIntoNonNullable>(root).ReadAsync().ToListAsync().AsTask();

        (await structIntoString.Should().ThrowAsync<NotSupportedException>()).WithMessage("*'address' is a struct*not String*");
        (await listIntoString.Should().ThrowAsync<NotSupportedException>()).WithMessage("*'tags' is a list*not String*");
        (await nullElement.Should().ThrowAsync<InvalidDataException>()).WithMessage("*'matrix[][]' has a null value*Int32*");
    }

    public sealed class BlobRow
    {
        public int Id { get; set; }
        public List<byte[]>? Blobs { get; set; }
    }

    [Test]
    public async Task Binary_values_in_nested_columns_are_copied_out_of_the_reader()
    {
        // Parquet.Net hands out binary values as views into a pooled buffer; they must survive the next column and row group.
        var root = Path.Combine(Path.GetTempPath(), "datalake-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var written = Enumerable.Range(0, 3000).Select(i => new BlobRow { Id = i, Blobs = [BitConverter.GetBytes(i), [(byte)(i % 256), 7]] }).ToList();
        await using (var file = File.Create(Path.Combine(root, "blobs.parquet")))
            await Parquet.Serialization.ParquetSerializer.SerializeAsync(written, file, new Parquet.ParquetOptions { RowGroupSize = 1000 });

        var typed = await DataLake.Open<BlobRow>(root).ReadAsync().ToListAsync();
        var untyped = await DataLake.Open<UntypedBlobRow>(root).ReadAsync().ToListAsync();

        typed.Should().BeEquivalentTo(written, o => o.WithStrictOrdering());
        untyped.Select(r => ((IList)r.Blobs!).Cast<byte[]>().ToList()).Should().BeEquivalentTo(written.Select(r => r.Blobs), o => o.WithStrictOrdering());
    }

    public sealed class UntypedBlobRow
    {
        public object? Blobs { get; set; }
    }

    private static string CopyFixture()
    {
        var root = Path.Combine(Path.GetTempPath(), "datalake-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        File.Copy(Path.Combine(Fixtures, "nested.parquet"), Path.Combine(root, "nested.parquet"));
        return root;
    }

    private static IEnumerable<DictionaryEntry> Entries(IDictionary d)
    {
        foreach (DictionaryEntry e in d) yield return e;
    }

    // Same shape as the JSON written by generate.py: lower-case keys, decimals as strings, time as ISO, uuid as text.
    private static JsonNode? ToJson(object? value) => value switch
    {
        null => null,
        string s => JsonValue.Create(s),
        long or int or bool or double => JsonSerializer.SerializeToNode(value),
        decimal m => JsonValue.Create(m.ToString(CultureInfo.InvariantCulture)),
        TimeOnly t => JsonValue.Create(t.ToString(t.Ticks % TimeSpan.TicksPerSecond == 0 ? "HH:mm:ss" : "HH:mm:ss.ffffff", CultureInfo.InvariantCulture)),
        Guid g => JsonValue.Create(g.ToString()),
        IDictionary d => new JsonObject(Entries(d).Select(e => KeyValuePair.Create(e.Key.ToString()!, ToJson(e.Value)))),
        IEnumerable items => new JsonArray(items.Cast<object?>().Select(ToJson).ToArray()),
        _ => new JsonObject(value.GetType().GetProperties().Select(p => KeyValuePair.Create(p.Name.ToLowerInvariant(), ToJson(p.GetValue(value))))),
    };
}
