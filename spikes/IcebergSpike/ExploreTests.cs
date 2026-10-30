using SolTechnology.Avro;

namespace IcebergSpike;

// Step 0: what does AvroConvert give us for Iceberg manifest files?
public class ExploreTests
{
    [Test]
    public void PrintManifestSchemas()
    {
        var metadata = Directory.GetFiles(Path.Combine(Fixtures.Warehouse, "shop", "orders_by_day", "metadata"), "*.avro");
        foreach (var file in metadata.OrderBy(f => f).Take(2))
        {
            TestContext.Out.WriteLine(Path.GetFileName(file));
            TestContext.Out.WriteLine(AvroConvert.GetSchema(file));
            TestContext.Out.WriteLine(AvroConvert.Avro2Json(File.ReadAllBytes(file)));
        }
    }

    [Test]
    public async Task DeserializeManifestsIntoClasses()
    {
        var metadata = TableMetadata.Load(Path.Combine(Fixtures.Warehouse, ExpectedMetadata("orders_by_day")));
        var listPath = Fixtures.Resolve(metadata.Snapshot().ManifestList);

        var manifests = new List<ManifestFile>();
        await foreach (var m in AvroConvert.Deserialize<ManifestFile>(File.OpenRead(listPath))) manifests.Add(m);
        TestContext.Out.WriteLine($"manifests: {manifests.Count}, first partitions: {manifests[0].Partitions?.Count}");

        var entries = new List<ManifestEntry>();
        await foreach (var e in AvroConvert.Deserialize<ManifestEntry>(File.OpenRead(Fixtures.Resolve(manifests[0].ManifestPath)))) entries.Add(e);
        var file = entries[0].DataFile;
        TestContext.Out.WriteLine($"{file.FilePath} rows={file.RecordCount}");
        object? partition = file.Partition;
        TestContext.Out.WriteLine($"partition type: {partition?.GetType().FullName}");
        if (partition is IDictionary<string, object?> dict)
            foreach (var (k, v) in dict) TestContext.Out.WriteLine($"partition {k} = {v} ({v?.GetType().Name})");
        TestContext.Out.WriteLine($"lower bounds: {file.LowerBounds?.Count}, null counts: {file.NullValueCounts?.Count}");
    }

    [Test]
    public async Task PrintParquetSchema()
    {
        var file = Directory.GetFiles(Path.Combine(Fixtures.Warehouse, "shop", "orders_by_day", "data"), "*.parquet", SearchOption.AllDirectories)[0];
        await using var stream = File.OpenRead(file);
        await using var reader = await Parquet.ParquetReader.CreateAsync(stream, new Parquet.ParquetOptions { UseDateOnlyTypeForDates = true });
        foreach (var f in reader.Schema.GetDataFields())
            TestContext.Out.WriteLine($"{f.Name} clr={f.ClrType.Name} nullable={f.IsNullable} fieldId={f.FieldId} element.FieldId={f.SchemaElement?.FieldId}");
        var prop = typeof(Parquet.Schema.DataField).GetProperty(nameof(Parquet.Schema.DataField.FieldId))!;
        TestContext.Out.WriteLine($"FieldId setter public: {prop.SetMethod?.IsPublic}, type {prop.PropertyType}");
        foreach (var m in typeof(Parquet.ParquetRowGroupReader).GetMethods().Where(m => m.Name.StartsWith("Read")))
            TestContext.Out.WriteLine($"{m.ReturnType} {m.Name}({string.Join(", ", m.GetParameters().Select(p => p.ParameterType.Name + " " + p.Name))})");
    }

    internal static string ExpectedMetadata(string table) =>
        System.Text.Json.JsonDocument.Parse(File.ReadAllText(Fixtures.Expected(table))).RootElement.GetProperty("metadata").GetString()!;
}
