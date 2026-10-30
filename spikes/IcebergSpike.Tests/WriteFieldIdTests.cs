using Parquet;
using Parquet.Schema;

namespace IcebergSpike;

// D1: can Parquet.Net write the field ids Iceberg needs?
public class WriteFieldIdTests
{
    public static string WrittenFile => Path.Combine(Fixtures.Root, "written", "field-ids.parquet");

    [Test]
    public async Task Parquet_Net_writes_field_ids()
    {
        var id = new DataField<long>("id") { FieldId = 1 };
        var name = new DataField<string>("name", true) { FieldId = 2 };
        var amount = new DataField<decimal?>("amount") { FieldId = 4 };

        Directory.CreateDirectory(Path.GetDirectoryName(WrittenFile)!);
        await using (var stream = File.Create(WrittenFile))
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(id, name, amount), stream))
        using (var group = writer.CreateRowGroup())
        {
            await group.WriteAsync(id, (ReadOnlyMemory<long>)new long[] { 1, 2, 3 });
            await group.WriteAsync(name, new[] { "a", null, "c" });
            await group.WriteAsync(amount, (ReadOnlyMemory<decimal?>)new decimal?[] { 1.10m, null, 3.30m });
        }

        await using var read = File.OpenRead(WrittenFile);
        await using var reader = await ParquetReader.CreateAsync(read);
        reader.Schema.GetDataFields().Select(f => (f.Name, f.SchemaElement?.FieldId))
            .Should().Equal(("id", 1), ("name", 2), ("amount", 4));
    }
}
