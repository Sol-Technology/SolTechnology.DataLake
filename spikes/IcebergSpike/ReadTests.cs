using System.Globalization;
using System.Text.Json;

namespace IcebergSpike;

// The spike passes when every snapshot of every fixture table reads exactly like PyIceberg reads it.
public class ReadTests
{
    private static readonly string[] Tables = ["orders_plain", "orders_by_day", "orders_bucket", "orders_evolved"];

    public static IEnumerable<TestCaseData> Snapshots() =>
        from table in Tables
        let expected = JsonDocument.Parse(File.ReadAllText(Fixtures.Expected(table))).RootElement
        from snapshot in expected.GetProperty("snapshots").EnumerateArray()
        select new TestCaseData(table, snapshot.GetProperty("snapshot_id").GetInt64()).SetArgDisplayNames(table, snapshot.GetProperty("snapshot_id").ToString());

    [TestCaseSource(nameof(Snapshots))]
    public async Task Reads_snapshot_like_PyIceberg(string tableName, long snapshotId)
    {
        var expected = Oracle.Rows(tableName, snapshotId);
        var table = Oracle.Table(tableName);

        var rows = (await table.Scan(snapshotId)).OrderBy(r => (long)r["id"]!).ToList();

        Oracle.AssertSame(rows, expected);
    }
}

internal static class Oracle
{
    public static JsonElement Expected(string table) => JsonDocument.Parse(File.ReadAllText(Fixtures.Expected(table))).RootElement;

    public static IcebergTable Table(string table) =>
        IcebergTable.Load(Path.Combine(Fixtures.Warehouse, Expected(table).GetProperty("metadata").GetString()!), Fixtures.Resolve);

    public static List<JsonElement> Rows(string table, long? snapshotId = null)
    {
        var root = Expected(table);
        var id = snapshotId ?? root.GetProperty("current_snapshot_id").GetInt64();
        return root.GetProperty("snapshots").EnumerateArray()
            .Single(s => s.GetProperty("snapshot_id").GetInt64() == id)
            .GetProperty("rows").EnumerateArray().ToList();
    }

    public static void AssertSame(List<Dictionary<string, object?>> actual, List<JsonElement> expected)
    {
        actual.Should().HaveCount(expected.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            var names = expected[i].EnumerateObject().Select(p => p.Name).ToList();
            actual[i].Keys.Should().BeEquivalentTo(names, $"columns of row {i}");
            foreach (var property in expected[i].EnumerateObject())
                Same(actual[i][property.Name], property.Value).Should().BeTrue(
                    $"row {i} column {property.Name}: actual {actual[i][property.Name]} ({actual[i][property.Name]?.GetType().Name}), expected {property.Value}");
        }
    }

    private static bool Same(object? actual, JsonElement expected) => actual switch
    {
        null => expected.ValueKind == JsonValueKind.Null,
        long l => expected.ValueKind == JsonValueKind.Number && expected.GetInt64() == l,
        double d => expected.ValueKind == JsonValueKind.Number && expected.GetDouble() == d,
        bool b => expected.ValueKind == (b ? JsonValueKind.True : JsonValueKind.False),
        string s => expected.GetString() == s,
        decimal m => decimal.Parse(expected.GetString()!, CultureInfo.InvariantCulture) == m,
        DateTimeOffset t => DateTimeOffset.Parse(expected.GetString()!, CultureInfo.InvariantCulture) == t,
        DateOnly d => DateOnly.Parse(expected.GetString()!, CultureInfo.InvariantCulture) == d,
        _ => false,
    };
}
