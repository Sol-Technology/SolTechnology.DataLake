using System.Globalization;
using System.Text.Json;

namespace SolTechnology.DataLake.IntegrationTests;

// Rows from the reader against rows PyIceberg read (JSON written by Docker/pyiceberg scripts).
internal static class RowAssert
{
    public static void Same(List<Dictionary<string, object?>> actual, JsonElement expected)
    {
        var rows = expected.GetProperty("rows").EnumerateArray().ToList();
        actual.Should().HaveCount(rows.Count);
        for (var i = 0; i < rows.Count; i++)
        {
            actual[i].Keys.Should().BeEquivalentTo(rows[i].EnumerateObject().Select(p => p.Name), $"columns of row {i}");
            foreach (var property in rows[i].EnumerateObject())
                Equal(actual[i][property.Name], property.Value).Should().BeTrue(
                    $"row {i} column {property.Name}: actual {actual[i][property.Name]}, expected {property.Value}");
        }
    }

    private static bool Equal(object? actual, JsonElement expected) => actual switch
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
