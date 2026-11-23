using System.Collections;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.CompatibilityTests;

/// <summary>
/// Every file of the corpus read by DataLake and compared with pyarrow (data/expected, see scripts/expected.py):
/// row count and the first 20 rows. Files DataLake can't read yet are listed in <see cref="KnownGaps"/>; such a test
/// fails when the file starts to work, so the list stays honest.
/// </summary>
public class CorpusTests
{
    private static readonly string Data = Path.Combine(AppContext.BaseDirectory, "data");

    public static IEnumerable<TestCaseData> Files() =>
        Directory.EnumerateFiles(Path.Combine(Data, "expected"), "*.json", SearchOption.AllDirectories)
            .Select(json => Path.GetRelativePath(Path.Combine(Data, "expected"), json)[..^".json".Length].Replace('\\', '/'))
            .Order(StringComparer.Ordinal)
            .Select(file => new TestCaseData(file).SetArgDisplayNames(file));

    [TestCaseSource(nameof(Files))]
    public async Task ReadsLikePyArrow(string file)
    {
        if (KnownGaps.NotToRead(file) is { } danger) Assert.Ignore($"Not read: {danger}");
        var expected = JsonNode.Parse(await File.ReadAllTextAsync(Path.Combine(Data, "expected", file + ".json")))!;
        var gap = KnownGaps.For(file);

        string? problem;
        try
        {
            problem = await Compare(file, expected);
        }
        catch (Exception e) when (gap is not null || expected["error"] is not null)
        {
            problem = $"{e.GetType().Name}: {e.Message}";
        }

        if (gap is not null)
        {
            problem.Should().NotBeNull($"{file} works now, remove it from KnownGaps ({gap})");
            problem.Should().Contain(Path.GetFileName(file), "the error must say which file failed");
            Assert.Ignore($"Known gap: {gap}. Now: {problem}");
        }
        if (expected["error"] is { } error)
        {
            // pyarrow can't read it either. DataLake may do better; nothing to compare with.
            Assert.Ignore($"pyarrow: {error}. DataLake: {problem ?? "reads it"}");
        }
        problem.Should().BeNull();
    }

    private static async Task<string?> Compare(string file, JsonNode expected)
    {
        var head = expected["head"]?.AsArray() ?? [];
        var rows = 0;
        await foreach (var row in UntypedReader.ReadAsync(LocalStorage.Instance, Path.Combine(Data, file), new FilesOptions().Formats(method => method)))
        {
            if (rows < head.Count)
            {
                var want = head[rows]!.AsObject();
                foreach (var (column, value) in want)
                {
                    if (!row.TryGetValue(column, out var actual)) return $"row {rows}: no column '{column}'";
                    var got = Canonical.Of(actual);
                    if (!Canonical.Same(value, got))
                        return $"row {rows}, column '{column}': expected {value?.ToJsonString() ?? "null"}, got {got?.ToJsonString() ?? "null"}";
                }
            }
            rows++;
        }
        var count = expected["rows"]?.GetValue<int>();
        return count is null || rows == count ? null : $"{rows} rows, expected {count}";
    }
}

/// <summary>DataLake values in the canonical JSON form of scripts/expected.py.</summary>
internal static class Canonical
{
    public static JsonNode? Of(object? value) => value switch
    {
        null => null,
        bool b => JsonValue.Create(b),
        string s => JsonValue.Create(s),
        ReadOnlyMemory<char> m => JsonValue.Create(m.ToString()),
        byte[] bytes => JsonValue.Create("base64:" + Convert.ToBase64String(bytes)),
        float f => Number(f),
        double d => Number(d),
        Half h => Number((double)h),
        decimal m => JsonValue.Create(Decimal(m)),
        sbyte or byte or short or ushort or int or uint or long => JsonValue.Create(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
        ulong u => JsonValue.Create(u),
        DateOnly d => JsonValue.Create(d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
        DateTime dt => JsonValue.Create(Timestamp(dt.Kind == DateTimeKind.Local ? dt.ToUniversalTime() : dt)),
        DateTimeOffset dto => JsonValue.Create(Timestamp(dto.UtcDateTime)),
        TimeOnly t => JsonValue.Create(Time(t.Ticks)),
        TimeSpan t => JsonValue.Create(Time(t.Ticks)),
        Guid g => JsonValue.Create(g.ToString("D")),
        // pyarrow reads INTERVAL as its 12 raw bytes: months, days, milliseconds, little-endian.
        Parquet.File.Values.Primitives.Interval i => JsonValue.Create("base64:" + Convert.ToBase64String([.. BitConverter.GetBytes(i.Months), .. BitConverter.GetBytes(i.Days), .. BitConverter.GetBytes(i.Millis)])),
        Dictionary<string, object?> record => new JsonObject(record.Select(kv => KeyValuePair.Create(kv.Key, Of(kv.Value)))),
        Dictionary<object, object?> map => new JsonArray(map.Select(kv => (JsonNode)new JsonArray(Of(kv.Key), Of(kv.Value))).ToArray()),
        IEnumerable list => new JsonArray(list.Cast<object?>().Select(Of).ToArray()),
        _ => JsonValue.Create($"{value.GetType().Name}:{value}"),
    };

    public static bool Same(JsonNode? expected, JsonNode? actual)
    {
        if (expected is null || actual is null) return expected is null && actual is null;
        switch (expected)
        {
            case JsonObject eo:
                return actual is JsonObject ao && eo.Count == ao.Count && eo.All(kv => ao.TryGetPropertyValue(kv.Key, out var v) && Same(kv.Value, v));
            case JsonArray ea:
                return actual is JsonArray aa && ea.Count == aa.Count && ea.Zip(aa).All(p => Same(p.First, p.Second));
        }
        var ev = (JsonValue)expected;
        if (actual is not JsonValue av) return false;
        if (ev.GetValueKind() == JsonValueKind.Number && av.GetValueKind() == JsonValueKind.Number)
        {
            var (e, a) = (ev.ToJsonString(), av.ToJsonString());
            if (long.TryParse(e, CultureInfo.InvariantCulture, out var el) && long.TryParse(a, CultureInfo.InvariantCulture, out var al)) return el == al;
            return double.Parse(e, CultureInfo.InvariantCulture).Equals(double.Parse(a, CultureInfo.InvariantCulture));
        }
        return JsonNode.DeepEquals(ev, av);
    }

    private static JsonNode Number(double d) =>
        double.IsNaN(d) ? JsonValue.Create("NaN")
        : double.IsPositiveInfinity(d) ? JsonValue.Create("Infinity")
        : double.IsNegativeInfinity(d) ? JsonValue.Create("-Infinity")
        : JsonValue.Create(d);

    private static string Decimal(decimal m)
    {
        var text = m.ToString("0.############################", CultureInfo.InvariantCulture);
        return text == "-0" ? "0" : text;
    }

    // Microseconds, like Python's datetime: .NET ticks are 100 ns, pyarrow output is cut to microseconds.
    private static string Timestamp(DateTime dt) =>
        new DateTime(dt.Ticks - dt.Ticks % 10).ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff", CultureInfo.InvariantCulture);

    private static string Time(long ticks) =>
        new TimeOnly(ticks - ticks % 10).ToString("HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
}
