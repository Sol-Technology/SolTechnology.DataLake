using System.Net.Http.Json;
using System.Text.Json;

namespace IcebergSpike;

// Iceberg REST catalog, read side: config (prefix) and loadTable with vended credentials. No auth (Lakekeeper allowall).
public sealed class RestCatalog(HttpClient http, Uri baseUri, string warehouse)
{
    private string? _prefix;

    public async Task<LoadedTable> LoadTableAsync(string @namespace, string table, CancellationToken cancellationToken = default)
    {
        var prefix = await PrefixAsync(cancellationToken);
        using var request = new HttpRequestMessage(HttpMethod.Get,
            new Uri(baseUri, $"v1/{Uri.EscapeDataString(prefix)}/namespaces/{Uri.EscapeDataString(@namespace)}/tables/{Uri.EscapeDataString(table)}"));
        request.Headers.Add("X-Iceberg-Access-Delegation", "vended-credentials");

        using var response = await http.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();
        var root = await response.Content.ReadFromJsonAsync<JsonElement>(cancellationToken);

        var credentials = root.TryGetProperty("storage-credentials", out var list) && list.ValueKind == JsonValueKind.Array
            ? list.EnumerateArray().Select(c => new StorageCredential(c.GetProperty("prefix").GetString()!, Properties(c.GetProperty("config")))).ToList()
            : [];

        return new LoadedTable(
            root.GetProperty("metadata-location").GetString()!,
            root.GetProperty("metadata").Deserialize<TableMetadata>()!,
            root.TryGetProperty("config", out var config) ? Properties(config) : new Dictionary<string, string>(),
            credentials);
    }

    private async Task<string> PrefixAsync(CancellationToken cancellationToken)
    {
        if (_prefix is not null) return _prefix;
        var config = await http.GetFromJsonAsync<JsonElement>(new Uri(baseUri, $"v1/config?warehouse={Uri.EscapeDataString(warehouse)}"), cancellationToken);
        // Overrides win over defaults (REST spec, GET /v1/config).
        _prefix = Lookup(config, "overrides") ?? Lookup(config, "defaults") ?? "";
        return _prefix;

        static string? Lookup(JsonElement config, string section) =>
            config.TryGetProperty(section, out var s) && s.TryGetProperty("prefix", out var p) ? p.GetString() : null;
    }

    private static Dictionary<string, string> Properties(JsonElement element) =>
        element.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetString() ?? "");
}

public sealed record StorageCredential(string Prefix, IReadOnlyDictionary<string, string> Config);

public sealed record LoadedTable(
    string MetadataLocation,
    TableMetadata Metadata,
    IReadOnlyDictionary<string, string> Config,
    IReadOnlyList<StorageCredential> StorageCredentials)
{
    // storage-credentials (newer spec) win over the flat config; the longest matching prefix wins.
    public IReadOnlyDictionary<string, string> PropertiesFor(string location) =>
        StorageCredentials.Where(c => location.StartsWith(c.Prefix, StringComparison.Ordinal))
            .OrderByDescending(c => c.Prefix.Length)
            .Select(c => c.Config)
            .FirstOrDefault() ?? Config;
}
