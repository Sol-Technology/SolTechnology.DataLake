using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace IcebergSpike;

// OAuth2 client credentials against an external identity provider (Keycloak, Entra ID, Okta...), the way
// Polaris, Unity, Snowflake Open Catalog and Lakekeeper expect it. Token cached until shortly before it expires.
public sealed class OAuth2ClientCredentials(HttpClient http, Uri tokenEndpoint, string clientId, string clientSecret, string? scope = null)
{
    private string? _token;
    private DateTimeOffset _expires;

    public async ValueTask<string> GetTokenAsync(CancellationToken cancellationToken = default)
    {
        if (_token is not null && DateTimeOffset.UtcNow < _expires) return _token;

        var form = new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = clientId,
            ["client_secret"] = clientSecret,
        };
        if (scope is not null) form["scope"] = scope;

        using var response = await http.PostAsync(tokenEndpoint, new FormUrlEncodedContent(form), cancellationToken);
        if (!response.IsSuccessStatusCode)
            throw new HttpRequestException($"OAuth2 token request to {tokenEndpoint} failed: {(int)response.StatusCode} {await response.Content.ReadAsStringAsync(cancellationToken)}");

        var token = await response.Content.ReadFromJsonAsync<TokenResponse>(cancellationToken) ?? throw new InvalidDataException("Empty token response.");
        _token = token.AccessToken;
        _expires = DateTimeOffset.UtcNow.AddSeconds(Math.Max(0, token.ExpiresIn - 30));
        return _token;
    }

    private sealed record TokenResponse(
        [property: JsonPropertyName("access_token")] string AccessToken,
        [property: JsonPropertyName("expires_in")] int ExpiresIn);
}
