using IcebergSpike;
using Testcontainers.Keycloak;

namespace SolTechnology.DataLake.IntegrationTests.Containers;

/// <summary>
/// Keycloak as the OAuth2 / OpenID provider of Lakekeeper. Realm <see cref="Realm"/> has one confidential client
/// with service accounts, used by the tests, PyIceberg and Trino (client credentials flow).
/// </summary>
/// <remarks>
/// <c>KC_HOSTNAME</c> pins the issuer to <see cref="InternalUrl"/>, so a token fetched from the host through the
/// mapped port has the same <c>iss</c> as one fetched inside the network, and Lakekeeper accepts both.
/// </remarks>
public sealed class KeycloakFixture(TestContainersContext context) : IAsyncDisposable
{
    private const string Image = "quay.io/keycloak/keycloak:26.8.1";
    public const string Realm = "datalake";
    public const string ClientId = "datalake-tests";
    public const string ClientSecret = "datalake-tests-secret";
    public const string Scope = "openid";
    public const string InternalUrl = "http://keycloak:8080";
    public const string InternalIssuer = $"{InternalUrl}/realms/{Realm}";
    public const string InternalTokenEndpoint = $"{InternalIssuer}/protocol/openid-connect/token";

    private KeycloakContainer? _container;

    private KeycloakContainer Container => _container ?? throw new InvalidOperationException("Call InitializeAsync first.");

    public Uri TokenEndpoint => new($"{Container.GetBaseAddress().TrimEnd('/')}/realms/{Realm}/protocol/openid-connect/token");

    /// <summary>Client credentials of the test client, token fetched from the host.</summary>
    public OAuth2ClientCredentials Credentials(HttpClient http) => new(http, TokenEndpoint, ClientId, ClientSecret, Scope);

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        var builder = new KeycloakBuilder(Image)
            .WithNetwork(context.Network)
            .WithNetworkAliases("keycloak")
            .WithEnvironment("KC_HOSTNAME", InternalUrl)
            .WithRealm(Path.Combine(AppContext.BaseDirectory, "Docker", "keycloak", "datalake-realm.json"));
        if (TestContainersContext.ReuseContainers) builder = builder.WithName("soltech-datalake-keycloak").WithReuse(true);

        _container = builder.Build();
        await _container.StartAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is not null && !TestContainersContext.ReuseContainers) await _container.DisposeAsync().ConfigureAwait(false);
    }
}
