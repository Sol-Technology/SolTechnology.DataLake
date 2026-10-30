using DotNet.Testcontainers.Containers;

namespace SolTechnology.DataLake.IntegrationTests.Containers;

/// <summary>Containers that do one job and exit (migrations, scripts).</summary>
internal static class OneShot
{
    public static async Task RunAsync(IContainer container, CancellationToken ct)
    {
        await container.StartAsync(ct).ConfigureAwait(false);
        var exitCode = await container.GetExitCodeAsync(ct).ConfigureAwait(false);
        if (exitCode == 0) return;

        var (stdout, stderr) = await container.GetLogsAsync(ct: ct).ConfigureAwait(false);
        throw new InvalidOperationException($"{container.Image.FullName} {string.Join(' ', container.Name)} exited with {exitCode}.\n{stdout}\n{stderr}");
    }
}
