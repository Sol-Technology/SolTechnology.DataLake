using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Images;

namespace SolTechnology.DataLake.IntegrationTests.Containers;

/// <summary>
/// PyIceberg as the reference implementation (image from Docker/pyiceberg). Scripts write tables through the
/// REST catalog and dump the rows PyIceberg reads to /out, which <see cref="RunAsync"/> returns.
/// </summary>
public sealed class PyIcebergFixture(TestContainersContext context)
{
    private IFutureDockerImage? _image;

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        _image = new ImageFromDockerfileBuilder()
            .WithDockerfileDirectory(Path.Combine(AppContext.BaseDirectory, "Docker", "pyiceberg"))
            .WithDockerfile("Dockerfile")
            .WithName("soltech-datalake-pyiceberg:0.10.0")
            .WithCleanUp(false)
            .Build();
        await _image.CreateAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Runs a script to completion and returns /out/<paramref name="outputFile"/>.</summary>
    public async Task<string> RunAsync(string outputFile, string[] command, CancellationToken ct = default)
    {
        var image = _image ?? throw new InvalidOperationException("Call InitializeAsync first.");
        await using var container = new ContainerBuilder(image).WithNetwork(context.Network).WithCommand(command).Build();
        await OneShot.RunAsync(container, ct).ConfigureAwait(false);
        return Encoding.UTF8.GetString(await container.ReadFileAsync("/out/" + outputFile, ct).ConfigureAwait(false));
    }
}
