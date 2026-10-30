using SolTechnology.Avro;

namespace IcebergSpike;

[SetUpFixture]
public class Setup
{
    [OneTimeSetUp]
    public void DeclareLicense() => AvroConvert.License = AvroLicense.NonCommercial;
}

internal static class Fixtures
{
    public static string Root { get; } = FindRoot();

    public static string Warehouse => Path.Combine(Root, "warehouse");

    public static string Expected(string table) => Path.Combine(Root, "expected", table + ".json");

    // Table metadata written in Docker points to file:///warehouse/...
    public static string Resolve(string location) =>
        location.StartsWith("file:///warehouse/", StringComparison.Ordinal)
            ? Path.Combine(Warehouse, location["file:///warehouse/".Length..])
            : throw new NotSupportedException(location);

    private static string FindRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "spikes", "fixtures");
            if (Directory.Exists(candidate)) return candidate;
        }
        throw new DirectoryNotFoundException("spikes/fixtures");
    }
}
