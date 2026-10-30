namespace IcebergSpike;

public sealed record IcebergType(string Name, int Precision = 0, int Scale = 0)
{
    public static IcebergType Parse(string text)
    {
        if (text.StartsWith("decimal(", StringComparison.Ordinal))
        {
            var parts = text["decimal(".Length..^1].Split(',');
            return new IcebergType("decimal", int.Parse(parts[0].Trim()), int.Parse(parts[1].Trim()));
        }
        return new IcebergType(text);
    }
}
