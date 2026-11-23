using System.CommandLine;
using System.Globalization;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using SolTechnology.DataLake.Formats;
using SolTechnology.DataLake.Partitions;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.Cli;

/// <summary><c>datalake</c>: look into a dataset from the command line. Exit codes: 0 success, 2 an error.</summary>
public static class Program
{
    internal const int Success = 0;
    internal const int Error = 2;

    private static readonly JsonSerializerOptions Json = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    /// <summary>Every format there is a package for: Parquet, Avro, Arrow IPC.</summary>
    internal static readonly FileFormats Formats = new FilesOptions().Avro().Arrow().Formats(method => $"update datalake, it should call {method}");

    /// <summary>Entry point.</summary>
    public static Task<int> Main(string[] args) => RunAsync(args, Console.Out, Console.Error);

    internal static async Task<int> RunAsync(string[] args, TextWriter output, TextWriter error)
    {
        try
        {
            return await BuildCommand(output).Parse(args).InvokeAsync(new InvocationConfiguration { Output = output, Error = error, EnableDefaultExceptionHandler = false });
        }
        catch (Exception e)
        {
            await error.WriteLineAsync($"{e.GetType().Name}: {e.Message}");
            return Error;
        }
    }

    internal static RootCommand BuildCommand(TextWriter output)
    {
        var root = new RootCommand("SolTechnology.DataLake on the command line. Locations: a local path, s3://bucket/prefix, abfss://container@account.dfs.core.windows.net/path.");
        root.Subcommands.Add(LsCommand(output));
        root.Subcommands.Add(SchemaCommand(output));
        root.Subcommands.Add(HeadCommand(output));
        root.Subcommands.Add(CatCommand(output));
        root.Subcommands.Add(StatsCommand(output));
        return root;
    }

    // datalake ls s3://shop/orders/
    private static Command LsCommand(TextWriter output)
    {
        var location = LocationArgument();
        var (s3Endpoint, azureConnection) = StorageOptions();
        var command = new Command("ls", "Data files of a dataset with their partitions and sizes. Hidden files (_SUCCESS, .crc, _temporary/) are left out.")
            { location, s3Endpoint, azureConnection };
        command.SetAction(async (parse, ct) =>
        {
            var storage = StorageFor(parse.GetValue(location)!, parse.GetValue(s3Endpoint), parse.GetValue(azureConnection));
            var files = await DatasetFiles.ListAsync(storage, parse.GetValue(location)!, Formats, ct);
            foreach (var (file, format) in files)
                await output.WriteLineAsync($"{Size(file.Length),10}  {format.Name,-7}  {file.RelativePath}");
            await output.WriteLineAsync($"{files.Count} files, {Size(files.Sum(f => f.File.Length))}");
            return Success;
        });
        return command;
    }

    // datalake schema s3://shop/orders/
    private static Command SchemaCommand(TextWriter output)
    {
        var location = LocationArgument();
        var (s3Endpoint, azureConnection) = StorageOptions();
        var command = new Command("schema", "Columns of the first data file, and the partition columns from its path.") { location, s3Endpoint, azureConnection };
        command.SetAction(async (parse, ct) =>
        {
            var storage = StorageFor(parse.GetValue(location)!, parse.GetValue(s3Endpoint), parse.GetValue(azureConnection));
            var (file, format) = (await DatasetFiles.ListAsync(storage, parse.GetValue(location)!, Formats, ct)).FirstOrDefault();
            if (file is null) throw new FileNotFoundException($"No Parquet, Avro or Arrow files under '{parse.GetValue(location)}'.");
            await output.WriteLineAsync($"# {file.RelativePath}");

            await using var stream = new RangeStream(storage, file);
            foreach (var line in await format.DescribeSchemaAsync(stream, file.Location, ct))
                await output.WriteLineAsync(line);

            var partitions = HivePartitions.Parse(file.RelativePath);
            if (partitions.Count > 0)
                await output.WriteLineAsync($"# partitions from the path: {string.Join(", ", partitions.Keys)}");
            return Success;
        });
        return command;
    }

    // datalake head s3://shop/orders/ -n 5
    private static Command HeadCommand(TextWriter output)
    {
        var location = LocationArgument();
        var count = new Option<int>("--count", "-n") { Description = "Number of rows.", DefaultValueFactory = _ => 10 };
        var (s3Endpoint, azureConnection) = StorageOptions();
        var command = new Command("head", "First rows as JSON, one object per line, partition values included.") { location, count, s3Endpoint, azureConnection };
        command.SetAction(async (parse, ct) =>
        {
            var storage = StorageFor(parse.GetValue(location)!, parse.GetValue(s3Endpoint), parse.GetValue(azureConnection));
            var left = parse.GetValue(count);
            if (left <= 0) return Success;
            await foreach (var row in UntypedReader.ReadAsync(storage, parse.GetValue(location)!, Formats, ct))
            {
                await output.WriteLineAsync(ToJson(row)!.ToJsonString(Json));
                if (--left == 0) break;
            }
            return Success;
        });
        return command;
    }

    // datalake cat s3://shop/orders/ --format csv
    private static Command CatCommand(TextWriter output)
    {
        var location = LocationArgument();
        var format = new Option<string>("--format", "-f") { Description = "json (one object per line) or csv.", DefaultValueFactory = _ => "json" };
        format.AcceptOnlyFromAmong("json", "csv");
        var limit = new Option<long?>("--limit") { Description = "Stop after this many rows." };
        var (s3Endpoint, azureConnection) = StorageOptions();
        var command = new Command("cat", "Every row, partition values included: JSON lines, or CSV with a header (nested values as JSON).")
            { location, format, limit, s3Endpoint, azureConnection };
        command.SetAction(async (parse, ct) =>
        {
            var storage = StorageFor(parse.GetValue(location)!, parse.GetValue(s3Endpoint), parse.GetValue(azureConnection));
            var left = parse.GetValue(limit) ?? long.MaxValue;
            var csv = parse.GetValue(format) == "csv";
            List<string>? header = null;
            if (left <= 0) return Success;
            await foreach (var row in UntypedReader.ReadAsync(storage, parse.GetValue(location)!, Formats, ct))
            {
                if (!csv)
                {
                    await output.WriteLineAsync(ToJson(row)!.ToJsonString(Json));
                }
                else
                {
                    if (header is null)
                    {
                        header = [.. row.Keys];
                        await output.WriteLineAsync(string.Join(',', header.Select(Csv)));
                    }
                    await output.WriteLineAsync(string.Join(',', header.Select(c => Csv(row.TryGetValue(c, out var v) ? Text(v) : ""))));
                }
                if (--left == 0) break;
            }
            return Success;
        });
        return command;
    }

    // datalake stats s3://shop/orders/
    private static Command StatsCommand(TextWriter output)
    {
        var location = LocationArgument();
        var (s3Endpoint, azureConnection) = StorageOptions();
        var command = new Command("stats", "Files, rows, row groups and partitions of a dataset; per column the null count and min / max from Parquet statistics.")
            { location, s3Endpoint, azureConnection };
        command.SetAction(async (parse, ct) =>
        {
            var storage = StorageFor(parse.GetValue(location)!, parse.GetValue(s3Endpoint), parse.GetValue(azureConnection));
            var stats = await DatasetStats.CollectAsync(storage, parse.GetValue(location)!, ct);
            await stats.WriteAsync(output);
            return Success;
        });
        return command;
    }

    private static string Text(object? value) => value switch
    {
        null => "",
        string s => s,
        _ => ToJson(value) switch
        {
            JsonValue v when v.TryGetValue<string>(out var s) => s,
            var node => node?.ToJsonString(Json) ?? "",
        },
    };

    private static string Csv(string value) =>
        value.IndexOfAny([',', '"', '\n', '\r']) >= 0 ? $"\"{value.Replace("\"", "\"\"")}\"" : value;

    private static Argument<string> LocationArgument() =>
        new("location") { Description = "Dataset location: a local path, s3://bucket/prefix, abfss://container@account.dfs.core.windows.net/path, az://container/path." };

    private static (Option<string?> S3Endpoint, Option<string?> AzureConnection) StorageOptions() => (
        new Option<string?>("--s3-endpoint") { Description = "S3-compatible endpoint, e.g. http://localhost:9000 for MinIO. Credentials and region come from the default AWS chain." },
        new Option<string?>("--azure-connection-string") { Description = "Azure Storage connection string. Default: AZURE_STORAGE_CONNECTION_STRING, else DefaultAzureCredential." });

    internal static ILakeStorage StorageFor(string location, string? s3Endpoint, string? azureConnection) =>
        StorageResolver.For(location, s3Endpoint, azureConnection ?? Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING"));

    internal static string Size(long bytes) => bytes switch
    {
        < 1024 => $"{bytes} B",
        < 1024 * 1024 => string.Create(CultureInfo.InvariantCulture, $"{bytes / 1024.0:0.0} KB"),
        < 1024L * 1024 * 1024 => string.Create(CultureInfo.InvariantCulture, $"{bytes / 1024.0 / 1024:0.0} MB"),
        _ => string.Create(CultureInfo.InvariantCulture, $"{bytes / 1024.0 / 1024 / 1024:0.0} GB"),
    };

    internal static JsonNode? ToJson(object? value) => value switch
    {
        null => null,
        IDictionary<string, object?> record => new JsonObject(record.Select(kv => KeyValuePair.Create(kv.Key, ToJson(kv.Value)))),
        System.Collections.IDictionary map => new JsonObject(Entries(map).Select(e => KeyValuePair.Create(Convert.ToString(e.Key, CultureInfo.InvariantCulture)!, ToJson(e.Value)))),
        byte[] bytes => JsonValue.Create(Convert.ToBase64String(bytes)),
        System.Collections.IEnumerable items and not string => new JsonArray(items.Cast<object?>().Select(ToJson).ToArray()),
        DateTime dt => JsonValue.Create(dt.ToString(dt.Kind == DateTimeKind.Utc ? "yyyy-MM-ddTHH:mm:ss.FFFFFFFK" : "yyyy-MM-ddTHH:mm:ss.FFFFFFF", CultureInfo.InvariantCulture)),
        _ => JsonSerializer.SerializeToNode(value, value.GetType(), Json),
    };

    private static IEnumerable<System.Collections.DictionaryEntry> Entries(System.Collections.IDictionary map)
    {
        foreach (System.Collections.DictionaryEntry entry in map) yield return entry;
    }
}
