using System.CommandLine;
using System.Globalization;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using Parquet;
using Parquet.Schema;
using SolTechnology.Avro;
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
            var files = await DatasetFiles.ListAsync(storage, parse.GetValue(location)!, ct);
            foreach (var file in files)
                await output.WriteLineAsync($"{Size(file.Length),10}  {DatasetFiles.FormatOf(file.RelativePath).ToString().ToLowerInvariant(),-7}  {file.RelativePath}");
            await output.WriteLineAsync($"{files.Count} files, {Size(files.Sum(f => f.Length))}");
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
            var file = (await DatasetFiles.ListAsync(storage, parse.GetValue(location)!, ct)).FirstOrDefault()
                       ?? throw new FileNotFoundException($"No Parquet or Avro files under '{parse.GetValue(location)}'.");
            await output.WriteLineAsync($"# {file.RelativePath}");

            await using var stream = new RangeStream(storage, file);
            if (DatasetFiles.FormatOf(file.RelativePath) == FileFormat.Avro)
            {
                await output.WriteLineAsync(JsonNode.Parse(AvroConvert.GetSchema(stream))!.ToJsonString(new JsonSerializerOptions { WriteIndented = true }));
            }
            else
            {
                await using var reader = await ParquetReader.CreateAsync(stream, leaveStreamOpen: true, cancellationToken: ct);
                foreach (var field in reader.Schema.Fields) await WriteField(output, field, "");
                await output.WriteLineAsync($"# {reader.RowGroupCount} row groups, written by {reader.Metadata?.CreatedBy}");
            }

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
            await foreach (var row in UntypedReader.ReadAsync(storage, parse.GetValue(location)!, ct))
            {
                await output.WriteLineAsync(ToJson(row)!.ToJsonString(Json));
                if (--left == 0) break;
            }
            return Success;
        });
        return command;
    }

    private static Argument<string> LocationArgument() =>
        new("location") { Description = "Dataset location: a local path, s3://bucket/prefix, abfss://container@account.dfs.core.windows.net/path, az://container/path." };

    private static (Option<string?> S3Endpoint, Option<string?> AzureConnection) StorageOptions() => (
        new Option<string?>("--s3-endpoint") { Description = "S3-compatible endpoint, e.g. http://localhost:9000 for MinIO. Credentials and region come from the default AWS chain." },
        new Option<string?>("--azure-connection-string") { Description = "Azure Storage connection string. Default: AZURE_STORAGE_CONNECTION_STRING, else DefaultAzureCredential." });

    internal static ILakeStorage StorageFor(string location, string? s3Endpoint, string? azureConnection) =>
        StorageResolver.For(location, s3Endpoint, azureConnection ?? Environment.GetEnvironmentVariable("AZURE_STORAGE_CONNECTION_STRING"));

    private static async Task WriteField(TextWriter output, Field field, string indent)
    {
        switch (field)
        {
            case DataField data:
                await output.WriteLineAsync($"{indent}{data.Name}: {TypeName(data)}{(data.IsArray ? "[]" : "")}{(data.IsNullable ? "" : " not null")}");
                break;
            case StructField structField:
                await output.WriteLineAsync($"{indent}{structField.Name}: struct{(structField.IsNullable ? "" : " not null")}");
                foreach (var child in structField.Fields) await WriteField(output, child, indent + "  ");
                break;
            case ListField list:
                await output.WriteLineAsync($"{indent}{list.Name}: list{(list.IsNullable ? "" : " not null")}");
                await WriteField(output, list.Item, indent + "  ");
                break;
            case MapField map:
                await output.WriteLineAsync($"{indent}{map.Name}: map{(map.IsNullable ? "" : " not null")}");
                await WriteField(output, map.Key, indent + "  ");
                await WriteField(output, map.Value, indent + "  ");
                break;
        }
    }

    private static string TypeName(DataField field)
    {
        var logical = field.SchemaElement?.LogicalType;
        if (logical?.TIMESTAMP is { } timestamp)
            return $"timestamp({Unit(timestamp.Unit)}{(timestamp.IsAdjustedToUTC ? ", UTC" : ", no zone")})";
        if (logical?.TIME is { } time) return $"time({Unit(time.Unit)})";
        if (field is DecimalDataField d) return $"decimal({d.Precision}, {d.Scale})";
        if (logical?.UUID is not null || field.ClrType == typeof(Guid)) return "uuid";
        if (field.ClrType == typeof(DateOnly) || logical?.DATE is not null) return "date";
        if (field.ClrType == typeof(ReadOnlyMemory<char>) || field.ClrType == typeof(string)) return "string";
        if (field.ClrType == typeof(byte[])) return "binary";
        return field.ClrType.Name.ToLowerInvariant();

        static string Unit(Parquet.Meta.TimeUnit? unit) =>
            unit?.NANOS is not null ? "nanos" : unit?.MICROS is not null ? "micros" : "millis";
    }

    private static string Size(long bytes) => bytes switch
    {
        < 1024 => $"{bytes} B",
        < 1024 * 1024 => string.Create(CultureInfo.InvariantCulture, $"{bytes / 1024.0:0.0} KB"),
        < 1024L * 1024 * 1024 => string.Create(CultureInfo.InvariantCulture, $"{bytes / 1024.0 / 1024:0.0} MB"),
        _ => string.Create(CultureInfo.InvariantCulture, $"{bytes / 1024.0 / 1024 / 1024:0.0} GB"),
    };

    private static JsonNode? ToJson(object? value) => value switch
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
