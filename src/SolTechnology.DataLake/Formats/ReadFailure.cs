namespace SolTechnology.DataLake.Formats;

/// <summary>
/// Errors from inside the Parquet reader (corrupt pages, unsupported encodings, values out of .NET range) come without
/// a file name. A scan over a thousand files must say which file and column failed.
/// </summary>
internal static class ReadFailure
{
    public static async Task<TResult> Guard<TResult>(Func<Task<TResult>> read, string location, string? column)
    {
        try
        {
            return await read().ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            throw Of(e, location, column);
        }
    }

    public static async Task Guard(Func<Task> read, string location, string? column)
    {
        try
        {
            await read().ConfigureAwait(false);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            throw Of(e, location, column);
        }
    }

    public static TResult Guard<TResult>(Func<TResult> read, string location)
    {
        try
        {
            return read();
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            throw Of(e, location, null);
        }
    }

    private static InvalidDataException Of(Exception e, string location, string? column) =>
        new(column is null ? $"Can't read {location}: {e.Message}" : $"Can't read column '{column}' of {location}: {e.Message}", e);
}
