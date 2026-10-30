using System.Text.Json;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake;

/// <summary>Options for <see cref="DataLake.Open{T}"/>.</summary>
public sealed class DataLakeOptions
{
    /// <summary>Storage for locations that are not local paths, e.g. <c>S3Storage</c> from SolTechnology.DataLake.Aws.</summary>
    public ILakeStorage? Storage { get; init; }

    /// <summary>
    /// How property names become column names when a property has no <c>[DataMember(Name = ...)]</c>,
    /// e.g. <see cref="JsonNamingPolicy.SnakeCaseLower"/> for <c>created_at</c>. Matching is case-insensitive either way.
    /// </summary>
    public JsonNamingPolicy? ColumnNaming { get; init; }
}
