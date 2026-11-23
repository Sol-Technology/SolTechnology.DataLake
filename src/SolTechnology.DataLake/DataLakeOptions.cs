using System.Text.Json;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake;

/// <summary>Options for <see cref="Dataset.Open{T}"/>. A <see cref="LakeContext"/> is configured with <see cref="LakeContextOptionsBuilder"/> instead.</summary>
public sealed class DataLakeOptions
{
    /// <summary>Storage for locations that are not local paths, e.g. <c>S3Storage</c> from SolTechnology.DataLake.Aws.</summary>
    public ILakeStorage? Storage { get; init; }

    /// <summary>
    /// How property names become column names when a property has no <c>[DataMember(Name = ...)]</c>,
    /// e.g. <see cref="JsonNamingPolicy.SnakeCaseLower"/> for <c>created_at</c>. Matching is case-insensitive either way.
    /// </summary>
    public JsonNamingPolicy? ColumnNaming { get; init; }

    /// <summary>
    /// The formats to read besides Parquet, e.g. <c>Files = f => f.Avro()</c> with SolTechnology.DataLake.Avro. The same
    /// <see cref="FilesOptions"/> as in <see cref="LakeContextOptionsBuilder.UseFiles"/>; Parquet write settings do not apply here.
    /// </summary>
    public Action<FilesOptions>? Files { get; init; }
}
