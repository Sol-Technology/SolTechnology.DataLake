namespace SolTechnology.DataLake;

/// <summary>
/// Precision and scale of a <see cref="decimal"/> property in written Parquet files. Without it a decimal is written as
/// <c>decimal(38, 18)</c>. A value that does not fit (more fractional digits than <paramref name="scale"/>, or more integer
/// digits than <paramref name="precision"/> minus <paramref name="scale"/>) fails the write instead of being rounded.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class DecimalPrecisionAttribute(int precision, int scale) : Attribute
{
    /// <summary>Total number of digits, 1 to 38.</summary>
    public int Precision { get; } = precision is >= 1 and <= 38 ? precision : throw new ArgumentOutOfRangeException(nameof(precision), "1 to 38.");

    /// <summary>Digits after the decimal point, 0 to <see cref="Precision"/>.</summary>
    public int Scale { get; } = scale >= 0 && scale <= precision ? scale : throw new ArgumentOutOfRangeException(nameof(scale), "0 to precision.");
}
