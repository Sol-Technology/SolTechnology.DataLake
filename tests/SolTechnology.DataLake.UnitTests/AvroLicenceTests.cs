using System.Reflection;
using SolTechnology.Avro;

namespace SolTechnology.DataLake.UnitTests;

// Avro in DataLake is read by AvroConvert under AvroConvert's own licence, so its licence notice shows as for any AvroConvert user.
[NonParallelizable]
public class AvroLicenceTests
{
    public sealed class IdOnly
    {
        public long Id { get; set; }
    }

    [Test]
    public async Task Reading_Avro_shows_the_AvroConvert_licence_notice()
    {
        using var lake = new TempDataset();
        File.Copy(Path.Combine(AppContext.BaseDirectory, "Fixtures", "orders.avro"), lake.PathOf("orders.avro"));
        var previousLicense = AvroConvert.License;
        var previousError = Console.Error;
        var stderr = new StringWriter();
        try
        {
            AvroConvert.License = null;
            Console.SetError(stderr);

            ResetNotice();
            await Dataset.Open<IdOnly>(lake.Root, new DataLakeOptions { Files = f => f.Avro() }).ReadAsync().ToListAsync();

            stderr.ToString().Should().Contain("[AvroConvert]", "DataLake does not cover AvroConvert's licence");
        }
        finally
        {
            Console.SetError(previousError);
            AvroConvert.License = previousLicense;
            ResetNotice();
        }
    }

    private static void ResetNotice() =>
        typeof(AvroConvert).GetMethod("ResetLicenseNoticeForTesting", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, null);
}
