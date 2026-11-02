using System.Reflection;
using SolTechnology.Avro;

namespace SolTechnology.DataLake.UnitTests;

// AvroConvert comes under the DataLake licence, so DataLake users do not see the AvroConvert licence notice.
[NonParallelizable]
public class AvroLicenceTests
{
    public sealed class IdOnly
    {
        public long Id { get; set; }
    }

    [Test]
    public async Task Reading_Avro_shows_no_AvroConvert_licence_notice()
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
            await DataLake.Open<IdOnly>(lake.Root).ReadAsync().ToListAsync();
            var afterDataLake = stderr.ToString();

            ResetNotice();
            AvroConvert.Serialize(new IdOnly { Id = 1 });

            afterDataLake.Should().NotContain("[AvroConvert]");
            stderr.ToString().Should().Contain("[AvroConvert]", "AvroConvert used without DataLake still shows its notice");
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
