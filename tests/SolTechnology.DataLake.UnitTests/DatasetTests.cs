using System.Runtime.Serialization;
using System.Text.Json;
using Parquet.Schema;

namespace SolTechnology.DataLake.UnitTests;

public class DatasetTests
{
    private static readonly DataField[] OrderColumns =
        [new DataField<long>("Id"), new DataField<decimal?>("Amount"), new DataField<string>("Customer")];

    private TempDataset _lake = null!;

    [SetUp]
    public void CreateLake() => _lake = new TempDataset();

    [TearDown]
    public void DeleteLake() => _lake.Dispose();

    public sealed class Order
    {
        public long Id { get; set; }
        public decimal? Amount { get; set; }
        public string? Customer { get; set; }
        public DateOnly Date { get; set; }
        public string Country { get; set; } = "";
    }

    // date=2026-10-01..03 / country=PL,DE : 6 files, 10 rows each, ids 0..59 in path order.
    private async Task WritePartitionedOrders()
    {
        var id = 0L;
        foreach (var day in new[] { "2026-10-01", "2026-10-02", "2026-10-03" })
        foreach (var country in new[] { "DE", "PL" })
        {
            var ids = Enumerable.Range(0, 10).Select(_ => id++).ToArray();
            await _lake.WriteAsync($"date={day}/country={country}/part-0.parquet", OrderColumns,
                [ids, ids.Select(i => i % 4 == 0 ? (decimal?)null : i * 1.5m).ToArray(), ids.Select(i => $"customer-{i % 3}").ToArray()]);
        }
    }

    [Test]
    public async Task Reads_every_row_with_partition_values_from_the_path()
    {
        await WritePartitionedOrders();

        var rows = await DataLake.Open<Order>(_lake.Root).ReadAsync().ToListAsync();

        rows.Should().HaveCount(60);
        rows.Select(r => r.Id).Should().Equal(Enumerable.Range(0, 60).Select(i => (long)i));
        rows[0].Should().BeEquivalentTo(new Order { Id = 0, Amount = null, Customer = "customer-0", Date = new DateOnly(2026, 10, 1), Country = "DE" });
        rows[25].Should().BeEquivalentTo(new Order { Id = 25, Amount = 37.5m, Customer = "customer-1", Date = new DateOnly(2026, 10, 2), Country = "DE" });
    }

    [Test]
    public async Task Filter_on_partitions_opens_only_matching_files()
    {
        await WritePartitionedOrders();
        var metrics = new ScanMetrics();
        var day = new DateOnly(2026, 10, 2);

        var rows = await DataLake.Open<Order>(_lake.Root).Where(o => o.Country == "PL" && o.Date == day).ReadAsync(metrics).ToListAsync();

        metrics.Files.Should().Be(6);
        metrics.FilesSkippedByPartition.Should().Be(5);
        metrics.RowsRead.Should().Be(10);
        rows.Select(r => r.Id).Should().Equal(Enumerable.Range(30, 10).Select(i => (long)i));
    }

    [Test]
    public async Task In_list_and_range_on_partitions()
    {
        await WritePartitionedOrders();
        var metrics = new ScanMetrics();
        var countries = new List<string> { "PL" };
        var from = new DateOnly(2026, 10, 2);

        var rows = await DataLake.Open<Order>(_lake.Root).Where(o => countries.Contains(o.Country)).Where(o => o.Date >= from).ReadAsync(metrics).ToListAsync();

        metrics.FilesSkippedByPartition.Should().Be(4);
        rows.Should().HaveCount(20).And.OnlyContain(r => r.Country == "PL" && r.Date >= from);
    }

    [Test]
    public async Task Array_Contains_works_although_CSharp_14_binds_it_to_spans()
    {
        await WritePartitionedOrders();
        var metrics = new ScanMetrics();

        var rows = await DataLake.Open<Order>(_lake.Root).Where(o => new[] { "PL" }.Contains(o.Country) && new[] { 3L, 13L }.Contains(o.Id)).ReadAsync(metrics).ToListAsync();

        metrics.FilesSkippedByPartition.Should().Be(3);
        rows.Select(r => r.Id).Should().Equal(13L);
    }

    [Test]
    public async Task Filter_on_a_column_skips_row_groups_by_statistics()
    {
        var groups = Enumerable.Range(0, 10).Select(g =>
        {
            var ids = Enumerable.Range(g * 100, 100).Select(i => (long)i).ToArray();
            return new Array[] { ids, ids.Select(i => (decimal?)i).ToArray(), ids.Select(i => $"c{i}").ToArray() };
        }).ToArray();
        await _lake.WriteAsync("orders.parquet", OrderColumns, groups);
        var metrics = new ScanMetrics();

        var rows = await DataLake.Open<Evolved>(_lake.Root).Where(o => o.Id >= 950).ReadAsync(metrics).ToListAsync();

        metrics.RowGroups.Should().Be(10);
        metrics.RowGroupsSkippedByStatistics.Should().Be(9);
        rows.Select(r => r.Id).Should().Equal(Enumerable.Range(950, 50).Select(i => (long)i));
    }

    [Test]
    public async Task Filters_the_pruning_can_not_use_still_filter_rows()
    {
        await WritePartitionedOrders();
        var metrics = new ScanMetrics();

        var rows = await DataLake.Open<Order>(_lake.Root).Where(o => o.Id % 7 == 0 && o.Customer!.EndsWith('1')).ReadAsync(metrics).ToListAsync();

        metrics.FilesSkippedByPartition.Should().Be(0);
        rows.Select(r => r.Id).Should().Equal(Enumerable.Range(0, 60).Where(i => i % 7 == 0 && i % 3 == 1).Select(i => (long)i));
    }

    [Test]
    public async Task Null_semantics_match_CSharp()
    {
        await WritePartitionedOrders();

        var nulls = await DataLake.Open<Order>(_lake.Root).Where(o => o.Amount == null).ReadAsync().ToListAsync();
        var notFifteen = await DataLake.Open<Order>(_lake.Root).Where(o => o.Amount != 15m).ReadAsync().ToListAsync();
        var greater = await DataLake.Open<Order>(_lake.Root).Where(o => o.Amount > 80m).ReadAsync().ToListAsync();

        nulls.Should().HaveCount(15).And.OnlyContain(r => r.Id % 4 == 0);
        notFifteen.Should().HaveCount(59, "null != 15 is true in C#");
        greater.Should().OnlyContain(r => r.Amount > 80m).And.HaveCount(Enumerable.Range(0, 60).Count(i => i % 4 != 0 && i * 1.5m > 80m));
    }

    public sealed class SnakeOrder
    {
        public long OrderId { get; set; }
        public string? CustomerName { get; set; }
        [DataMember(Name = "amt")] public decimal? Amount { get; set; }
    }

    [Test]
    public async Task Column_names_from_DataMember_naming_policy_and_case_insensitive()
    {
        await _lake.WriteAsync("x.parquet", [new DataField<long>("ORDER_ID"), new DataField<string>("customer_name"), new DataField<decimal?>("AMT")],
            [new long[] { 7 }, new string?[] { "Ann" }, new decimal?[] { 1.5m }]);

        var rows = await DataLake.Open<SnakeOrder>(_lake.Root, new DataLakeOptions { ColumnNaming = JsonNamingPolicy.SnakeCaseLower }).ReadAsync().ToListAsync();

        rows.Should().ContainSingle().Which.Should().BeEquivalentTo(new SnakeOrder { OrderId = 7, CustomerName = "Ann", Amount = 1.5m });
    }

    public sealed class Evolved
    {
        public long Id { get; set; }
        public string? Note { get; set; }
    }

    public sealed class Strict
    {
        public long Id { get; set; }
        public long Version { get; set; }
    }

    [Test]
    public async Task A_column_added_later_is_null_in_older_files_and_an_error_for_a_non_nullable_property()
    {
        await _lake.WriteAsync("a/old.parquet", [new DataField<long>("Id")], [new long[] { 1 }]);
        await _lake.WriteAsync("b/new.parquet", [new DataField<long>("Id"), new DataField<string>("Note"), new DataField<long>("Version")],
            [new long[] { 2 }, new string?[] { "added" }, new long[] { 3 }]);

        var rows = await DataLake.Open<Evolved>(_lake.Root).ReadAsync().ToListAsync();
        var strict = () => DataLake.Open<Strict>(_lake.Root).ReadAsync().ToListAsync().AsTask();

        rows.Should().BeEquivalentTo([new Evolved { Id = 1, Note = null }, new Evolved { Id = 2, Note = "added" }]);
        (await strict.Should().ThrowAsync<InvalidDataException>()).WithMessage("*old.parquet has no column 'Version'*nullable*");
    }

    public sealed class NotNullAmount
    {
        public decimal Amount { get; set; }
    }

    [Test]
    public async Task Null_in_a_column_read_into_a_non_nullable_property_is_an_error()
    {
        await _lake.WriteAsync("x.parquet", [new DataField<decimal?>("Amount")], [new decimal?[] { 1m, null }]);

        var read = () => DataLake.Open<NotNullAmount>(_lake.Root).ReadAsync().ToListAsync().AsTask();

        (await read.Should().ThrowAsync<InvalidDataException>()).WithMessage("*'Amount' has a null value*Decimal?*");
    }

    public enum Status { New, Paid }

    public sealed class Widened
    {
        public long Count { get; set; }
        public double? Ratio { get; set; }
        public Status Status { get; set; }
    }

    [Test]
    public async Task Widening_and_enums()
    {
        await _lake.WriteAsync("x.parquet", [new DataField<int>("Count"), new DataField<int?>("Ratio"), new DataField<string>("Status")],
            [new[] { 5 }, new int?[] { 2 }, new string?[] { "paid" }]);

        var row = (await DataLake.Open<Widened>(_lake.Root).ReadAsync().ToListAsync()).Single();

        row.Should().BeEquivalentTo(new Widened { Count = 5, Ratio = 2, Status = Status.Paid });
    }

    public sealed class Lossy
    {
        public int Id { get; set; }
    }

    [Test]
    public async Task Narrowing_is_an_error()
    {
        await _lake.WriteAsync("x.parquet", [new DataField<long>("Id")], [new long[] { 1 }]);

        var read = () => DataLake.Open<Lossy>(_lake.Root).ReadAsync().ToListAsync().AsTask();

        (await read.Should().ThrowAsync<NotSupportedException>()).WithMessage("*'Id' of type Int64 can't be read into Int32*");
    }

    public sealed class Event
    {
        public DateTimeOffset At { get; set; }
    }

    public sealed class LocalEvent
    {
        public DateTime At { get; set; }
    }

    [Test]
    public async Task UTC_timestamps_read_into_DateTimeOffset_local_ones_only_into_DateTime()
    {
        var at = new DateTime(2026, 10, 29, 12, 30, 0, DateTimeKind.Utc);
        await _lake.WriteAsync("utc/x.parquet", [new DateTimeDataField("At", Parquet.Schema.DateTimeFormat.Timestamp, isAdjustedToUTC: true)], [new[] { at }]);
        await _lake.WriteAsync("local/x.parquet", [new DateTimeDataField("At", Parquet.Schema.DateTimeFormat.Timestamp, isAdjustedToUTC: false)], [new[] { at }]);

        var utc = (await DataLake.Open<Event>(_lake.PathOf("utc")).ReadAsync().ToListAsync()).Single();
        var utcAsDateTime = (await DataLake.Open<LocalEvent>(_lake.PathOf("utc")).ReadAsync().ToListAsync()).Single();
        var local = (await DataLake.Open<LocalEvent>(_lake.PathOf("local")).ReadAsync().ToListAsync()).Single();
        var localAsOffset = () => DataLake.Open<Event>(_lake.PathOf("local")).ReadAsync().ToListAsync().AsTask();

        utc.At.Should().Be(new DateTimeOffset(at));
        utcAsDateTime.At.Kind.Should().Be(DateTimeKind.Utc);
        local.At.Should().Be(new DateTime(2026, 10, 29, 12, 30, 0)).And.Subject!.Value.Kind.Should().Be(DateTimeKind.Unspecified);
        (await localAsOffset.Should().ThrowAsync<NotSupportedException>()).WithMessage("*without a time zone*DateTime*");
    }

    [Test]
    public async Task Ignores_hidden_and_non_parquet_files()
    {
        await _lake.WriteAsync("date=2026-10-01/country=PL/part-0.parquet", OrderColumns, [new long[] { 1 }, new decimal?[] { 1m }, new string?[] { "a" }]);
        await _lake.WriteAsync("_temporary/0/part-0.parquet", OrderColumns, [new long[] { 2 }, new decimal?[] { 2m }, new string?[] { "b" }]);
        _lake.Touch("_SUCCESS");
        _lake.Touch("date=2026-10-01/country=PL/.part-0.parquet.crc");
        _lake.Touch("README.md");
        var metrics = new ScanMetrics();

        var rows = await DataLake.Open<Order>(_lake.Root).ReadAsync(metrics).ToListAsync();

        metrics.Files.Should().Be(1);
        rows.Should().ContainSingle().Which.Id.Should().Be(1);
    }

    [Test]
    public async Task Hive_default_partition_is_null()
    {
        await _lake.WriteAsync("country=__HIVE_DEFAULT_PARTITION__/part-0.parquet", [new DataField<long>("Id")], [new long[] { 1 }]);

        var nullable = await DataLake.Open<CountryRow>(_lake.Root).ReadAsync().ToListAsync();

        nullable.Should().ContainSingle().Which.Country.Should().BeNull();
    }

    public sealed class CountryRow
    {
        public long Id { get; set; }
        public string? Country { get; set; }
    }

    [Test]
    public void Location_without_storage_is_an_error()
    {
        var open = () => DataLake.Open<Order>("s3://bucket/orders/");

        open.Should().Throw<NotSupportedException>().WithMessage("*No storage for 's3://bucket/orders/'*SolTechnology.DataLake.Aws*");
    }
}
