using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using SolTechnology.DataLake.Storage;

namespace SolTechnology.DataLake.UnitTests;

public class LakeContextTests
{
    public sealed class Order
    {
        public long Id { get; set; }
        public DateOnly Date { get; set; }
        public decimal OrderTotal { get; set; }
    }

    public interface IShopLake : IDataLake
    {
        IDataset<Order> Orders { get; }
    }

    public class ShopLake : LakeContext, IShopLake
    {
        public ShopLake(LakeContextOptions<ShopLake> options) : base(options) { }

        protected ShopLake(LakeContextOptions options) : base(options) { }

        public IDataset<Order> Orders => Dataset<Order>("orders");

        protected override void OnModelCreating(LakeModelBuilder model) =>
            model.Dataset<Order>("orders").PartitionBy(o => o.Date);
    }

    public interface IArchiveLake : IShopLake;

    // The same model on a second registration: a subclass, as in EF.
    public sealed class ArchiveLake(LakeContextOptions<ArchiveLake> options) : ShopLake(options), IArchiveLake;

    private static readonly Order[] Orders = [new() { Id = 1, Date = new DateOnly(2026, 10, 1), OrderTotal = 10m }, new() { Id = 2, Date = new DateOnly(2026, 10, 2), OrderTotal = 20m }];

    [Test]
    public async Task A_context_reads_and_writes_its_declared_datasets()
    {
        var lake = new ShopLake(LakeContextOptions.For<ShopLake>(o => o.UseInMemory()));

        await lake.Orders.AppendAsync(Orders);

        (await lake.Orders.Where(o => o.Date == new DateOnly(2026, 10, 2)).ReadAsync().ToListAsync()).Should().ContainSingle().Which.Id.Should().Be(2);
        lake.Orders.Should().BeSameAs(lake.Dataset<Order>("orders"));
    }

    [Test]
    public async Task Registered_contexts_are_singletons_and_resolve_through_their_interfaces()
    {
        var memory = new MemoryStorage();
        var services = new ServiceCollection()
            .AddSingleton(memory)
            .AddDataLake<IShopLake, ShopLake>((sp, o) => o.UseInMemory(sp.GetRequiredService<MemoryStorage>()))
            .BuildServiceProvider();

        var lake = services.GetRequiredService<IShopLake>();
        await lake.Orders.AppendAsync(Orders);

        lake.Should().BeSameAs(services.GetRequiredService<ShopLake>());
        (await services.GetRequiredService<IShopLake>().Orders.CountAsync()).Should().Be(2);
        memory.Locations.Should().HaveCount(2);
    }

    [Test]
    public async Task Two_contexts_side_by_side_keep_their_own_storage_and_options()
    {
        var services = new ServiceCollection()
            .AddDataLake<IShopLake, ShopLake>(o => o.UseInMemory())
            .AddDataLake<IArchiveLake, ArchiveLake>(o => o.UseInMemory().ColumnNaming(JsonNamingPolicy.SnakeCaseLower))
            .BuildServiceProvider();
        var shop = services.GetRequiredService<IShopLake>();
        var archive = services.GetRequiredService<IArchiveLake>();

        await shop.Orders.AppendAsync(Orders);
        await archive.Orders.AppendAsync([Orders[0]]);

        archive.Should().NotBeSameAs(shop);
        (await shop.Orders.CountAsync()).Should().Be(2);
        (await archive.Orders.ReadAsync().ToListAsync()).Should().ContainSingle().Which.OrderTotal.Should().Be(10m);
    }

    [Test]
    public async Task The_same_context_class_works_with_different_naming()
    {
        var memory = new MemoryStorage();
        var snake = new ShopLake(LakeContextOptions.For<ShopLake>(o => o.UseInMemory(memory).ColumnNaming(JsonNamingPolicy.SnakeCaseLower)));
        var plain = new ShopLake(LakeContextOptions.For<ShopLake>(o => o.UseInMemory(new MemoryStorage())));

        await snake.Orders.AppendAsync(Orders);
        await plain.Orders.AppendAsync(Orders);

        memory.Locations.Should().OnlyContain(l => l.Contains("/orders/date=2026-10-0"), "partition folders use the column name of the naming policy");
        (await snake.Orders.ReadAsync().Select(o => o.OrderTotal).ToListAsync()).Should().BeEquivalentTo([10m, 20m]);
        (await plain.Orders.ReadAsync().Select(o => o.OrderTotal).ToListAsync()).Should().BeEquivalentTo([10m, 20m]);
    }

    [Test]
    public void A_context_needs_exactly_one_provider()
    {
        FluentActions.Invoking(() => LakeContextOptions.For<ShopLake>(_ => { })).Should().Throw<InvalidOperationException>().WithMessage("*UseParquet*UseInMemory*");
        FluentActions.Invoking(() => LakeContextOptions.For<ShopLake>(o => o.UseInMemory().UseParquet("/data")))
            .Should().Throw<InvalidOperationException>().WithMessage("*one provider*second context class*");
        FluentActions.Invoking(() => LakeContextOptions.For<ShopLake>(o => o.UseParquet("s3://shop/")))
            .Should().Throw<ArgumentException>().WithMessage("*not a local path*S3Storage*");
    }

    [Test]
    public async Task EnsureCreated_has_nothing_to_do_for_folders()
    {
        var lake = new ShopLake(LakeContextOptions.For<ShopLake>(o => o.UseInMemory()));

        await lake.EnsureCreatedAsync();

        (await lake.Orders.CountAsync()).Should().Be(0);
    }

    [Test]
    public async Task A_read_only_storage_fails_on_write()
    {
        var lake = LakeContext.Create(o => o.UseParquet("ro://lake", new ReadOnlyStorage()), m => m.Dataset<Order>("orders"));

        var write = () => lake.Dataset<Order>("orders").AppendAsync(Orders);

        (await write.Should().ThrowAsync<NotSupportedException>()).WithMessage("ReadOnlyStorage is read-only.");
    }

    private sealed class ReadOnlyStorage : ILakeStorage
    {
        public bool CanHandle(string location) => location.StartsWith("ro://");
        public IAsyncEnumerable<LakeFile> ListAsync(string location, CancellationToken cancellationToken = default) => AsyncEnumerable.Empty<LakeFile>();
        public ValueTask<int> ReadAsync(LakeFile file, long offset, Memory<byte> buffer, CancellationToken cancellationToken = default) => ValueTask.FromResult(0);
    }
}
