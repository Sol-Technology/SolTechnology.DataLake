using System.Text.Json;

namespace SolTechnology.DataLake.CompatibilityTests;

/// <summary>Real files from other writers read into classes, the way applications use DataLake. Each test opens one file.</summary>
public class TypedTests
{
    private static readonly string Data = Path.Combine(AppContext.BaseDirectory, "data");
    private static readonly DataLakeOptions Snake = new() { ColumnNaming = JsonNamingPolicy.SnakeCaseLower };

    public sealed class TreeNode
    {
        public int Id { get; set; }
        public double Prediction { get; set; }
        public double[]? ImpurityStats { get; set; }
        public Split? Split { get; set; }
    }

    public sealed class Split
    {
        public int FeatureIndex { get; set; }
        public List<double>? LeftCategoriesOrThreshold { get; set; }
        public int NumCategories { get; set; }
    }

    [Test]
    public async Task Two_level_lists_from_spark_1_x()
    {
        // parquet-mr 1.8: optional group impurityStats (LIST) { repeated double array; }, also inside a struct.
        var nodes = await Dataset.Open<TreeNode>(Path.Combine(Data, "parquet-dotnet", "legacy-list-onearray.parquet")).ReadAsync().ToListAsync();

        nodes.Should().HaveCount(7);
        nodes[0].ImpurityStats.Should().HaveCount(24).And.StartWith([24.0, 11.0, 9.0]);
        nodes[0].Split!.LeftCategoriesOrThreshold.Should().Equal(0.0);
        nodes[0].Split!.NumCategories.Should().Be(3);
        nodes[6].Split!.LeftCategoriesOrThreshold.Should().BeEmpty();
    }

    public sealed class ProtoMessage
    {
        public int DataRequired32 { get; set; }
        public long DataRequired64 { get; set; }
        public string? DataOptionalMissing { get; set; }
        public string? DataOptionalExisting { get; set; }
        public int[] DataRepeated { get; set; } = [];
        public List<Group> DataGroup { get; set; } = [];
        public List<string> DataList { get; set; } = [];
    }

    public sealed class Group
    {
        public string Nested { get; set; } = "";
    }

    [Test]
    public async Task Protobuf_style_repeated_group_is_a_list_of_structs()
    {
        // parquet-dotnet#681, parquet-mr 1.15: repeated group data_group { required binary nested (UTF8); } with no LIST annotation.
        var messages = await Dataset.Open<ProtoMessage>(Path.Combine(Data, "parquet-dotnet", "issues_681_nested.parquet"), Snake).ReadAsync().ToListAsync();

        var message = messages.Should().ContainSingle().Subject;
        message.DataRequired32.Should().Be(32);
        message.DataRequired64.Should().Be(64);
        message.DataOptionalMissing.Should().BeNull();
        message.DataOptionalExisting.Should().Be("hello");
        message.DataRepeated.Should().Equal(10, 20);
        message.DataGroup.Should().ContainSingle().Which.Nested.Should().Be("nested!");
        message.DataList.Should().Equal("element1", "element2");
    }

    public sealed class JsonRow
    {
        public string? JsonField { get; set; }
    }

    public sealed class Nation
    {
        public int NNationkey { get; set; }
        public byte[]? NName { get; set; }
    }

    [Test]
    public async Task Binary_columns_into_byte_arrays()
    {
        // Impala writes text as plain binary, without the UTF8 annotation.
        var nations = await Dataset.Open<Nation>(Path.Combine(Data, "parquet-dotnet", "real_nation.impala.parquet"), Snake).ReadAsync().ToListAsync();

        nations.Should().HaveCount(25);
        System.Text.Encoding.UTF8.GetString(nations[0].NName!).Should().Be("ALGERIA");
        System.Text.Encoding.UTF8.GetString(nations[24].NName!).Should().Be("UNITED STATES");
    }

    [Test]
    public async Task Json_columns_read_as_text()
    {
        var rows = await Dataset.Open<JsonRow>(Path.Combine(Data, "parquet-testing", "json.parquet"), Snake).ReadAsync().ToListAsync();

        rows.Should().HaveCount(4);
        rows[0].JsonField.Should().Be("""{"a":1}""");
    }
}
