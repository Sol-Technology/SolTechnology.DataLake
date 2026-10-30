using System.Linq.Expressions;
using SolTechnology.DataLake.Filtering;

namespace SolTechnology.DataLake.UnitTests;

// Pruning must never say False for a file or row group that has a matching row.
public class PredicateTests
{
    public sealed class Row
    {
        public long Id { get; set; }
        public decimal? Amount { get; set; }
        public string Country { get; set; } = "";
        public bool Paid { get; set; }
    }

    private static Tri Evaluate(Expression<Func<Row, bool>> filter, ColumnFacts? facts) =>
        PredicateTranslator.Translate(filter).Evaluate(_ => facts);

    private static ColumnFacts.Range Range(object? min, object? max, long nulls = 0, long rows = 100) => new(min, max, nulls, rows);

    [Test]
    public void Comparisons_against_min_max()
    {
        var ids = Range(10L, 20L);

        Evaluate(r => r.Id == 5, ids).Should().Be(Tri.False);
        Evaluate(r => r.Id == 15, ids).Should().Be(Tri.Unknown);
        Evaluate(r => r.Id < 10, ids).Should().Be(Tri.False);
        Evaluate(r => r.Id <= 10, ids).Should().Be(Tri.Unknown);
        Evaluate(r => r.Id < 21, ids).Should().Be(Tri.True);
        Evaluate(r => r.Id > 20, ids).Should().Be(Tri.False);
        Evaluate(r => r.Id >= 10, ids).Should().Be(Tri.True);
        Evaluate(r => 25 > r.Id, ids).Should().Be(Tri.True, "constant on the left flips the operator");
        Evaluate(r => r.Id != 15, Range(15L, 15L)).Should().Be(Tri.False);
    }

    [Test]
    public void Nulls_behave_like_CSharp()
    {
        var someNulls = Range(1m, 9m, nulls: 5);
        var allNull = Range(null, null, nulls: 100);

        Evaluate(r => r.Amount > 0m, someNulls).Should().Be(Tri.Unknown, "rows with null fail the comparison");
        Evaluate(r => r.Amount > 0m, allNull).Should().Be(Tri.False);
        Evaluate(r => r.Amount != 5m, allNull).Should().Be(Tri.True, "null != 5 is true");
        Evaluate(r => r.Amount == null, allNull).Should().Be(Tri.True);
        Evaluate(r => r.Amount == null, Range(1m, 9m)).Should().Be(Tri.False);
        Evaluate(r => r.Amount != null, someNulls).Should().Be(Tri.Unknown);
    }

    [Test]
    public void Logic_is_three_valued()
    {
        var pl = new ColumnFacts.Exact("PL");

        Evaluate(r => r.Country == "PL" || r.Id % 2 == 0, pl).Should().Be(Tri.True);
        Evaluate(r => r.Country == "DE" && r.Id % 2 == 0, pl).Should().Be(Tri.False);
        Evaluate(r => r.Country == "DE" || r.Id % 2 == 0, pl).Should().Be(Tri.Unknown);
        Evaluate(r => !(r.Country == "PL"), pl).Should().Be(Tri.False);
        Evaluate(r => new[] { "DE", "FR" }.Contains(r.Country), pl).Should().Be(Tri.False);
        Evaluate(r => new[] { "DE", "PL" }.Contains(r.Country), pl).Should().Be(Tri.True);
    }

    [Test]
    public void Unknown_facts_or_expressions_never_prune()
    {
        Evaluate(r => r.Id == 5, null).Should().Be(Tri.Unknown);
        Evaluate(r => r.Country.StartsWith('P'), new ColumnFacts.Exact("DE")).Should().Be(Tri.Unknown);
        Evaluate(r => r.Id == 5, Range(null, null, nulls: 0)).Should().Be(Tri.Unknown, "no min/max");
        Evaluate(r => r.Paid, new ColumnFacts.Exact(false)).Should().Be(Tri.False);
    }
}
