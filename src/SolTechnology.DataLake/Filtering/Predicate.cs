using System.Collections;
using System.Linq.Expressions;

namespace SolTechnology.DataLake.Filtering;

internal enum Op { Eq, Ne, Lt, Le, Gt, Ge }

internal enum Tri { False, True, Unknown }

/// <summary>What is known about a column before reading rows: one exact value (a partition) or min/max statistics.</summary>
internal abstract record ColumnFacts
{
    public sealed record Exact(object? Value) : ColumnFacts;

    /// <summary>Statistics of a file or row group. Min and Max are null when unknown or when all values are null.</summary>
    public sealed record Range(object? Min, object? Max, long? NullCount, long RowCount) : ColumnFacts
    {
        public bool AllNull => NullCount == RowCount;
        public bool NoNulls => NullCount == 0;
    }
}

/// <summary>A filter as a tree the pruning can reason about. Parts it can't understand are <see cref="Opaque"/>.</summary>
internal abstract record Predicate
{
    public sealed record And(Predicate Left, Predicate Right) : Predicate;
    public sealed record Or(Predicate Left, Predicate Right) : Predicate;
    public sealed record Not(Predicate Inner) : Predicate;
    public sealed record Compare(string Property, Op Op, object? Value) : Predicate;
    public sealed record In(string Property, IReadOnlyList<object?> Values) : Predicate;
    public sealed record Opaque : Predicate;

    public static readonly Predicate Unknown = new Opaque();

    /// <summary>Properties the predicate looks at.</summary>
    public IEnumerable<string> Properties() => this switch
    {
        And a => a.Left.Properties().Concat(a.Right.Properties()),
        Or o => o.Left.Properties().Concat(o.Right.Properties()),
        Not n => n.Inner.Properties(),
        Compare c => [c.Property],
        In i => [i.Property],
        _ => [],
    };

    /// <summary>
    /// False only when no row can match, given the facts. Same null semantics as C#: <c>null == x</c> and
    /// <c>null &lt; x</c> are false, <c>null != x</c> is true.
    /// </summary>
    public Tri Evaluate(Func<string, ColumnFacts?> facts) => this switch
    {
        And a => AndTri(a.Left.Evaluate(facts), a.Right.Evaluate(facts)),
        Or o => OrTri(o.Left.Evaluate(facts), o.Right.Evaluate(facts)),
        Not n => NotTri(n.Inner.Evaluate(facts)),
        Compare c => CompareTri(facts(c.Property), c.Op, c.Value),
        In i => i.Values.Select(v => CompareTri(facts(i.Property), Op.Eq, v)).Aggregate(Tri.False, OrTri),
        _ => Tri.Unknown,
    };

    private static Tri CompareTri(ColumnFacts? facts, Op op, object? value)
    {
        try
        {
            return facts switch
            {
                ColumnFacts.Exact e => FromBool(Holds(e.Value, op, value)),
                ColumnFacts.Range r => RangeTri(r, op, value),
                _ => Tri.Unknown,
            };
        }
        catch (Exception e) when (e is ArgumentException or InvalidCastException or NotSupportedException)
        {
            return Tri.Unknown;
        }
    }

    private static Tri RangeTri(ColumnFacts.Range r, Op op, object? value)
    {
        if (value is null)
        {
            // x == null / x != null
            var isNull = r.AllNull ? Tri.True : r.NoNulls ? Tri.False : Tri.Unknown;
            return op switch { Op.Eq => isNull, Op.Ne => NotTri(isNull), _ => Tri.False };
        }

        if (r.AllNull) return op == Op.Ne ? Tri.True : Tri.False;
        if (r.Min is null || r.Max is null) return Tri.Unknown;

        var belowMin = Values.Compare(value, r.Min) < 0;
        var aboveMax = Values.Compare(value, r.Max) > 0;
        var single = Values.Compare(r.Min, r.Max) == 0 && Values.Compare(value, r.Min) == 0;

        // A row with null fails every comparison except !=.
        Tri Passes(bool allPass, bool nonePass) => nonePass ? Tri.False : allPass && r.NoNulls ? Tri.True : Tri.Unknown;

        return op switch
        {
            Op.Eq => Passes(single, belowMin || aboveMax),
            Op.Ne => single && r.NoNulls ? Tri.False : belowMin || aboveMax ? Tri.True : Tri.Unknown,
            Op.Lt => Passes(Values.Compare(r.Max, value) < 0, Values.Compare(r.Min, value) >= 0),
            Op.Le => Passes(Values.Compare(r.Max, value) <= 0, Values.Compare(r.Min, value) > 0),
            Op.Gt => Passes(Values.Compare(r.Min, value) > 0, Values.Compare(r.Max, value) <= 0),
            Op.Ge => Passes(Values.Compare(r.Min, value) >= 0, Values.Compare(r.Max, value) < 0),
            _ => Tri.Unknown,
        };
    }

    private static bool Holds(object? left, Op op, object? right)
    {
        if (left is null || right is null)
            return op switch { Op.Eq => left is null && right is null, Op.Ne => !(left is null && right is null), _ => false };
        var c = Values.Compare(left, right);
        return op switch { Op.Eq => c == 0, Op.Ne => c != 0, Op.Lt => c < 0, Op.Le => c <= 0, Op.Gt => c > 0, _ => c >= 0 };
    }

    private static Tri FromBool(bool b) => b ? Tri.True : Tri.False;
    private static Tri NotTri(Tri t) => t switch { Tri.True => Tri.False, Tri.False => Tri.True, _ => Tri.Unknown };
    private static Tri AndTri(Tri a, Tri b) => a == Tri.False || b == Tri.False ? Tri.False : a == Tri.True && b == Tri.True ? Tri.True : Tri.Unknown;
    private static Tri OrTri(Tri a, Tri b) => a == Tri.True || b == Tri.True ? Tri.True : a == Tri.False && b == Tri.False ? Tri.False : Tri.Unknown;
}

internal static class Values
{
    public static int Compare(object a, object b)
    {
        if (a.GetType() == b.GetType() && a is IComparable comparable) return comparable.CompareTo(b);
        if (IsNumber(a) && IsNumber(b)) return System.Convert.ToDecimal(a).CompareTo(System.Convert.ToDecimal(b));
        if (a is Enum && b is Enum && a.GetType() == b.GetType()) return ((IComparable)a).CompareTo(b);
        throw new NotSupportedException($"Can't compare {a.GetType().Name} with {b.GetType().Name}.");
    }

    private static bool IsNumber(object o) => o is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal;
}

/// <summary>LINQ expression to <see cref="Predicate"/>. Property access on the parameter, compared with anything that does not depend on it.</summary>
internal static class PredicateTranslator
{
    public static Predicate Translate(LambdaExpression lambda) => Visit(SpanContains.Rewrite(lambda.Body), lambda.Parameters[0]);

    private static Predicate Visit(Expression e, ParameterExpression p) => e switch
    {
        BinaryExpression { NodeType: ExpressionType.AndAlso or ExpressionType.And } b when b.Type == typeof(bool) => new Predicate.And(Visit(b.Left, p), Visit(b.Right, p)),
        BinaryExpression { NodeType: ExpressionType.OrElse or ExpressionType.Or } b when b.Type == typeof(bool) => new Predicate.Or(Visit(b.Left, p), Visit(b.Right, p)),
        UnaryExpression { NodeType: ExpressionType.Not } u => new Predicate.Not(Visit(u.Operand, p)),
        BinaryExpression b when ToOp(b.NodeType) is { } op => Comparison(b.Left, op, b.Right, p),
        MemberExpression m when m.Type == typeof(bool) && PropertyOf(m, p) is { } name => new Predicate.Compare(name, Op.Eq, true),
        MethodCallExpression call when Contains(call, p) is { } contains => contains,
        _ => Predicate.Unknown,
    };

    private static Predicate Comparison(Expression left, Op op, Expression right, ParameterExpression p)
    {
        if (PropertyOf(left, p) is { } l && Independent(right, p)) return new Predicate.Compare(l, op, Evaluate(right));
        if (PropertyOf(right, p) is { } r && Independent(left, p)) return new Predicate.Compare(r, Flip(op), Evaluate(left));
        return Predicate.Unknown;
    }

    // list.Contains(x.P), Enumerable.Contains(list, x.P), array.Contains(x.P)
    private static Predicate? Contains(MethodCallExpression call, ParameterExpression p)
    {
        if (call.Method.Name != nameof(Enumerable.Contains)) return null;
        var (collection, item) = call.Object is null && call.Arguments.Count == 2 ? (call.Arguments[0], call.Arguments[1])
            : call.Object is not null && call.Arguments.Count == 1 ? (call.Object, call.Arguments[0])
            : (null, null);
        if (collection is null || item is null || PropertyOf(item, p) is not { } name || !Independent(collection, p)) return null;
        return Evaluate(collection) is IEnumerable values and not string
            ? new Predicate.In(name, values.Cast<object?>().ToList())
            : null;
    }

    private static string? PropertyOf(Expression e, ParameterExpression p)
    {
        while (e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u) e = u.Operand;
        return e is MemberExpression { Member: System.Reflection.PropertyInfo prop } m && m.Expression == p ? prop.Name : null;
    }

    private static bool Independent(Expression e, ParameterExpression p)
    {
        var finder = new ParameterFinder(p);
        finder.Visit(e);
        return !finder.Found;
    }

    private static object? Evaluate(Expression e) => e is ConstantExpression c
        ? c.Value
        : Expression.Lambda<Func<object?>>(Expression.Convert(e, typeof(object))).Compile()();

    private static Op? ToOp(ExpressionType type) => type switch
    {
        ExpressionType.Equal => Op.Eq,
        ExpressionType.NotEqual => Op.Ne,
        ExpressionType.LessThan => Op.Lt,
        ExpressionType.LessThanOrEqual => Op.Le,
        ExpressionType.GreaterThan => Op.Gt,
        ExpressionType.GreaterThanOrEqual => Op.Ge,
        _ => null,
    };

    private static Op Flip(Op op) => op switch { Op.Lt => Op.Gt, Op.Le => Op.Ge, Op.Gt => Op.Lt, Op.Ge => Op.Le, _ => op };

    private sealed class ParameterFinder(ParameterExpression parameter) : ExpressionVisitor
    {
        public bool Found { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            Found |= node == parameter;
            return node;
        }
    }
}

/// <summary>
/// C# 14 binds <c>array.Contains(x)</c> to <c>MemoryExtensions.Contains(ReadOnlySpan&lt;T&gt;, T)</c> with an implicit
/// array-to-span conversion, which expression trees can't compile (InvalidProgramException). Rewrites it to
/// <c>Enumerable.Contains(array, x)</c>.
/// </summary>
internal sealed class SpanContains : ExpressionVisitor
{
    private static readonly SpanContains Instance = new();

    public static Expression Rewrite(Expression e) => Instance.Visit(e);

    public static Expression<Func<T, bool>> Rewrite<T>(Expression<Func<T, bool>> lambda) =>
        Expression.Lambda<Func<T, bool>>(Rewrite(lambda.Body), lambda.Parameters);

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.DeclaringType == typeof(MemoryExtensions) && node.Method.Name == nameof(MemoryExtensions.Contains)
            && node.Arguments.Count == 2 && ArrayBehindSpan(node.Arguments[0]) is { } array)
        {
            var element = array.Type.GetElementType()!;
            return Expression.Call(typeof(Enumerable), nameof(Enumerable.Contains), [element], Visit(array), Visit(node.Arguments[1]));
        }
        return base.VisitMethodCall(node);
    }

    private static Expression? ArrayBehindSpan(Expression e) => e switch
    {
        UnaryExpression { NodeType: ExpressionType.Convert, Operand.Type.IsArray: true } u => u.Operand,
        MethodCallExpression { Method.Name: "op_Implicit", Arguments: [{ Type.IsArray: true } a] } => a,
        _ => null,
    };
}
