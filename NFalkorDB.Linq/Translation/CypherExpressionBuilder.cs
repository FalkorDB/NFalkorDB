using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using NFalkorDB.Linq.Mapping;

namespace NFalkorDB.Linq.Translation;

/// <summary>
/// Binds a lambda parameter to the pattern alias it stands for.
/// </summary>
internal sealed class AliasBinding
{
    internal AliasBinding(string alias, EntityMetadata metadata)
    {
        Alias = alias;
        Metadata = metadata;
    }

    internal string Alias { get; }

    internal EntityMetadata Metadata { get; }
}

/// <summary>
/// A rendered Cypher fragment together with the precedence of its outermost operator, so the
/// renderer only adds parentheses where they are actually needed.
/// </summary>
internal readonly struct CypherFragment
{
    internal CypherFragment(string text, int precedence)
    {
        Text = text;
        Precedence = precedence;
    }

    internal string Text { get; }

    internal int Precedence { get; }

    public override string ToString() => Text;
}

/// <summary>
/// Translates the body of a LINQ lambda into a Cypher expression. Every literal encountered is
/// bound as a query parameter rather than being written into the query text.
/// </summary>
internal sealed class CypherExpressionBuilder
{
    private const int PrecedenceOr = 1;
    private const int PrecedenceXor = 2;
    private const int PrecedenceAnd = 3;
    private const int PrecedenceNot = 4;
    private const int PrecedenceComparison = 5;
    private const int PrecedenceAdditive = 6;
    private const int PrecedenceMultiplicative = 7;
    private const int PrecedencePower = 8;
    private const int PrecedenceUnary = 9;
    private const int PrecedenceAtom = 10;

    private static readonly Dictionary<string, string> MathFunctions = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["Abs"] = "abs",
        ["Ceiling"] = "ceil",
        ["Floor"] = "floor",
        ["Round"] = "round",
        ["Sqrt"] = "sqrt",
        ["Exp"] = "exp",
        ["Log"] = "log",
        ["Log10"] = "log10",
        ["Sign"] = "sign",
        ["Sin"] = "sin",
        ["Cos"] = "cos",
        ["Tan"] = "tan",
        ["Asin"] = "asin",
        ["Acos"] = "acos",
        ["Atan"] = "atan",
        ["Atan2"] = "atan2"
    };

    private readonly ParameterBag _parameters;
    private readonly IReadOnlyDictionary<ParameterExpression, AliasBinding> _bindings;

    internal CypherExpressionBuilder(ParameterBag parameters, IReadOnlyDictionary<ParameterExpression, AliasBinding> bindings)
    {
        _parameters = parameters;
        _bindings = bindings;
    }

    /// <summary>
    /// Translates <paramref name="expression"/> into Cypher.
    /// </summary>
    internal string Translate(Expression expression) => Visit(expression).Text;

    /// <summary>
    /// Translates a boolean expression for use in a WHERE clause. A bare boolean property such as
    /// <c>p.IsActive</c> is a valid Cypher predicate on its own.
    /// </summary>
    /// <remarks>
    /// The result is rendered at <c>AND</c> precedence because successive <c>Where</c> calls are
    /// joined with <c>AND</c>. Without this, <c>.Where(a || b).Where(c)</c> would render
    /// <c>a OR b AND c</c>, which Cypher groups as <c>a OR (b AND c)</c>.
    /// </remarks>
    internal string TranslatePredicate(Expression expression) => Render(Visit(expression), PrecedenceAnd);

    private CypherFragment Visit(Expression expression)
    {
        expression = Unwrap(expression);

        switch (expression)
        {
            case ParameterExpression parameter:
                return Atom(ResolveBinding(parameter).Alias);

            case ConstantExpression constant:
                return Atom(_parameters.Add(constant.Value));

            case MemberExpression member:
                return VisitMember(member);

            case BinaryExpression binary:
                return VisitBinary(binary);

            case UnaryExpression unary:
                return VisitUnary(unary);

            case MethodCallExpression call:
                return VisitMethodCall(call);

            case ConditionalExpression conditional:
                return new CypherFragment(
                    "CASE WHEN " + Visit(conditional.Test).Text +
                    " THEN " + Visit(conditional.IfTrue).Text +
                    " ELSE " + Visit(conditional.IfFalse).Text + " END",
                    PrecedenceAtom);

            case NewArrayExpression newArray when newArray.NodeType == ExpressionType.NewArrayInit:
                return new CypherFragment(
                    "[" + string.Join(", ", newArray.Expressions.Select(e => Visit(e).Text).ToArray()) + "]",
                    PrecedenceAtom);
        }

        throw Unsupported(expression);
    }

    private CypherFragment VisitMember(MemberExpression member)
    {
        if (member.Expression != null)
        {
            var nullableUnderlying = Nullable.GetUnderlyingType(member.Expression.Type);

            if (nullableUnderlying != null)
            {
                if (member.Member.Name == "Value")
                {
                    return Visit(member.Expression);
                }

                if (member.Member.Name == "HasValue")
                {
                    return new CypherFragment(Render(Visit(member.Expression), PrecedenceComparison) + " IS NOT NULL", PrecedenceComparison);
                }
            }
        }

        if (member.Expression is ParameterExpression parameter && _bindings.ContainsKey(parameter))
        {
            var binding = ResolveBinding(parameter);
            var metadata = binding.Metadata;

            if (metadata.IdProperty != null && metadata.IdProperty.ClrName == member.Member.Name)
            {
                return Atom("id(" + binding.Alias + ")");
            }

            if (metadata.TryGetProperty(member.Member.Name, out var property))
            {
                return Atom(binding.Alias + "." + CypherIdentifier.Escape(property.GraphName));
            }

            if (metadata.TryGetNavigation(member.Member.Name, out _))
            {
                throw new NotSupportedException(
                    $"Navigation property '{metadata.ClrType.Name}.{member.Member.Name}' cannot be used inside an expression. Traverse it with SelectMany or Traverse<TEdge, TTarget>() instead.");
            }

            throw new NotSupportedException(
                $"Property '{metadata.ClrType.Name}.{member.Member.Name}' is not mapped to a graph property, so it cannot be translated to Cypher.");
        }

        if (member.Expression != null)
        {
            // size() covers both string length and collection cardinality in Cypher.
            if (member.Member.Name == "Length" && member.Expression.Type == typeof(string))
            {
                return Atom("size(" + Visit(member.Expression).Text + ")");
            }

            if ((member.Member.Name == "Length" || member.Member.Name == "Count") &&
                member.Expression.Type != typeof(string) &&
                ScalarTypes.TryGetElementType(member.Expression.Type, out _))
            {
                return Atom("size(" + Visit(member.Expression).Text + ")");
            }
        }

        throw Unsupported(member);
    }

    private CypherFragment VisitBinary(BinaryExpression binary)
    {
        var left = Unwrap(binary.Left);
        var right = Unwrap(binary.Right);

        switch (binary.NodeType)
        {
            case ExpressionType.Equal:
            case ExpressionType.NotEqual:
            {
                var isNullCheck = binary.NodeType == ExpressionType.Equal ? " IS NULL" : " IS NOT NULL";

                if (IsNullConstant(right))
                {
                    return new CypherFragment(Render(Visit(left), PrecedenceComparison) + isNullCheck, PrecedenceComparison);
                }

                if (IsNullConstant(left))
                {
                    return new CypherFragment(Render(Visit(right), PrecedenceComparison) + isNullCheck, PrecedenceComparison);
                }

                return Comparison(left, right, binary.NodeType == ExpressionType.Equal ? "=" : "<>");
            }

            case ExpressionType.GreaterThan:
                return RelationalComparison(left, right, ">");
            case ExpressionType.GreaterThanOrEqual:
                return RelationalComparison(left, right, ">=");
            case ExpressionType.LessThan:
                return RelationalComparison(left, right, "<");
            case ExpressionType.LessThanOrEqual:
                return RelationalComparison(left, right, "<=");

            case ExpressionType.AndAlso:
                return Infix(left, right, "AND", PrecedenceAnd);
            case ExpressionType.OrElse:
                return Infix(left, right, "OR", PrecedenceOr);
            case ExpressionType.And:
                return BooleanOnly(binary, left, right, "AND", PrecedenceAnd);
            case ExpressionType.Or:
                return BooleanOnly(binary, left, right, "OR", PrecedenceOr);
            case ExpressionType.ExclusiveOr:
                return BooleanOnly(binary, left, right, "XOR", PrecedenceXor);

            case ExpressionType.Add:
            case ExpressionType.AddChecked:
                return Infix(left, right, "+", PrecedenceAdditive);
            case ExpressionType.Subtract:
            case ExpressionType.SubtractChecked:
                return Infix(left, right, "-", PrecedenceAdditive);
            case ExpressionType.Multiply:
            case ExpressionType.MultiplyChecked:
                return Infix(left, right, "*", PrecedenceMultiplicative);
            case ExpressionType.Divide:
                return Infix(left, right, "/", PrecedenceMultiplicative);
            case ExpressionType.Modulo:
                return Infix(left, right, "%", PrecedenceMultiplicative);
            case ExpressionType.Power:
                return Infix(left, right, "^", PrecedencePower);

            case ExpressionType.Coalesce:
                return Atom("coalesce(" + Visit(left).Text + ", " + Visit(right).Text + ")");
        }

        throw Unsupported(binary);
    }

    private CypherFragment VisitUnary(UnaryExpression unary)
    {
        switch (unary.NodeType)
        {
            case ExpressionType.Not:
                if (unary.Operand.Type == typeof(bool) || unary.Operand.Type == typeof(bool?))
                {
                    // Cypher's NOT propagates null, so `NOT (n0.score > $p0)` is null when the
                    // property is missing and the row is dropped. C# evaluates the comparison to
                    // false first, making the negation true, so the operand is coalesced to match.
                    return new CypherFragment(
                        "NOT coalesce(" + Visit(unary.Operand).Text + ", false)",
                        PrecedenceNot);
                }

                break;

            case ExpressionType.Negate:
            case ExpressionType.NegateChecked:
                return new CypherFragment("-" + Render(Visit(unary.Operand), PrecedenceUnary), PrecedenceUnary);

            case ExpressionType.Quote:
                return Visit(unary.Operand);

            case ExpressionType.Convert:
            case ExpressionType.ConvertChecked:
            case ExpressionType.TypeAs:
                // Unwrap deliberately preserves casts that can change a value; dropping them here
                // would silently translate a different predicate than the one that was written.
                if (!IsValuePreservingConversion(unary.Operand.Type, unary.Type))
                {
                    var operandType = ScalarTypes.Unwrap(unary.Operand.Type);

                    if (operandType.IsEnum)
                    {
                        throw new NotSupportedException(
                            $"The enum '{operandType.Name}' cannot be converted to '{unary.Type.Name}' in a query, because enum values are stored as member names rather than as their underlying numbers. Compare the enum for equality instead, or map the property to a numeric type.");
                    }

                    throw new NotSupportedException(
                        $"The cast '({unary.Type.Name}){unary.Operand}' cannot be translated to Cypher, because FalkorDB has no equivalent conversion and ignoring the cast would change the result. Apply the conversion to the stored value instead, or compare against a value of type '{unary.Operand.Type.Name}'.");
                }

                return Visit(unary.Operand);
        }

        throw Unsupported(unary);
    }

    private CypherFragment VisitMethodCall(MethodCallExpression call)
    {
        var declaringType = call.Method.DeclaringType;

        // The partial evaluator refuses to fold a subtree that carries an IQueryable, so a nested
        // query arrives here intact instead of having been compiled and executed behind the
        // caller's back. Cypher has no subquery form to translate it into, so it is reported up
        // front -- before Contains and friends can claim the call and render something misleading.
        if (typeof(IQueryable).IsAssignableFrom(declaringType) ||
            (call.Object != null && typeof(IQueryable).IsAssignableFrom(call.Object.Type)) ||
            call.Arguments.Any(argument => typeof(IQueryable).IsAssignableFrom(argument.Type)))
        {
            throw new NotSupportedException(
                $"The nested query '{call}' cannot be translated to Cypher. Running it would mean executing a second query while translating this one, which the provider does not do implicitly. Evaluate it first and capture the result, for example with ToList() or CountAsync().");
        }

        if (declaringType == typeof(string))
        {
            var fragment = VisitStringCall(call);

            if (fragment.HasValue)
            {
                return fragment.Value;
            }
        }

        if (declaringType == typeof(Math))
        {
            if (call.Method.Name == "Pow" && call.Arguments.Count == 2)
            {
                return Infix(call.Arguments[0], call.Arguments[1], "^", PrecedencePower);
            }

            if (MathFunctions.TryGetValue(call.Method.Name, out var function) && call.Arguments.Count >= 1)
            {
                // Overloads such as Math.Round(x, digits) have no single-call Cypher equivalent.
                var expectedArguments = call.Method.Name == "Atan2" ? 2 : 1;

                if (call.Arguments.Count == expectedArguments)
                {
                    return Atom(function + "(" + string.Join(", ", call.Arguments.Select(a => Visit(a).Text).ToArray()) + ")");
                }
            }
        }

        if (call.Method.Name == "Contains")
        {
            // A collection Contains becomes IN; string.Contains was handled above as CONTAINS.
            if (call.Object != null && call.Arguments.Count == 1 && call.Object.Type != typeof(string))
            {
                // A set built around a custom comparer decides membership by that comparer, but IN
                // always uses default equality, so translating it would change the result.
                RejectNonDefaultComparer(call.Object, call.Method.Name);

                return new CypherFragment(
                    Render(Visit(call.Arguments[0]), PrecedenceComparison) + " IN " + Render(Visit(call.Object), PrecedenceComparison),
                    PrecedenceComparison);
            }

            if (call.Object == null && call.Arguments.Count >= 2 && call.Arguments.Count <= 3 &&
                call.Arguments[0].Type != typeof(string))
            {
                // .NET 10 binds `array.Contains(x)` to the MemoryExtensions overload that also takes
                // an equality comparer when the element type does not implement IEquatable<T>, which
                // is the case for every enum. A null comparer means default equality, which is what
                // IN does; a real comparer would change the result, so it is rejected.
                if (call.Arguments.Count == 3 && !IsNullConstant(call.Arguments[2]))
                {
                    throw new NotSupportedException(
                        $"'{call.Method.Name}' with a custom equality comparer cannot be translated to Cypher, because FalkorDB's IN operator always uses default equality. Drop the comparer, or evaluate the query in memory first.");
                }

                var collection = StripSpanConversion(call.Arguments[0]);

                RejectNonDefaultComparer(collection, call.Method.Name);

                return new CypherFragment(
                    Render(Visit(call.Arguments[1]), PrecedenceComparison) + " IN " + Render(Visit(collection), PrecedenceComparison),
                    PrecedenceComparison);
            }
        }

        if (call.Method.Name == "ToString" && call.Arguments.Count == 0 && call.Object != null)
        {
            return Atom("toString(" + Visit(call.Object).Text + ")");
        }

        throw Unsupported(call);
    }

    /// <summary>
    /// C# 14 binds <c>array.Contains(x)</c> to <c>MemoryExtensions.Contains(ReadOnlySpan&lt;T&gt;, T)</c>,
    /// inserting an implicit array-to-span conversion. The span has no graph equivalent, so the
    /// conversion is peeled back off to recover the underlying collection.
    /// </summary>
    private static Expression StripSpanConversion(Expression expression)    {
        while (expression is MethodCallExpression call &&
               call.Object == null &&
               call.Arguments.Count == 1 &&
               (call.Method.Name == "op_Implicit" || call.Method.Name == "AsSpan"))
        {
            expression = call.Arguments[0];
        }

        return expression;
    }

    private CypherFragment? VisitStringCall(MethodCallExpression call)
    {        switch (call.Method.Name)
        {
            case "StartsWith" when call.Arguments.Count == 1:
                return new CypherFragment(
                    Render(Visit(call.Object), PrecedenceComparison) + " STARTS WITH " + Render(Visit(call.Arguments[0]), PrecedenceComparison),
                    PrecedenceComparison);

            case "EndsWith" when call.Arguments.Count == 1:
                return new CypherFragment(
                    Render(Visit(call.Object), PrecedenceComparison) + " ENDS WITH " + Render(Visit(call.Arguments[0]), PrecedenceComparison),
                    PrecedenceComparison);

            case "Contains" when call.Object != null && call.Arguments.Count == 1:
                return new CypherFragment(
                    Render(Visit(call.Object), PrecedenceComparison) + " CONTAINS " + Render(Visit(call.Arguments[0]), PrecedenceComparison),
                    PrecedenceComparison);

            case "ToUpper" when call.Arguments.Count == 0:
                return Atom("toUpper(" + Visit(call.Object).Text + ")");

            case "ToLower" when call.Arguments.Count == 0:
                return Atom("toLower(" + Visit(call.Object).Text + ")");

            case "Trim" when call.Arguments.Count == 0:
                return Atom("trim(" + Visit(call.Object).Text + ")");

            case "TrimStart" when call.Arguments.Count == 0:
                return Atom("lTrim(" + Visit(call.Object).Text + ")");

            case "TrimEnd" when call.Arguments.Count == 0:
                return Atom("rTrim(" + Visit(call.Object).Text + ")");

            case "Substring" when call.Arguments.Count == 1 || call.Arguments.Count == 2:
                return Atom("substring(" + Visit(call.Object).Text + ", " +
                            string.Join(", ", call.Arguments.Select(a => Visit(a).Text).ToArray()) + ")");

            case "Replace" when call.Arguments.Count == 2 && call.Arguments[0].Type == typeof(string):
                return Atom("replace(" + Visit(call.Object).Text + ", " +
                            Visit(call.Arguments[0]).Text + ", " + Visit(call.Arguments[1]).Text + ")");

            case "Concat" when call.Object == null && call.Arguments.Count == 2:
                return Infix(call.Arguments[0], call.Arguments[1], "+", PrecedenceAdditive);

            case "IsNullOrEmpty" when call.Object == null && call.Arguments.Count == 1:
            {
                var operand = Render(Visit(call.Arguments[0]), PrecedenceComparison);

                return new CypherFragment(operand + " IS NULL OR size(" + operand + ") = 0", PrecedenceOr);
            }
        }

        return null;
    }

    /// <summary>
    /// Renders an ordered comparison, rejecting enum operands.
    /// </summary>
    /// <remarks>
    /// Enum values are written to the graph as member names, so <c>&lt;</c> and <c>&gt;</c> would
    /// compare those names lexically rather than by the underlying value. Equality is unaffected,
    /// because two names are equal exactly when the members are.
    /// </remarks>
    private CypherFragment RelationalComparison(Expression left, Expression right, string @operator)
    {
        var enumType = FindEnumType(left) ?? FindEnumType(right);

        if (enumType != null)
        {
            throw new NotSupportedException(
                $"'{@operator}' cannot be applied to the enum '{enumType.Name}', because enum values are stored as member names and Cypher would compare them lexically instead of by their underlying value. Compare for equality instead, or map the property to a numeric type.");
        }

        return Comparison(left, right, @operator);
    }

    private CypherFragment Comparison(Expression left, Expression right, string @operator)
    {
        // An enum compared against an integral literal has to be bound as the enum member name,
        // because that is how enum values are written to the graph.
        var enumType = FindEnumType(left) ?? FindEnumType(right);

        var leftFragment = Visit(CoerceEnum(left, enumType));
        var rightFragment = Visit(CoerceEnum(right, enumType));

        var text = Render(leftFragment, PrecedenceComparison) + " " + @operator + " " + Render(rightFragment, PrecedenceComparison);

        if (@operator == "<>")
        {
            // C# reports two values as unequal when exactly one of them is null, but Cypher's <>
            // yields null there and the row is filtered out. The fallback restores the C# answer:
            // <> is only null when a side is null, and then the values differ unless both are null.
            var leftNullable = CanBeNull(left);
            var rightNullable = CanBeNull(right);

            if (leftNullable || rightNullable)
            {
                var fallback = leftNullable && rightNullable
                    ? Render(leftFragment, PrecedenceComparison) + " IS NOT NULL OR " +
                      Render(rightFragment, PrecedenceComparison) + " IS NOT NULL"
                    : "true";

                return Atom("coalesce(" + text + ", " + fallback + ")");
            }
        }

        if (@operator == "=")
        {
            // Cypher's = yields null when either side is null, so the row is dropped. C# keeps it
            // when both sides are null, because null == null is true. Only the both-nullable case
            // differs: when exactly one side can be null, Cypher's null and C#'s false are both
            // rejected by WHERE, so the plain comparison is left alone and stays indexable.
            if (CanBeNull(left) && CanBeNull(right))
            {
                return Atom(
                    "coalesce(" + text + ", " +
                    Render(leftFragment, PrecedenceComparison) + " IS NULL AND " +
                    Render(rightFragment, PrecedenceComparison) + " IS NULL)");
            }
        }

        return new CypherFragment(text, PrecedenceComparison);
    }

    /// <summary>
    /// True when the expression can evaluate to null, either because its type is nullable or
    /// because it reads a property that the node may simply not have.
    /// </summary>
    private static bool CanBeNull(Expression expression)
    {
        expression = Unwrap(expression);

        if (expression is ConstantExpression constant)
        {
            return constant.Value == null;
        }

        return !expression.Type.IsValueType || Nullable.GetUnderlyingType(expression.Type) != null;
    }

    /// <summary>
    /// Rejects a <c>Contains</c> receiver that decides membership with a non-default comparer.
    /// </summary>
    /// <remarks>
    /// Cypher's <c>IN</c> always uses default equality, so a set built around, say,
    /// <see cref="StringComparer.OrdinalIgnoreCase"/> would silently become a case-sensitive test.
    /// The collection has already been folded to a constant by the partial evaluator, so the
    /// comparer it actually carries can be inspected here.
    /// </remarks>
    private static void RejectNonDefaultComparer(Expression source, string methodName)
    {
        if (!(Unwrap(source) is ConstantExpression constant) || constant.Value == null)
        {
            return;
        }

        foreach (var propertyName in new[] { "Comparer", "KeyComparer" })
        {
            var property = constant.Value.GetType().GetProperty(propertyName, BindingFlags.Public | BindingFlags.Instance);

            if (property == null || property.GetIndexParameters().Length != 0 || !IsComparer(property.PropertyType))
            {
                continue;
            }

            if (IsDefaultComparer(property.GetValue(constant.Value), property.PropertyType))
            {
                continue;
            }

            throw new NotSupportedException(
                $"'{methodName}' on a '{constant.Value.GetType().Name}' that was built with a custom comparer cannot be translated to Cypher, because the IN operator always uses default equality and the comparer would be silently discarded. Materialize the collection with default equality first, or filter in memory.");
        }
    }

    private static bool IsComparer(Type type) =>
        type.IsGenericType &&
        (type.GetGenericTypeDefinition() == typeof(IEqualityComparer<>) ||
         type.GetGenericTypeDefinition() == typeof(IComparer<>));

    private static bool IsDefaultComparer(object comparer, Type comparerType)
    {
        if (comparer == null)
        {
            return true;
        }

        var element = comparerType.GetGenericArguments()[0];

        var holder = comparerType.GetGenericTypeDefinition() == typeof(IEqualityComparer<>)
            ? typeof(EqualityComparer<>).MakeGenericType(element)
            : typeof(Comparer<>).MakeGenericType(element);

        var @default = holder.GetProperty("Default", BindingFlags.Public | BindingFlags.Static)?.GetValue(null);

        return ReferenceEquals(comparer, @default);
    }

    private CypherFragment Infix(Expression left, Expression right, string @operator, int precedence)
    {
        var leftText = Render(Visit(left), precedence);
        var rightText = Render(Visit(right), precedence + 1);

        return new CypherFragment(leftText + " " + @operator + " " + rightText, precedence);
    }

    /// <summary>
    /// <see cref="ExpressionType.And"/>, <see cref="ExpressionType.Or"/> and
    /// <see cref="ExpressionType.ExclusiveOr"/> are also the bitwise operators, which Cypher has no
    /// equivalent for. Only the boolean form is translatable.
    /// </summary>
    private CypherFragment BooleanOnly(BinaryExpression node, Expression left, Expression right, string @operator, int precedence)
    {
        if (!IsBoolean(left.Type) || !IsBoolean(right.Type))
        {
            throw new NotSupportedException(
                $"The bitwise operator '{node.NodeType}' in '{node}' cannot be translated to Cypher. Use the boolean operators '&&', '||' and '!' instead.");
        }

        return Infix(left, right, @operator, precedence);
    }

    private static bool IsBoolean(Type type) =>
        type == typeof(bool) || type == typeof(bool?);

    private static string Render(CypherFragment fragment, int minimumPrecedence) =>
        fragment.Precedence < minimumPrecedence ? "(" + fragment.Text + ")" : fragment.Text;

    private static CypherFragment Atom(string text) => new CypherFragment(text, PrecedenceAtom);

    private AliasBinding ResolveBinding(ParameterExpression parameter)
    {
        if (_bindings.TryGetValue(parameter, out var binding))
        {
            return binding;
        }

        throw new NotSupportedException(
            $"The lambda parameter '{parameter.Name}' is not bound to a pattern alias and cannot be translated to Cypher.");
    }

    /// <summary>
    /// Reports whether a conversion leaves the value FalkorDB would see unchanged, so that dropping
    /// it from the translated expression cannot alter the result.
    /// </summary>
    private static bool IsValuePreservingConversion(Type operandType, Type targetType)
    {
        var operand = ScalarTypes.Unwrap(operandType);
        var target = ScalarTypes.Unwrap(targetType);

        // Identity, and the nullable lifting the compiler inserts around optional members.
        if (operand == target)
        {
            return true;
        }

        // Boxing and reference conversions do not change the underlying value.
        if (target == typeof(object) || (!target.IsValueType && target.IsAssignableFrom(operand)))
        {
            return true;
        }

        // An enum-to-underlying conversion is deliberately NOT treated as value preserving. Enums
        // are stored as member names, so stripping the cast would render the name where the query
        // asked for a number. The comparison path strips it locally instead, where the constant on
        // the other side can be coerced back to a member name.
        return false;
    }

    /// <summary>
    /// Strips the conversions <see cref="Unwrap"/> removes, plus the compiler-inserted
    /// enum-to-underlying conversion, so the enum operand of a comparison can be recognised.
    /// </summary>
    private static Expression UnwrapEnumConversion(Expression expression)
    {
        expression = Unwrap(expression);

        while (expression is UnaryExpression unary &&
               (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked) &&
               ScalarTypes.Unwrap(unary.Operand.Type).IsEnum)
        {
            expression = Unwrap(unary.Operand);
        }

        return expression;
    }

    private static Expression Unwrap(Expression expression)
    {
        while (expression is UnaryExpression unary &&
               (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked || unary.NodeType == ExpressionType.Quote))
        {
            if (unary.NodeType != ExpressionType.Quote &&
                !IsValuePreservingConversion(unary.Operand.Type, unary.Type))
            {
                break;
            }

            expression = unary.Operand;
        }

        return expression;
    }

    private static bool IsNullConstant(Expression expression) =>
        expression is ConstantExpression constant && constant.Value == null;

    private static Type FindEnumType(Expression expression)
    {
        var type = ScalarTypes.Unwrap(UnwrapEnumConversion(expression).Type);

        return type.IsEnum ? type : null;
    }

    /// <summary>
    /// Rewrites one side of a comparison whose other side is an enum so that it renders as the
    /// stored member name.
    /// </summary>
    /// <remarks>
    /// Only two operand shapes survive: another enum expression, which already renders as a member
    /// name, and a literal that can be rebound to <paramref name="enumType"/>. Anything else -- most
    /// importantly a second property read such as <c>p.Age</c> -- is rejected, because emitting it
    /// verbatim would compare a member name against a number and silently never match.
    /// </remarks>
    private static Expression CoerceEnum(Expression expression, Type enumType)
    {
        if (enumType == null)
        {
            return expression;
        }

        var original = expression;

        // The enum side keeps its enum type so it renders as the stored member name rather than
        // being rejected as an untranslatable cast.
        expression = UnwrapEnumConversion(expression);

        if (ScalarTypes.Unwrap(expression.Type).IsEnum || IsNullConstant(expression))
        {
            return expression;
        }

        if (expression is ConstantExpression constant && constant.Value is IConvertible)
        {
            if (constant.Value is Enum)
            {
                return constant;
            }

            try
            {
                return Expression.Constant(Enum.ToObject(enumType, constant.Value), enumType);
            }
            catch (ArgumentException)
            {
                // Falls through to the rejection below: the literal is not a valid underlying value.
            }
        }

        throw new NotSupportedException(
            $"'{original}' cannot be compared with the enum '{enumType.Name}', because enum values are stored as member names and the comparison would test that name against a '{ScalarTypes.Unwrap(expression.Type).Name}'. Compare against a value of type '{enumType.Name}' or another '{enumType.Name}' property instead.");
    }

    private static NotSupportedException Unsupported(Expression expression) =>
        new NotSupportedException(
            $"The expression '{expression}' (node type {expression.NodeType}) cannot be translated to Cypher by the NFalkorDB LINQ provider.");
}
