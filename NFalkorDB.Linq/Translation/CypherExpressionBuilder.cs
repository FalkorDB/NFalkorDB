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
    internal string TranslatePredicate(Expression expression) => Visit(expression).Text;

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
                return Comparison(left, right, ">");
            case ExpressionType.GreaterThanOrEqual:
                return Comparison(left, right, ">=");
            case ExpressionType.LessThan:
                return Comparison(left, right, "<");
            case ExpressionType.LessThanOrEqual:
                return Comparison(left, right, "<=");

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
                    return new CypherFragment("NOT " + Render(Visit(unary.Operand), PrecedenceNot), PrecedenceNot);
                }

                break;

            case ExpressionType.Negate:
            case ExpressionType.NegateChecked:
                return new CypherFragment("-" + Render(Visit(unary.Operand), PrecedenceUnary), PrecedenceUnary);

            case ExpressionType.Quote:
            case ExpressionType.Convert:
            case ExpressionType.ConvertChecked:
            case ExpressionType.TypeAs:
                return Visit(unary.Operand);
        }

        throw Unsupported(unary);
    }

    private CypherFragment VisitMethodCall(MethodCallExpression call)
    {
        var declaringType = call.Method.DeclaringType;

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
                return new CypherFragment(
                    Render(Visit(call.Arguments[0]), PrecedenceComparison) + " IN " + Render(Visit(call.Object), PrecedenceComparison),
                    PrecedenceComparison);
            }

            if (call.Object == null && call.Arguments.Count == 2 && call.Arguments[0].Type != typeof(string))
            {
                var collection = StripSpanConversion(call.Arguments[0]);

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
    private static Expression StripSpanConversion(Expression expression)
    {
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

    private CypherFragment Comparison(Expression left, Expression right, string @operator)
    {
        // An enum compared against an integral literal has to be bound as the enum member name,
        // because that is how enum values are written to the graph.
        var enumType = FindEnumType(left) ?? FindEnumType(right);

        var leftFragment = Visit(CoerceEnum(left, enumType));
        var rightFragment = Visit(CoerceEnum(right, enumType));

        return new CypherFragment(
            Render(leftFragment, PrecedenceComparison) + " " + @operator + " " + Render(rightFragment, PrecedenceComparison),
            PrecedenceComparison);
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

    private static Expression Unwrap(Expression expression)
    {
        while (expression is UnaryExpression unary &&
               (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked || unary.NodeType == ExpressionType.Quote))
        {
            var operandType = ScalarTypes.Unwrap(unary.Operand.Type);
            var targetType = ScalarTypes.Unwrap(unary.Type);

            // Keep a genuine cast such as (double)someInt; only strip boxing, nullable lifting and
            // the enum-to-underlying conversions the C# compiler inserts.
            var isLifting = targetType == operandType || targetType == typeof(object) || operandType.IsEnum;

            if (!isLifting)
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
        var type = ScalarTypes.Unwrap(Unwrap(expression).Type);

        return type.IsEnum ? type : null;
    }

    private static Expression CoerceEnum(Expression expression, Type enumType)
    {
        if (enumType == null)
        {
            return expression;
        }

        if (!(Unwrap(expression) is ConstantExpression constant) || constant.Value == null)
        {
            return expression;
        }

        if (constant.Value is Enum)
        {
            return constant;
        }

        if (!(constant.Value is IConvertible))
        {
            return expression;
        }

        try
        {
            return Expression.Constant(Enum.ToObject(enumType, constant.Value), enumType);
        }
        catch (ArgumentException)
        {
            return expression;
        }
    }

    private static NotSupportedException Unsupported(Expression expression) =>
        new NotSupportedException(
            $"The expression '{expression}' (node type {expression.NodeType}) cannot be translated to Cypher by the NFalkorDB LINQ provider.");
}
