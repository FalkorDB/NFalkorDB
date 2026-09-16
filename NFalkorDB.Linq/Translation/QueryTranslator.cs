using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using NFalkorDB.Linq.Mapping;
using NFalkorDB.Linq.Materialization;

namespace NFalkorDB.Linq.Translation;

/// <summary>
/// Walks a LINQ expression tree and builds the equivalent <see cref="CypherQueryModel"/>.
/// Anything that cannot be expressed in Cypher raises a <see cref="NotSupportedException"/>;
/// the provider never falls back to evaluating part of the query in memory.
/// </summary>
internal sealed class QueryTranslator
{
    private static readonly HashSet<string> ReservedAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "all", "and", "any", "as", "asc", "ascending", "by", "case", "contains", "create", "delete",
        "desc", "descending", "distinct", "else", "end", "ends", "exists", "false", "in", "is",
        "limit", "match", "merge", "none", "not", "null", "on", "optional", "or", "order", "remove",
        "return", "set", "single", "skip", "starts", "then", "true", "union", "unwind", "when",
        "where", "with", "xor", "yield"
    };

    private readonly CypherQueryModel _model = new CypherQueryModel();
    private readonly ParameterBag _parameters = new ParameterBag();

    private string _currentAlias;
    private EntityMetadata _currentMetadata;
    private int _nodeAliases;
    private int _relationshipAliases;
    private bool _projectionApplied;
    private ProjectionShape _projection;
    private TerminalOperator _terminal = TerminalOperator.Sequence;
    private Type _resultType;

    /// <summary>
    /// Translates a LINQ expression tree into Cypher.
    /// </summary>
    internal static CompiledQuery Translate(Expression expression) =>
        Translate(expression, TerminalOperator.Sequence, resultType: null);

    /// <summary>
    /// Translates a LINQ expression tree and applies a terminal operator on top of it. The async
    /// operators use this so they share a single implementation with their synchronous
    /// <see cref="Queryable"/> counterparts.
    /// </summary>
    internal static CompiledQuery Translate(Expression expression, TerminalOperator terminal, Type resultType)
    {
        var translator = new QueryTranslator();

        translator.Visit(PartialEvaluator.Evaluate(expression));

        if (terminal != TerminalOperator.Sequence)
        {
            translator.ApplyTerminal(terminal, resultType);
        }

        return translator.Build();
    }

    private CompiledQuery Build()
    {
        if (_model.AggregateExpression == null)
        {
            EnsureDefaultProjection();
        }

        return new CompiledQuery(
            CypherQueryRenderer.Render(_model),
            _parameters.Values,
            _projection,
            _terminal,
            _resultType);
    }

    private void Visit(Expression expression)
    {
        switch (expression)
        {
            case ConstantExpression constant when constant.Value is IQueryable queryable:
                InitializeRoot(queryable.ElementType);
                return;

            case MethodCallExpression call:
                VisitMethodCall(call);
                return;
        }

        throw new NotSupportedException(
            $"The expression '{expression}' cannot start a FalkorDB LINQ query. Start from GraphContext.Nodes<T>() or GraphContext.Relationships<T>().");
    }

    private void VisitMethodCall(MethodCallExpression call)
    {
        var declaringType = call.Method.DeclaringType;

        if (declaringType == typeof(GraphQueryableExtensions))
        {
            VisitGraphExtension(call);
            return;
        }

        if (declaringType != typeof(Queryable) && declaringType != typeof(Enumerable))
        {
            throw new NotSupportedException(
                $"The method '{declaringType?.Name}.{call.Method.Name}' is not a supported FalkorDB LINQ query operator.");
        }

        Visit(call.Arguments[0]);

        switch (call.Method.Name)
        {
            case nameof(Queryable.Where):
                ApplyWhere(GetUnaryLambda(call, 1), negate: false);
                return;

            case nameof(Queryable.Select):
                ApplySelect(GetUnaryLambda(call, 1));
                return;

            case nameof(Queryable.SelectMany):
                ApplySelectMany(call);
                return;

            case nameof(Queryable.OrderBy):
                ApplyOrderBy(GetUnaryLambda(call, 1), descending: false, reset: true);
                return;

            case nameof(Queryable.OrderByDescending):
                ApplyOrderBy(GetUnaryLambda(call, 1), descending: true, reset: true);
                return;

            case nameof(Queryable.ThenBy):
                ApplyOrderBy(GetUnaryLambda(call, 1), descending: false, reset: false);
                return;

            case nameof(Queryable.ThenByDescending):
                ApplyOrderBy(GetUnaryLambda(call, 1), descending: true, reset: false);
                return;

            case nameof(Queryable.Skip):
                ApplySkip(GetCount(call, 1));
                return;

            case nameof(Queryable.Take):
                ApplyTake(GetCount(call, 1));
                return;

            case nameof(Queryable.Distinct) when call.Arguments.Count == 1:
                ApplyDistinct();
                return;

            case nameof(Queryable.First):
                ApplyOptionalPredicate(call);
                ApplyTerminal(TerminalOperator.First, call.Method.ReturnType);
                return;

            case nameof(Queryable.FirstOrDefault):
                ApplyOptionalPredicate(call);
                ApplyTerminal(TerminalOperator.FirstOrDefault, call.Method.ReturnType);
                return;

            case nameof(Queryable.Single):
                ApplyOptionalPredicate(call);
                ApplyTerminal(TerminalOperator.Single, call.Method.ReturnType);
                return;

            case nameof(Queryable.SingleOrDefault):
                ApplyOptionalPredicate(call);
                ApplyTerminal(TerminalOperator.SingleOrDefault, call.Method.ReturnType);
                return;

            case nameof(Queryable.Any):
                ApplyOptionalPredicate(call);
                ApplyTerminal(TerminalOperator.Any, typeof(bool));
                return;

            case nameof(Queryable.All) when call.Arguments.Count == 2:
                ApplyWhere(GetUnaryLambda(call, 1), negate: true);
                ApplyTerminal(TerminalOperator.None, typeof(bool));
                return;

            case nameof(Queryable.Count):
                ApplyOptionalPredicate(call);
                ApplyTerminal(TerminalOperator.Count, typeof(int));
                return;

            case nameof(Queryable.LongCount):
                ApplyOptionalPredicate(call);
                ApplyTerminal(TerminalOperator.LongCount, typeof(long));
                return;

            case nameof(Queryable.Sum):
                ApplyOptionalSelector(call);
                ApplyTerminal(TerminalOperator.Sum, call.Method.ReturnType);
                return;

            case nameof(Queryable.Min):
                ApplyOptionalSelector(call);
                ApplyTerminal(TerminalOperator.Min, call.Method.ReturnType);
                return;

            case nameof(Queryable.Max):
                ApplyOptionalSelector(call);
                ApplyTerminal(TerminalOperator.Max, call.Method.ReturnType);
                return;

            case nameof(Queryable.Average):
                ApplyOptionalSelector(call);
                ApplyTerminal(TerminalOperator.Average, call.Method.ReturnType);
                return;
        }

        throw new NotSupportedException(
            $"The LINQ operator '{call.Method.Name}' is not supported by the NFalkorDB LINQ provider.");
    }

    private void VisitGraphExtension(MethodCallExpression call)
    {
        if (call.Method.Name != nameof(GraphQueryableExtensions.Traverse))
        {
            throw new NotSupportedException(
                $"The operator '{call.Method.Name}' cannot be translated to Cypher.");
        }

        Visit(call.Arguments[0]);

        var genericArguments = call.Method.GetGenericArguments();
        var edgeType = genericArguments[0];
        var targetType = genericArguments[1];

        LambdaExpression edgePredicate = null;
        var direction = TraversalDirection.Outgoing;

        for (var i = 1; i < call.Arguments.Count; i++)
        {
            var argument = StripQuotes(call.Arguments[i]);

            if (argument is LambdaExpression lambda)
            {
                edgePredicate = lambda;
            }
            else if (argument is ConstantExpression constant && constant.Value is TraversalDirection value)
            {
                direction = value;
            }
        }

        var edgeMetadata = EntityMetadataCache.Get(edgeType);

        if (edgeMetadata.Kind != EntityKind.Relationship)
        {
            throw new NotSupportedException(
                $"Traverse<{edgeType.Name}, {targetType.Name}>() needs a relationship type for its first type argument. Annotate '{edgeType.Name}' with [Relationship].");
        }

        var edgeAlias = Traverse(edgeMetadata.RelationshipType, direction, targetType);

        if (edgePredicate != null)
        {
            var bindings = new Dictionary<ParameterExpression, AliasBinding>
            {
                [edgePredicate.Parameters[0]] = new AliasBinding(edgeAlias, edgeMetadata)
            };

            _model.WhereClauses.Add(new CypherExpressionBuilder(_parameters, bindings).TranslatePredicate(edgePredicate.Body));
        }
    }

    private void InitializeRoot(Type elementType)
    {
        var metadata = EntityMetadataCache.Get(elementType);

        if (metadata.Kind == EntityKind.Node)
        {
            var alias = NextNodeAlias();

            _model.Pattern.Add(new NodePatternElement(alias, metadata.Labels));

            _currentAlias = alias;
        }
        else
        {
            var alias = NextRelationshipAlias();

            _model.Pattern.Add(new NodePatternElement(null, null));
            _model.Pattern.Add(new RelationshipPatternElement(alias, metadata.RelationshipType, TraversalDirection.Outgoing));
            _model.Pattern.Add(new NodePatternElement(null, null));

            _currentAlias = alias;
        }

        _currentMetadata = metadata;
        _resultType = elementType;
    }

    private string Traverse(string relationshipType, TraversalDirection direction, Type targetType)
    {
        GuardPatternExtension();

        if (_currentMetadata.Kind != EntityKind.Node)
        {
            throw new NotSupportedException(
                $"Cannot traverse from '{_currentMetadata.ClrType.Name}' because it is mapped as a relationship. Traversals start from a node.");
        }

        var targetMetadata = EntityMetadataCache.Get(targetType);

        if (targetMetadata.Kind != EntityKind.Node)
        {
            throw new NotSupportedException(
                $"A traversal must end on a node type, but '{targetType.Name}' is mapped as a relationship.");
        }

        var edgeAlias = NextRelationshipAlias();
        var nodeAlias = NextNodeAlias();

        _model.Pattern.Add(new RelationshipPatternElement(edgeAlias, relationshipType, direction));
        _model.Pattern.Add(new NodePatternElement(nodeAlias, targetMetadata.Labels));

        _currentAlias = nodeAlias;
        _currentMetadata = targetMetadata;
        _resultType = targetType;

        return edgeAlias;
    }

    private void ApplyWhere(LambdaExpression predicate, bool negate)
    {
        if (_projectionApplied)
        {
            throw new NotSupportedException(
                "Where cannot be applied after Select. Filter before projecting, so the predicate can be translated against the matched entity.");
        }

        GuardNotPaged("Where");

        var bindings = BindCurrent(predicate.Parameters[0]);
        var translated = new CypherExpressionBuilder(_parameters, bindings).TranslatePredicate(predicate.Body);

        // Cypher's three-valued logic makes a plain NOT the wrong negation: a predicate over a
        // missing property evaluates to null, and NOT null is null, so the row would be dropped
        // rather than counted as a violation. LINQ treats it as false, so coalesce it first.
        _model.WhereClauses.Add(negate ? "NOT coalesce(" + translated + ", false)" : translated);
    }

    private void ApplySelect(LambdaExpression selector)
    {
        if (_projectionApplied)
        {
            throw new NotSupportedException(
                "Two consecutive Select calls cannot be translated to Cypher. Combine them into a single projection.");
        }

        // RETURN DISTINCT would deduplicate the projected values rather than the matched rows, so
        // the result would differ from the LINQ semantics.
        if (_model.Distinct)
        {
            throw new NotSupportedException(
                "Select cannot be applied after Distinct, because Cypher would deduplicate the projected values instead of the matched rows. Project first, then call Distinct.");
        }

        var bindings = BindCurrent(selector.Parameters[0]);

        BuildProjection(selector.Body, bindings);
    }

    private void ApplySelectMany(MethodCallExpression call)
    {
        if (call.Arguments.Count != 2 && call.Arguments.Count != 3)
        {
            throw new NotSupportedException(
                "Only the SelectMany overloads taking a collection selector, optionally with a result selector, are supported.");
        }

        var collectionSelector = GetUnaryLambda(call, 1);

        // The compiler inserts a Convert when the navigation property is typed as a concrete
        // collection but SelectMany wants IEnumerable<T>.
        var collectionBody = StripConvert(collectionSelector.Body);

        if (!(collectionBody is MemberExpression member) ||
            !(member.Expression is ParameterExpression parameter) ||
            parameter != collectionSelector.Parameters[0])
        {
            throw new NotSupportedException(
                $"SelectMany can only traverse a navigation property, for example `SelectMany(p => p.Knows)`. '{collectionSelector.Body}' is not a navigation property.");
        }

        if (!_currentMetadata.TryGetNavigation(member.Member.Name, out var navigation))
        {
            throw new NotSupportedException(
                $"'{_currentMetadata.ClrType.Name}.{member.Member.Name}' is not a navigation property. Annotate it with [Relationship] to make it traversable.");
        }

        var sourceAliasBeforeTraversal = _currentAlias;
        var sourceMetadataBeforeTraversal = _currentMetadata;

        Traverse(navigation.RelationshipType, navigation.Direction, navigation.TargetType);

        if (call.Arguments.Count == 3)
        {
            var resultSelector = GetLambda(call, 2);

            var bindings = new Dictionary<ParameterExpression, AliasBinding>
            {
                [resultSelector.Parameters[0]] = new AliasBinding(sourceAliasBeforeTraversal, sourceMetadataBeforeTraversal),
                [resultSelector.Parameters[1]] = new AliasBinding(_currentAlias, _currentMetadata)
            };

            BuildProjection(resultSelector.Body, bindings);
        }
    }

    private static Expression StripConvert(Expression expression)
    {
        while (expression.NodeType == ExpressionType.Convert || expression.NodeType == ExpressionType.ConvertChecked)
        {
            expression = ((UnaryExpression)expression).Operand;
        }

        return expression;
    }

    private void ApplyOrderBy(LambdaExpression keySelector, bool descending, bool reset)
    {
        if (_projectionApplied)
        {
            throw new NotSupportedException(
                "OrderBy cannot be applied after Select. Order before projecting, so the sort key can be translated against the matched entity.");
        }

        GuardNotPaged("OrderBy");

        if (reset)
        {
            _model.OrderByTerms.Clear();
        }

        var bindings = BindCurrent(keySelector.Parameters[0]);
        var expression = new CypherExpressionBuilder(_parameters, bindings).Translate(keySelector.Body);

        // Boxing is stripped before the guard: the expression builder also strips it, so
        // `OrderBy(p => (object)p.Rating)` would otherwise order by the stored member name while
        // the key type reads as object.
        GuardNotEnumOrdered(UnwrapProjection(keySelector.Body).Type, descending ? "OrderByDescending" : "OrderBy");

        _model.OrderByTerms.Add(new OrderByTerm(expression, descending));
    }

    /// <summary>
    /// The type an aggregate actually reads. A boxed selector such as
    /// <c>Min(p =&gt; (object)p.Rating)</c> declares <see cref="object"/> as its return type, but the
    /// projection still reads the enum underneath, so the declared type cannot be trusted here.
    /// </summary>
    private Type EffectiveResultType(Type resultType)
    {
        var projected = _projection?.ValueType;

        return projected != null && (resultType == null || resultType == typeof(object))
            ? projected
            : resultType;
    }

    /// <summary>
    /// Rejects ordering or extrema over an enum, which the graph stores as a member name.
    /// </summary>    /// <remarks>
    /// Cypher would sort those names lexically — <c>Good, Great, Poor</c> — rather than by the
    /// underlying values that LINQ orders by — <c>Poor, Good, Great</c>.
    /// </remarks>
    private static void GuardNotEnumOrdered(Type type, string @operator)
    {        var unwrapped = ScalarTypes.Unwrap(type);

        if (unwrapped.IsEnum)
        {
            throw new NotSupportedException(
                $"'{@operator}' cannot be applied to the enum '{unwrapped.Name}', because enum values are stored as member names and Cypher would order them lexically instead of by their underlying value. Order by a numeric property instead, or map the property to its numeric value.");
        }
    }

    private void ApplySkip(long count)
    {
        count = Math.Max(0, count);

        if (_model.Limit.HasValue)
        {
            _model.Limit = Math.Max(0, _model.Limit.Value - count);
        }

        _model.Skip = (_model.Skip ?? 0) + count;
    }

    private void ApplyTake(long count)
    {
        count = Math.Max(0, count);

        _model.Limit = _model.Limit.HasValue ? Math.Min(_model.Limit.Value, count) : count;
    }

    private void ApplyDistinct()
    {
        if (_model.Skip.HasValue || _model.Limit.HasValue)
        {
            throw new NotSupportedException(
                "Distinct cannot be applied after Skip or Take, because Cypher would de-duplicate a different row set. Call Distinct before paging.");
        }

        GuardDistinctEquality();

        _model.Distinct = true;
    }

    /// <summary>
    /// Rejects <c>Distinct</c> when LINQ and Cypher would not agree on which rows are duplicates.
    /// </summary>
    /// <remarks>
    /// <c>RETURN DISTINCT</c> compares the projected values, while LINQ compares with
    /// <see cref="EqualityComparer{T}.Default"/>. The two only provably agree when the projected type
    /// carries the compiler's own structural equality, so only anonymous types, records that kept
    /// their generated equality, scalars and strings are allowed. A hand-written <c>Equals</c> is not
    /// enough: it may ignore members, compare them case-insensitively, or call everything equal.
    /// <para>
    /// A whole-node projection is the one deliberate exception. <c>RETURN DISTINCT n</c> removes
    /// duplicates by graph identity, which is exactly what makes <c>Distinct</c> useful after a
    /// traversal that reaches the same node twice, so it is kept and documented rather than rejected.
    /// </para>
    /// </remarks>
    private void GuardDistinctEquality()
    {
        if (_projection is ObjectShape shape && !HasProvableValueEquality(shape.ResultType))
        {
            var name = shape.ResultType.Name;

            throw new NotSupportedException(
                $"Distinct cannot be applied to the projection '{name}', because LINQ compares '{name}' with EqualityComparer<T>.Default -- reference equality unless the type defines its own -- while RETURN DISTINCT compares the projected values, and the provider cannot prove the two agree. Project an anonymous type or a record that keeps its generated equality, or project the node itself to remove duplicates by graph identity.");
        }

        if (_projection is ColumnShape column && column.Entity == null && IsCollectionLike(column.ValueType))
        {
            throw new NotSupportedException(
                $"Distinct cannot be applied to the '{column.ValueType.Name}' column, because LINQ compares a collection by reference and would keep every row, while Cypher compares lists and maps by value and would collapse them. Project a scalar, or call Distinct after materializing the rows.");
        }
    }

    private static bool HasProvableValueEquality(Type type) =>
        type.IsValueType ||
        type == typeof(string) ||
        IsAnonymous(type) ||
        IsRecordWithGeneratedEquality(type);

    private static bool IsAnonymous(Type type) =>
        type.IsGenericType &&
        type.Name.IndexOf("AnonymousType", StringComparison.Ordinal) >= 0 &&
        type.GetCustomAttributes(typeof(CompilerGeneratedAttribute), inherit: false).Length > 0;

    /// <summary>
    /// True for a record that still uses the equality the compiler generated for it. A record may
    /// override <c>Equals(T)</c>, and then only the <c>Equals(object)</c> wrapper stays generated, so
    /// the strongly typed overload is the one worth testing.
    /// </summary>
    private static bool IsRecordWithGeneratedEquality(Type type)
    {
        var equalityContract = type.GetProperty(
            "EqualityContract",
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

        if (equalityContract == null)
        {
            return false;
        }

        var typed = type.GetMethod("Equals", new[] { type });

        return typed != null &&
               typed.GetCustomAttributes(typeof(CompilerGeneratedAttribute), inherit: false).Length > 0;
    }

    private static bool IsCollectionLike(Type type) =>
        type != typeof(string) && typeof(IEnumerable).IsAssignableFrom(type);

    private void ApplyOptionalPredicate(MethodCallExpression call)
    {
        if (call.Arguments.Count == 2)
        {
            ApplyWhere(GetUnaryLambda(call, 1), negate: false);
        }
    }

    private void ApplyOptionalSelector(MethodCallExpression call)
    {
        if (call.Arguments.Count == 2)
        {
            ApplySelect(GetUnaryLambda(call, 1));
        }
    }

    private void ApplyTerminal(TerminalOperator terminal, Type resultType)
    {
        switch (terminal)
        {
            case TerminalOperator.Sequence:
                break;

            case TerminalOperator.First:
            case TerminalOperator.FirstOrDefault:
                ApplyTake(1);
                break;

            case TerminalOperator.Single:
            case TerminalOperator.SingleOrDefault:
                // Two rows is enough to detect the "more than one" case.
                ApplyTake(2);
                break;

            case TerminalOperator.Any:
                ApplyAggregate("count", "*", "> 0", nameof(Queryable.Any));
                break;

            case TerminalOperator.None:
                ApplyAggregate("count", "*", "= 0", nameof(Queryable.All));
                break;

            case TerminalOperator.Count:
            case TerminalOperator.LongCount:
                ApplyAggregate("count", "*", null, nameof(Queryable.Count));
                break;

            case TerminalOperator.Sum:
                ApplyAggregate("sum", null, null, nameof(Queryable.Sum));
                break;

            case TerminalOperator.Min:
                GuardNotEnumOrdered(EffectiveResultType(resultType), nameof(Queryable.Min));
                ApplyAggregate("min", null, null, nameof(Queryable.Min));
                break;

            case TerminalOperator.Max:
                GuardNotEnumOrdered(EffectiveResultType(resultType), nameof(Queryable.Max));
                ApplyAggregate("max", null, null, nameof(Queryable.Max));
                break;

            case TerminalOperator.Average:
                ApplyAggregate("avg", null, null, nameof(Queryable.Average));
                break;

            default:
                throw new NotSupportedException($"The terminal operator '{terminal}' is not supported.");
        }

        _terminal = terminal;

        if (resultType != null)
        {
            _resultType = resultType;
        }

        if (_model.AggregateExpression != null)
        {
            _projection = new ColumnShape(0, _resultType, null, $"the result of {terminal}");
        }
    }

    private void ApplyAggregate(string function, string explicitArgument, string comparison, string linqOperator)
    {
        var useWithClause = _model.Distinct || _model.Skip.HasValue || _model.Limit.HasValue;

        string argument;

        if (explicitArgument != null)
        {
            argument = explicitArgument;

            if (useWithClause)
            {
                EnsureDefaultProjection();
            }
        }
        else
        {
            if (!_projectionApplied)
            {
                throw new NotSupportedException(
                    $"'{linqOperator}' needs a value to aggregate. Project one with a selector, for example `.{linqOperator}(p => p.Age)`.");
            }

            if (_model.ReturnItems.Count != 1)
            {
                throw new NotSupportedException(
                    $"'{linqOperator}' needs a single projected column to aggregate but the projection produced {_model.ReturnItems.Count}.");
            }

            argument = useWithClause
                ? CypherQueryRenderer.WithAlias(_model.ReturnItems, 0)
                : _model.ReturnItems[0].Expression;
        }

        _model.UseWithClause = useWithClause;
        _model.AggregateExpression = comparison == null
            ? function + "(" + argument + ")"
            : function + "(" + argument + ") " + comparison;
    }

    private void EnsureDefaultProjection()
    {
        if (_projectionApplied)
        {
            return;
        }

        _model.ReturnItems.Clear();
        _model.ReturnItems.Add(new ReturnItem(_currentAlias, null));

        _projection = new ColumnShape(0, _currentMetadata.ClrType, _currentMetadata, _currentMetadata.ClrType.Name);
        _projectionApplied = true;
        _resultType = _currentMetadata.ClrType;
    }

    private void BuildProjection(Expression body, Dictionary<ParameterExpression, AliasBinding> bindings)
    {
        _model.ReturnItems.Clear();

        var usedAliases = new HashSet<string>(StringComparer.Ordinal);

        _projection = BuildProjectionShape(body, bindings, usedAliases, memberName: null);
        _projectionApplied = true;
        _resultType = body.Type;
    }

    private ProjectionShape BuildProjectionShape(
        Expression body,
        Dictionary<ParameterExpression, AliasBinding> bindings,
        HashSet<string> usedAliases,
        string memberName)
    {
        switch (body)
        {
            case NewExpression newExpression when newExpression.Constructor != null:
            {
                var arguments = BuildConstructorArguments(newExpression, bindings, usedAliases);

                return new ObjectShape(
                    newExpression.Type,
                    newExpression.Constructor,
                    arguments,
                    new KeyValuePair<MemberInfo, ProjectionShape>[0]);
            }

            case MemberInitExpression memberInit:
            {
                if (memberInit.NewExpression.Constructor == null)
                {
                    throw new NotSupportedException(
                        $"The projection '{body}' cannot be materialized because '{memberInit.Type.Name}' has no usable constructor.");
                }

                var arguments = BuildConstructorArguments(memberInit.NewExpression, bindings, usedAliases);
                var memberBindings = new List<KeyValuePair<MemberInfo, ProjectionShape>>();

                foreach (var binding in memberInit.Bindings)
                {
                    if (!(binding is MemberAssignment assignment))
                    {
                        throw new NotSupportedException(
                            $"The projection binding '{binding.Member.Name}' uses '{binding.BindingType}', which the FalkorDB LINQ provider cannot translate. Use simple member assignments.");
                    }

                    memberBindings.Add(new KeyValuePair<MemberInfo, ProjectionShape>(
                        assignment.Member,
                        BuildProjectionShape(assignment.Expression, bindings, usedAliases, assignment.Member.Name)));
                }

                return new ObjectShape(memberInit.Type, memberInit.NewExpression.Constructor, arguments, memberBindings);
            }
        }

        return BuildColumn(body, bindings, usedAliases, memberName);
    }

    private ProjectionShape[] BuildConstructorArguments(
        NewExpression newExpression,
        Dictionary<ParameterExpression, AliasBinding> bindings,
        HashSet<string> usedAliases)
    {
        var parameters = newExpression.Constructor.GetParameters();
        var arguments = new ProjectionShape[newExpression.Arguments.Count];

        for (var i = 0; i < newExpression.Arguments.Count; i++)
        {
            var name = newExpression.Members != null && i < newExpression.Members.Count
                ? newExpression.Members[i].Name
                : parameters[i].Name;

            arguments[i] = BuildProjectionShape(newExpression.Arguments[i], bindings, usedAliases, name);
        }

        return arguments;
    }

    private ProjectionShape BuildColumn(
        Expression body,
        Dictionary<ParameterExpression, AliasBinding> bindings,
        HashSet<string> usedAliases,
        string memberName)
    {
        var index = _model.ReturnItems.Count;
        var expression = new CypherExpressionBuilder(_parameters, bindings).Translate(body);

        // A boxing or reference cast such as `p => (object)p` does not change what the row holds, so
        // the value is read as the operand's type. The column keeps the declared type, because that
        // is what the caller's IQueryable<T> promised and what the result list is built from.
        var unwrapped = UnwrapProjection(body);

        EntityMetadata entityMetadata = null;

        if (unwrapped is ParameterExpression parameter && bindings.TryGetValue(parameter, out var binding))
        {
            entityMetadata = binding.Metadata;
        }

        _model.ReturnItems.Add(new ReturnItem(expression, ResolveAlias(memberName, index, usedAliases)));

        return new ColumnShape(index, body.Type, unwrapped.Type, entityMetadata, memberName ?? body.Type.Name);
    }

    /// <summary>
    /// Strips boxing and reference conversions from a projected expression, matching what the
    /// expression builder treats as value preserving. Conversions that change the value are left in
    /// place; the expression builder rejects those.
    /// </summary>
    internal static Expression UnwrapProjection(Expression body)
    {
        while (body is UnaryExpression unary &&
               (unary.NodeType == ExpressionType.Convert ||
                unary.NodeType == ExpressionType.ConvertChecked ||
                unary.NodeType == ExpressionType.TypeAs) &&
               (unary.Type == typeof(object) ||
                (!unary.Type.IsValueType && unary.Type.IsAssignableFrom(unary.Operand.Type))))
        {
            body = unary.Operand;
        }

        return body;
    }

    private static string ResolveAlias(string memberName, int index, HashSet<string> usedAliases)
    {
        if (memberName == null)
        {
            return null;
        }

        var alias = memberName;

        if (ReservedAliases.Contains(memberName) || usedAliases.Contains(memberName))
        {
            // A projected member can already be named like a generated alias, so advance until the
            // name is free rather than trusting the column index to be unique.
            do
            {
                alias = "c" + index.ToString(CultureInfo.InvariantCulture);
                index++;
            }
            while (usedAliases.Contains(alias));
        }

        usedAliases.Add(alias);

        return alias;
    }

    private Dictionary<ParameterExpression, AliasBinding> BindCurrent(ParameterExpression parameter) =>
        new Dictionary<ParameterExpression, AliasBinding>
        {
            [parameter] = new AliasBinding(_currentAlias, _currentMetadata)
        };

    private void GuardNotPaged(string @operator)
    {
        if (_model.Skip.HasValue || _model.Limit.HasValue)
        {
            throw new NotSupportedException(
                $"{@operator} cannot be applied after Skip or Take, because Cypher applies SKIP and LIMIT last. Reorder the query so paging comes last.");
        }

        if (_model.Distinct)
        {
            throw new NotSupportedException(
                $"{@operator} cannot be applied after Distinct, because Cypher de-duplicates the projected rows. Reorder the query so Distinct comes last.");
        }
    }

    private void GuardPatternExtension()
    {
        if (_projectionApplied)
        {
            throw new NotSupportedException(
                "A traversal cannot follow Select. Traverse the graph first, then project the result.");
        }

        if (_model.Skip.HasValue || _model.Limit.HasValue || _model.Distinct || _model.OrderByTerms.Count > 0)
        {
            throw new NotSupportedException(
                "A traversal cannot follow OrderBy, Skip, Take or Distinct, because Cypher extends the MATCH pattern before those clauses run. Traverse first, then order and page.");
        }
    }

    private string NextNodeAlias() => "n" + (_nodeAliases++).ToString(CultureInfo.InvariantCulture);

    private string NextRelationshipAlias() => "r" + (_relationshipAliases++).ToString(CultureInfo.InvariantCulture);

    private static LambdaExpression GetLambda(MethodCallExpression call, int index)
    {
        if (StripQuotes(call.Arguments[index]) is LambdaExpression lambda)
        {
            return lambda;
        }

        throw new NotSupportedException(
            $"Expected a lambda argument for '{call.Method.Name}' but found '{call.Arguments[index]}'.");
    }

    private static LambdaExpression GetUnaryLambda(MethodCallExpression call, int index)
    {
        var lambda = GetLambda(call, index);

        if (lambda.Parameters.Count != 1)
        {
            throw new NotSupportedException(
                $"The index-aware overload of '{call.Method.Name}' cannot be translated to Cypher because a row's ordinal is not available server side.");
        }

        return lambda;
    }

    private static long GetCount(MethodCallExpression call, int index)
    {
        if (StripQuotes(call.Arguments[index]) is ConstantExpression constant && constant.Value is int count)
        {
            return count;
        }

        throw new NotSupportedException(
            $"'{call.Method.Name}' requires a constant count but found '{call.Arguments[index]}'.");
    }

    private static Expression StripQuotes(Expression expression)
    {
        while (expression.NodeType == ExpressionType.Quote)
        {
            expression = ((UnaryExpression)expression).Operand;
        }

        return expression;
    }
}
