using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace NFalkorDB.Linq.Translation;

/// <summary>
/// Evaluates the parts of an expression tree that do not depend on a query parameter and replaces
/// them with constants. This is what turns a captured local (<c>minimumAge</c>) or a call like
/// <c>DateTime.UtcNow.AddDays(-7)</c> into a value the translator can bind as a Cypher parameter.
/// </summary>
internal static class PartialEvaluator
{
    internal static Expression Evaluate(Expression expression)
    {
        var candidates = new Nominator().Nominate(expression);

        return new SubtreeEvaluator(candidates).Visit(expression);
    }

    /// <summary>
    /// Marks the largest subtrees that contain no <see cref="ParameterExpression"/>, since those
    /// can be computed locally.
    /// </summary>
    private sealed class Nominator : ExpressionVisitor
    {
        private readonly HashSet<Expression> _candidates = new HashSet<Expression>();

        private bool _dependsOnParameter;

        internal HashSet<Expression> Nominate(Expression expression)
        {
            Visit(expression);

            return _candidates;
        }

        public override Expression Visit(Expression node)
        {
            if (node == null)
            {
                return null;
            }

            var parentDependsOnParameter = _dependsOnParameter;

            _dependsOnParameter = false;

            base.Visit(node);

            if (!_dependsOnParameter)
            {
                if (CanBeEvaluated(node))
                {
                    _candidates.Add(node);
                }
                else
                {
                    _dependsOnParameter = true;
                }
            }

            _dependsOnParameter |= parentDependsOnParameter;

            return node;
        }

        private static bool CanBeEvaluated(Expression node)
        {
            if (node.NodeType == ExpressionType.Parameter || node.NodeType == ExpressionType.Lambda)
            {
                return false;
            }

            // A queryable is the query itself, not a value to fold away. Folding one would compile
            // and execute it during translation -- a second round trip, and exactly the silent
            // client-side evaluation the provider promises never to do. Testing the node type also
            // catches a captured queryable reached through a member access, not just a query root.
            if (typeof(IQueryable).IsAssignableFrom(node.Type))
            {
                return false;
            }

            if (node is ConstantExpression constant && constant.Value is IQueryable)
            {
                return false;
            }

            // A captured queryable is often held in a variable typed as IEnumerable<T>, which the
            // type test above cannot see. The compiler stores a captured variable as a field on a
            // closure, and reading a field cannot run user code, so the value is safe to inspect.
            if (node is MemberExpression && TryReadCapturedValue(node, out var captured) && captured is IQueryable)
            {
                return false;
            }

            // A ref struct such as ReadOnlySpan<T> cannot be boxed into a constant. C# 14 produces
            // these for `array.Contains(x)`, which now binds to MemoryExtensions.Contains.
            if (IsByRefLike(node.Type))
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Reads a constant, or a chain of field accesses rooted at one, without invoking any
        /// property getter or other user code.
        /// </summary>
        private static bool TryReadCapturedValue(Expression node, out object value)
        {
            value = null;

            if (node is ConstantExpression constant)
            {
                value = constant.Value;
                return true;
            }

            if (!(node is MemberExpression member) || !(member.Member is FieldInfo field))
            {
                return false;
            }

            object target = null;

            if (member.Expression != null && !TryReadCapturedValue(member.Expression, out target))
            {
                return false;
            }

            try
            {
                value = field.GetValue(target);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private static bool IsByRefLike(Type type) =>
            type.IsValueType &&
            type.GetCustomAttributes(inherit: false)
                .Any(a => a.GetType().FullName == "System.Runtime.CompilerServices.IsByRefLikeAttribute");
    }

    private sealed class SubtreeEvaluator : ExpressionVisitor
    {
        private readonly HashSet<Expression> _candidates;

        internal SubtreeEvaluator(HashSet<Expression> candidates) => _candidates = candidates;

        public override Expression Visit(Expression node)
        {
            if (node == null)
            {
                return null;
            }

            return _candidates.Contains(node) ? EvaluateSubtree(node) : base.Visit(node);
        }

        /// <summary>
        /// A member-init's constructor call is always foldable on its own, but folding it away
        /// would leave the member-init without a NewExpression. Only the constructor arguments are
        /// visited instead.
        /// </summary>
        protected override Expression VisitMemberInit(MemberInitExpression node)
        {
            if (_candidates.Contains(node))
            {
                return EvaluateSubtree(node);
            }

            return node.Update(
                node.NewExpression.Update(Visit(node.NewExpression.Arguments)),
                Visit(node.Bindings, VisitMemberBinding));
        }

        /// <inheritdoc cref="VisitMemberInit"/>
        protected override Expression VisitListInit(ListInitExpression node)
        {
            if (_candidates.Contains(node))
            {
                return EvaluateSubtree(node);
            }

            return node.Update(
                node.NewExpression.Update(Visit(node.NewExpression.Arguments)),
                Visit(node.Initializers, VisitElementInit));
        }

        private static Expression EvaluateSubtree(Expression node)
        {
            if (node.NodeType == ExpressionType.Constant)
            {
                return node;
            }

            RejectQueryableSource(node);

            if (TryEvaluateDirectly(node, out var evaluated))
            {
                return RejectQueryableValue(evaluated, node);
            }

            var lambda = Expression.Lambda<Func<object>>(Expression.Convert(node, typeof(object)));

            object value;

            try
            {
                value = lambda.Compile()();
            }
            catch (System.Reflection.TargetInvocationException ex) when (ex.InnerException != null)
            {
                throw ex.InnerException;
            }

            return RejectQueryableValue(value, node);
        }

        /// <summary>
        /// Rejects a sequence operator whose source turns out to be a graph query.
        /// </summary>
        /// <remarks>
        /// A captured query can be held behind any member typed as <see cref="IEnumerable{T}"/>, so
        /// neither the static type nor a field read proves that a source is a local collection. The
        /// source is evaluated on its own first: that is strictly less work than folding the whole
        /// call, which would evaluate the source anyway and then run the query.
        /// </remarks>
        private static void RejectQueryableSource(Expression node)
        {
            if (!(node is MethodCallExpression call))
            {
                return;
            }

            var declaring = call.Method.DeclaringType;

            if (declaring != typeof(System.Linq.Enumerable) && declaring != typeof(System.Linq.Queryable))
            {
                return;
            }

            var source = call.Object ?? (call.Arguments.Count > 0 ? call.Arguments[0] : null);

            if (source == null || !TryEvaluateQuietly(source, out var value) || !(value is IQueryable))
            {
                return;
            }

            throw new NotSupportedException(
                $"'{call.Method.Name}' cannot be evaluated while translating, because its source is a graph query. Running it here would issue a second query and fold the answer in as a literal. Materialize the source first, for example with ToList(), if its values are meant to be parameters.");
        }

        private static Expression RejectQueryableValue(object value, Expression node)
        {
            if (value is IQueryable)
            {
                throw new NotSupportedException(
                    $"The expression '{node}' is a graph query, not a value, so it cannot be folded into the query being translated. Materialize it first, for example with ToList(), if its values are meant to be parameters.");
            }

            return Expression.Constant(value, node.Type);
        }

        private static bool TryEvaluateQuietly(Expression node, out object value)
        {
            value = null;

            try
            {
                value = Expression.Lambda<Func<object>>(Expression.Convert(node, typeof(object))).Compile()();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>
        /// Reads the shapes that make up virtually every captured value — a closure field, a
        /// property on a captured object, and the conversions the compiler wraps them in — without
        /// compiling a delegate, because compiling emits a dynamic method per subtree per
        /// translation.
        /// </summary>
        private static bool TryEvaluateDirectly(Expression node, out object value)
        {
            value = null;

            switch (node)
            {
                case ConstantExpression constant:
                    value = constant.Value;
                    return true;

                case MemberExpression member:
                    object instance = null;

                    if (member.Expression != null && !TryEvaluateDirectly(member.Expression, out instance))
                    {
                        return false;
                    }

                    switch (member.Member)
                    {
                        case FieldInfo field:
                            value = field.GetValue(instance);
                            return true;

                        case PropertyInfo property when property.GetIndexParameters().Length == 0 && property.CanRead:
                            try
                            {
                                value = property.GetValue(instance, null);
                            }
                            catch (System.Reflection.TargetInvocationException ex) when (ex.InnerException != null)
                            {
                                throw ex.InnerException;
                            }

                            return true;

                        default:
                            return false;
                    }

                case UnaryExpression unary
                    when unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked:

                    if (unary.Method != null || !TryEvaluateDirectly(unary.Operand, out var operand))
                    {
                        return false;
                    }

                    // Only reference conversions and boxing are safe to reproduce here; anything
                    // numeric or user-defined falls through to the compiled path.
                    if (operand == null)
                    {
                        return !unary.Type.IsValueType || IsNullable(unary.Type);
                    }

                    if (!unary.Type.IsInstanceOfType(operand))
                    {
                        return false;
                    }

                    value = operand;
                    return true;

                default:
                    return false;
            }
        }

        private static bool IsNullable(Type type) =>
            type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
    }
}
