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

            // A ref struct such as ReadOnlySpan<T> cannot be boxed into a constant. C# 14 produces
            // these for `array.Contains(x)`, which now binds to MemoryExtensions.Contains.
            if (IsByRefLike(node.Type))
            {
                return false;
            }

            return true;
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

            if (TryEvaluateDirectly(node, out var evaluated))
            {
                return Expression.Constant(evaluated, node.Type);
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

            return Expression.Constant(value, node.Type);
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
