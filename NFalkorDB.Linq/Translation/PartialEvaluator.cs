using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

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

            // A queryable root is the query itself, not a value to fold away.
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
    }
}
