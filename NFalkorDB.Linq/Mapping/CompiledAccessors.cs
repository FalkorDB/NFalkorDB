using System;
using System.Linq.Expressions;
using System.Reflection;

namespace NFalkorDB.Linq.Mapping;

/// <summary>
/// Builds compiled delegates for property access so materialization does not pay the cost of
/// <see cref="PropertyInfo.GetValue(object)"/> / <see cref="PropertyInfo.SetValue(object, object)"/>
/// on every row.
/// </summary>
internal static class CompiledAccessors
{
    internal static Func<object, object> CreateGetter(PropertyInfo property)
    {
        if (!property.CanRead)
        {
            return null;
        }

        var instance = Expression.Parameter(typeof(object), "instance");

        var body = Expression.Convert(
            Expression.Property(Expression.Convert(instance, property.DeclaringType), property),
            typeof(object));

        return Expression.Lambda<Func<object, object>>(body, instance).Compile();
    }

    internal static Action<object, object> CreateSetter(PropertyInfo property)
    {
        var setMethod = property.GetSetMethod(nonPublic: true);

        if (setMethod == null)
        {
            return null;
        }

        var instance = Expression.Parameter(typeof(object), "instance");
        var value = Expression.Parameter(typeof(object), "value");

        var body = Expression.Call(
            Expression.Convert(instance, property.DeclaringType),
            setMethod,
            Expression.Convert(value, property.PropertyType));

        return Expression.Lambda<Action<object, object>>(body, instance, value).Compile();
    }

    internal static Func<object> CreateFactory(Type type)
    {
        var ctor = type.GetConstructor(Type.EmptyTypes);

        if (ctor == null)
        {
            return null;
        }

        return Expression.Lambda<Func<object>>(Expression.Convert(Expression.New(ctor), typeof(object))).Compile();
    }
}
