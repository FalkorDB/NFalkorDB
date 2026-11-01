using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace NFalkorDB.Linq.Mapping;

/// <summary>
/// Reflects CLR types into <see cref="EntityMetadata"/> and caches the result per type.
/// </summary>
public static class EntityMetadataCache
{
    private static readonly ConcurrentDictionary<Type, EntityMetadata> Cache =
        new ConcurrentDictionary<Type, EntityMetadata>();

    /// <summary>
    /// Gets (building and caching on first use) the mapping for <paramref name="type"/>.
    /// </summary>
    /// <param name="type">The CLR type to map.</param>
    /// <returns>The mapping for the type.</returns>
    /// <exception cref="GraphMappingException">The type cannot be mapped.</exception>
    public static EntityMetadata Get(Type type)
    {
        if (type == null)
        {
            throw new ArgumentNullException(nameof(type));
        }

        return Cache.GetOrAdd(type, Build);
    }

    /// <summary>
    /// Gets (building and caching on first use) the mapping for <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The CLR type to map.</typeparam>
    /// <returns>The mapping for the type.</returns>
    public static EntityMetadata Get<T>() => Get(typeof(T));

    /// <summary>
    /// Removes every cached mapping. Intended for tests.
    /// </summary>
    public static void Clear() => Cache.Clear();

    private static EntityMetadata Build(Type type)
    {
        var nodeAttribute = type.GetCustomAttribute<NodeAttribute>(inherit: true);
        var relationshipAttribute = type.GetCustomAttribute<RelationshipAttribute>(inherit: true);

        if (nodeAttribute != null && relationshipAttribute != null)
        {
            throw new GraphMappingException(
                $"Type '{type.FullName}' is annotated with both [Node] and [Relationship]. A CLR type maps to either a node or a relationship, not both.");
        }

        if (type.IsAbstract || type.IsInterface)
        {
            throw new GraphMappingException(
                $"Type '{type.FullName}' cannot be mapped because it is abstract or an interface. Map a concrete type instead.");
        }

        var kind = relationshipAttribute != null ? EntityKind.Relationship : EntityKind.Node;

        var labels = kind == EntityKind.Node
            ? (IReadOnlyList<string>)(nodeAttribute?.Labels ?? new[] { type.Name })
            : new string[0];

        var relationshipType = kind == EntityKind.Relationship
            ? relationshipAttribute.Type ?? type.Name
            : null;

        EntityPropertyMetadata idProperty = null;
        var properties = new List<EntityPropertyMetadata>();
        var navigations = new List<NavigationMetadata>();
        var graphNames = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var member in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (member.GetIndexParameters().Length > 0)
            {
                continue;
            }

            if (!member.CanRead || member.GetCustomAttribute<IgnoreAttribute>(inherit: true) != null)
            {
                continue;
            }

            var navigationAttribute = member.GetCustomAttribute<RelationshipAttribute>(inherit: true);

            if (navigationAttribute != null)
            {
                navigations.Add(BuildNavigation(type, member, navigationAttribute));
                continue;
            }

            if (member.GetCustomAttribute<GraphIdAttribute>(inherit: true) != null)
            {
                if (idProperty != null)
                {
                    throw new GraphMappingException(
                        $"Type '{type.FullName}' declares more than one [GraphId] property ('{idProperty.ClrName}' and '{member.Name}'). Exactly one is allowed.");
                }

                var idType = ScalarTypes.Unwrap(member.PropertyType);

                if (idType != typeof(int) && idType != typeof(long))
                {
                    throw new GraphMappingException(
                        $"Property '{type.FullName}.{member.Name}' is annotated with [GraphId] but is of type '{member.PropertyType.Name}'. FalkorDB entity ids are integers, so the property must be int, long or a nullable of those.");
                }

                idProperty = new EntityPropertyMetadata(member, graphName: null, isId: true);
                continue;
            }

            if (!ScalarTypes.IsStorable(member.PropertyType))
            {
                throw new GraphMappingException(
                    $"Property '{type.FullName}.{member.Name}' has type '{member.PropertyType.Name}', which FalkorDB cannot store as a graph property. " +
                    "Annotate it with [Relationship] if it is a traversal, or with [Ignore] to exclude it from the mapping.");
            }

            var graphName = member.GetCustomAttribute<PropertyAttribute>(inherit: true)?.Name ?? member.Name;

            if (graphNames.TryGetValue(graphName, out var existing))
            {
                throw new GraphMappingException(
                    $"Type '{type.FullName}' maps both '{existing}' and '{member.Name}' to the graph property '{graphName}'. Property keys must be unique.");
            }

            graphNames.Add(graphName, member.Name);
            properties.Add(new EntityPropertyMetadata(member, graphName, isId: false));
        }

        return new EntityMetadata(type, kind, labels, relationshipType, idProperty, properties, navigations);
    }

    private static NavigationMetadata BuildNavigation(Type declaringType, PropertyInfo member, RelationshipAttribute attribute)
    {
        var isCollection = ScalarTypes.TryGetElementType(member.PropertyType, out var elementType);
        var targetType = isCollection ? elementType : member.PropertyType;

        if (ScalarTypes.IsScalar(targetType))
        {
            throw new GraphMappingException(
                $"Property '{declaringType.FullName}.{member.Name}' is annotated with [Relationship] but its target type '{targetType.Name}' is a scalar. A navigation must point at a mapped node or relationship type.");
        }

        return new NavigationMetadata(member, attribute.Type ?? member.Name, attribute.Direction, targetType, isCollection);
    }
}
