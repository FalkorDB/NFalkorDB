using System;
using System.Collections.Concurrent;
using System.Linq;
using NFalkorDB.Linq.Mapping;

namespace NFalkorDB.Linq.Materialization;

/// <summary>
/// Turns a <see cref="Node"/> or <see cref="Edge"/> returned by FalkorDB into a mapped CLR object.
/// </summary>
internal static class EntityMaterializer
{
    private static readonly ConcurrentDictionary<Type, Func<object>> Factories =
        new ConcurrentDictionary<Type, Func<object>>();

    internal static object Materialize(object value, EntityMetadata metadata)
    {
        if (value == null)
        {
            return null;
        }

        if (!(value is GraphEntity entity))
        {
            throw new GraphMappingException(
                $"Expected a graph node or relationship to materialize '{metadata.ClrType.Name}' but the query returned a value of type '{value.GetType().Name}'. " +
                "Return the matched entity itself (for example `Select(p => p)`) when projecting onto a mapped type.");
        }

        if (metadata.Kind == EntityKind.Node && !(entity is Node))
        {
            throw new GraphMappingException(
                $"'{metadata.ClrType.Name}' is mapped as a node but the query returned a relationship.");
        }

        if (metadata.Kind == EntityKind.Relationship && !(entity is Edge))
        {
            throw new GraphMappingException(
                $"'{metadata.ClrType.Name}' is mapped as a relationship but the query returned a node.");
        }

        var factory = Factories.GetOrAdd(metadata.ClrType, CreateFactory);
        var instance = factory();

        if (metadata.IdProperty?.Setter != null)
        {
            metadata.IdProperty.Setter(
                instance,
                ValueConverter.Convert(entity.Id, metadata.IdProperty.ClrType, $"{metadata.ClrType.Name}.{metadata.IdProperty.ClrName}"));
        }

        foreach (var property in metadata.Properties)
        {
            if (property.Setter == null)
            {
                continue;
            }

            if (!entity.PropertyMap.TryGetValue(property.GraphName, out var graphProperty))
            {
                continue;
            }

            property.Setter(
                instance,
                ValueConverter.Convert(graphProperty.Value, property.ClrType, $"{metadata.ClrType.Name}.{property.ClrName}"));
        }

        return instance;
    }

    private static Func<object> CreateFactory(Type type)
    {
        var factory = CompiledAccessors.CreateFactory(type);

        if (factory == null)
        {
            throw new GraphMappingException(
                $"Type '{type.FullName}' cannot be materialized because it has no public parameterless constructor.");
        }

        return factory;
    }
}
