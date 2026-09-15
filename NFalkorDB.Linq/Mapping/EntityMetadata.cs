using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using NFalkorDB.Linq.Translation;

namespace NFalkorDB.Linq.Mapping;

/// <summary>
/// Whether a mapped CLR type represents a node or a relationship.
/// </summary>
public enum EntityKind
{
    /// <summary>
    /// The type maps to a graph node.
    /// </summary>
    Node = 0,

    /// <summary>
    /// The type maps to a graph relationship.
    /// </summary>
    Relationship = 1
}

/// <summary>
/// The reflected mapping between a CLR type and a FalkorDB node or relationship.
/// </summary>
public sealed class EntityMetadata
{
    private readonly Dictionary<string, EntityPropertyMetadata> _byClrName;
    private readonly Dictionary<string, NavigationMetadata> _navigationsByClrName;

    internal EntityMetadata(
        Type clrType,
        EntityKind kind,
        IReadOnlyList<string> labels,
        string relationshipType,
        EntityPropertyMetadata idProperty,
        IReadOnlyList<EntityPropertyMetadata> properties,
        IReadOnlyList<NavigationMetadata> navigations)
    {
        ClrType = clrType;
        Kind = kind;
        Labels = labels;
        RelationshipType = relationshipType;
        IdProperty = idProperty;
        Properties = properties;
        Navigations = navigations;

        _byClrName = properties.ToDictionary(p => p.ClrName, StringComparer.Ordinal);
        _navigationsByClrName = navigations.ToDictionary(n => n.ClrName, StringComparer.Ordinal);
    }

    /// <summary>
    /// The mapped CLR type.
    /// </summary>
    public Type ClrType { get; }

    /// <summary>
    /// Whether the type maps to a node or a relationship.
    /// </summary>
    public EntityKind Kind { get; }

    /// <summary>
    /// The node labels. Empty for relationship types.
    /// </summary>
    public IReadOnlyList<string> Labels { get; }

    /// <summary>
    /// The relationship type. <c>null</c> for node types.
    /// </summary>
    public string RelationshipType { get; }

    /// <summary>
    /// The property carrying the FalkorDB internal entity id, or <c>null</c> when the type does
    /// not declare one.
    /// </summary>
    public EntityPropertyMetadata IdProperty { get; }

    /// <summary>
    /// The scalar properties mapped to graph property keys.
    /// </summary>
    public IReadOnlyList<EntityPropertyMetadata> Properties { get; }

    /// <summary>
    /// The navigation properties declared on the type.
    /// </summary>
    public IReadOnlyList<NavigationMetadata> Navigations { get; }

    /// <summary>
    /// Looks a mapped scalar property up by its CLR property name.
    /// </summary>
    /// <param name="clrName">The CLR property name.</param>
    /// <param name="property">The mapped property when found.</param>
    /// <returns><c>true</c> when the CLR property is mapped.</returns>
    public bool TryGetProperty(string clrName, out EntityPropertyMetadata property) =>
        _byClrName.TryGetValue(clrName, out property);

    /// <summary>
    /// Looks a navigation property up by its CLR property name.
    /// </summary>
    /// <param name="clrName">The CLR property name.</param>
    /// <param name="navigation">The navigation when found.</param>
    /// <returns><c>true</c> when the CLR property is a navigation.</returns>
    public bool TryGetNavigation(string clrName, out NavigationMetadata navigation) =>
        _navigationsByClrName.TryGetValue(clrName, out navigation);

    /// <summary>
    /// The label pattern fragment for a node, for example <c>:Person:Employee</c>.
    /// Labels that are not bare identifiers are backtick-escaped, so the fragment is always safe to
    /// splice into a Cypher pattern. Empty when the type declares no labels.
    /// </summary>
    public string LabelPattern
    {
        get
        {
            if (Labels.Count == 0)
            {
                return string.Empty;
            }

            var escaped = new string[Labels.Count];

            for (var index = 0; index < Labels.Count; index++)
            {
                escaped[index] = CypherIdentifier.Escape(Labels[index]);
            }

            return ":" + string.Join(":", escaped);
        }
    }
}

/// <summary>
/// The mapping between a single CLR property and a graph property key.
/// </summary>
public sealed class EntityPropertyMetadata
{
    internal EntityPropertyMetadata(PropertyInfo member, string graphName, bool isId)
    {
        Member = member;
        ClrName = member.Name;
        GraphName = graphName;
        IsId = isId;
        ClrType = member.PropertyType;
        Getter = CompiledAccessors.CreateGetter(member);
        Setter = member.CanWrite ? CompiledAccessors.CreateSetter(member) : null;
    }

    /// <summary>
    /// The reflected CLR property.
    /// </summary>
    public PropertyInfo Member { get; }

    /// <summary>
    /// The CLR property name.
    /// </summary>
    public string ClrName { get; }

    /// <summary>
    /// The graph property key the CLR property is stored under.
    /// </summary>
    public string GraphName { get; }

    /// <summary>
    /// The CLR type of the property.
    /// </summary>
    public Type ClrType { get; }

    /// <summary>
    /// Whether the property carries the FalkorDB internal entity id.
    /// </summary>
    public bool IsId { get; }

    /// <summary>
    /// A compiled getter for the property.
    /// </summary>
    public Func<object, object> Getter { get; }

    /// <summary>
    /// A compiled setter for the property, or <c>null</c> when the property is read-only.
    /// </summary>
    public Action<object, object> Setter { get; }
}

/// <summary>
/// A navigation property: a CLR property that stands for a relationship traversal rather than a
/// stored graph property.
/// </summary>
public sealed class NavigationMetadata
{
    internal NavigationMetadata(PropertyInfo member, string relationshipType, TraversalDirection direction, Type targetType, bool isCollection)
    {
        Member = member;
        ClrName = member.Name;
        RelationshipType = relationshipType;
        Direction = direction;
        TargetType = targetType;
        IsCollection = isCollection;
    }

    /// <summary>
    /// The reflected CLR property.
    /// </summary>
    public PropertyInfo Member { get; }

    /// <summary>
    /// The CLR property name.
    /// </summary>
    public string ClrName { get; }

    /// <summary>
    /// The relationship type traversed by this navigation.
    /// </summary>
    public string RelationshipType { get; }

    /// <summary>
    /// The direction the relationship is traversed in.
    /// </summary>
    public TraversalDirection Direction { get; }

    /// <summary>
    /// The CLR type at the far end of the relationship.
    /// </summary>
    public Type TargetType { get; }

    /// <summary>
    /// Whether the navigation is a collection (one-to-many) rather than a single reference.
    /// </summary>
    public bool IsCollection { get; }
}
