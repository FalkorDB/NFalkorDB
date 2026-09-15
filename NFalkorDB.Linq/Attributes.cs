using System;

namespace NFalkorDB.Linq;

/// <summary>
/// Marks a CLR type as a graph node and declares the label(s) it is stored under.
/// When the attribute is absent the CLR type name is used as the single label.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = true)]
public sealed class NodeAttribute : Attribute
{
    /// <summary>
    /// Declares one or more labels for the annotated type.
    /// </summary>
    /// <param name="labels">The labels the node is stored under. At least one label is required, and no label may be null or blank.</param>
    public NodeAttribute(params string[] labels)
    {
        if (labels == null || labels.Length == 0)
        {
            throw new ArgumentException("A [Node] attribute requires at least one label.", nameof(labels));
        }

        foreach (var label in labels)
        {
            if (string.IsNullOrWhiteSpace(label))
            {
                throw new ArgumentException("A [Node] label cannot be null, empty or blank.", nameof(labels));
            }
        }

        Labels = labels;
    }

    /// <summary>
    /// The labels the node is stored under.
    /// </summary>
    public string[] Labels { get; }
}

/// <summary>
/// Marks a CLR type as a graph relationship, or a CLR property as a relationship traversal
/// (a navigation property). When applied to a type and no name is given, the CLR type name is
/// used as the relationship type.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class RelationshipAttribute : Attribute
{
    /// <summary>
    /// Declares a relationship using the CLR type or property name as the relationship type.
    /// </summary>
    public RelationshipAttribute()
    {
    }

    /// <summary>
    /// Declares a relationship with an explicit relationship type.
    /// </summary>
    /// <param name="type">The relationship type, for example <c>KNOWS</c>.</param>
    public RelationshipAttribute(string type)
    {
        if (string.IsNullOrEmpty(type))
        {
            throw new ArgumentException("A [Relationship] type cannot be null or empty.", nameof(type));
        }

        Type = type;
    }

    /// <summary>
    /// The relationship type, or <c>null</c> when the CLR type/property name should be used.
    /// </summary>
    public string Type { get; }

    /// <summary>
    /// The direction the relationship is traversed in. Only meaningful on navigation properties.
    /// Defaults to <see cref="TraversalDirection.Outgoing"/>.
    /// </summary>
    public TraversalDirection Direction { get; set; } = TraversalDirection.Outgoing;
}

/// <summary>
/// Marks the CLR property that holds the FalkorDB internal entity id. The property is never
/// read from or written to the entity property map; it is translated to <c>id(alias)</c>.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class GraphIdAttribute : Attribute
{
}

/// <summary>
/// Overrides the graph property key a CLR property is mapped to. When the attribute is absent
/// the CLR property name is used as the key.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class PropertyAttribute : Attribute
{
    /// <summary>
    /// Maps the annotated CLR property to an explicit graph property key.
    /// </summary>
    /// <param name="name">The graph property key.</param>
    public PropertyAttribute(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("A [Property] name cannot be null or empty.", nameof(name));
        }

        Name = name;
    }

    /// <summary>
    /// The graph property key.
    /// </summary>
    public string Name { get; }
}

/// <summary>
/// Excludes a CLR property from the mapping entirely. Ignored properties can neither be queried
/// nor materialized.
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class IgnoreAttribute : Attribute
{
}

/// <summary>
/// The direction a relationship is traversed in.
/// </summary>
public enum TraversalDirection
{
    /// <summary>
    /// Traverse from the source to the target, rendered as <c>-[:TYPE]-&gt;</c>.
    /// </summary>
    Outgoing = 0,

    /// <summary>
    /// Traverse from the target back to the source, rendered as <c>&lt;-[:TYPE]-</c>.
    /// </summary>
    Incoming = 1,

    /// <summary>
    /// Traverse in either direction, rendered as <c>-[:TYPE]-</c>.
    /// </summary>
    Any = 2
}
