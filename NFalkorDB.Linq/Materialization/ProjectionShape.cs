using System;
using System.Collections.Generic;
using System.Reflection;
using NFalkorDB.Linq.Mapping;

namespace NFalkorDB.Linq.Materialization;

/// <summary>
/// Describes how one row of a <see cref="ResultSet"/> is turned into a CLR value. The shape is
/// built once when the query is translated and then applied to every row.
/// </summary>
internal abstract class ProjectionShape
{
    internal abstract Type ResultType { get; }

    /// <summary>
    /// The type the projected value is actually read as. This differs from
    /// <see cref="ResultType"/> only when the projection boxes its result.
    /// </summary>
    internal virtual Type ValueType => ResultType;

    internal abstract object Materialize(Record record);
}

/// <summary>
/// Reads a single column of the row.
/// </summary>
internal sealed class ColumnShape : ProjectionShape
{
    private readonly int _index;
    private readonly Type _resultType;
    private readonly Type _valueType;
    private readonly EntityMetadata _entity;
    private readonly string _context;

    internal ColumnShape(int index, Type resultType, EntityMetadata entity, string context)
        : this(index, resultType, resultType, entity, context)
    {
    }

    /// <summary>
    /// Creates a column whose declared type differs from the type its value is converted to, which
    /// happens when a projection boxes its result — <c>Select(p =&gt; (object)p.Age)</c> is declared
    /// as <see cref="object"/> but must still be read as an <see cref="int"/>.
    /// </summary>
    /// <param name="index">The zero-based column index in the row.</param>
    /// <param name="resultType">The type the caller's query is declared to return.</param>
    /// <param name="valueType">The type the column value is converted to before boxing.</param>
    /// <param name="entity">The entity to materialize, or null for a scalar column.</param>
    /// <param name="context">A description of the column used in error messages.</param>
    internal ColumnShape(int index, Type resultType, Type valueType, EntityMetadata entity, string context)
    {
        _index = index;
        _resultType = resultType;
        _valueType = valueType;
        _entity = entity;
        _context = context;
    }

    internal override Type ResultType => _resultType;

    internal override Type ValueType => _valueType;

    internal override object Materialize(Record record)
    {
        if (_index >= record.Values.Count)
        {
            throw new GraphMappingException(
                $"The query returned {record.Values.Count} column(s) but the projection expected at least {_index + 1}.");
        }

        var value = record.Values[_index];

        return _entity != null
            ? EntityMaterializer.Materialize(value, _entity)
            : ValueConverter.Convert(value, _valueType, _context);
    }
}

/// <summary>
/// Builds an object from constructor arguments and/or member assignments, which covers both
/// anonymous types and member-init projections.
/// </summary>
internal sealed class ObjectShape : ProjectionShape
{
    private readonly ConstructorInfo _constructor;
    private readonly ProjectionShape[] _arguments;
    private readonly IReadOnlyList<KeyValuePair<MemberInfo, ProjectionShape>> _bindings;
    private readonly Type _resultType;

    internal ObjectShape(
        Type resultType,
        ConstructorInfo constructor,
        ProjectionShape[] arguments,
        IReadOnlyList<KeyValuePair<MemberInfo, ProjectionShape>> bindings)
    {
        _resultType = resultType;
        _constructor = constructor;
        _arguments = arguments;
        _bindings = bindings;
    }

    internal override Type ResultType => _resultType;

    internal override object Materialize(Record record)
    {
        var arguments = new object[_arguments.Length];

        for (var i = 0; i < _arguments.Length; i++)
        {
            arguments[i] = _arguments[i].Materialize(record);
        }

        var instance = _constructor.Invoke(arguments);

        foreach (var binding in _bindings)
        {
            var value = binding.Value.Materialize(record);

            switch (binding.Key)
            {
                case PropertyInfo property:
                    property.SetValue(instance, value);
                    break;
                case FieldInfo field:
                    field.SetValue(instance, value);
                    break;
                default:
                    throw new GraphMappingException(
                        $"Cannot assign the projected member '{binding.Key.Name}' of '{_resultType.Name}'.");
            }
        }

        return instance;
    }
}
