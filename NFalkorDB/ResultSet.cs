using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;

namespace NFalkorDB;

/// <summary>
/// Represents the result from a FalkorDB query.
/// </summary>
public sealed class ResultSet : IReadOnlyCollection<Record>
{
    internal enum ResultSetScalarType
    {
        VALUE_UNKNOWN,
        VALUE_NULL,
        VALUE_STRING,
        VALUE_INT64,
        VALUE_BOOLEAN,
        VALUE_DOUBLE,
        VALUE_ARRAY,
        VALUE_EDGE,
        VALUE_NODE,
        VALUE_PATH,
        VALUE_MAP,
        VALUE_POINT,
        VALUE_VECTOR
    }

    private readonly RedisResult[] _rawResults;
    private readonly IGraphCache _graphCache;

    internal ResultSet(RedisResult result, IGraphCache graphCache)
    {
        if (result.Resp2Type == ResultType.Array)
        {
            var resultArray = (RedisResult[])result;

            ScanForErrors(resultArray);

            _graphCache = graphCache;

            if (resultArray.Length == 3)
            {
                Header = new Header(resultArray[0]);
                Statistics = new Statistics(resultArray[2]);

                _rawResults = (RedisResult[])resultArray[1];

                Count = _rawResults.Length;
            }
            else
            {
                Statistics = new Statistics(resultArray[resultArray.Length - 1]);
                Count = 0;
            }
        }
        else
        {
            if (result.Resp2Type == ResultType.Error)
            {
                throw new NFalkorDBRunTimeException(result.ToString());
            }

            Statistics = new Statistics(result);
            Count = 0;
        }
    }

    /// <summary>
    /// FalkorDB statistics associated with this result set.
    /// </summary>
    /// <value></value>
    public Statistics Statistics { get; }

    /// <summary>
    /// FalkorDB header associated with this result set.
    /// </summary>
    /// <value></value>
    public Header Header { get; }

    /// <summary>
    /// Number of records in the result.
    /// </summary>
    /// <value></value>
    public int Count { get; }

    /// <summary>
    /// Get the enumerator for this result set.
    /// </summary>
    /// <returns></returns>
    public IEnumerator<Record> GetEnumerator() => RecordIterator().GetEnumerator();

    /// <summary>
    /// Get the enumerator for this result set.
    /// </summary>
    /// <returns></returns>
    IEnumerator IEnumerable.GetEnumerator() => RecordIterator().GetEnumerator();

    private IEnumerable<Record> RecordIterator()
    {
        if (_rawResults == default)
        {
            yield break;
        }
        else
        {
            foreach (RedisResult[] row in _rawResults)
            {
                var parsedRow = new List<object>(row.Length);

                for (int i = 0; i < row.Length; i++)
                {
                    var obj = (RedisResult[])row[i];

                    parsedRow.Add(DeserializeScalar(obj));
                }

                yield return new Record(Header.SchemaNames, parsedRow);
            }

            yield break;
        }
    }

    private Node DeserializeNode(RedisResult[] rawNodeData)
    {
        var node = new Node();

        DeserializeGraphEntityId(node, rawNodeData[0]);

        var labelIndices = (int[])rawNodeData[1];

        foreach (var labelIndex in labelIndices)
        {
            var label = _graphCache.GetLabel(labelIndex);

            node.AddLabel(label);
        }

        DeserializeGraphEntityProperties(node, (RedisResult[])rawNodeData[2]);

        return node;
    }

    private Edge DeserializeEdge(RedisResult[] rawEdgeData)
    {
        var edge = new Edge();

        DeserializeGraphEntityId(edge, rawEdgeData[0]);

        edge.RelationshipType = _graphCache.GetRelationshipType((int)rawEdgeData[1]);
        edge.Source = (int)rawEdgeData[2];
        edge.Destination = (int)rawEdgeData[3];

        DeserializeGraphEntityProperties(edge, (RedisResult[])rawEdgeData[4]);

        return edge;
    }

    private object DeserializeScalar(RedisResult[] rawScalarData)
    {
        var type = GetValueTypeFromObject(rawScalarData[0]);

        switch (type)
        {
            case ResultSetScalarType.VALUE_NULL:
                return null;
            case ResultSetScalarType.VALUE_BOOLEAN:
                return bool.Parse((string)rawScalarData[1]);
            case ResultSetScalarType.VALUE_DOUBLE:
                return (double)rawScalarData[1];
            case ResultSetScalarType.VALUE_INT64:
                return (long)rawScalarData[1];
            case ResultSetScalarType.VALUE_STRING:
                return (string)rawScalarData[1];
            case ResultSetScalarType.VALUE_ARRAY:
                return DeserializeArray((RedisResult[])rawScalarData[1]);
            case ResultSetScalarType.VALUE_NODE:
                return DeserializeNode((RedisResult[])rawScalarData[1]);
            case ResultSetScalarType.VALUE_EDGE:
                return DeserializeEdge((RedisResult[])rawScalarData[1]);
            case ResultSetScalarType.VALUE_PATH:
              return DeserializePath((RedisResult[])rawScalarData[1]);
            case ResultSetScalarType.VALUE_MAP:
              return DeserializeMap((RedisResult[])rawScalarData[1]);
            case ResultSetScalarType.VALUE_POINT:
                return new Point((double)rawScalarData[1][0], (double)rawScalarData[1][1]);
            case ResultSetScalarType.VALUE_VECTOR:
                return rawScalarData[1];
            case ResultSetScalarType.VALUE_UNKNOWN:
            default:
                return (object)rawScalarData[1];
        }
    }

    private static void DeserializeGraphEntityId(GraphEntity graphEntity, RedisResult rawEntityId) =>
        graphEntity.Id = (int)rawEntityId;

    private void DeserializeGraphEntityProperties(GraphEntity graphEntity, RedisResult[] rawProperties)
    {
        foreach (RedisResult[] rawProperty in rawProperties)
        {
            var property = new Property
            {
                Name = _graphCache.GetPropertyName((int)rawProperty[0]),
                Value = DeserializeScalar(rawProperty.Skip(1).ToArray())
            };

            graphEntity.AddProperty(property);
        }
    }

    private object DeserializeMap(RedisResult[] serializedMap)
    {
        var map = new Dictionary<string, object>(serializedMap.Length / 2);
        for (var i = 0; i < serializedMap.Length; i+=2)
        {
            var key = (String)serializedMap[i];
            map[key] = DeserializeScalar((RedisResult[])serializedMap[i+1]);
        }
        return map;
    }

    private object[] DeserializeArray(RedisResult[] serializedArray)
    {
        var result = new object[serializedArray.Length];

        for (var i = 0; i < serializedArray.Length; i++)
        {
            result[i] = DeserializeScalar((RedisResult[])serializedArray[i]);
        }

        return result;
    }

    private Path DeserializePath(RedisResult[] rawPath)
    {
        var deserializedNodes = (object[])DeserializeScalar((RedisResult[])rawPath[0]);
        var nodes = Array.ConvertAll(deserializedNodes, n => (Node)n);

        var deserializedEdges = (object[])DeserializeScalar((RedisResult[])rawPath[1]);
        var edges = Array.ConvertAll(deserializedEdges, p => (Edge)p);

        return new Path(nodes, edges);
    }

    private static ResultSetScalarType GetValueTypeFromObject(RedisResult rawScalarType) =>
    (ResultSetScalarType)(int)rawScalarType;

    private static void ScanForErrors(RedisResult[] results)
    {
        foreach (var result in results)
        {
            if (result.Resp2Type == ResultType.Error)
            {
                throw new NFalkorDBRunTimeException(result.ToString());
            }
        }
    }
}