using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;
using static NFalkorDB.FalkorDBUtilities;

namespace NFalkorDB;

/// <summary>
/// FalkorDB client.
/// 
/// This class facilitates querying FalkorDB and parsing the results.
/// </summary>
public sealed class FalkorDB
{
    private readonly IDatabase _db;

    /// <summary>
    /// Creates a FalkorDB client that leverages a specified instance of `IDatabase`.
    /// </summary>
    /// <param name="db"></param>
    public FalkorDB(IDatabase db) => _db = db;

    public Graph SelectGraph(string graphId)
    {
        var graph = new Graph(graphId, _db);
        return graph;
    }
}