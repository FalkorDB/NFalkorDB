using System.Linq;

namespace NFalkorDB;

internal class GraphCacheList
{
    protected readonly string GraphId;
    protected readonly string Procedure;
    protected readonly Graph Graph;
    
    private readonly object _locker = new object();
    
    private string[] _data;

    internal GraphCacheList(string graphId, string procedure, Graph graph)
    {
        GraphId = graphId;
        Procedure = procedure;
        Graph = graph;
    }

    // TODO: Change this to use Lazy<T>?
    internal string GetCachedData(int index)
    {
        if (_data == null || index >= _data.Length)
        {
            lock(_locker)
            {
                if (_data == null || index >= _data.Length)
                {
                    GetProcedureInfo();
                }
            }
        }

        return _data.ElementAtOrDefault(index);
    }

    private void GetProcedureInfo()
    {
        var resultSet = CallProcedure();
        var newData = new string[resultSet.Count];
        var i = 0;

        foreach (var record in resultSet)
        {
            newData[i++] = record.GetString(0);
        }

        _data = newData;
    }

    internal void Refresh()
    {
        lock (_locker)
        {
            _data = null;
            GetProcedureInfo();
        }
    }

    protected virtual ResultSet CallProcedure() =>
        Graph.CallProcedure(Procedure);
}

internal class ReadOnlyGraphCacheList : GraphCacheList
{
    internal ReadOnlyGraphCacheList(string graphId, string procedure, Graph graph) : 
        base(graphId, procedure, graph)
    {
    }

    protected override ResultSet CallProcedure() =>
        Graph.CallProcedureReadOnly(Procedure);
}