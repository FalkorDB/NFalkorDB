using System.Linq;

namespace NFalkorDB
{
    internal class GraphCacheList
    {
        protected readonly string GraphId;
        protected readonly string Procedure;
        protected readonly FalkorDB FalkorDB;
        
        private readonly object _locker = new object();
        
        private string[] _data;

        internal GraphCacheList(string graphId, string procedure, FalkorDB falkordb)
        {
            GraphId = graphId;
            Procedure = procedure;
            FalkorDB = falkordb;
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

        protected virtual ResultSet CallProcedure() =>
            FalkorDB.CallProcedure(GraphId, Procedure);
    }

    internal class ReadOnlyGraphCacheList : GraphCacheList
    {
        internal ReadOnlyGraphCacheList(string graphId, string procedure, FalkorDB falkordb) : 
            base(graphId, procedure, falkordb)
        {
        }

        protected override ResultSet CallProcedure() =>
            FalkorDB.CallProcedureReadOnly(GraphId, Procedure);
    }
}