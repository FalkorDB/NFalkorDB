namespace NFalkorDB
{
    internal interface IGraphCache
    {
        string GetLabel(int index);
        string GetRelationshipType(int index);
        string GetPropertyName(int index);
    }
    
    internal abstract class BaseGraphCache : IGraphCache
    {
        protected GraphCacheList Labels { get; set; }
        protected GraphCacheList PropertyNames { get; set; }
        protected GraphCacheList RelationshipTypes { get; set; }

        public string GetLabel(int index) => Labels.GetCachedData(index);

        public string GetRelationshipType(int index) => RelationshipTypes.GetCachedData(index);

        public string GetPropertyName(int index) => PropertyNames.GetCachedData(index);
    }

    internal sealed class GraphCache : BaseGraphCache
    {
        public GraphCache(string graphId, FalkorDB falkordb)
        {
            Labels = new GraphCacheList(graphId, "db.labels", falkordb);
            PropertyNames = new GraphCacheList(graphId, "db.propertyKeys", falkordb);
            RelationshipTypes = new GraphCacheList(graphId, "db.relationshipTypes", falkordb);
        }
    }

    internal sealed class ReadOnlyGraphCache : BaseGraphCache
    {
        public ReadOnlyGraphCache(string graphId, FalkorDB falkordb)
        {
            Labels = new ReadOnlyGraphCacheList(graphId, "db.labels", falkordb);
            PropertyNames = new ReadOnlyGraphCacheList(graphId, "db.propertyKeys", falkordb);
            RelationshipTypes = new ReadOnlyGraphCacheList(graphId, "db.relationshipTypes", falkordb);
        }
    }
}