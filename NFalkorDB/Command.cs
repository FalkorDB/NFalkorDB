namespace NFalkorDB;

internal static class Command
{
    internal const string QUERY    = "graph.QUERY";
    internal const string DELETE   = "graph.DELETE";
    internal const string RO_QUERY = "graph.RO_QUERY";

    // Phase 1: DB-level commands
    internal const string LIST     = "graph.LIST";
    internal const string CONFIG   = "graph.CONFIG";

    // Phase 2: Graph-level commands
    internal const string COPY       = "graph.COPY";
    internal const string EXPLAIN    = "graph.EXPLAIN";
    internal const string PROFILE    = "graph.PROFILE";
    internal const string SLOWLOG    = "graph.SLOWLOG";
    internal const string CONSTRAINT = "graph.CONSTRAINT";
    internal const string UDF        = "graph.UDF";
}
