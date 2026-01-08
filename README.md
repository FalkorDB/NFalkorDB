![FalkorDB Official  NET support-04-25](https://github.com/user-attachments/assets/659113e1-7e5b-433a-8a1d-199324278e22)
[![Discord](https://img.shields.io/discord/1146782921294884966?style=flat-square)](https://discord.gg/ErBEqN9E)
[![license](https://img.shields.io/github/license/FalkorDB/NFalkorDB.svg)](https://github.com/FalkorDB/NFalkorDB/blob/master/LICENSE)
[![Release](https://img.shields.io/github/release/FalkorDB/NFalkorDB.svg)](https://github.com/FalkorDB/NFalkorDB/releases/latest)
[![Build Status](https://github.com/falkordb/NFalkorDB/actions/workflows/dotnet.yml/badge.svg)](https://github.com/falkordb/NFalkorDB/actions/workflows/dotnet.yml)

# NFalkorDB

[![Try Free](https://img.shields.io/badge/Try%20Free-FalkorDB%20Cloud-FF8101?labelColor=FDE900&style=for-the-badge&link=https://app.falkordb.cloud)](https://app.falkordb.cloud)

## What is NFalkorDB?

**NFalkorDB** extends [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis) with a .NET-friendly API for working with the [FalkorDB](https://www.falkordb.com) Redis module—bringing graph-native commands into your C# projects with ease.

Built on top of the native `Execute` and `ExecuteAsync` methods, NFalkorDB offers a set of extension methods that mirrors the command structure of [Jedis](https://github.com/xetorthio/jedis)'s FalkorDB support, giving you familiar, fluent access to graph operations.

## Install

```
PM> Install-Package NFalkorDB -Version 1.0.0
```
## Prerequisites

Before using NFalkorDB, ensure the FalkorDB module is installed on your Redis server.

To verify:

```
MODULE LIST
```

Expected output (version may vary):

```
1) "name"
2) "graph"
3) "ver"
4) 4) (integer) 20811
```

## Usage

NFalkorDB exposes FalkorDB commands as C# extension methods through StackExchange.Redis.

### Connection & DB-level commands (Phase 1)

`FalkorDB` can be constructed either from an existing `IDatabase` or from a StackExchange.Redis configuration string.

```c#
// Using a configuration string / URL
var client = new FalkorDB("localhost:6379");

// Select a graph
Graph graph = client.SelectGraph("social");

// DB-level helpers
var graphs = client.ListGraphs();              // GRAPH.LIST
string timeout = client.GetConfig("TIMEOUT_MS"); // GRAPH.CONFIG GET TIMEOUT_MS
client.SetConfig("TIMEOUT_MS", 1000);         // GRAPH.CONFIG SET TIMEOUT_MS 1000
```

Async counterparts `ListGraphsAsync`, `GetConfigAsync`, and `SetConfigAsync` are also available.

### Graph commands & introspection (Phase 2)

The `Graph` class wraps core FalkorDB graph commands:

- `Query` / `QueryAsync` and `ReadOnlyQuery` / `ReadOnlyQueryAsync` with optional `timeout` in milliseconds.
- `Copy` / `CopyAsync` for `GRAPH.COPY`.
- `Delete` / `DeleteAsync` for `GRAPH.DELETE`.
- `Explain` / `ExplainAsync` for `GRAPH.EXPLAIN`.
- `Profile` / `ProfileAsync` for `GRAPH.PROFILE`.
- `Slowlog`, `SlowlogAsync`, `SlowlogReset`, `SlowlogResetAsync` for `GRAPH.SLOWLOG`.

### Index helpers (Phase 3)

NFalkorDB provides helpers to create, drop, and list indices:

```c#
// Node indices
graph.CreateNodeRangeIndex("Person", "name");
graph.CreateNodeFulltextIndex("Person", "bio");
graph.CreateNodeVectorIndex("Person", dimension: 1536, similarityFunction: "euclidean", "embedding");

// Edge indices
graph.CreateEdgeRangeIndex("KNOWS", "since");

// Drop indices
graph.DropNodeRangeIndex("Person", "name");

// List indices (also ListIndicesAsync)
var indexResult = graph.ListIndices(); // CALL db.indexes
```

### Constraint helpers (Phase 4)

Constraint APIs mirror FalkorDB `GRAPH.CONSTRAINT` support and auto-create required range indices for unique constraints:

```c#
// Node constraints
graph.CreateNodeUniqueConstraint("Person", "id");
graph.CreateNodeMandatoryConstraint("Person", "name");

// Edge constraints
graph.CreateEdgeUniqueConstraint("KNOWS", "since");

// Drop constraints
graph.DropNodeUniqueConstraint("Person", "id");

// List constraints (also ListConstraintsAsync)
var constraints = graph.ListConstraints(); // CALL DB.CONSTRAINTS
```

### Schema cache & version mismatch (Phase 5)

NFalkorDB caches labels, relationship types, and property keys. If the server responds with a `"version mismatch"` error, the client:

1. Throws `SchemaVersionMismatchException` internally.
2. Refreshes the schema cache (`db.labels`, `db.propertyKeys`, `db.relationshipTypes`).
3. Retries the query once transparently.

### Extended scalar types (Phase 6)

`ResultSet` now decodes additional scalar types returned by FalkorDB:

- `VALUE_DATETIME` → `DateTime` (UTC) via Unix time in ms.
- `VALUE_DATE` → `DateTime.Date`.
- `VALUE_TIME` → `TimeSpan` (time since midnight in ms).
- `VALUE_DURATION` → `TimeSpan` (duration in ms).

Existing types (nodes, edges, paths, maps, arrays, points, vectors) remain supported.

### Async parity (Phase 7)

Wherever synchronous helpers exist, async counterparts are being added. In particular:

- DB-level: `ListGraphsAsync`, `GetConfigAsync`, `SetConfigAsync`.
- Graph-level: `QueryAsync`, `ReadOnlyQueryAsync`, `CopyAsync`, `DeleteAsync`, `ExplainAsync`, `ProfileAsync`, `SlowlogAsync`, `SlowlogResetAsync`.
- Listing operations: `ListIndicesAsync`, `ListConstraintsAsync`.

Use these in combination with `await` to integrate FalkorDB graph operations into async application flows.

### Getting Started

```c#
  // Connect the database and pick a Graph
  ConnectionMultiplexer muxr = ConnectionMultiplexer.Connect(ConnectionString).
  Graph graph = new FalkorDB(muxr.GetDatabase()).SelectGraph("social");

  // Create the Graph
  graph.Query("""CREATE (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
           (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
           (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})""");

  // Query the Graph
  ResultSet resultSet = graph.ReadOnlyQuery("MATCH (a:person)-[r:knows]->(b:person) RETURN a, r, b");
```

### More examples

For real-world usage and supported operations, see our integration tests:

👉 [NFalkorDBAPITest.cs](https://github.com/falkordb/NFalkorDB/blob/master/NFalkorDB.Tests/FalkorDBAPITest.cs)

These tests cover core functionality, including querying, creating, updating, and deleting graph data.
[Integration Tests](https://github.com/falkordb/NFalkorDB/blob/master/NFalkorDB.Tests/FalkorDBAPITest.cs)

## License

NFalkorDB is licensed under the Apache-2.0 [license ](https://github.com/FalkorDB/NFalkorDB/blob/master/LICENSE).
