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
