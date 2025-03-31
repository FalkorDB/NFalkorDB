![FalkorDB Official  NET support-04-25](https://github.com/user-attachments/assets/659113e1-7e5b-433a-8a1d-199324278e22)
# NFalkorDB

[![Build Status](https://github.com/falkordb/NFalkorDB/actions/workflows/dotnet.yml/badge.svg)](https://github.com/falkordb/NFalkorDB/actions/workflows/dotnet.yml)

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

For real-world usage and supported operations, see our integration tests:

👉 [NFalkorDBAPITest.cs](https://github.com/falkordb/NFalkorDB/blob/master/NFalkorDB.Tests/FalkorDBAPITest.cs)

These tests cover core functionality, including querying, creating, updating, and deleting graph data.
