![FalkorDB Official  NET support-04-25](https://github.com/user-attachments/assets/659113e1-7e5b-433a-8a1d-199324278e22)
[![Discord](https://img.shields.io/discord/1146782921294884966?style=flat-square)](https://discord.gg/6M4QwDXn2w)
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

## LINQ (NFalkorDB.Linq)

`NFalkorDB.Linq` adds a LINQ-to-Cypher `IQueryable` provider on top of the driver. LINQ expressions are
translated into Cypher and executed **server side** — the provider never falls back to client-side
evaluation. Anything it cannot translate throws a descriptive `NotSupportedException`.

### Mapping your entities

```c#
[Node("Person")]              // multiple labels: [Node("Company", "Organization")]
public class Person
{
    [GraphId]                 // FalkorDB's internal entity id
    public int Id { get; set; }

    [Property("name")]        // key override; without it the CLR name is used
    public string Name { get; set; }

    public int Age { get; set; }

    [Ignore]                  // never read or written
    public string Scratch { get; set; }

    [Relationship("KNOWS")]   // navigation property, see below
    public List<Person> Knows { get; set; }
}

[Relationship("KNOWS")]
public class Knows
{
    [GraphId] public int Id { get; set; }
    public int Since { get; set; }
}
```

Attributes are optional: without them the CLR type name becomes the label and CLR property names
become property keys. Metadata is built once by reflection and cached per type.

### Querying

```c#
var graph = new FalkorDB(muxr.GetDatabase()).SelectGraph("social");
var context = new GraphContext(graph);

// MATCH (n0:Person) WHERE n0.age > $p0 RETURN n0 ORDER BY n0.name ASC LIMIT 10
var adults = context.Nodes<Person>()
    .Where(p => p.Age > 21)
    .OrderBy(p => p.Name)
    .Take(10)
    .ToList();

// MATCH ()-[r0:KNOWS]->() WHERE r0.since >= $p0 RETURN r0
var recent = context.Relationships<Knows>().Where(k => k.Since >= 2020).ToList();
```

Supported operators: `Where`, `Select`, `SelectMany`, `OrderBy(Descending)`, `ThenBy(Descending)`,
`Skip`, `Take`, `Distinct`, `First(OrDefault)`, `Single(OrDefault)`, `Any`, `All`, `Count`,
`LongCount`, `Sum`, `Min`, `Max`, `Average`.

Supported predicates: `== != < <= > >=`, `&& || !`, `null` comparisons (`IS NULL` / `IS NOT NULL`),
`string.StartsWith/EndsWith/Contains` (`STARTS WITH` / `ENDS WITH` / `CONTAINS`),
`Enumerable.Contains` (`IN`), `string.ToUpper/ToLower` (`toUpper()` / `toLower()`), and common `Math`
functions. Captured variables are evaluated locally and emitted as parameters.

### Traversal

`Traverse<TEdge, TTarget>()` is the primary traversal design — it is explicit about the relationship
type and the target label:

```c#
// MATCH (n0:Person)-[r0:KNOWS]->(n1:Person) WHERE n0.name = $p0 RETURN n1
var friends = context.Nodes<Person>()
    .Where(p => p.Name == "Alice")
    .Traverse<Knows, Person>()
    .ToList();

// Filter on the relationship, and walk incoming edges
var mentors = context.Nodes<Person>()
    .Traverse<Knows, Person>(k => k.Since > 2015, TraversalDirection.Incoming)
    .ToList();
```

Traversals chain, so `.Traverse<Knows, Person>().Traverse<WorksAt, Company>()` renders a single
multi-hop `MATCH`. As sugar, `SelectMany` over a navigation property does the same thing:

```c#
var pairs = context.Nodes<Person>()
    .SelectMany(p => p.Knows, (p, friend) => new { Who = p.Name, Friend = friend.Name })
    .ToList();
```

### Async

```c#
var people  = await context.Nodes<Person>().Where(p => p.Active).ToListAsync();
var oldest  = await context.Nodes<Person>().OrderByDescending(p => p.Age).FirstOrDefaultAsync();
var total   = await context.Nodes<Person>().CountAsync();
var average = await context.Nodes<Person>().AverageAsync(p => p.Age);
```

`ToListAsync`, `ToArrayAsync`, `FirstAsync`, `FirstOrDefaultAsync`, `SingleAsync`,
`SingleOrDefaultAsync`, `CountAsync`, `LongCountAsync`, `AnyAsync`, `AllAsync`, `SumAsync`,
`MinAsync`, `MaxAsync` and `AverageAsync` are all available, each accepting a `CancellationToken`.
Pure reads use `ReadOnlyQuery`/`ReadOnlyQueryAsync` so they can be served by replicas.

### Inspecting the generated query

```c#
var query = context.Nodes<Person>().Where(p => p.Age > 21).ToCypherQuery();

query.Cypher;      // MATCH (n0:Person) WHERE n0.age > $p0 RETURN n0
query.Parameters;  // { p0 = 21 }

context.Nodes<Person>().Where(p => p.Age > 21).Explain();  // execution plan
context.Nodes<Person>().Where(p => p.Age > 21).Profile();  // profiled plan
```

### Parameterization

Every user value — including captured closure variables and collection literals — is routed through
the parameters dictionary that `FalkorDBUtilities.PrepareQuery` renders as a `CYPHER k=v` prefix.
Values are **never** concatenated into the Cypher text, so hostile input such as
`" OR 1=1 --` is matched as a literal string rather than changing the query.

### Ordering rule

`Where` and `OrderBy` must come before `Select`, and before `Skip`/`Take`/`Distinct`, so the sort and
filter keys can be translated against the matched entity. `Distinct` must come *after* `Select`,
because `RETURN DISTINCT` would otherwise remove duplicates from the projected values rather than
from the matched rows. Reordering them throws a `NotSupportedException` explaining what to do instead.

### Equality semantics

Cypher compares values, while LINQ compares with `EqualityComparer<T>.Default`. Where the two cannot
agree the provider refuses to translate rather than quietly returning a different row set:

- **`Contains` over a collection built with a custom comparer.** `IN` always uses default equality, so
  a `HashSet<string>(StringComparer.OrdinalIgnoreCase)` would silently become a case-sensitive test.
- **`Contains` on anything that is not a standard collection.** Only the standard .NET collection types and the
  LINQ helpers are known to define `Contains` as membership by default equality. A type of your own
  might define it as a range test or some other rule, so it is rejected rather than assumed. A
  dictionary is rejected too, because its `Contains` asks about a key — use `map.Keys.Contains(x)`.
- **`Distinct` over a projection whose equality the provider cannot prove.** `Select(p => new Company { ... })`
  produces objects that LINQ compares by reference — every row is distinct — while `RETURN DISTINCT`
  collapses rows with equal properties. Only anonymous types, records that kept their generated
  equality, scalars and strings are accepted. A hand-written `Equals` is *not* enough, because it may
  ignore members, compare them case-insensitively, or call everything equal.
- **`Distinct` over a collection-valued column.** `Select(p => p.Tags).Distinct()` would compare
  `string[]` by reference in LINQ and keep every row, while Cypher compares lists and maps by value
  and collapses them. The same applies to a collection *member*: `Select(p => new { p.Tags })` has
  structural equality on the outside but still compares the array by reference.
- **`==` and `!=` between collection operands.** `Where(p => p.Tags == tags)` matches nothing in
  memory, because C# compares arrays by reference, but would become `n0.tags = $p0` and match on
  equal contents. Use `Contains` to test membership instead.
- **`Min` and `Max` over a projection the CLR cannot order.** `Select(p => new { p.Age }).Max()`
  renders as `max(n0.age)` while the caller was promised the anonymous type back, and LINQ itself
  throws on a type that is not `IComparable`.

`Distinct` over a whole node is the one deliberate exception, and it is allowed: `RETURN DISTINCT n`
removes duplicates by graph identity, which is exactly what makes `Distinct` useful after a traversal
that reaches the same node twice. Materializing produces a new object per row, so an in-memory
`Distinct` would *not* agree — the provider prefers the graph's own notion of identity here, and says
so rather than leaving it implicit.

A nested query such as `Where(p => p.Age > other.Count())` is rejected for a related reason: evaluating
it would mean running a second query against the server mid-translation. Materialize it first.

### Names that are not bare identifiers

Labels, relationship types and property keys are escaped as Cypher identifiers, so
`[Property("first-name")]` renders as ``n0.`first-name` `` rather than being parsed as a subtraction.

### Temporal values

FalkorDB has no `datetime()` constructor — `RETURN datetime()` answers `Unknown function 'datetime'` —
so a date has to be stored as either a string or a number, and the provider has to pick one. It binds
`DateTime` and `DateTimeOffset` as round-trip ISO-8601 strings **normalized to UTC**
(`2020-01-02T03:04:05.0000000Z`) and `TimeSpan` as whole milliseconds:

```csharp
context.Nodes<Person>().Where(p => p.Joined > cutoff)
// MATCH (n0:Person) WHERE n0.joined > $p0 RETURN n0
// $p0 = "2020-01-02T03:04:05.0000000Z"
```

Normalizing to UTC is what makes the comparison sound. The `"o"` format renders a local `DateTime`
with a `+02:00` suffix, an unspecified one with no suffix and a `DateTimeOffset` with whatever offset
it carries, so three spellings of the same instant would not compare equal to each other and would
sort by wall-clock text rather than by instant. Converted to UTC they are all the same width, so the
lexical order is the chronological order, and `>`, `<` and `ORDER BY` all behave. A `DateTime` with
`DateTimeKind.Unspecified` has no offset to apply and is read as UTC.

**Store your dates the same way.** Cypher compares mismatched types as `false` rather than raising an
error — `RETURN '2020-07-15' > 1600000000` answers `false` — so if a property holds epoch milliseconds
while the mapped CLR property is a `DateTime`, every predicate quietly matches nothing. A schemaless
database gives the provider no way to detect that, so map an epoch-milliseconds property as `long`
and convert it yourself.

### Enums

Enum properties are stored as member names, which keeps the graph readable and makes equality work
naturally: `Where(p => p.Rating == Rating.Great)` renders `n0.rating = $p0` with `$p0` bound to
`"Great"`.

The trade-off is that Cypher then compares those names lexically. Ordering `Poor`, `Good`, `Great`
by name gives `Good, Great, Poor`, not the `0, 1, 2` order LINQ uses. Rather than return a different
order than LINQ would, the provider rejects the operations where this matters — `OrderBy`,
`OrderByDescending`, `ThenBy`, `ThenByDescending`, `Min`, `Max`, and the relational operators
`<`, `<=`, `>`, `>=` — with a `NotSupportedException`. Equality, `!=` and `Contains` are unaffected.
Map the property to a numeric type if you need to order by it.

Casting an enum to its underlying type is rejected for the same reason, because the stored value is
a name rather than a number:

```csharp
context.Nodes<Person>().Select(p => (int)p.Rating);   // NotSupportedException
context.Nodes<Person>().OrderBy(p => (int)p.Rating);  // NotSupportedException
```

Inside a comparison the cast is fine, since the provider rebinds the other side to its member name.
Both of these render `n0.rating = $p0` with `$p0` bound to `"Great"`:

```csharp
context.Nodes<Person>().Where(p => p.Rating == Rating.Great);
context.Nodes<Person>().Where(p => (int)p.Rating == 2);
```

## Null semantics

FalkorDB is schemaless, so a property can simply be absent, and Cypher propagates `null` through
comparisons. C# does not: `null != 0` is `true`, and `null > 0` is `false`. Translating a predicate
literally would therefore silently drop rows that LINQ would have kept.

The provider closes that gap rather than leaving it to the caller:

| C# | Cypher |
| --- | --- |
| `p.Score != 0` | `coalesce(n0.score <> $p0, true)` |
| `p.Name == p.Nickname` | `coalesce(n0.name = n0.nickname, n0.name IS NULL AND n0.nickname IS NULL)` |
| `!(p.Score > 0)` | `NOT coalesce(n0.score > $p0, false)` |
| `All(p => p.Active)` | `NOT coalesce(n0.active, false)` counted as a violation |

Equality only needs the fallback when **both** sides can be null, which is the one case C# and Cypher
disagree on: C# says `null == null` is `true`, Cypher says `null`. When just one side can be null,
a Cypher `null` and a C# `false` are both rejected by `WHERE` alike, so `Where(p => p.Name == "Alice")`
stays the plain `n0.name = $p0` and remains indexable. The `<>` fallback is likewise only emitted when
an operand can actually be null, so `Where(p => p.Age != 30)` on a non-nullable `int` renders bare.

### Model optional properties as nullable

That last sentence is a contract, not an accident: **a non-nullable mapped property is assumed to be
present on every matched node.** Because FalkorDB is schemaless, nothing enforces that, so if the
graph really can omit a property, map it as `int?`, `DateTime?`, or a reference type.

The difference is visible when a node has no `age` at all:

| Mapping | `Where(p => p.Age != 30)` | Matches the node? |
| --- | --- | --- |
| `int Age` | `n0.age <> $p0` | no |
| `int? Age` | `coalesce(n0.age <> $p0, true)` | yes |

The provider deliberately does *not* coalesce non-nullable reads to `default(T)`, even though the
materializer leaves an absent `age` as `0`. Two reasons:

- **It would erase every index.** `EXPLAIN` on an indexed `:P(age)` gives `Node By Index Scan` for
  `n.age = 0` but `Node By Label Scan` plus a `Filter` for `coalesce(n.age, 0) = 0`. Every indexed
  predicate in the provider would quietly become a full label scan.
- **Absent is not zero.** In a graph an absent property means *unknown*, so making
  `Where(p => p.Age == 0)` return everyone whose age was never recorded would be worse than the
  mismatch it fixes.

Mapping the property as nullable gives you the LINQ answer, and it is cheaper than the table above
suggests. The `coalesce` only wraps the *comparison*, never the indexed property read, so it costs
nothing on an inequality: `EXPLAIN` gives `Node By Label Scan` plus a `Filter` for both `n.age <> 5`
and `coalesce(n.age <> 5, true)`, because an index cannot serve `<>` to begin with. And an equality
test against a non-null constant is emitted bare as `n0.score = $p0`, which an index still serves —
the null-safe form is only used when *both* sides can be null.

## License

NFalkorDB is licensed under the Apache-2.0 [license ](https://github.com/FalkorDB/NFalkorDB/blob/master/LICENSE).
