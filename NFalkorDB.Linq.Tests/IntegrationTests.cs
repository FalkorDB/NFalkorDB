using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NFalkorDB.Linq.Tests.Model;
using StackExchange.Redis;
using Xunit;

namespace NFalkorDB.Linq.Tests;

/// <summary>
/// End-to-end tests against a real FalkorDB. Follows the connection pattern used by the driver's
/// own test suite: <c>REDIS_CONNECTION_STRING</c> with <c>AllowAdmin</c> so the fixture can flush.
/// </summary>
/// <remarks>
/// The seeded graph lives in a dedicated logical database because the driver's own suite runs in
/// parallel with this assembly and issues <c>FLUSHDB</c> against database 0.
/// </remarks>
public sealed class GraphFixture : IDisposable
{
    private const string GraphId = "nfalkordb-linq-tests";

    private const int DatabaseIndex = 9;

    private readonly ConnectionMultiplexer _multiplexer;

    public GraphFixture()
    {
        var connectionString = Environment.GetEnvironmentVariable("REDIS_CONNECTION_STRING") ?? "localhost";
        var options = ConfigurationOptions.Parse(connectionString);

        options.AllowAdmin = true;
        options.DefaultDatabase = DatabaseIndex;

        _multiplexer = ConnectionMultiplexer.Connect(options);

        var client = new FalkorDB(_multiplexer.GetDatabase(DatabaseIndex));

        Graph = client.SelectGraph(GraphId);
        Context = new GraphContext(Graph);

        Seed();
    }

    public Graph Graph { get; }

    public GraphContext Context { get; }

    public void Dispose()
    {
        try
        {
            Graph.Query("MATCH (n) DETACH DELETE n");
        }
        catch (RedisException)
        {
            // The graph may already be gone; nothing to clean up.
        }

        _multiplexer.Dispose();
    }

    private void Seed()
    {
        Graph.Query("MATCH (n) DETACH DELETE n");

        Graph.Query(
            @"CREATE
                (alice:Person {name:'Alice', age:34, height:1.68, active:true, rating:'Great', joined:'2019-03-01T00:00:00.0000000Z', tags:['a','b'], score:10}),
                (bob:Person {name:'Bob', age:28, height:1.81, active:true, rating:'Good', joined:'2020-07-15T00:00:00.0000000Z', tags:['b'], score:20}),
                (carol:Person {name:'Carol', age:45, height:1.72, active:false, rating:'Poor', joined:'2018-01-09T00:00:00.0000000Z', tags:[]}),
                (dave:Person {name:'Dave', age:19, height:1.90, active:true, rating:'Good', joined:'2021-11-30T00:00:00.0000000Z', tags:['c'], score:5}),
                (acme:Company:Organization {name:'Acme', founded:1999}),
                (globex:Company:Organization {name:'Globex', founded:2010}),
                (alice)-[:KNOWS {since:2015, weight:0.9}]->(bob),
                (alice)-[:KNOWS {since:2019, weight:0.4}]->(carol),
                (bob)-[:KNOWS {since:2021, weight:0.7}]->(dave),
                (alice)-[:WORKS_AT {role:'Engineer'}]->(acme),
                (bob)-[:WORKS_AT {role:'Analyst'}]->(globex)");
    }
}

[CollectionDefinition(nameof(GraphCollection))]
public class GraphCollection : ICollectionFixture<GraphFixture>
{
}

[Collection(nameof(GraphCollection))]
public class IntegrationTests
{
    private readonly GraphFixture _fixture;

    public IntegrationTests(GraphFixture fixture) => _fixture = fixture;

    private GraphContext Context => _fixture.Context;

    [Fact]
    public void Whole_nodes_materialize_into_mapped_pocos()
    {
        var alice = Context.Nodes<Person>().Single(p => p.Name == "Alice");

        Assert.Equal("Alice", alice.Name);
        Assert.Equal(34, alice.Age);
        Assert.Equal(1.68, alice.Height, 3);
        Assert.True(alice.Active);
        Assert.Equal(Rating.Great, alice.Rating);
        Assert.Equal(new DateTime(2019, 3, 1, 0, 0, 0, DateTimeKind.Utc), alice.Joined);
        Assert.Equal(new[] { "a", "b" }, alice.Tags);
        Assert.Equal(10, alice.Score);
        Assert.True(alice.Id >= 0);
    }

    [Fact]
    public void A_missing_property_materializes_as_null()
    {
        var carol = Context.Nodes<Person>().Single(p => p.Name == "Carol");

        Assert.Null(carol.Score);
        Assert.Null(carol.Nickname);
        Assert.Empty(carol.Tags);
    }

    [Fact]
    public void Filtering_happens_server_side()
    {
        var names = Context.Nodes<Person>()
            .Where(p => p.Age >= 28 && p.Active)
            .OrderBy(p => p.Name)
            .Select(p => p.Name)
            .ToList();

        Assert.Equal(new[] { "Alice", "Bob" }, names);
    }

    [Fact]
    public void String_operators_run_server_side()
    {
        Assert.Equal(
            new[] { "Alice" },
            Context.Nodes<Person>().Where(p => p.Name.StartsWith("Al")).Select(p => p.Name).ToList());

        Assert.Equal(
            new[] { "Bob" },
            Context.Nodes<Person>().Where(p => p.Name.EndsWith("ob")).Select(p => p.Name).ToList());

        Assert.Equal(
            new[] { "Carol" },
            Context.Nodes<Person>().Where(p => p.Name.Contains("aro")).Select(p => p.Name).ToList());
    }

    [Fact]
    public void In_predicates_run_server_side()
    {
        var wanted = new[] { "Alice", "Dave" };

        var found = Context.Nodes<Person>()
            .Where(p => wanted.Contains(p.Name))
            .OrderBy(p => p.Name)
            .Select(p => p.Name)
            .ToList();

        Assert.Equal(new[] { "Alice", "Dave" }, found);
    }

    [Fact]
    public void Null_checks_run_server_side()
    {
        var withScore = Context.Nodes<Person>()
            .Where(p => p.Score != null)
            .OrderBy(p => p.Name)
            .Select(p => p.Name)
            .ToList();

        Assert.Equal(new[] { "Alice", "Bob", "Dave" }, withScore);
    }

    [Fact]
    public void Enum_properties_round_trip_through_a_predicate()
    {
        var good = Context.Nodes<Person>()
            .Where(p => p.Rating == Rating.Good)
            .OrderBy(p => p.Name)
            .Select(p => p.Name)
            .ToList();

        Assert.Equal(new[] { "Bob", "Dave" }, good);
    }

    [Fact]
    public void Ordering_and_paging_run_server_side()
    {
        var page = Context.Nodes<Person>()
            .OrderByDescending(p => p.Age)
            .Skip(1)
            .Take(2)
            .Select(p => p.Name)
            .ToList();

        Assert.Equal(new[] { "Alice", "Bob" }, page);
    }

    [Fact]
    public void Anonymous_projections_materialize()
    {
        var rows = Context.Nodes<Person>()
            .Where(p => p.Age > 30)
            .OrderBy(p => p.Name)
            .Select(p => new { p.Name, p.Age })
            .ToList();

        Assert.Equal(2, rows.Count);
        Assert.Equal("Alice", rows[0].Name);
        Assert.Equal(34, rows[0].Age);
        Assert.Equal("Carol", rows[1].Name);
        Assert.Equal(45, rows[1].Age);
    }

    [Fact]
    public void Member_init_projections_materialize()
    {
        var rows = Context.Nodes<Company>()
            .OrderBy(c => c.Name)
            .Select(c => new Company { Name = c.Name, Founded = c.Founded })
            .ToList();

        Assert.Equal(new[] { "Acme", "Globex" }, rows.Select(r => r.Name).ToArray());
        Assert.Equal(new[] { 1999, 2010 }, rows.Select(r => r.Founded).ToArray());
    }

    [Fact]
    public void Distinct_runs_server_side()
    {
        var ratings = Context.Nodes<Person>()
            .Select(p => p.Rating)
            .Distinct()
            .ToList();

        Assert.Equal(3, ratings.Count);
        Assert.Contains(Rating.Great, ratings);
    }

    [Fact]
    public void Aggregates_run_server_side()
    {
        Assert.Equal(4, Context.Nodes<Person>().Count());
        Assert.Equal(3, Context.Nodes<Person>().Count(p => p.Active));
        Assert.Equal(126, Context.Nodes<Person>().Sum(p => p.Age));
        Assert.Equal(19, Context.Nodes<Person>().Min(p => p.Age));
        Assert.Equal(45, Context.Nodes<Person>().Max(p => p.Age));
        Assert.Equal(31.5, Context.Nodes<Person>().Average(p => p.Age), 3);
        Assert.True(Context.Nodes<Person>().Any(p => p.Age > 40));
        Assert.False(Context.Nodes<Person>().Any(p => p.Age > 100));
        Assert.True(Context.Nodes<Person>().All(p => p.Age > 15));
        Assert.False(Context.Nodes<Person>().All(p => p.Active));
    }

    [Fact]
    public void An_aggregate_over_a_page_runs_server_side()
    {
        var topTwoAges = Context.Nodes<Person>()
            .OrderByDescending(p => p.Age)
            .Take(2)
            .Select(p => p.Age)
            .Sum();

        Assert.Equal(79, topTwoAges);
    }

    [Fact]
    public void Traversal_returns_the_far_nodes()
    {
        var friends = Context.Nodes<Person>()
            .Where(p => p.Name == "Alice")
            .Traverse<Knows, Person>()
            .OrderBy(p => p.Name)
            .Select(p => p.Name)
            .ToList();

        Assert.Equal(new[] { "Bob", "Carol" }, friends);
    }

    [Fact]
    public void A_traversal_can_filter_on_the_relationship()
    {
        var recent = Context.Nodes<Person>()
            .Where(p => p.Name == "Alice")
            .Traverse<Knows, Person>(k => k.Since >= 2019)
            .Select(p => p.Name)
            .ToList();

        Assert.Equal(new[] { "Carol" }, recent);
    }

    [Fact]
    public void An_incoming_traversal_walks_the_other_way()
    {
        var knownBy = Context.Nodes<Person>()
            .Where(p => p.Name == "Bob")
            .Traverse<Knows, Person>(TraversalDirection.Incoming)
            .Select(p => p.Name)
            .ToList();

        Assert.Equal(new[] { "Alice" }, knownBy);
    }

    [Fact]
    public void Traversals_can_be_chained()
    {
        var employers = Context.Nodes<Person>()
            .Where(p => p.Name == "Alice")
            .Traverse<Knows, Person>()
            .Traverse<WorksAt, Company>()
            .Select(c => c.Name)
            .ToList();

        Assert.Equal(new[] { "Globex" }, employers);
    }

    [Fact]
    public void Select_many_traverses_a_navigation_property()
    {
        var pairs = Context.Nodes<Person>()
            .SelectMany(p => p.Knows, (p, friend) => new { Who = p.Name, Friend = friend.Name })
            .ToList();

        Assert.Equal(3, pairs.Count);
        Assert.Contains(pairs, p => p.Who == "Alice" && p.Friend == "Bob");
        Assert.Contains(pairs, p => p.Who == "Alice" && p.Friend == "Carol");
        Assert.Contains(pairs, p => p.Who == "Bob" && p.Friend == "Dave");
    }

    [Fact]
    public void Relationships_can_be_queried_directly()
    {
        var since = Context.Relationships<Knows>()
            .OrderBy(k => k.Since)
            .Select(k => k.Since)
            .ToList();

        Assert.Equal(new[] { 2015, 2019, 2021 }, since);
    }

    [Fact]
    public void Relationship_entities_materialize()
    {
        var earliest = Context.Relationships<Knows>().OrderBy(k => k.Since).First();

        Assert.Equal(2015, earliest.Since);
        Assert.Equal(0.9, earliest.Weight, 3);
        Assert.True(earliest.Id >= 0);
    }

    [Fact]
    public void First_or_default_returns_null_when_nothing_matches()
    {
        Assert.Null(Context.Nodes<Person>().FirstOrDefault(p => p.Name == "Nobody"));
    }

    [Fact]
    public void First_throws_when_nothing_matches()
    {
        Assert.Throws<InvalidOperationException>(() => Context.Nodes<Person>().First(p => p.Name == "Nobody"));
    }

    [Fact]
    public void Single_throws_when_more_than_one_row_matches()
    {
        Assert.Throws<InvalidOperationException>(() => Context.Nodes<Person>().Single(p => p.Active));
    }

    [Fact]
    public async Task Async_terminals_return_the_same_results()
    {
        Assert.Equal(4, await Context.Nodes<Person>().CountAsync());
        Assert.Equal(4L, await Context.Nodes<Person>().LongCountAsync());
        Assert.True(await Context.Nodes<Person>().AnyAsync(p => p.Age > 40));
        Assert.Equal(126, await Context.Nodes<Person>().SumAsync(p => p.Age));
        Assert.Equal(31.5, await Context.Nodes<Person>().AverageAsync(p => p.Age), 3);
        Assert.Equal(19, await Context.Nodes<Person>().MinAsync(p => p.Age));
        Assert.Equal(45, await Context.Nodes<Person>().MaxAsync(p => p.Age));

        var names = await Context.Nodes<Person>().OrderBy(p => p.Name).Select(p => p.Name).ToListAsync();

        Assert.Equal(new[] { "Alice", "Bob", "Carol", "Dave" }, names);

        var alice = await Context.Nodes<Person>().FirstOrDefaultAsync(p => p.Name == "Alice");

        Assert.NotNull(alice);
        Assert.Equal(34, alice.Age);
    }

    [Fact]
    public void A_hostile_value_matches_nothing_instead_of_changing_the_query()
    {
        var hostile = "\" OR 1=1 --";

        var matches = Context.Nodes<Person>().Where(p => p.Name == hostile).ToList();

        Assert.Empty(matches);
        Assert.Equal(4, Context.Nodes<Person>().Count());
    }

    [Fact]
    public void Mapped_names_that_are_not_bare_identifiers_work_end_to_end()
    {
        _fixture.Graph.Query("CREATE (:`Odd Label`:`Second Label` {`first-name`:'Zoe', plain:'p'})");

        try
        {
            var found = Context.Nodes<AwkwardlyNamedNode>().Single(x => x.FirstName == "Zoe");

            Assert.Equal("Zoe", found.FirstName);
            Assert.Equal("p", found.Plain);
        }
        finally
        {
            _fixture.Graph.Query("MATCH (n:`Odd Label`) DETACH DELETE n");
        }
    }

    [Node("Odd Label", "Second Label")]
    private class AwkwardlyNamedNode
    {
        [Property("first-name")]
        public string FirstName { get; set; }

        [Property("plain")]
        public string Plain { get; set; }
    }

    [Fact]
    public void A_hostile_value_is_stored_and_read_back_verbatim()
    {
        var hostile = "\") RETURN 1 //";

        _fixture.Graph.Query(
            "CREATE (:Person {name:$name, age:1})",
            new Dictionary<string, object> { ["name"] = hostile });

        try
        {
            var found = Context.Nodes<Person>().Single(p => p.Name == hostile);

            Assert.Equal(hostile, found.Name);
        }
        finally
        {
            _fixture.Graph.Query(
                "MATCH (p:Person {name:$name}) DETACH DELETE p",
                new Dictionary<string, object> { ["name"] = hostile });
        }
    }

    [Fact]
    public void A_type_mismatch_is_reported_clearly()
    {
        var exception = Assert.Throws<GraphMappingException>(
            () => Context.Nodes<MistypedPerson>().Where(p => p.Age > 0).ToList());

        Assert.Contains("Name", exception.Message);
    }

    [Node("Person")]
    private class MistypedPerson
    {
        [Property("name")]
        public int Name { get; set; }

        [Property("age")]
        public int Age { get; set; }
    }
}
