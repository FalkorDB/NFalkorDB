using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NFalkorDB.Linq.Tests.Model;
using Xunit;

namespace NFalkorDB.Linq.Tests;

[Collection(nameof(GraphCollection))]
public class ReadmeSnippets
{
    private readonly GraphFixture _fixture;

    public ReadmeSnippets(GraphFixture fixture) => _fixture = fixture;

    private GraphContext context => _fixture.Context;

    [Fact]
    public async Task Snippets_compile_and_run()
    {
        var adults = context.Nodes<Person>().Where(p => p.Age > 21).OrderBy(p => p.Name).Take(10).ToList();
        Assert.NotEmpty(adults);

        var recent = context.Relationships<Knows>().Where(k => k.Since >= 2020).ToList();
        Assert.NotEmpty(recent);

        var friends = context.Nodes<Person>().Where(p => p.Name == "Alice").Traverse<Knows, Person>().ToList();
        Assert.NotEmpty(friends);

        var mentors = context.Nodes<Person>().Traverse<Knows, Person>(k => k.Since > 2015, TraversalDirection.Incoming).ToList();
        Assert.NotEmpty(mentors);

        var chained = context.Nodes<Person>().Traverse<Knows, Person>().Traverse<WorksAt, Company>().ToList();
        Assert.NotNull(chained);

        var pairs = context.Nodes<Person>()
            .SelectMany(p => p.Knows, (p, friend) => new { Who = p.Name, Friend = friend.Name })
            .ToList();
        Assert.NotEmpty(pairs);

        var people = await context.Nodes<Person>().Where(p => p.Active).ToListAsync();
        var oldest = await context.Nodes<Person>().OrderByDescending(p => p.Age).FirstOrDefaultAsync();
        var total = await context.Nodes<Person>().CountAsync();
        var average = await context.Nodes<Person>().AverageAsync(p => p.Age);
        Assert.NotEmpty(people);
        Assert.NotNull(oldest);
        Assert.Equal(4, total);
        Assert.True(average > 0);

        var query = context.Nodes<Person>().Where(p => p.Age > 21).ToCypherQuery();
        Assert.Equal("MATCH (n0:Person) WHERE n0.age > $p0 RETURN n0", query.Cypher);
        Assert.Equal(new Dictionary<string, object> { ["p0"] = 21L }, query.Parameters);

        Assert.NotEmpty(context.Nodes<Person>().Where(p => p.Age > 21).Explain());
        Assert.NotEmpty(context.Nodes<Person>().Where(p => p.Age > 21).Profile());
    }
}
