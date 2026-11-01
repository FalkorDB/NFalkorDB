using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NFalkorDB.Linq.Tests.Model;
using Xunit;

namespace NFalkorDB.Linq.Tests;

/// <summary>
/// The async terminal operators have to translate exactly like their synchronous counterparts.
/// </summary>
public class AsyncTranslationTests
{
    private static async Task<string> CypherOf(Func<QueryHarness, Task> query)
    {
        var harness = new QueryHarness();

        await query(harness);

        return harness.CapturedCypher;
    }

    [Fact]
    public async Task To_list_async_sends_the_plain_query()
    {
        Assert.Equal(
            "MATCH (n0:Person) RETURN n0",
            await CypherOf(h => h.Nodes<Person>().ToListAsync()));
    }

    [Fact]
    public async Task To_array_async_sends_the_plain_query()
    {
        Assert.Equal(
            "MATCH (n0:Person) RETURN n0",
            await CypherOf(h => h.Nodes<Person>().ToArrayAsync()));
    }

    [Fact]
    public async Task First_async_limits_to_one_row()
    {
        Assert.Equal(
            "MATCH (n0:Person) RETURN n0 LIMIT 1",
            await CypherOf(h => h.Nodes<Person>().FirstOrDefaultAsync()));

        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.age > $p0 RETURN n0 LIMIT 1",
            await CypherOf(h => h.Nodes<Person>().FirstOrDefaultAsync(p => p.Age > 30)));
    }

    [Fact]
    public async Task Single_async_limits_to_two_rows()
    {
        Assert.Equal(
            "MATCH (n0:Person) RETURN n0 LIMIT 2",
            await CypherOf(h => h.Nodes<Person>().SingleOrDefaultAsync()));
    }

    [Fact]
    public async Task Count_async_uses_a_count_aggregate()
    {
        Assert.Equal(
            "MATCH (n0:Person) RETURN count(*)",
            await CypherOf(h => h.Nodes<Person>().CountAsync()));

        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.active RETURN count(*)",
            await CypherOf(h => h.Nodes<Person>().LongCountAsync(p => p.Active)));
    }

    [Fact]
    public async Task Any_and_all_async_use_count_comparisons()
    {
        Assert.Equal(
            "MATCH (n0:Person) RETURN count(*) > 0",
            await CypherOf(h => h.Nodes<Person>().AnyAsync()));

        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.active RETURN count(*) > 0",
            await CypherOf(h => h.Nodes<Person>().AnyAsync(p => p.Active)));

        // Must match the synchronous All exactly: a bare NOT would drop rows whose property is
        // missing, so a violating row could go uncounted.
        var syncHarness = new QueryHarness();

        syncHarness.Nodes<Person>().All(p => p.Active);

        var asyncAll = await CypherOf(h => h.Nodes<Person>().AllAsync(p => p.Active));

        Assert.Equal("MATCH (n0:Person) WHERE NOT coalesce(n0.active, false) RETURN count(*) = 0", asyncAll);
        Assert.Equal(syncHarness.CapturedCypher, asyncAll);
    }

    [Fact]
    public async Task Aggregate_async_operators_wrap_the_projected_column()
    {        Assert.Equal(
            "MATCH (n0:Person) RETURN sum(n0.age)",
            await CypherOf(h => h.Nodes<Person>().SumAsync(p => p.Age)));

        Assert.Equal(
            "MATCH (n0:Person) RETURN avg(n0.age)",
            await CypherOf(h => h.Nodes<Person>().AverageAsync(p => p.Age)));

        Assert.Equal(
            "MATCH (n0:Person) RETURN min(n0.height)",
            await CypherOf(h => h.Nodes<Person>().MinAsync(p => p.Height)));

        Assert.Equal(
            "MATCH (n0:Person) RETURN max(n0.name)",
            await CypherOf(h => h.Nodes<Person>().MaxAsync(p => p.Name)));
    }

    [Fact]
    public async Task Aggregate_async_operators_accept_an_already_projected_query()
    {
        // Sum and Average previously had selector overloads only, so an already projected query
        // could be fed to MinAsync/MaxAsync but not to SumAsync/AverageAsync.
        Assert.Equal(
            "MATCH (n0:Person) RETURN sum(n0.age)",
            await CypherOf(h => h.Nodes<Person>().Select(p => p.Age).SumAsync()));

        Assert.Equal(
            "MATCH (n0:Person) RETURN sum(n0.height)",
            await CypherOf(h => h.Nodes<Person>().Select(p => p.Height).SumAsync()));

        Assert.Equal(
            "MATCH (n0:Person) RETURN avg(n0.age)",
            await CypherOf(h => h.Nodes<Person>().Select(p => p.Age).AverageAsync()));

        Assert.Equal(
            "MATCH (n0:Person) RETURN min(n0.age)",
            await CypherOf(h => h.Nodes<Person>().Select(p => p.Age).MinAsync()));
    }

    [Theory]
    [InlineData("Min")]
    [InlineData("Max")]
    public async Task The_async_extrema_reject_enums_exactly_like_the_synchronous_ones(string @operator)
    {
        // The guard lives in the shared terminal path, so the async entry points cannot bypass it.
        var syncMessage = Assert.Throws<NotSupportedException>(() => @operator == "Min"
            ? (object)QueryHarnessExtensions.Nodes<Person>().Min(p => p.Rating)
            : QueryHarnessExtensions.Nodes<Person>().Max(p => p.Rating)).Message;

        var asyncMessage = (await Assert.ThrowsAsync<NotSupportedException>(() => @operator == "Min"
            ? QueryHarnessExtensions.Nodes<Person>().MinAsync(p => p.Rating)
            : QueryHarnessExtensions.Nodes<Person>().MaxAsync(p => p.Rating))).Message;

        Assert.Contains("lexically", asyncMessage);
        Assert.Equal(syncMessage, asyncMessage);
    }

    [Fact]
    public async Task An_already_cancelled_token_stops_before_the_query_is_sent()
    {
        using var cancellation = new CancellationTokenSource();

        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => QueryHarnessExtensions.Nodes<Person>().ToListAsync(cancellation.Token));
    }

    [Fact]
    public async Task A_null_source_is_rejected()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => ((IQueryable<Person>)null).ToListAsync());
    }

    [Fact]
    public async Task A_null_predicate_is_rejected()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => QueryHarnessExtensions.Nodes<Person>().AnyAsync(null));
    }
}
