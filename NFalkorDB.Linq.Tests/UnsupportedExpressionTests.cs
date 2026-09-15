using System;
using System.Collections.Generic;
using System.Linq;
using NFalkorDB.Linq.Tests.Model;
using Xunit;

namespace NFalkorDB.Linq.Tests;

/// <summary>
/// Anything the provider cannot turn into Cypher has to fail loudly. Falling back to client-side
/// evaluation would quietly pull the whole graph over the wire.
/// </summary>
public class UnsupportedExpressionTests
{
    private static string Throws(Action action)
    {
        var exception = Assert.Throws<NotSupportedException>(action);

        Assert.False(string.IsNullOrWhiteSpace(exception.Message));

        return exception.Message;
    }

    [Fact]
    public void An_untranslatable_method_names_the_expression()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Where(p => p.Name.Normalize() == "x")
            .ToCypherQuery());

        Assert.Contains("p.Name.Normalize()", message);
        Assert.Contains("Call", message);
    }

    [Fact]
    public void An_untranslatable_operator_names_the_operator()
    {
        var message = Throws(() => new QueryHarness().Nodes<Person>().GroupBy(p => p.Age).ToList());

        Assert.Contains("GroupBy", message);
    }

    [Fact]
    public void Join_is_rejected()
    {
        var harness = new QueryHarness();

        var message = Throws(() => harness.Nodes<Person>()
            .Join(harness.Nodes<Company>(), p => p.Age, c => c.Founded, (p, c) => p.Name)
            .ToList());

        Assert.Contains("Join", message);
    }

    [Fact]
    public void The_index_aware_where_overload_is_rejected()
    {
        var message = Throws(() => new QueryHarness().Nodes<Person>().Where((p, i) => i > 2).ToList());

        Assert.Contains("Where", message);
        Assert.Contains("ordinal", message);
    }

    [Fact]
    public void The_index_aware_select_overload_is_rejected()
    {
        var message = Throws(() => new QueryHarness().Nodes<Person>().Select((p, i) => p.Name).ToList());

        Assert.Contains("Select", message);
        Assert.Contains("ordinal", message);
    }

    [Fact]
    public void Filtering_after_projecting_is_rejected()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Select(p => p.Name)
            .Where(n => n == "x")
            .ToCypherQuery());

        Assert.Contains("Where cannot be applied after Select", message);
    }

    [Fact]
    public void Ordering_after_projecting_is_rejected()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Select(p => p.Name)
            .OrderBy(n => n)
            .ToCypherQuery());

        Assert.Contains("OrderBy cannot be applied after Select", message);
    }

    [Fact]
    public void Filtering_after_paging_is_rejected()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Take(5)
            .Where(p => p.Active)
            .ToCypherQuery());

        Assert.Contains("Where", message);
    }

    [Fact]
    public void Two_consecutive_projections_are_rejected()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Select(p => new { p.Name, p.Age })
            .Select(x => x.Name)
            .ToCypherQuery());

        Assert.Contains("Select", message);
    }

    [Fact]
    public void Traversing_after_projecting_is_rejected()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Select(p => p.Name)
            .Traverse<Knows, Person>()
            .ToCypherQuery());

        Assert.Contains("traversal cannot follow Select", message);
    }

    [Fact]
    public void Traversing_after_paging_is_rejected()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Take(3)
            .Traverse<Knows, Person>()
            .ToCypherQuery());

        Assert.Contains("traversal", message);
    }

    [Fact]
    public void Traversing_from_a_relationship_is_rejected()
    {
        var message = Throws(() => QueryHarnessExtensions.Relationships<Knows>()
            .Traverse<WorksAt, Company>()
            .ToCypherQuery());

        Assert.Contains("Knows", message);
    }

    [Fact]
    public void Select_many_over_a_non_navigation_property_is_rejected()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .SelectMany(p => p.Tags)
            .ToCypherQuery());

        Assert.Contains("navigation property", message);
    }

    [Fact]
    public void An_aggregate_without_a_projection_is_rejected()
    {
        var message = Throws(() => new QueryHarness().Nodes<Person>().Max());

        Assert.Contains("Max", message);
    }

    [Fact]
    public void An_ignored_property_cannot_be_filtered_on()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Where(p => p.Transient == "x")
            .ToCypherQuery());

        Assert.Contains("Transient", message);
    }

    [Fact]
    public void A_query_from_another_provider_is_rejected()
    {
        var message = Throws(() => new[] { new Person() }.AsQueryable().ToCypherQuery());

        Assert.Contains("GraphContext", message);
    }
}
