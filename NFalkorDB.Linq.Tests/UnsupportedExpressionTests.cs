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

    [Fact]
    public void A_bitwise_and_is_rejected_rather_than_translated_as_a_boolean_and()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Where(p => (p.Age & 1) == 0)
            .ToCypherQuery());

        Assert.Contains("bitwise", message);
        Assert.Contains("And", message);
    }

    [Fact]
    public void A_bitwise_or_is_rejected_rather_than_translated_as_a_boolean_or()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Where(p => (p.Age | 1) == 1)
            .ToCypherQuery());

        Assert.Contains("bitwise", message);
        Assert.Contains("Or", message);
    }

    [Fact]
    public void A_non_short_circuiting_boolean_and_is_still_translated()
    {
        var cypher = QueryHarnessExtensions.Nodes<Person>()
            .Where(p => p.Active & p.Age > 3)
            .Cypher();

        Assert.Equal("MATCH (n0:Person) WHERE n0.active AND n0.age > $p0 RETURN n0", cypher);
    }

    [Fact]
    public void Select_after_Distinct_is_rejected_because_it_would_deduplicate_the_projection()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Distinct()
            .Select(p => p.Name)
            .ToCypherQuery());

        Assert.Contains("Distinct", message);
        Assert.Contains("Project first", message);
    }

    [Fact]
    public void Distinct_after_Select_is_still_supported()
    {
        var cypher = QueryHarnessExtensions.Nodes<Person>()
            .Select(p => p.Name)
            .Distinct()
            .Cypher();

        Assert.Equal("MATCH (n0:Person) RETURN DISTINCT n0.name", cypher);
    }

    [Fact]
    public void A_truncating_cast_is_rejected_rather_than_silently_dropped()
    {
        // (int)2.7 == 2 in C#, but Cypher has no cast, so dropping it would compare the stored
        // 2.7 against 2 and quietly return the wrong rows.
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Where(p => (int)p.Height == 2)
            .ToCypherQuery());

        Assert.Contains("cast", message);
        Assert.Contains("change the result", message);
    }

    [Fact]
    public void A_widening_cast_is_rejected_rather_than_silently_dropped()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Where(p => (double)p.Age == 2.0)
            .ToCypherQuery());

        Assert.Contains("(Double)", message);
    }

    [Fact]
    public void An_enum_cast_is_value_preserving_and_still_translates()
    {
        // Enums are bound by name, so the compiler's enum-to-int conversion is resolved by the
        // parameter binder rather than by the rendered expression.
        var query = QueryHarnessExtensions.Nodes<Person>()
            .Where(p => (int)p.Rating == 2)
            .ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.rating = $p0 RETURN n0", query.Cypher);
        Assert.Equal("Great", Assert.Contains("p0", query.Parameters));
    }

    [Fact]
    public void The_index_aware_SelectMany_overload_is_rejected()
    {
        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .SelectMany((p, index) => p.Knows)
            .ToCypherQuery());

        Assert.Contains("SelectMany", message);
        Assert.Contains("ordinal", message);
    }

    [Fact]
    public void A_queryable_Contains_source_is_rejected_instead_of_being_executed()
    {
        // Enumerating the inner queryable would run a second query and pull its rows to the client
        // in the middle of translating this one.
        var names = QueryHarnessExtensions.Nodes<Person>().Select(p => p.Name);

        var message = Throws(() => QueryHarnessExtensions.Nodes<Person>()
            .Where(p => names.Contains(p.Name))
            .ToCypherQuery());

        Assert.Contains("IQueryable", message);
        Assert.Contains("ToList()", message);
    }

    [Fact]
    public void A_materialized_Contains_source_is_still_supported()
    {
        var names = QueryHarnessExtensions.Nodes<Person>().Select(p => p.Name).ToList();

        var query = QueryHarnessExtensions.Nodes<Person>()
            .Where(p => names.Contains(p.Name))
            .ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.name IN $p0 RETURN n0", query.Cypher);
    }
}
