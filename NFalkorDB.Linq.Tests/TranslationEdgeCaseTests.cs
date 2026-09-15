using System;
using System.Collections.Generic;
using System.Linq;
using NFalkorDB.Linq.Tests.Model;
using NFalkorDB.Linq.Translation;
using Xunit;

namespace NFalkorDB.Linq.Tests;

/// <summary>
/// Regressions for the sharper edges of translation: Cypher's three-valued logic, alias collisions
/// in a generated WITH clause, and parameter shapes that are easy to mis-detect.
/// </summary>
public class TranslationEdgeCaseTests
{
    private static IQueryable<T> Nodes<T>() where T : class, new() => QueryHarnessExtensions.Nodes<T>();

    [Node("Collide")]
    private class CollidingProjection
    {
        [Property("age")]
        public int Age { get; set; }

        [Property("name")]
        public string Name { get; set; }
    }

    [Fact]
    public void An_order_term_carried_into_a_with_clause_does_not_reuse_a_projected_alias()
    {
        // The projection already declares `o0`, so the carried sort key has to pick another name or
        // the WITH would declare the same variable twice.
        var harness = new QueryHarness();

        harness.Nodes<CollidingProjection>()
            .OrderBy(x => x.Age)
            .Select(x => new { o0 = x.Name })
            .Take(3)
            .Count();

        Assert.Equal(
            "MATCH (n0:Collide) WITH n0.name AS o0, n0.age AS o1 ORDER BY o1 ASC LIMIT 3 RETURN count(*)",
            harness.CapturedCypher);
    }

    [Fact]
    public void A_projected_member_named_like_a_source_alias_does_not_capture_the_sort_key()
    {
        // `n0` here is a projected column, not the matched node, so `n0.age` is out of scope after
        // the WITH and the sort key has to be carried explicitly.
        var harness = new QueryHarness();

        harness.Nodes<CollidingProjection>()
            .OrderBy(x => x.Age)
            .Select(x => new { n0 = x.Name })
            .Take(3)
            .Count();

        Assert.Equal(
            "MATCH (n0:Collide) WITH n0.name AS n0, n0.age AS o0 ORDER BY o0 ASC LIMIT 3 RETURN count(*)",
            harness.CapturedCypher);
    }

    [Fact]
    public void A_whole_node_projection_still_lets_the_sort_key_resolve_through_the_with()
    {
        var harness = new QueryHarness();

        harness.Nodes<Person>().OrderBy(p => p.Age).Take(3).Count();

        Assert.Equal(
            "MATCH (n0:Person) WITH n0 ORDER BY n0.age ASC LIMIT 3 RETURN count(*)",
            harness.CapturedCypher);
    }

    [Fact]
    public void Ordering_by_a_column_outside_a_distinct_projection_is_rejected()
    {
        var exception = Assert.Throws<NotSupportedException>(
            () => new QueryHarness().Nodes<Person>()
                .OrderBy(p => p.Age)
                .Select(p => p.Name)
                .Distinct()
                .Count());

        Assert.Contains("Distinct", exception.Message);
        Assert.Contains("duplicates", exception.Message);
    }

    [Fact]
    public void Ordering_by_a_projected_column_still_works_with_distinct()
    {
        var harness = new QueryHarness();

        harness.Nodes<Person>().OrderBy(p => p.Name).Select(p => p.Name).Distinct().Count();

        Assert.Equal(
            "MATCH (n0:Person) WITH DISTINCT n0.name AS c0 ORDER BY c0 ASC RETURN count(*)",
            harness.CapturedCypher);
    }

    [Fact]
    public void All_coalesces_the_negated_predicate_so_nulls_count_as_violations()
    {
        var harness = new QueryHarness();

        harness.Nodes<Person>().All(p => p.Score > 0);

        Assert.Equal(
            "MATCH (n0:Person) WHERE NOT coalesce(n0.score > $p0, false) RETURN count(*) = 0",
            harness.CapturedCypher);
    }

    [Fact]
    public void An_aggregate_without_a_projection_names_the_linq_operator()
    {
        var exception = Assert.Throws<NotSupportedException>(() => new QueryHarness().Nodes<Person>().Max());

        Assert.Contains("'Max'", exception.Message);
        Assert.Contains(".Max(p => p.Age)", exception.Message);
        Assert.DoesNotContain("'max'", exception.Message);
    }

    [Fact]
    public void An_aggregate_over_several_columns_names_the_linq_operator()
    {
        var exception = Assert.Throws<NotSupportedException>(
            () => new QueryHarness().Nodes<Person>().Select(p => new { p.Name, p.Age }).Max());

        Assert.Contains("'Max'", exception.Message);
        Assert.Contains("produced 2", exception.Message);
    }

    [Fact]
    public void A_typed_map_parameter_is_bound_as_a_map_rather_than_a_collection()
    {
        var normalized = Assert.IsType<Dictionary<string, object>>(
            ParameterBag.Normalize(new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 }));

        Assert.Equal(1L, normalized["a"]);
        Assert.Equal(2L, normalized["b"]);
    }

    [Fact]
    public void A_node_attribute_rejects_a_blank_label()
    {
        Assert.Throws<ArgumentException>(() => new NodeAttribute("Person", null));
        Assert.Throws<ArgumentException>(() => new NodeAttribute("Person", "  "));
        Assert.Throws<ArgumentException>(() => new NodeAttribute("Person", string.Empty));
    }
}
