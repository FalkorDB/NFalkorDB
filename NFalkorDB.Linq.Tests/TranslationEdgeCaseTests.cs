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

    [Fact]
    public void Successive_Where_calls_preserve_the_grouping_of_an_Or()
    {
        // Joining the clauses with a bare AND would render `a OR b AND c`, which Cypher groups as
        // `a OR (b AND c)` -- a different predicate than the one that was written.
        var cypher = QueryHarnessExtensions.Nodes<Person>()
            .Where(p => p.Age > 30 || p.Active)
            .Where(p => p.Name == "Alice")
            .Cypher();

        Assert.Equal("MATCH (n0:Person) WHERE (n0.age > $p0 OR n0.active) AND n0.name = $p1 RETURN n0", cypher);
    }

    [Fact]
    public void A_single_Where_does_not_gain_redundant_grouping()
    {
        var cypher = QueryHarnessExtensions.Nodes<Person>()
            .Where(p => p.Age > 30 && p.Active)
            .Cypher();

        Assert.Equal("MATCH (n0:Person) WHERE n0.age > $p0 AND n0.active RETURN n0", cypher);
    }

    [Fact]
    public void A_generated_column_alias_skips_names_the_projection_already_uses()
    {
        // `match` is reserved, so it needs a generated alias -- which must not collide with the
        // member that is already called c1.
        var cypher = QueryHarnessExtensions.Nodes<Person>()
            .Select(p => new { c1 = p.Name, match = p.Age })
            .Take(3)
            .Cypher();

        Assert.Equal("MATCH (n0:Person) RETURN n0.name AS c1, n0.age AS c2 LIMIT 3", cypher);
    }

    [Fact]
    public void Ordering_by_an_unprojected_column_is_rejected_on_the_return_path_too()
    {
        // The aggregate path already rejected this; the plain RETURN DISTINCT path did not.
        var exception = Assert.Throws<NotSupportedException>(() => QueryHarnessExtensions.Nodes<Person>()
            .OrderBy(p => p.Age)
            .Select(p => p.Name)
            .Distinct()
            .Cypher());

        Assert.Contains("Distinct", exception.Message);
        Assert.Contains("n0.age", exception.Message);
    }

    [Fact]
    public void Ordering_by_a_projected_column_still_works_with_Distinct()
    {
        var cypher = QueryHarnessExtensions.Nodes<Person>()
            .OrderBy(p => p.Name)
            .Select(p => p.Name)
            .Distinct()
            .Cypher();

        Assert.Equal("MATCH (n0:Person) RETURN DISTINCT n0.name ORDER BY n0.name ASC", cypher);
    }

    [Fact]
    public void An_enum_still_compares_against_operands_that_can_be_bound_as_a_member_name()
    {
        // The rejection of enum-versus-number comparisons must not catch the shapes that do work:
        // another enum-typed property, an enum literal, and an integral literal that rebinds.
        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.rating = n0.rating RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Rating == p.Rating).Cypher());

        var literal = QueryHarnessExtensions.Nodes<Person>().Where(p => p.Rating == Rating.Great).ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.rating = $p0 RETURN n0", literal.Cypher);
        Assert.Equal("Great", Assert.Single(literal.Parameters).Value);

        var ordinal = QueryHarnessExtensions.Nodes<Person>().Where(p => (int)p.Rating == 2).ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.rating = $p0 RETURN n0", ordinal.Cypher);
        Assert.Equal("Great", Assert.Single(ordinal.Parameters).Value);
    }

    [Fact]
    public void Inequality_is_null_safe_only_where_a_side_can_be_null()
    {
        // int? Score and string Name can both be absent, so the comparison needs the fallback that
        // restores C#'s "exactly one side is null means unequal" answer.
        Assert.Equal(
            "MATCH (n0:Person) WHERE coalesce(n0.score <> $p0, true) RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Score != 0).Cypher());

        Assert.Equal(
            "MATCH (n0:Person) WHERE coalesce(n0.name <> $p0, true) RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Name != "Alice").Cypher());

        // A non-nullable column compared against a non-null constant cannot produce null, so the
        // fallback would be dead weight. This is the documented contract: a non-nullable mapped
        // property is assumed present, which is what keeps the predicate indexable.
        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.age <> $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Age != 30).Cypher());

        // Equality needs no fallback: C# and Cypher both reject a null operand from a WHERE.
        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.score = $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Score == 0).Cypher());

        // A null constant still uses the dedicated IS NULL form rather than the fallback.
        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.score IS NOT NULL RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Score != null).Cypher());
    }

    [Fact]
    public void Negation_coalesces_its_operand_before_inverting_it()
    {
        Assert.Equal(
            "MATCH (n0:Person) WHERE NOT coalesce(n0.score > $p0, false) RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => !(p.Score > 0)).Cypher());

        Assert.Equal(
            "MATCH (n0:Person) WHERE NOT coalesce(n0.active AND n0.age > $p0, false) RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => !(p.Active && p.Age > 18)).Cypher());
    }
}
