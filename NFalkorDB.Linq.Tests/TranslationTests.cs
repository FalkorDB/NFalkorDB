using System;
using System.Collections.Generic;
using System.Linq;
using NFalkorDB.Linq.Tests.Model;
using Xunit;

namespace NFalkorDB.Linq.Tests;

/// <summary>
/// Asserts the exact Cypher and parameter dictionary produced for each supported operator. These
/// tests never touch a server.
/// </summary>
public class TranslationTests
{
    private static IQueryable<T> Nodes<T>() where T : class, new() => QueryHarnessExtensions.Nodes<T>();

    private static void AssertQuery(IQueryable query, string expectedCypher, params object[] expectedParameters)
    {
        var translated = query.ToCypherQuery();

        Assert.Equal(expectedCypher, translated.Cypher);
        AssertParameters(translated.Parameters, expectedParameters);
    }

    private static void AssertParameters(IReadOnlyDictionary<string, object> actual, object[] expected)
    {
        Assert.Equal(expected.Length, actual.Count);

        for (var i = 0; i < expected.Length; i++)
        {
            var key = "p" + i;

            Assert.True(actual.ContainsKey(key), $"Expected parameter '{key}'. Got: {string.Join(", ", actual.Keys)}");

            if (expected[i] is Array expectedArray)
            {
                Assert.Equal(expectedArray.Cast<object>().ToArray(), ((Array)actual[key]).Cast<object>().ToArray());
            }
            else
            {
                Assert.Equal(expected[i], actual[key]);
            }
        }
    }

    // ---------------------------------------------------------------- roots

    [Fact]
    public void Node_root_matches_the_mapped_label()
    {
        AssertQuery(Nodes<Person>(), "MATCH (n0:Person) RETURN n0");
    }

    [Fact]
    public void Node_root_matches_every_mapped_label()
    {
        AssertQuery(Nodes<Company>(), "MATCH (n0:Company:Organization) RETURN n0");
    }

    [Fact]
    public void Unannotated_type_falls_back_to_the_clr_name()
    {
        AssertQuery(Nodes<City>(), "MATCH (n0:City) RETURN n0");
    }

    [Fact]
    public void Relationship_root_matches_an_anonymous_pattern()
    {
        AssertQuery(QueryHarnessExtensions.Relationships<Knows>(), "MATCH ()-[r0:KNOWS]->() RETURN r0");
    }

    // ------------------------------------------------------------ predicates

    [Theory]
    [InlineData("=")]
    public void Equality_becomes_a_parameterized_comparison(string @operator)
    {
        AssertQuery(
            Nodes<Person>().Where(p => p.Name == "Alice"),
            $"MATCH (n0:Person) WHERE n0.name {@operator} $p0 RETURN n0",
            "Alice");
    }

    [Fact]
    public void Inequality_becomes_a_not_equal_comparison()
    {
        AssertQuery(
            Nodes<Person>().Where(p => p.Age != 30),
            "MATCH (n0:Person) WHERE n0.age <> $p0 RETURN n0",
            30L);
    }

    [Fact]
    public void Relational_operators_are_translated()
    {
        AssertQuery(Nodes<Person>().Where(p => p.Age < 30), "MATCH (n0:Person) WHERE n0.age < $p0 RETURN n0", 30L);
        AssertQuery(Nodes<Person>().Where(p => p.Age <= 30), "MATCH (n0:Person) WHERE n0.age <= $p0 RETURN n0", 30L);
        AssertQuery(Nodes<Person>().Where(p => p.Age > 30), "MATCH (n0:Person) WHERE n0.age > $p0 RETURN n0", 30L);
        AssertQuery(Nodes<Person>().Where(p => p.Age >= 30), "MATCH (n0:Person) WHERE n0.age >= $p0 RETURN n0", 30L);
    }

    [Fact]
    public void Boolean_operators_keep_their_precedence()
    {
        AssertQuery(
            Nodes<Person>().Where(p => p.Age > 30 && (p.Active || p.Name == "x")),
            "MATCH (n0:Person) WHERE n0.age > $p0 AND (n0.active OR n0.name = $p1) RETURN n0",
            30L,
            "x");
    }

    [Fact]
    public void Redundant_parentheses_are_not_emitted()
    {
        AssertQuery(
            Nodes<Person>().Where(p => (p.Active || p.Age > 30) && p.Name == "x"),
            "MATCH (n0:Person) WHERE (n0.active OR n0.age > $p0) AND n0.name = $p1 RETURN n0",
            30L,
            "x");

        AssertQuery(
            Nodes<Person>().Where(p => p.Active && p.Age > 30 && p.Height > 1.0),
            "MATCH (n0:Person) WHERE n0.active AND n0.age > $p0 AND n0.height > $p1 RETURN n0",
            30L,
            1.0);
    }

    [Fact]
    public void Negation_becomes_not()
    {
        AssertQuery(Nodes<Person>().Where(p => !p.Active), "MATCH (n0:Person) WHERE NOT n0.active RETURN n0");
    }

    [Fact]
    public void Bare_boolean_property_is_used_as_a_predicate()
    {
        AssertQuery(Nodes<Person>().Where(p => p.Active), "MATCH (n0:Person) WHERE n0.active RETURN n0");
    }

    [Fact]
    public void Null_comparisons_become_is_null()
    {
        AssertQuery(Nodes<Person>().Where(p => p.Nickname == null), "MATCH (n0:Person) WHERE n0.nickname IS NULL RETURN n0");
        AssertQuery(Nodes<Person>().Where(p => p.Nickname != null), "MATCH (n0:Person) WHERE n0.nickname IS NOT NULL RETURN n0");
    }

    [Fact]
    public void Nullable_has_value_becomes_is_not_null()
    {
        AssertQuery(Nodes<Person>().Where(p => p.Score.HasValue), "MATCH (n0:Person) WHERE n0.score IS NOT NULL RETURN n0");
    }

    [Fact]
    public void String_predicates_map_to_their_cypher_operators()
    {
        AssertQuery(
            Nodes<Person>().Where(p => p.Name.StartsWith("Al")),
            "MATCH (n0:Person) WHERE n0.name STARTS WITH $p0 RETURN n0",
            "Al");

        AssertQuery(
            Nodes<Person>().Where(p => p.Name.EndsWith("ce")),
            "MATCH (n0:Person) WHERE n0.name ENDS WITH $p0 RETURN n0",
            "ce");

        AssertQuery(
            Nodes<Person>().Where(p => p.Name.Contains("li")),
            "MATCH (n0:Person) WHERE n0.name CONTAINS $p0 RETURN n0",
            "li");
    }

    [Fact]
    public void String_case_functions_are_translated()
    {
        AssertQuery(
            Nodes<Person>().Where(p => p.Name.ToUpper() == "ALICE"),
            "MATCH (n0:Person) WHERE toUpper(n0.name) = $p0 RETURN n0",
            "ALICE");

        AssertQuery(
            Nodes<Person>().Where(p => p.Name.ToLower() == "alice"),
            "MATCH (n0:Person) WHERE toLower(n0.name) = $p0 RETURN n0",
            "alice");
    }

    [Fact]
    public void String_length_becomes_size()
    {
        AssertQuery(
            Nodes<Person>().Where(p => p.Name.Length > 3),
            "MATCH (n0:Person) WHERE size(n0.name) > $p0 RETURN n0",
            3L);
    }

    [Fact]
    public void Enumerable_contains_becomes_in()
    {
        var names = new[] { "Alice", "Bob" };

        AssertQuery(
            Nodes<Person>().Where(p => names.Contains(p.Name)),
            "MATCH (n0:Person) WHERE n0.name IN $p0 RETURN n0",
            (object)new object[] { "Alice", "Bob" });
    }

    [Fact]
    public void List_contains_becomes_in()
    {
        var ages = new List<int> { 10, 20 };

        AssertQuery(
            Nodes<Person>().Where(p => ages.Contains(p.Age)),
            "MATCH (n0:Person) WHERE n0.age IN $p0 RETURN n0",
            (object)new object[] { 10L, 20L });
    }

    [Fact]
    public void Math_functions_are_translated()
    {
        AssertQuery(
            Nodes<Person>().Where(p => Math.Abs(p.Height) > 1.5),
            "MATCH (n0:Person) WHERE abs(n0.height) > $p0 RETURN n0",
            1.5);

        AssertQuery(
            Nodes<Person>().Where(p => Math.Floor(p.Height) > 1.0),
            "MATCH (n0:Person) WHERE floor(n0.height) > $p0 RETURN n0",
            1.0);

        AssertQuery(
            Nodes<Person>().Where(p => Math.Pow(p.Height, 2.0) > 4.0),
            "MATCH (n0:Person) WHERE n0.height ^ $p0 > $p1 RETURN n0",
            2.0,
            4.0);
    }

    [Fact]
    public void Arithmetic_is_translated()
    {
        AssertQuery(
            Nodes<Person>().Where(p => p.Age + 1 > 30),
            "MATCH (n0:Person) WHERE n0.age + $p0 > $p1 RETURN n0",
            1L,
            30L);

        AssertQuery(
            Nodes<Person>().Where(p => p.Age * 2 - 1 > 30),
            "MATCH (n0:Person) WHERE n0.age * $p0 - $p1 > $p2 RETURN n0",
            2L,
            1L,
            30L);
    }

    [Fact]
    public void Graph_id_becomes_the_id_function()
    {
        AssertQuery(
            Nodes<Person>().Where(p => p.Id == 7),
            "MATCH (n0:Person) WHERE id(n0) = $p0 RETURN n0",
            7L);
    }

    [Fact]
    public void Enum_values_are_bound_by_name()
    {
        AssertQuery(
            Nodes<Person>().Where(p => p.Rating == Rating.Great),
            "MATCH (n0:Person) WHERE n0.rating = $p0 RETURN n0",
            "Great");
    }

    [Fact]
    public void Captured_locals_are_evaluated_and_parameterized()
    {
        var minimumAge = 21;

        AssertQuery(
            Nodes<Person>().Where(p => p.Age >= minimumAge),
            "MATCH (n0:Person) WHERE n0.age >= $p0 RETURN n0",
            21L);
    }

    [Fact]
    public void Captured_expressions_are_evaluated_before_being_parameterized()
    {
        var baseline = 20;

        AssertQuery(
            Nodes<Person>().Where(p => p.Age >= baseline + 1),
            "MATCH (n0:Person) WHERE n0.age >= $p0 RETURN n0",
            21L);
    }

    [Fact]
    public void Datetime_parameters_are_bound_as_iso_8601()
    {
        var cutoff = new DateTime(2020, 5, 17, 13, 45, 0, DateTimeKind.Utc);

        AssertQuery(
            Nodes<Person>().Where(p => p.Joined > cutoff),
            "MATCH (n0:Person) WHERE n0.joined > $p0 RETURN n0",
            "2020-05-17T13:45:00.0000000Z");
    }

    [Fact]
    public void Successive_where_calls_are_anded_together()
    {
        AssertQuery(
            Nodes<Person>().Where(p => p.Age > 30).Where(p => p.Active),
            "MATCH (n0:Person) WHERE n0.age > $p0 AND n0.active RETURN n0",
            30L);
    }

    // ----------------------------------------------------------- projections

    [Fact]
    public void Member_projection_returns_the_property()
    {
        AssertQuery(Nodes<Person>().Select(p => p.Name), "MATCH (n0:Person) RETURN n0.name");
    }

    [Fact]
    public void Anonymous_type_projection_aliases_each_column()
    {
        AssertQuery(
            Nodes<Person>().Select(p => new { p.Name, p.Age }),
            "MATCH (n0:Person) RETURN n0.name AS Name, n0.age AS Age");
    }

    [Fact]
    public void Member_init_projection_aliases_each_bound_member()
    {
        AssertQuery(
            Nodes<Person>().Select(p => new Company { Name = p.Name, Founded = p.Age }),
            "MATCH (n0:Person) RETURN n0.name AS Name, n0.age AS Founded");
    }

    [Fact]
    public void Identity_projection_returns_the_entity()
    {
        AssertQuery(Nodes<Person>().Select(p => p), "MATCH (n0:Person) RETURN n0");
    }

    [Fact]
    public void Computed_projections_are_translated()
    {
        AssertQuery(
            Nodes<Person>().Select(p => p.Name.ToUpper()),
            "MATCH (n0:Person) RETURN toUpper(n0.name)");
    }

    // ------------------------------------------------------ ordering, paging

    [Fact]
    public void Order_by_emits_an_explicit_direction()
    {
        AssertQuery(Nodes<Person>().OrderBy(p => p.Name), "MATCH (n0:Person) RETURN n0 ORDER BY n0.name ASC");
        AssertQuery(Nodes<Person>().OrderByDescending(p => p.Name), "MATCH (n0:Person) RETURN n0 ORDER BY n0.name DESC");
    }

    [Fact]
    public void Then_by_appends_a_sort_term()
    {
        AssertQuery(
            Nodes<Person>().OrderByDescending(p => p.Age).ThenBy(p => p.Name),
            "MATCH (n0:Person) RETURN n0 ORDER BY n0.age DESC, n0.name ASC");

        AssertQuery(
            Nodes<Person>().OrderBy(p => p.Age).ThenByDescending(p => p.Name),
            "MATCH (n0:Person) RETURN n0 ORDER BY n0.age ASC, n0.name DESC");
    }

    [Fact]
    public void A_second_order_by_replaces_the_previous_sort()
    {
        AssertQuery(
            Nodes<Person>().OrderBy(p => p.Age).OrderBy(p => p.Name),
            "MATCH (n0:Person) RETURN n0 ORDER BY n0.name ASC");
    }

    [Fact]
    public void Skip_and_take_become_skip_and_limit()
    {
        AssertQuery(Nodes<Person>().Skip(5), "MATCH (n0:Person) RETURN n0 SKIP 5");
        AssertQuery(Nodes<Person>().Take(10), "MATCH (n0:Person) RETURN n0 LIMIT 10");
        AssertQuery(Nodes<Person>().Skip(5).Take(10), "MATCH (n0:Person) RETURN n0 SKIP 5 LIMIT 10");
    }

    [Fact]
    public void Distinct_is_applied_to_the_return_clause()
    {
        AssertQuery(Nodes<Person>().Select(p => p.Name).Distinct(), "MATCH (n0:Person) RETURN DISTINCT n0.name");
    }

    [Fact]
    public void A_full_chain_renders_every_clause_in_cypher_order()
    {
        AssertQuery(
            Nodes<Person>().Where(p => p.Age > 30).OrderBy(p => p.Name).Skip(1).Take(2),
            "MATCH (n0:Person) WHERE n0.age > $p0 RETURN n0 ORDER BY n0.name ASC SKIP 1 LIMIT 2",
            30L);
    }

    // -------------------------------------------------------------- traversal

    [Fact]
    public void Traverse_extends_the_match_pattern()
    {
        AssertQuery(
            Nodes<Person>().Traverse<Knows, Person>(),
            "MATCH (n0:Person)-[r0:KNOWS]->(n1:Person) RETURN n1");
    }

    [Fact]
    public void Traverse_keeps_the_filter_on_the_source_node()
    {
        AssertQuery(
            Nodes<Person>().Where(p => p.Name == "Alice").Traverse<Knows, Person>(),
            "MATCH (n0:Person)-[r0:KNOWS]->(n1:Person) WHERE n0.name = $p0 RETURN n1",
            "Alice");
    }

    [Fact]
    public void Traverse_can_filter_the_relationship()
    {
        AssertQuery(
            Nodes<Person>().Traverse<Knows, Person>(k => k.Since > 2000),
            "MATCH (n0:Person)-[r0:KNOWS]->(n1:Person) WHERE r0.since > $p0 RETURN n1",
            2000L);
    }

    [Fact]
    public void Traverse_honours_the_requested_direction()
    {
        AssertQuery(
            Nodes<Person>().Traverse<Knows, Person>(TraversalDirection.Incoming),
            "MATCH (n0:Person)<-[r0:KNOWS]-(n1:Person) RETURN n1");

        AssertQuery(
            Nodes<Person>().Traverse<Knows, Person>(TraversalDirection.Any),
            "MATCH (n0:Person)-[r0:KNOWS]-(n1:Person) RETURN n1");
    }

    [Fact]
    public void Traversals_can_be_chained()
    {
        AssertQuery(
            Nodes<Person>().Traverse<Knows, Person>().Traverse<WorksAt, Company>(),
            "MATCH (n0:Person)-[r0:KNOWS]->(n1:Person)-[r1:WORKS_AT]->(n2:Company:Organization) RETURN n2");
    }

    [Fact]
    public void Select_many_traverses_a_navigation_property()
    {
        AssertQuery(
            Nodes<Person>().SelectMany(p => p.Knows),
            "MATCH (n0:Person)-[r0:KNOWS]->(n1:Person) RETURN n1");
    }

    [Fact]
    public void Select_many_with_a_result_selector_can_project_both_ends()
    {
        AssertQuery(
            Nodes<Person>().SelectMany(p => p.Knows, (p, friend) => new { Who = p.Name, Friend = friend.Name }),
            "MATCH (n0:Person)-[r0:KNOWS]->(n1:Person) RETURN n0.name AS Who, n1.name AS Friend");
    }

    [Fact]
    public void A_predicate_after_a_traversal_binds_to_the_far_node()
    {
        AssertQuery(
            Nodes<Person>().Traverse<Knows, Person>().Where(p => p.Age > 18),
            "MATCH (n0:Person)-[r0:KNOWS]->(n1:Person) WHERE n1.age > $p0 RETURN n1",
            18L);
    }

    // --------------------------------------------------- terminal operators

    private static void AssertTerminal(Action<QueryHarness> query, string expectedCypher, params object[] expectedParameters)
    {
        var harness = new QueryHarness();

        query(harness);

        Assert.Equal(expectedCypher, harness.CapturedCypher);
        AssertParameters(
            harness.CapturedParameters.ToDictionary(p => p.Key, p => p.Value),
            expectedParameters);
    }

    [Fact]
    public void Count_becomes_a_count_aggregate()
    {
        AssertTerminal(h => h.Nodes<Person>().Count(), "MATCH (n0:Person) RETURN count(*)");
        AssertTerminal(h => h.Nodes<Person>().LongCount(), "MATCH (n0:Person) RETURN count(*)");
    }

    [Fact]
    public void Count_with_a_predicate_filters_first()
    {
        AssertTerminal(
            h => h.Nodes<Person>().Count(p => p.Age > 30),
            "MATCH (n0:Person) WHERE n0.age > $p0 RETURN count(*)",
            30L);
    }

    [Fact]
    public void Any_becomes_a_count_comparison()
    {
        AssertTerminal(h => h.Nodes<Person>().Any(), "MATCH (n0:Person) RETURN count(*) > 0");
        AssertTerminal(h => h.Nodes<Person>().Any(p => p.Active), "MATCH (n0:Person) WHERE n0.active RETURN count(*) > 0");
    }

    [Fact]
    public void All_becomes_a_negated_count_comparison()
    {
        AssertTerminal(h => h.Nodes<Person>().All(p => p.Active), "MATCH (n0:Person) WHERE NOT n0.active RETURN count(*) = 0");
    }

    [Fact]
    public void First_limits_to_one_row()
    {
        AssertTerminal(h => h.Nodes<Person>().First(), "MATCH (n0:Person) RETURN n0 LIMIT 1");
        AssertTerminal(h => h.Nodes<Person>().FirstOrDefault(), "MATCH (n0:Person) RETURN n0 LIMIT 1");
    }

    [Fact]
    public void Single_limits_to_two_rows_so_a_duplicate_can_be_detected()
    {
        AssertTerminal(h => h.Nodes<Person>().Single(), "MATCH (n0:Person) RETURN n0 LIMIT 2");
        AssertTerminal(h => h.Nodes<Person>().SingleOrDefault(), "MATCH (n0:Person) RETURN n0 LIMIT 2");
    }

    [Fact]
    public void Aggregates_wrap_the_projected_column()
    {
        AssertTerminal(h => h.Nodes<Person>().Sum(p => p.Age), "MATCH (n0:Person) RETURN sum(n0.age)");
        AssertTerminal(h => h.Nodes<Person>().Min(p => p.Age), "MATCH (n0:Person) RETURN min(n0.age)");
        AssertTerminal(h => h.Nodes<Person>().Max(p => p.Age), "MATCH (n0:Person) RETURN max(n0.age)");
        AssertTerminal(h => h.Nodes<Person>().Average(p => p.Age), "MATCH (n0:Person) RETURN avg(n0.age)");
    }

    [Fact]
    public void An_aggregate_over_an_existing_projection_needs_no_selector()
    {
        AssertTerminal(h => h.Nodes<Person>().Select(p => p.Age).Max(), "MATCH (n0:Person) RETURN max(n0.age)");
    }

    [Fact]
    public void An_aggregate_over_a_paged_query_uses_a_with_clause()
    {
        AssertTerminal(h => h.Nodes<Person>().Take(5).Count(), "MATCH (n0:Person) WITH n0 LIMIT 5 RETURN count(*)");

        AssertTerminal(
            h => h.Nodes<Person>().Select(p => p.Name).Distinct().Count(),
            "MATCH (n0:Person) WITH DISTINCT n0.name AS c0 RETURN count(*)");
    }

    [Fact]
    public void An_ordered_page_aggregate_orders_by_the_carried_column()
    {
        AssertTerminal(
            h => h.Nodes<Person>().OrderBy(p => p.Age).Take(3).Select(p => p.Age).Sum(),
            "MATCH (n0:Person) WITH n0.age AS c0 ORDER BY c0 ASC LIMIT 3 RETURN sum(c0)");
    }

    [Fact]
    public void An_ordered_page_count_keeps_the_entity_in_scope()
    {
        AssertTerminal(
            h => h.Nodes<Person>().OrderBy(p => p.Name).Take(3).Count(),
            "MATCH (n0:Person) WITH n0 ORDER BY n0.name ASC LIMIT 3 RETURN count(*)");
    }

    [Fact]
    public void Enumeration_sends_the_query_unchanged()
    {
        AssertTerminal(h => h.Nodes<Person>().ToList(), "MATCH (n0:Person) RETURN n0");
    }

    // ------------------------------------------------------------- rendering

    [Fact]
    public void To_string_shows_the_cypher_and_its_parameters()
    {
        var text = Nodes<Person>().Where(p => p.Name == "Alice").ToString();

        Assert.Equal("MATCH (n0:Person) WHERE n0.name = $p0 RETURN n0 -- p0=Alice", text);
    }
}
