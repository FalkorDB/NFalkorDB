using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.ObjectModel;
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
            "MATCH (n0:Collide) WITH n0.name AS o0, n0.age AS o1 ORDER BY o1 ASC LIMIT $p0 RETURN count(*)",
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
            "MATCH (n0:Collide) WITH n0.name AS n0, n0.age AS o0 ORDER BY o0 ASC LIMIT $p0 RETURN count(*)",
            harness.CapturedCypher);
    }

    [Fact]
    public void A_whole_node_projection_still_lets_the_sort_key_resolve_through_the_with()
    {
        var harness = new QueryHarness();

        harness.Nodes<Person>().OrderBy(p => p.Age).Take(3).Count();

        Assert.Equal(
            "MATCH (n0:Person) WITH n0 ORDER BY n0.age ASC LIMIT $p0 RETURN count(*)",
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
        Assert.Contains("anonymous type", exception.Message);
    }

    [Fact]
    public void Min_and_max_need_a_projection_the_clr_can_order()
    {
        // max(n0.age) would come back as a number while the caller was promised the anonymous type,
        // and LINQ itself throws here because the type has no ordering.
        Assert.Contains(
            "Person",
            Assert.Throws<NotSupportedException>(
                () => new QueryHarness().Nodes<Person>().Min()).Message);

        // IComparable is not proof of Cypher-compatible ordering: this type orders by name length
        // while min(n0.name) would order lexically.
        Assert.Contains(
            nameof(OrdersByLength),
            Assert.Throws<NotSupportedException>(
                () => new QueryHarness().Nodes<Person>().Select(p => new OrdersByLength(p.Name)).Min()).Message);

        Assert.Contains(
            "String[]",
            Assert.Throws<NotSupportedException>(
                () => new QueryHarness().Nodes<Person>().Select(p => p.Tags).Min()).Message);

        // Scalars, nullable scalars and strings all stay translatable.
        var harness = new QueryHarness();
        harness.Nodes<Person>().Select(p => p.Age).Min();
        Assert.Equal("MATCH (n0:Person) RETURN min(n0.age)", harness.CapturedCypher);

        var nullable = new QueryHarness();
        nullable.Nodes<Person>().Select(p => p.Score).Max();
        Assert.Equal("MATCH (n0:Person) RETURN max(n0.score)", nullable.CapturedCypher);

        var text = new QueryHarness();
        text.Nodes<Person>().Select(p => p.Name).Min();
        Assert.Equal("MATCH (n0:Person) RETURN min(n0.name)", text.CapturedCypher);

        var moment = new QueryHarness();
        moment.Nodes<Person>().Select(p => p.Joined).Max();
        Assert.Equal("MATCH (n0:Person) RETURN max(n0.joined)", moment.CapturedCypher);
    }

    [Fact]
    public void A_datetime_binds_as_a_round_trip_iso_8601_string()
    {
        // FalkorDB has no datetime() constructor, so the README documents ISO-8601 in UTC as the
        // storage contract. That format also sorts lexically in chronological order.
        var cutoff = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var query = QueryHarnessExtensions.Nodes<Person>().Where(p => p.Joined > cutoff).ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.joined > $p0 RETURN n0", query.Cypher);
        Assert.Equal("2020-01-02T03:04:05.0000000Z", Assert.IsType<string>(query.Parameters["p0"]));

        Assert.Equal(1500L, ParameterBag.Normalize(TimeSpan.FromMilliseconds(1500)));
    }

    [Fact]
    public void Comparing_a_collection_with_equals_is_rejected()
    {
        // C# compares string[] by reference, so this matches nothing in memory, while Cypher would
        // compare the lists structurally and return rows.
        var tags = new[] { "a" };

        Assert.Contains(
            "String[]",
            Assert.Throws<NotSupportedException>(
                () => QueryHarnessExtensions.Nodes<Person>().Where(p => p.Tags == tags).Cypher()).Message);

        Assert.Contains(
            "'!='",
            Assert.Throws<NotSupportedException>(
                () => QueryHarnessExtensions.Nodes<Person>().Where(p => p.Tags != tags).Cypher()).Message);

        // Membership is still the supported way to ask about a collection, and scalar equality is
        // untouched.
        Assert.Equal(
            "MATCH (n0:Person) WHERE $p0 IN n0.tags RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Tags.Contains("a")).Cypher());

        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.name = $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Name == "Alice").Cypher());
    }

    [Fact]
    public void Distinct_over_a_projection_holding_a_collection_member_is_rejected()
    {
        // The outer anonymous type has structural equality, but it compares its string[] member by
        // reference, so RETURN DISTINCT n0.tags would still collapse rows LINQ keeps.
        var message = Assert.Throws<NotSupportedException>(() =>
            QueryHarnessExtensions.Nodes<Person>().Select(p => new { p.Tags }).Distinct().Cypher()).Message;

        Assert.Contains("Distinct", message);
        Assert.Contains("String[]", message);

        // A projection of scalars, or one holding a whole node, is still allowed.
        Assert.Equal(
            "MATCH (n0:Person) RETURN DISTINCT n0.name AS Name, n0.age AS Age",
            QueryHarnessExtensions.Nodes<Person>().Select(p => new { p.Name, p.Age }).Distinct().Cypher());

        Assert.Equal(
            "MATCH (n0:Person) RETURN DISTINCT n0 AS p, n0.name AS Name",
            QueryHarnessExtensions.Nodes<Person>().Select(p => new { p, p.Name }).Distinct().Cypher());
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

        Assert.Equal("MATCH (n0:Person) RETURN n0.name AS c1, n0.age AS c2 LIMIT $p0", cypher);
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
    public void Equality_is_null_safe_only_when_both_sides_can_be_null()
    {
        // C# says null == null is true, but Cypher's = yields null there and WHERE drops the row.
        // Two nullable property reads are the only shape where that difference is observable.
        Assert.Equal(
            "MATCH (n0:Person) WHERE coalesce(n0.name = n0.nickname, n0.name IS NULL AND n0.nickname IS NULL) RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Name == p.Nickname).Cypher());

        // One nullable side against a non-null constant needs nothing: Cypher's null and C#'s false
        // are both rejected by WHERE. Leaving it bare is what keeps the predicate indexable.
        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.name = $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Name == "Alice").Cypher());

        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.score = $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Score == 10).Cypher());

        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.age = $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Age == 30).Cypher());

        // A null literal still takes the dedicated IS NULL form rather than the fallback.
        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.name IS NULL RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Name == null).Cypher());
    }

    [Fact]
    public void Distinct_is_allowed_for_projections_that_carry_value_equality()
    {
        // The reference-projection guard must not catch anonymous types (which the compiler gives
        // structural equality), scalars, or a whole node (de-duplicated by graph identity).
        Assert.Equal(
            "MATCH (n0:Person) RETURN DISTINCT n0.name AS Name",
            QueryHarnessExtensions.Nodes<Person>().Select(p => new { p.Name }).Distinct().Cypher());

        Assert.Equal(
            "MATCH (n0:Person) RETURN DISTINCT n0.name",
            QueryHarnessExtensions.Nodes<Person>().Select(p => p.Name).Distinct().Cypher());

        Assert.Equal(
            "MATCH (n0:Person) RETURN DISTINCT n0",
            QueryHarnessExtensions.Nodes<Person>().Distinct().Cypher());

        // A record keeps the compiler's structural equality, which does agree with RETURN DISTINCT.
        Assert.Equal(
            "MATCH (n0:Person) RETURN DISTINCT n0.name AS Name",
            QueryHarnessExtensions.Nodes<Person>().Select(p => new NameRecord(p.Name)).Distinct().Cypher());

        // A whole node projected as a column still de-duplicates by graph identity.
        Assert.Equal(
            "MATCH (n0:Person) RETURN DISTINCT n0",
            QueryHarnessExtensions.Nodes<Person>().Select(p => p).Distinct().Cypher());
    }

    [Fact]
    public void Distinct_over_a_hand_written_equality_is_rejected()
    {
        // OverridesEquals used to accept any Equals override, but a hand-written Equals may ignore
        // members or call everything equal, and then LINQ and RETURN DISTINCT disagree on the rows.
        var message = Assert.Throws<NotSupportedException>(() =>
            QueryHarnessExtensions.Nodes<Person>()
                .Select(p => new AlwaysEqual { Name = p.Name })
                .Distinct()
                .Cypher()).Message;

        Assert.Contains("Distinct", message);
        Assert.Contains(nameof(AlwaysEqual), message);

        // A record may replace its generated Equals too, so the record shape alone is not enough.
        var record = Assert.Throws<NotSupportedException>(() =>
            QueryHarnessExtensions.Nodes<Person>()
                .Select(p => new AlwaysEqualRecord(p.Name))
                .Distinct()
                .Cypher()).Message;

        Assert.Contains(nameof(AlwaysEqualRecord), record);
    }

    [Fact]
    public void Distinct_over_a_collection_column_is_rejected()
    {
        // LINQ compares string[] by reference and keeps every row, while Cypher compares lists by
        // value and collapses them, so RETURN DISTINCT n0.tags would quietly return fewer rows.
        var message = Assert.Throws<NotSupportedException>(() =>
            QueryHarnessExtensions.Nodes<Person>().Select(p => p.Tags).Distinct().Cypher()).Message;

        Assert.Contains("Distinct", message);
        Assert.Contains("reference", message);
    }

    [Fact]
    public void A_collection_without_a_custom_comparer_still_becomes_in()
    {
        // The comparer and allow-list guards must only reject collections that genuinely define
        // membership differently, so the whole standard collection surface is pinned here.
        var list = new List<string> { "Alice" };
        var set = new HashSet<string> { "Alice" };
        var queue = new Queue<string>();
        var stack = new Stack<string>();
        var linked = new LinkedList<string>();
        var array = new[] { "Alice" };
        var immutableArray = ImmutableArray.Create("Alice");
        var immutableList = ImmutableList.Create("Alice");
        var readOnly = new ReadOnlyCollection<string>(list);
        var bag = new ConcurrentBag<string>();
        var map = new Dictionary<string, int> { { "Alice", 1 } };
        var legacy = new ArrayList { "Alice" };
        ICollection<string> collection = list;
        IList<string> indexed = list;
        ISet<string> unique = set;
        IEnumerable<string> sequence = list;
        IReadOnlyList<string> readOnlyList = list;

        foreach (var cypher in new[]
                 {
                     QueryHarnessExtensions.Nodes<Person>().Where(p => list.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => set.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => queue.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => stack.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => linked.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => array.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => immutableArray.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => immutableList.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => readOnly.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => bag.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => map.Keys.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => legacy.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => collection.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => indexed.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => unique.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => sequence.Contains(p.Name)).Cypher(),
                     QueryHarnessExtensions.Nodes<Person>().Where(p => readOnlyList.Contains(p.Name)).Cypher(),
                 })
        {
            Assert.Equal("MATCH (n0:Person) WHERE n0.name IN $p0 RETURN n0", cypher);
        }

        // An enum array binds to the three-argument MemoryExtensions overload, and a graph property
        // puts the collection on the right of IN rather than the left.
        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.rating IN $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => new[] { Rating.Great }.Contains(p.Rating)).Cypher());

        Assert.Equal(
            "MATCH (n0:Person) WHERE $p0 IN n0.tags RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => p.Tags.Contains("a")).Cypher());
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
    [Fact]
    public void Distinct_looks_through_a_projection_to_its_nested_members()
    {
        // The outer anonymous type has compiler-generated equality, but that equality delegates to
        // whatever the nested member defines, so checking only the root was not enough.
        var message = Assert.Throws<NotSupportedException>(() =>
            QueryHarnessExtensions.Nodes<Person>()
                .Select(p => new { Inner = new AlwaysEqual { Name = p.Name } })
                .Distinct()
                .Cypher()).Message;

        Assert.Contains("nested", message);
        Assert.Contains(nameof(AlwaysEqual), message);

        // A nested projection that does carry provable equality is still fine.
        Assert.Equal(
            "MATCH (n0:Person) RETURN DISTINCT n0.name AS Name",
            QueryHarnessExtensions.Nodes<Person>()
                .Select(p => new { Inner = new NameRecord(p.Name) })
                .Distinct()
                .Cypher());

        // A record struct keeps the compiler's equality too.
        Assert.Equal(
            "MATCH (n0:Person) RETURN DISTINCT n0.name AS Name",
            QueryHarnessExtensions.Nodes<Person>().Select(p => new NameRecordStruct(p.Name)).Distinct().Cypher());
    }

    [Fact]
    public void A_terminal_operator_that_carries_its_own_default_is_rejected()
    {
        // The .NET 6 overloads add a default value as a trailing argument. Matching on the method
        // name alone accepted them and then read only the two-argument shape, so both the predicate
        // and the default were dropped and the query returned an unfiltered row.
        var withBoth = Assert.Throws<NotSupportedException>(
            () => new QueryHarness().Nodes<Person>().FirstOrDefault(p => p.Age > 30, null));

        Assert.Contains("'FirstOrDefault' overload that takes a default value", withBoth.Message);
        Assert.Contains("?? fallback", withBoth.Message);

        Assert.Contains(
            "'SingleOrDefault' overload that takes a default value",
            Assert.Throws<NotSupportedException>(
                () => new QueryHarness().Nodes<Person>().SingleOrDefault(p => p.Age > 30, null)).Message);

        // The single-argument default overload is rejected for the same reason.
        Assert.Contains(
            "'FirstOrDefault' overload that takes a default value",
            Assert.Throws<NotSupportedException>(
                () => new QueryHarness().Nodes<Person>().FirstOrDefault((Person)null)).Message);

        // The predicate overloads the provider does support keep working.
        var harness = new QueryHarness();
        harness.Nodes<Person>().FirstOrDefault(p => p.Age > 30);

        Assert.Equal("MATCH (n0:Person) WHERE n0.age > $p0 RETURN n0 LIMIT 1", harness.CapturedCypher);
    }

    [Fact]
    public void A_queryable_held_in_an_enumerable_variable_is_not_folded_away()
    {
        // The static type here is IEnumerable<Person>, so the type test for IQueryable never fired
        // and the partial evaluator compiled and ran `other.Count()` during translation -- a second
        // round trip, and exactly the silent client-side evaluation the provider promises to reject.
        IEnumerable<Person> other = QueryHarnessExtensions.Nodes<Person>();

        Assert.Contains(
            "cannot be translated",
            Assert.Throws<NotSupportedException>(
                () => QueryHarnessExtensions.Nodes<Person>().Where(p => p.Age > other.Count()).Cypher()).Message);

        // A query reached through a member cannot be read without invoking the member, so the value
        // is only known once the source has been evaluated on its own.
        Assert.Contains(
            "its source is a graph query",
            Assert.Throws<NotSupportedException>(
                () => QueryHarnessExtensions.Nodes<Person>().Where(p => p.Age > QueryProperty.Count()).Cypher()).Message);

        Assert.Contains(
            "its source is a graph query",
            Assert.Throws<NotSupportedException>(
                () => QueryHarnessExtensions.Nodes<Person>().Where(p => p.Age > QueryMethod().Count()).Cypher()).Message);
    }

    private static IEnumerable<Person> QueryProperty => QueryHarnessExtensions.Nodes<Person>();

    private static IEnumerable<Person> QueryMethod() => QueryHarnessExtensions.Nodes<Person>();

    [Fact]
    public void A_key_view_is_judged_by_the_comparer_of_the_map_behind_it()
    {
        // Dictionary<,>.KeyCollection answers Contains through the dictionary's key lookup, so it
        // uses the dictionary's comparer while exposing no comparer of its own. Reading only the
        // view turned a case-insensitive lookup into a case-sensitive IN.
        var insensitive = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase) { ["alice"] = 1 };

        Assert.Contains(
            "custom comparer",
            Assert.Throws<NotSupportedException>(
                () => QueryHarnessExtensions.Nodes<Person>().Where(p => insensitive.Keys.Contains(p.Name)).Cypher()).Message);

        var sorted = new SortedDictionary<string, int>(StringComparer.OrdinalIgnoreCase) { ["alice"] = 1 };

        Assert.Contains(
            "custom comparer",
            Assert.Throws<NotSupportedException>(
                () => QueryHarnessExtensions.Nodes<Person>().Where(p => sorted.Keys.Contains(p.Name)).Cypher()).Message);

        // The default-comparer map is unaffected.
        var ordinary = new Dictionary<string, int> { ["alice"] = 1 };

        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.name IN $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => ordinary.Keys.Contains(p.Name)).Cypher());

        // So is the value view: it compares with EqualityComparer<TValue>.Default whatever the key
        // comparer is, so Cypher's IN already matches it and rejecting it would be wrong.
        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.age IN $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => insensitive.Values.Contains(p.Age)).Cypher());
    }

    [Fact]
    public void Captured_in_memory_collections_are_still_folded()
    {
        // The guard above must not reach any further than a real queryable: an ordinary captured
        // collection has no server round trip to trigger and has to keep folding into a parameter.
        var names = new List<string> { "Alice", "Bob" };
        IEnumerable<string> sequence = names;
        var ages = new[] { 30, 40 };

        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.name IN $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => names.Contains(p.Name)).Cypher());

        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.name IN $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => sequence.Contains(p.Name)).Cypher());

        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.age IN $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => ages.Contains(p.Age)).Cypher());

        // A method call over a captured collection is a local computation, not a query.
        var query = QueryHarnessExtensions.Nodes<Person>().Where(p => p.Age > names.Count()).ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.age > $p0 RETURN n0", query.Cypher);
        Assert.Equal(2L, query.Parameters["p0"]);

        // A chain of local operators is still just a local computation.
        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.name IN $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => names.Distinct().Contains(p.Name)).Cypher());
    }

    // ------------------------------------------------------------------ paging

    [Fact]
    public void Caller_supplied_paging_counts_are_parameters_but_operator_limits_are_not()
    {
        // Two pages of the same query must produce one cached plan, not one per offset.
        var first = QueryHarnessExtensions.Nodes<Person>().Skip(0).Take(25).ToCypherQuery();
        var second = QueryHarnessExtensions.Nodes<Person>().Skip(25).Take(25).ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) RETURN n0 SKIP $p0 LIMIT $p1", first.Cypher);
        Assert.Equal(first.Cypher, second.Cypher);
        Assert.Equal(0L, first.Parameters["p0"]);
        Assert.Equal(25L, second.Parameters["p0"]);

        // The LIMIT that `First`/`Single` add is the provider's own, not a caller value, so it
        // stays literal -- binding it would only add a parameter that never varies.
        var harness = new QueryHarness();
        try
        {
            harness.Nodes<Person>().First();
        }
        catch (InvalidOperationException)
        {
        }

        Assert.Equal("MATCH (n0:Person) RETURN n0 LIMIT 1", harness.CapturedCypher);
        Assert.Empty(harness.CapturedParameters);
    }

    // --------------------------------------------------------------- ordering

    [Fact]
    public void An_order_by_that_carries_a_clr_comparer_is_rejected()
    {
        // The comparer runs in the CLR; Cypher sorts on the server and never sees it. Accepting
        // the overload would silently order by the server's rules instead.
        var error = Assert.Throws<NotSupportedException>(
            () => QueryHarnessExtensions.Nodes<Person>().OrderBy(p => p.Name, StringComparer.OrdinalIgnoreCase).Cypher());

        Assert.Contains("IComparer", error.Message);

        Assert.Throws<NotSupportedException>(
            () => QueryHarnessExtensions.Nodes<Person>().OrderByDescending(p => p.Name, StringComparer.Ordinal).Cypher());
        Assert.Throws<NotSupportedException>(
            () => QueryHarnessExtensions.Nodes<Person>().OrderBy(p => p.Age).ThenBy(p => p.Name, StringComparer.Ordinal).Cypher());
        Assert.Throws<NotSupportedException>(
            () => QueryHarnessExtensions.Nodes<Person>().OrderBy(p => p.Age).ThenByDescending(p => p.Name, StringComparer.Ordinal).Cypher());

        // The ordinary overloads are untouched.
        Assert.Equal(
            "MATCH (n0:Person) RETURN n0 ORDER BY n0.name ASC",
            QueryHarnessExtensions.Nodes<Person>().OrderBy(p => p.Name).Cypher());
    }

    [Fact]
    public void A_sort_key_the_clr_cannot_order_is_rejected()
    {
        // `Comparer<Point>.Default` throws, so in-memory LINQ fails outright. Cypher will order a
        // point quite happily, so without this guard the same query silently succeeded.
        var error = Assert.Throws<NotSupportedException>(
            () => QueryHarnessExtensions.Nodes<Surveyed>().OrderBy(p => p.Spot).Cypher());

        Assert.Contains("Point", error.Message);

        Assert.Throws<NotSupportedException>(
            () => QueryHarnessExtensions.Nodes<Person>().OrderBy(p => p.Tags).Cypher());

        // Min/Max share the rule, since they are just ordering with the rows thrown away.
        var harness = new QueryHarness();
        Assert.Throws<NotSupportedException>(() => harness.Nodes<Surveyed>().Max(p => p.Spot));

        // Everything the CLR can sort still translates.
        Assert.Equal(
            "MATCH (n0:Person) RETURN n0 ORDER BY n0.joined ASC, n0.score DESC",
            QueryHarnessExtensions.Nodes<Person>().OrderBy(p => p.Joined).ThenByDescending(p => p.Score).Cypher());
    }

    // ---------------------------------------------------------------- values

    [Fact]
    public void A_timespan_that_would_lose_precision_is_rejected()
    {
        // Durations go over the wire as whole milliseconds, so a sub-millisecond value would have
        // been truncated to a different duration and matched the wrong rows.
        var oneTick = TimeSpan.FromTicks(1);
        var error = Assert.Throws<NotSupportedException>(
            () => QueryHarnessExtensions.Nodes<Surveyed>().Where(p => p.Elapsed > oneTick).Cypher());

        Assert.Contains("millisecond", error.Message);

        var halfMillisecond = TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond / 2);
        Assert.Throws<NotSupportedException>(
            () => QueryHarnessExtensions.Nodes<Surveyed>().Where(p => p.Elapsed > halfMillisecond).Cypher());

        // Whole milliseconds are unaffected, including values far past int range.
        var exact = QueryHarnessExtensions.Nodes<Surveyed>()
            .Where(p => p.Elapsed > TimeSpan.FromMilliseconds(5))
            .ToCypherQuery();
        Assert.Equal(5L, exact.Parameters["p0"]);

        var day = QueryHarnessExtensions.Nodes<Surveyed>()
            .Where(p => p.Elapsed > TimeSpan.FromDays(1))
            .ToCypherQuery();
        Assert.Equal(86_400_000L, day.Parameters["p0"]);
    }

    [Fact]
    public void Contains_over_a_null_collection_is_rejected()
    {
        // `IN null` matches nothing in Cypher, but these bindings throw in the CLR, so translating
        // them would turn an error into a wrong answer.
        List<string> missingList = null;
        var error = Assert.Throws<NotSupportedException>(
            () => QueryHarnessExtensions.Nodes<Person>().Where(p => missingList.Contains(p.Name)).Cypher());

        Assert.Contains("null", error.Message);

        IEnumerable<string> missingSequence = null;
        Assert.Throws<NotSupportedException>(
            () => QueryHarnessExtensions.Nodes<Person>().Where(p => missingSequence.Contains(p.Name)).Cypher());

        // A null array is the one shape that is not an error: the compiler binds it to the
        // MemoryExtensions overload, which reads it as an empty span and returns false, and `IN null`
        // already matches nothing. Rejecting it would refuse a query the CLR answers happily.
        string[] missingArray = null;
        Assert.False(missingArray.Contains("Alice"));
        var nullArray = QueryHarnessExtensions.Nodes<Person>().Where(p => missingArray.Contains(p.Name)).ToCypherQuery();
        Assert.Equal("MATCH (n0:Person) WHERE n0.name IN $p0 RETURN n0", nullArray.Cypher);
        Assert.Null(nullArray.Parameters["p0"]);

        // An empty collection is a real, answerable query and still translates.
        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.name IN $p0 RETURN n0",
            QueryHarnessExtensions.Nodes<Person>().Where(p => new List<string>().Contains(p.Name)).Cypher());
    }
}

/// <summary>A node with property types that are storable but not sortable.</summary>
[Node("Person")]
public class Surveyed
{
    /// <summary>The internal entity id.</summary>
    [GraphId]
    public int Id { get; set; }

    /// <summary>A point, which FalkorDB can store and order but the CLR cannot compare.</summary>
    public Point Spot { get; set; }

    /// <summary>A duration, stored as whole milliseconds.</summary>
    public TimeSpan Elapsed { get; set; }
}

/// <summary>A record struct, whose generated equality is structural over its fields.</summary>
/// <param name="Name">The projected name.</param>
public record struct NameRecordStruct(string Name);

/// <summary>A type whose ordering has nothing to do with how Cypher orders the same value.</summary>
public class OrdersByLength : IComparable
{
    /// <summary>Creates the wrapper.</summary>
    /// <param name="name">The projected name.</param>
    public OrdersByLength(string name) => Name = name;

    /// <summary>The projected name.</summary>
    public string Name { get; set; }

    /// <summary>Orders by name length rather than lexically.</summary>
    /// <param name="obj">The instance to compare with.</param>
    /// <returns>The relative order by length.</returns>
    public int CompareTo(object obj) => Name.Length.CompareTo(((OrdersByLength)obj).Name.Length);
}

/// <summary>A record that keeps the equality the compiler generated for it.</summary>
/// <param name="Name">The projected name.</param>
public record NameRecord(string Name);

/// <summary>A record that replaces its generated equality, so only the wrapper stays generated.</summary>
/// <param name="Name">The projected name.</param>
public record AlwaysEqualRecord(string Name)
{
    /// <summary>Calls every instance equal, which no Cypher comparison does.</summary>
    /// <param name="other">The instance to compare with.</param>
    /// <returns>Always true.</returns>
    public virtual bool Equals(AlwaysEqualRecord other) => true;

    /// <inheritdoc />
    public override int GetHashCode() => 0;
}

/// <summary>A class whose hand-written equality ignores its members entirely.</summary>
public class AlwaysEqual
{
    /// <summary>The projected name.</summary>
    public string Name { get; set; }

    /// <inheritdoc />
    public override bool Equals(object obj) => obj is AlwaysEqual;

    /// <inheritdoc />
    public override int GetHashCode() => 0;
}