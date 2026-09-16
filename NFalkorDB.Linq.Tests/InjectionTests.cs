using System.Collections.Generic;
using System.Linq;
using NFalkorDB.Linq.Tests.Model;
using Xunit;

namespace NFalkorDB.Linq.Tests;

/// <summary>
/// The provider must never put a caller supplied value into the Cypher text. These tests feed the
/// translator strings that would break out of a query if they were concatenated, and assert that
/// they only ever turn up in the parameter dictionary.
/// </summary>
public class InjectionTests
{
    public static IEnumerable<object[]> HostileValues => new[]
    {
        new object[] { "\" OR 1=1 --" },
        new object[] { "\") RETURN 1 //" },
        new object[] { "') RETURN 1 //" },
        new object[] { "Alice' OR '1'='1" },
        new object[] { "x\\\" RETURN 1 //" },
        new object[] { "'; MATCH (n) DETACH DELETE n; //" },
        new object[] { "\n MATCH (n) DETACH DELETE n" },
        new object[] { "}) DETACH DELETE n //" }
    };

    [Theory]
    [MemberData(nameof(HostileValues))]
    public void Equality_values_only_reach_the_parameter_dictionary(string hostile)
    {
        var query = QueryHarnessExtensions.Nodes<Person>().Where(p => p.Name == hostile).ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.name = $p0 RETURN n0", query.Cypher);
        Assert.Equal(hostile, Assert.Single(query.Parameters).Value);
        Assert.DoesNotContain(hostile, query.Cypher);
    }

    [Theory]
    [MemberData(nameof(HostileValues))]
    public void String_operator_values_only_reach_the_parameter_dictionary(string hostile)
    {
        var startsWith = QueryHarnessExtensions.Nodes<Person>().Where(p => p.Name.StartsWith(hostile)).ToCypherQuery();
        var contains = QueryHarnessExtensions.Nodes<Person>().Where(p => p.Name.Contains(hostile)).ToCypherQuery();
        var endsWith = QueryHarnessExtensions.Nodes<Person>().Where(p => p.Name.EndsWith(hostile)).ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.name STARTS WITH $p0 RETURN n0", startsWith.Cypher);
        Assert.Equal("MATCH (n0:Person) WHERE n0.name CONTAINS $p0 RETURN n0", contains.Cypher);
        Assert.Equal("MATCH (n0:Person) WHERE n0.name ENDS WITH $p0 RETURN n0", endsWith.Cypher);

        Assert.Equal(hostile, Assert.Single(startsWith.Parameters).Value);
        Assert.Equal(hostile, Assert.Single(contains.Parameters).Value);
        Assert.Equal(hostile, Assert.Single(endsWith.Parameters).Value);
    }

    [Fact]
    public void Collection_values_only_reach_the_parameter_dictionary()
    {
        var hostile = new[] { "\" OR 1=1 --", "\") RETURN 1 //" };

        var query = QueryHarnessExtensions.Nodes<Person>().Where(p => hostile.Contains(p.Name)).ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.name IN $p0 RETURN n0", query.Cypher);
        Assert.Equal(hostile, ((object[])Assert.Single(query.Parameters).Value).Cast<string>().ToArray());
    }

    [Fact]
    public void A_hostile_ordering_value_cannot_reach_the_order_by_clause()
    {
        var hostile = "\") RETURN 1 //";

        var query = QueryHarnessExtensions.Nodes<Person>()
            .OrderBy(p => p.Name == hostile)
            .ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) RETURN n0 ORDER BY n0.name = $p0 ASC", query.Cypher);
        Assert.Equal(hostile, Assert.Single(query.Parameters).Value);
    }

    [Fact]
    public void A_hostile_projection_value_cannot_reach_the_return_clause()
    {
        var hostile = "\") RETURN 1 //";

        var query = QueryHarnessExtensions.Nodes<Person>()
            .Select(p => new { Flag = p.Name == hostile })
            .ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) RETURN n0.name = $p0 AS Flag", query.Cypher);
        Assert.Equal(hostile, Assert.Single(query.Parameters).Value);
    }

    [Fact]
    public void A_value_that_looks_like_a_placeholder_is_still_bound_as_a_value()
    {
        var hostile = "$p0";

        var query = QueryHarnessExtensions.Nodes<Person>().Where(p => p.Name == hostile).ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.name = $p0 RETURN n0", query.Cypher);
        Assert.Equal("$p0", query.Parameters["p0"]);
    }

    [Fact]
    public void Every_generated_query_fragment_is_provider_owned()
    {
        var hostile = "\" OR 1=1 --";
        var hostileNumber = long.MinValue;

        var query = QueryHarnessExtensions.Nodes<Person>()
            .Where(p => p.Name == hostile && p.Age > 1 && p.Nickname.StartsWith(hostile))
            .OrderBy(p => p.Name)
            .Skip(1)
            .Take(2)
            .ToCypherQuery();

        // Only identifiers the provider itself generated may appear in the text.
        Assert.Equal(
            "MATCH (n0:Person) WHERE n0.name = $p0 AND n0.age > $p1 AND n0.nickname STARTS WITH $p2 " +
            "RETURN n0 ORDER BY n0.name ASC SKIP $p3 LIMIT $p4",
            query.Cypher);

        Assert.Equal(5, query.Parameters.Count);
        Assert.Equal(hostile, query.Parameters["p0"]);
        Assert.Equal(1L, query.Parameters["p1"]);
        Assert.Equal(hostile, query.Parameters["p2"]);
        Assert.NotEqual(hostileNumber, query.Parameters["p1"]);

        // Paging counts are bound too, so no caller-supplied number reaches the text either.
        Assert.Equal(1L, query.Parameters["p3"]);
        Assert.Equal(2L, query.Parameters["p4"]);
    }
}
