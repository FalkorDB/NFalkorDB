using System.Collections.Generic;
using System.Linq;
using NFalkorDB.Linq.Translation;
using Xunit;

namespace NFalkorDB.Linq.Tests;

/// <summary>
/// Mapped names — labels, relationship types, property keys and map keys — are the only strings the
/// provider writes into the query text verbatim. They are escaped as Cypher identifiers so that a
/// name which is not a bare identifier renders correctly and cannot alter the query structure.
/// </summary>
public class IdentifierEscapingTests
{
    [Node("Odd Label", "Second Label")]
    private class AwkwardlyNamed
    {
        [Property("first-name")]
        public string FirstName { get; set; }

        [Property("plain")]
        public string Plain { get; set; }
    }

    [Node("X`) RETURN 1 //")]
    private class HostileLabel
    {
        [Property("y`) RETURN 1 //")]
        public string Value { get; set; }
    }

    [Relationship("HAS-A")]
    private class AwkwardEdge
    {
        [Property("since-when")]
        public int Since { get; set; }
    }

    [Fact]
    public void Labels_that_are_not_bare_identifiers_are_backtick_quoted()
    {
        Assert.Equal(
            "MATCH (n0:`Odd Label`:`Second Label`) RETURN n0",
            QueryHarnessExtensions.Nodes<AwkwardlyNamed>().Cypher());
    }

    [Fact]
    public void Property_keys_that_are_not_bare_identifiers_are_backtick_quoted()
    {
        var query = QueryHarnessExtensions.Nodes<AwkwardlyNamed>().Where(x => x.FirstName == "Alice");

        Assert.Equal("MATCH (n0:`Odd Label`:`Second Label`) WHERE n0.`first-name` = $p0 RETURN n0", query.Cypher());
    }

    [Fact]
    public void Bare_identifiers_are_left_alone()
    {
        var query = QueryHarnessExtensions.Nodes<AwkwardlyNamed>().Where(x => x.Plain == "Alice");

        Assert.Contains("n0.plain = $p0", query.Cypher());
    }

    [Fact]
    public void Relationship_types_that_are_not_bare_identifiers_are_backtick_quoted()
    {
        Assert.Equal(
            "MATCH ()-[r0:`HAS-A`]->() RETURN r0",
            QueryHarnessExtensions.Relationships<AwkwardEdge>().Cypher());
    }

    [Fact]
    public void A_mapped_name_cannot_break_out_of_the_pattern()
    {
        var cypher = QueryHarnessExtensions.Nodes<HostileLabel>().Where(x => x.Value == "z").Cypher();

        // The backtick in the hostile name is doubled, so the quoting cannot be terminated early.
        Assert.Equal("MATCH (n0:`X``) RETURN 1 //`) WHERE n0.`y``) RETURN 1 //` = $p0 RETURN n0", cypher);
        Assert.DoesNotContain("RETURN 1 //`)\n", cypher);
    }

    [Theory]
    [InlineData("name", "name")]
    [InlineData("_private", "_private")]
    [InlineData("a1", "a1")]
    [InlineData("first-name", "`first-name`")]
    [InlineData("1st", "`1st`")]
    [InlineData("has space", "`has space`")]
    [InlineData("", "``")]
    [InlineData("back`tick", "`back``tick`")]
    [InlineData("}) DETACH DELETE n //", "`}) DETACH DELETE n //`")]
    public void Escape_quotes_only_what_needs_quoting(string name, string expected) =>
        Assert.Equal(expected, CypherIdentifier.Escape(name));

    [Fact]
    public void Hostile_map_keys_are_escaped_before_reaching_the_formatter()
    {
        var bag = new ParameterBag();

        var name = bag.Add(new Dictionary<string, object>
        {
            ["safe"] = 1,
            ["}) DETACH DELETE n //"] = 2
        });

        var map = Assert.IsType<Dictionary<string, object>>(bag.Values[name.TrimStart('$')]);

        Assert.Contains("safe", map.Keys);
        Assert.Contains("`}) DETACH DELETE n //`", map.Keys);
        Assert.DoesNotContain("}) DETACH DELETE n //", map.Keys);
    }
}
