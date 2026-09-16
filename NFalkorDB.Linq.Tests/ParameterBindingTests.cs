using System;
using System.Collections.Generic;
using System.Linq;
using NFalkorDB.Linq.Tests.Model;
using NFalkorDB.Linq.Translation;
using Xunit;

namespace NFalkorDB.Linq.Tests;

/// <summary>
/// The parameter bag is the boundary every caller supplied value crosses. It has to hand the
/// driver a value <c>FalkorDBUtilities.ValueToString</c> can render safely, because that method
/// falls back to an unquoted <c>ToString()</c> for anything it does not recognise.
/// </summary>
public class ParameterBindingTests
{
    [Fact]
    public void Placeholders_are_allocated_in_order()
    {
        var bag = new ParameterBag();

        Assert.Equal("$p0", bag.Add(1));
        Assert.Equal("$p1", bag.Add(2));
        Assert.Equal("$p2", bag.Add(3));

        Assert.Equal(new[] { "p0", "p1", "p2" }, bag.Values.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());
    }

    [Theory]
    [InlineData((byte)7)]
    [InlineData((sbyte)7)]
    [InlineData((short)7)]
    [InlineData((ushort)7)]
    [InlineData(7)]
    [InlineData(7u)]
    public void Integral_values_widen_to_long(object value)
    {
        Assert.Equal(7L, ParameterBag.Normalize(value));
    }

    [Fact]
    public void Real_values_widen_to_double()
    {
        Assert.Equal(1.5d, ParameterBag.Normalize(1.5f));
        Assert.Equal(1.5d, ParameterBag.Normalize(1.5d));
        Assert.Equal(1.5d, ParameterBag.Normalize(1.5m));
    }

    [Fact]
    public void Strings_and_booleans_pass_through()
    {
        Assert.Equal("x", ParameterBag.Normalize("x"));
        Assert.Equal(true, ParameterBag.Normalize(true));
    }

    [Fact]
    public void Null_passes_through()
    {
        Assert.Null(ParameterBag.Normalize(null));
    }

    [Fact]
    public void Chars_become_strings()
    {
        Assert.Equal("a", ParameterBag.Normalize('a'));
    }

    [Fact]
    public void Enums_are_bound_by_name()
    {
        Assert.Equal("Great", ParameterBag.Normalize(Rating.Great));
    }

    [Fact]
    public void Dates_are_bound_as_round_trip_strings()
    {
        var value = new DateTime(2020, 5, 17, 13, 45, 0, DateTimeKind.Utc);

        Assert.Equal("2020-05-17T13:45:00.0000000Z", ParameterBag.Normalize(value));
        Assert.Equal(
            "2020-05-17T13:45:00.0000000+00:00",
            ParameterBag.Normalize(new DateTimeOffset(value)));
    }

    [Fact]
    public void Time_spans_are_bound_as_milliseconds()
    {
        Assert.Equal(90000L, ParameterBag.Normalize(TimeSpan.FromMinutes(1.5)));
    }

    [Fact]
    public void Guids_are_bound_as_strings()
    {
        var value = Guid.Parse("11111111-2222-3333-4444-555555555555");

        Assert.Equal("11111111-2222-3333-4444-555555555555", ParameterBag.Normalize(value));
    }

    [Fact]
    public void Collections_are_normalized_element_by_element()
    {
        var value = (object[])ParameterBag.Normalize(new[] { 1, 2, 3 });

        Assert.Equal(new object[] { 1L, 2L, 3L }, value);
    }

    [Fact]
    public void Maps_are_normalized_value_by_value()
    {
        var value = (Dictionary<string, object>)ParameterBag.Normalize(
            new Dictionary<string, object> { ["a"] = 1, ["b"] = 2.5f });

        Assert.Equal(1L, value["a"]);
        Assert.Equal(2.5d, value["b"]);
    }

    [Fact]
    public void A_point_cannot_be_bound()
    {
        var exception = Assert.Throws<NotSupportedException>(() => ParameterBag.Normalize(new Point(1.0, 2.0)));

        Assert.Contains("Point", exception.Message);
    }

    [Fact]
    public void An_unmappable_type_cannot_be_bound()
    {
        var exception = Assert.Throws<NotSupportedException>(() => ParameterBag.Normalize(new object()));

        Assert.Contains("cannot be used as a FalkorDB query parameter", exception.Message);
    }

    private class Filter
    {
        public int MinimumAge { get; set; }

        public string Name { get; set; }

        public Filter Nested { get; set; }

        public int Throws => throw new InvalidOperationException("boom");
    }

    [Fact]
    public void A_captured_local_is_evaluated_locally()
    {
        var minimumAge = 21;

        var query = QueryHarnessExtensions.Nodes<Person>().Where(p => p.Age > minimumAge).ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.age > $p0 RETURN n0", query.Cypher);
        Assert.Equal(21L, query.Parameters["p0"]);
    }

    [Fact]
    public void A_property_on_a_captured_object_is_evaluated_locally()
    {
        var filter = new Filter { MinimumAge = 30, Name = "Alice" };

        var query = QueryHarnessExtensions.Nodes<Person>()
            .Where(p => p.Age > filter.MinimumAge && p.Name == filter.Name)
            .ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.age > $p0 AND n0.name = $p1 RETURN n0", query.Cypher);
        Assert.Equal(30L, query.Parameters["p0"]);
        Assert.Equal("Alice", query.Parameters["p1"]);
    }

    [Fact]
    public void A_nested_property_chain_is_evaluated_locally()
    {
        var filter = new Filter { Nested = new Filter { MinimumAge = 40 } };

        var query = QueryHarnessExtensions.Nodes<Person>()
            .Where(p => p.Age > filter.Nested.MinimumAge)
            .ToCypherQuery();

        Assert.Equal(40L, query.Parameters["p0"]);
    }

    [Fact]
    public void A_method_call_on_a_captured_value_falls_back_to_the_compiled_path()
    {
        var filter = new Filter { Name = "  alice  " };

        var query = QueryHarnessExtensions.Nodes<Person>()
            .Where(p => p.Name == filter.Name.Trim().ToUpperInvariant())
            .ToCypherQuery();

        Assert.Equal("MATCH (n0:Person) WHERE n0.name = $p0 RETURN n0", query.Cypher);
        Assert.Equal("ALICE", query.Parameters["p0"]);
    }

    [Fact]
    public void An_exception_from_a_captured_property_surfaces_unwrapped()
    {
        var filter = new Filter();

        var exception = Assert.Throws<InvalidOperationException>(
            () => QueryHarnessExtensions.Nodes<Person>().Where(p => p.Age > filter.Throws).ToCypherQuery());

        Assert.Equal("boom", exception.Message);
    }

    [Fact]
    public void A_ulong_that_does_not_fit_a_signed_integer_is_rejected()
    {
        // FalkorDB integers are signed 64-bit and the server clamps rather than failing:
        //   CYPHER x=18446744073709551615 RETURN $x  ->  9223372036854775807
        // Binding it as a double instead only trades clamping for precision loss, so neither form
        // can carry the value and it is refused.
        var exception = Assert.Throws<NotSupportedException>(() => ParameterBag.Normalize(ulong.MaxValue));

        Assert.Contains("18446744073709551615", exception.Message);
        Assert.Contains("64-bit", exception.Message);

        // Everything that does fit still binds exactly, including the boundary.
        Assert.Equal(long.MaxValue, ParameterBag.Normalize((ulong)long.MaxValue));
        Assert.Equal(42L, ParameterBag.Normalize((ulong)42));
        Assert.Equal(0L, ParameterBag.Normalize((ulong)0));
    }
}
