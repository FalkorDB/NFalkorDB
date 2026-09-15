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
}
