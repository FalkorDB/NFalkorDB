using System;
using System.Collections.Generic;
using System.Linq;
using NFalkorDB.Linq.Materialization;
using NFalkorDB.Linq.Tests.Model;
using Xunit;

namespace NFalkorDB.Linq.Tests;

/// <summary>
/// FalkorDB widens every integer to <see cref="long"/> and every real to <see cref="double"/>, so
/// materialization has to narrow values back to whatever the query asked for.
/// </summary>
public class ValueConversionTests
{
    private static object Convert(object value, Type target) => ValueConverter.Convert(value, target, "Person.test");

    [Fact]
    public void Longs_narrow_to_the_requested_integer_type()
    {
        Assert.Equal(7, Convert(7L, typeof(int)));
        Assert.Equal((short)7, Convert(7L, typeof(short)));
        Assert.Equal((byte)7, Convert(7L, typeof(byte)));
        Assert.Equal(7L, Convert(7L, typeof(long)));
    }

    [Fact]
    public void Doubles_convert_to_the_requested_real_type()
    {
        Assert.Equal(1.5f, Convert(1.5d, typeof(float)));
        Assert.Equal(1.5m, Convert(1.5d, typeof(decimal)));
        Assert.Equal(1.5d, Convert(1.5d, typeof(double)));
    }

    [Fact]
    public void An_integer_widens_into_a_real_property()
    {
        Assert.Equal(7d, Convert(7L, typeof(double)));
    }

    [Fact]
    public void Nulls_become_the_clr_default_for_a_value_type()
    {
        Assert.Equal(0, Convert(null, typeof(int)));
        Assert.Null(Convert(null, typeof(int?)));
        Assert.Null(Convert(null, typeof(string)));
    }

    [Fact]
    public void Nullable_targets_unwrap_to_their_underlying_type()
    {
        Assert.Equal(7, Convert(7L, typeof(int?)));
    }

    [Fact]
    public void Enums_accept_both_names_and_numbers()
    {
        Assert.Equal(Rating.Great, Convert("Great", typeof(Rating)));
        Assert.Equal(Rating.Great, Convert("great", typeof(Rating)));
        Assert.Equal(Rating.Great, Convert(2L, typeof(Rating)));
    }

    [Fact]
    public void An_unknown_enum_name_is_reported_clearly()
    {
        var exception = Assert.Throws<GraphMappingException>(() => Convert("Nope", typeof(Rating)));

        Assert.Contains("Nope", exception.Message);
        Assert.Contains(nameof(Rating), exception.Message);
        Assert.Contains("Person.test", exception.Message);
    }

    [Fact]
    public void Dates_are_accepted_as_values_strings_and_epoch_milliseconds()
    {
        var expected = new DateTime(2020, 5, 17, 13, 45, 0, DateTimeKind.Utc);

        Assert.Equal(expected, Convert(expected, typeof(DateTime)));
        Assert.Equal(expected, Convert("2020-05-17T13:45:00.0000000Z", typeof(DateTime)));
        Assert.Equal(
            expected,
            Convert(new DateTimeOffset(expected).ToUnixTimeMilliseconds(), typeof(DateTime)));
    }

    [Fact]
    public void Durations_are_accepted_as_values_and_milliseconds()
    {
        Assert.Equal(TimeSpan.FromMinutes(1.5), Convert(TimeSpan.FromMinutes(1.5), typeof(TimeSpan)));
        Assert.Equal(TimeSpan.FromMinutes(1.5), Convert(90000L, typeof(TimeSpan)));
    }

    [Fact]
    public void Arrays_and_lists_are_converted_element_by_element()
    {
        var array = (int[])Convert(new object[] { 1L, 2L }, typeof(int[]));
        var list = (List<string>)Convert(new object[] { "a", "b" }, typeof(List<string>));

        Assert.Equal(new[] { 1, 2 }, array);
        Assert.Equal(new[] { "a", "b" }, list);
    }

    [Fact]
    public void Maps_are_converted_value_by_value()
    {
        var map = (Dictionary<string, int>)Convert(
            new Dictionary<string, object> { ["a"] = 1L, ["b"] = 2L },
            typeof(Dictionary<string, int>));

        Assert.Equal(1, map["a"]);
        Assert.Equal(2, map["b"]);
    }

    [Fact]
    public void A_point_passes_through_unchanged()
    {
        var point = new Point(1.5, 2.5);

        Assert.Same(point, Convert(point, typeof(Point)));
    }

    [Fact]
    public void Object_targets_pass_the_raw_value_through()
    {
        Assert.Equal(7L, Convert(7L, typeof(object)));
    }

    [Fact]
    public void A_type_mismatch_names_the_value_the_target_and_the_column()
    {
        var exception = Assert.Throws<GraphMappingException>(() => Convert("not a number", typeof(int)));

        Assert.Contains("String", exception.Message);
        Assert.Contains("Int32", exception.Message);
        Assert.Contains("Person.test", exception.Message);
    }

    [Fact]
    public void A_boolean_is_not_silently_coerced_to_a_number()
    {
        Assert.Throws<GraphMappingException>(() => Convert(true, typeof(int)));
    }

    [Fact]
    public void An_overflowing_value_is_reported_clearly()
    {
        Assert.Throws<GraphMappingException>(() => Convert(long.MaxValue, typeof(byte)));
    }
}
