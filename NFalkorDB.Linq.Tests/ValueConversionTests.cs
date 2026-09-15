using System;
using System.Collections.Generic;
using System.Globalization;
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

    [Fact]
    public void A_set_interface_is_materialized_as_a_hash_set()
    {
        // ISet<T> is accepted as a storable property type, so it has to be materializable: the
        // interface has no constructor and List<T> does not implement it.
        var converted = Convert(new object[] { "a", "b", "a" }, typeof(ISet<string>));

        var set = Assert.IsType<HashSet<string>>(converted);

        Assert.Equal(new[] { "a", "b" }, set.OrderBy(v => v).ToArray());
    }

    [Fact]
    public void A_concrete_hash_set_property_is_still_materialized()
    {
        var converted = Convert(new object[] { 1L, 2L }, typeof(HashSet<int>));

        Assert.Equal(new[] { 1, 2 }, Assert.IsType<HashSet<int>>(converted).OrderBy(v => v).ToArray());
    }

    [Fact]
    public void A_date_time_offset_keeps_the_offset_it_was_written_with()
    {
        // The binder writes these with the round-trip "o" format, which carries the offset. Parsing
        // as DateTime first reinterpreted the instant in local time and then threw when it was
        // paired with a zero offset.
        var source = new DateTimeOffset(2024, 1, 15, 10, 30, 0, TimeSpan.FromHours(2));

        var converted = (DateTimeOffset)Convert(
            source.ToString("o", CultureInfo.InvariantCulture), typeof(DateTimeOffset));

        Assert.Equal(source.Offset, converted.Offset);
        Assert.Equal(source.UtcDateTime, converted.UtcDateTime);
    }

    [Fact]
    public void A_date_time_offset_reads_an_unqualified_value_as_utc()
    {
        // FalkorDB has no offset-aware temporal type, so the result must not depend on the time
        // zone of the machine doing the reading.
        var converted = (DateTimeOffset)Convert(new DateTime(2024, 1, 15, 8, 30, 0), typeof(DateTimeOffset));

        Assert.Equal(TimeSpan.Zero, converted.Offset);
        Assert.Equal(new DateTime(2024, 1, 15, 8, 30, 0, DateTimeKind.Utc), converted.UtcDateTime);
    }

    [Fact]
    public void An_enum_is_read_from_a_member_name_or_an_integral_value()
    {
        Assert.Equal(Rating.Great, Convert("Great", typeof(Rating)));
        Assert.Equal(Rating.Great, Convert(2L, typeof(Rating)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(2.5d)]
    [InlineData(1.0f)]
    public void A_non_integral_value_is_not_silently_turned_into_an_enum_member(object value)
    {
        // Convert.ToInt64 would turn `true` into the member with value 1 and round 2.5 down to the
        // member with value 2, hiding a real type mismatch.
        Assert.Throws<GraphMappingException>(() => Convert(value, typeof(Rating)));
    }
}
