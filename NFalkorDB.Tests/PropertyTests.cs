using System;
using System.Collections.Generic;
using Xunit;

namespace NFalkorDB.Tests;

public class PropertyTests
{
    [Fact]
    public void HashCodeIsDeterministic()
    {
        var propertyA = new Property();
        propertyA.Name = "Hello";
        propertyA.Value = "World";

        var propertyB = new Property();
        propertyB.Name = "Hello";
        propertyB.Value = "World";

        Assert.Equal(propertyA.GetHashCode(), propertyB.GetHashCode());
    }

    [Fact]
    public void HashCodeIsDeterministicWithEnumerableValue()
    {
        var propertyA = new Property();
        propertyA.Name = "Collection";
        propertyA.Value = new[] { 1, 2, 3 };

        var propertyB = new Property();
        propertyB.Name = "Collection";
        propertyB.Value = new[] { 1, 2, 3 };

        Assert.Equal(propertyA.GetHashCode(), propertyB.GetHashCode());
    }

    [Theory]
    [MemberData(nameof(EquivalentValues))]
    public void PropertiesWithEquivalentValuesAreEqualAndHashAlike(object value1, object value2)
    {
        var propertyA = new Property("value", value1);
        var propertyB = new Property("value", value2);

        Assert.Equal(propertyA, propertyB);
        Assert.Equal(propertyA.GetHashCode(), propertyB.GetHashCode());
    }

    public static TheoryData<object, object> EquivalentValues() => new()
    {
        { null, null },
        { new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc) },
        { TimeSpan.FromMilliseconds(1234), TimeSpan.FromMilliseconds(1234) },
        { new Point(1.5, 2.5), new Point(1.5, 2.5) },
        { new[] { 1, 2, 3 }, new[] { 1, 2, 3 } },
        { new[] { 1L, 2L, 3L }, new[] { 1L, 2L, 3L } },
        { new object[] { "a", 1L, null }, new object[] { "a", 1L, null } },
        {
            new Dictionary<string, object> { { "a", 1L }, { "b", "two" } },
            new Dictionary<string, object> { { "b", "two" }, { "a", 1L } }
        },
        {
            new object[] { new[] { 1, 2 }, new Dictionary<string, object> { { "a", 1L } } },
            new object[] { new[] { 1, 2 }, new Dictionary<string, object> { { "a", 1L } } }
        }
    };

    [Theory]
    [MemberData(nameof(DifferingValues))]
    public void PropertiesWithDifferingValuesAreNotEqual(object value1, object value2)
    {
        var propertyA = new Property("value", value1);
        var propertyB = new Property("value", value2);

        Assert.NotEqual(propertyA, propertyB);
    }

    public static TheoryData<object, object> DifferingValues() => new()
    {
        { null, "value" },
        { new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), new DateTime(2024, 1, 2, 3, 4, 6, DateTimeKind.Utc) },
        { TimeSpan.FromMilliseconds(1234), TimeSpan.FromMilliseconds(4321) },
        { new Point(1.5, 2.5), new Point(2.5, 1.5) },
        { new[] { 1, 2, 3 }, new[] { 1, 2 } },
        { new[] { 1, 2, 3 }, new[] { 1, 2, 4 } },
        {
            new Dictionary<string, object> { { "a", 1L } },
            new Dictionary<string, object> { { "a", 2L } }
        },
        {
            new Dictionary<string, object> { { "a", 1L } },
            new Dictionary<string, object> { { "b", 1L } }
        }
    };

    [Fact]
    public void HashCodeToleratesNullNameAndValue()
    {
        var property = new Property();

        var exception = Xunit.Record.Exception(() => property.GetHashCode());

        Assert.Null(exception);
    }
}