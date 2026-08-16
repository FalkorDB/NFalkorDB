using Xunit;

namespace NFalkorDB.Tests;

public class EdgeTests
{
    [Fact]
    public void HashCodeIsDeterministic()
    {
        var edgeA = new Edge();
        edgeA.Id = 100;
        edgeA.RelationshipType = "R1";
        edgeA.Source = 1;
        edgeA.Destination = 2;
        edgeA.AddProperty("Hello", "World");

        var edgeB = new Edge();
        edgeB.Id = 100;
        edgeB.RelationshipType = "R1";
        edgeB.Source = 1;
        edgeB.Destination = 2;
        edgeB.AddProperty("Hello", "World");

        Assert.Equal(edgeA.GetHashCode(), edgeB.GetHashCode());
    }

    [Fact]
    public void EqualityAndHashCodeIgnorePropertyInsertionOrder()
    {
        var edgeA = new Edge();
        edgeA.Id = 100;
        edgeA.RelationshipType = "R1";
        edgeA.Source = 1;
        edgeA.Destination = 2;
        edgeA.AddProperty("Hello", "World");
        edgeA.AddProperty("Goodbye", "Moon");

        var edgeB = new Edge();
        edgeB.Id = 100;
        edgeB.RelationshipType = "R1";
        edgeB.Source = 1;
        edgeB.Destination = 2;
        edgeB.AddProperty("Goodbye", "Moon");
        edgeB.AddProperty("Hello", "World");

        Assert.Equal(edgeA, edgeB);
        Assert.Equal(edgeA.GetHashCode(), edgeB.GetHashCode());
    }

    [Fact]
    public void HashCodeToleratesNullRelationshipType()
    {
        var edge = new Edge();
        edge.Id = 100;
        edge.Source = 1;
        edge.Destination = 2;

        var exception = Xunit.Record.Exception(() => edge.GetHashCode());

        Assert.Null(exception);
    }

    [Fact]
    public void ToStringToleratesNullPropertyEntries()
    {
        var edge = new Edge();
        edge.Id = 100;
        edge.RelationshipType = "R1";
        edge.PropertyMap["missing"] = null;

        var exception = Xunit.Record.Exception(() => edge.ToString());

        Assert.Null(exception);
    }
}        