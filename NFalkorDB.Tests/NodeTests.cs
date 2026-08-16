using System.Collections.Generic;
using Xunit;

namespace NFalkorDB.Tests;

public class NodeTests
{
    [Fact]
    public void HashCodeIsDeterministic()
    {
        var nodeA = new Node();
        nodeA.Id = 100;
        nodeA.AddLabel("L1");
        nodeA.AddProperty("Hello", "World");
        nodeA.AddProperty(new Property("array", new object[] { 1, 2, 3 }));

        var nodeB = new Node();
        nodeB.Id = 100;
        nodeB.AddLabel("L1");
        nodeB.AddProperty("Hello", "World");
        nodeB.AddProperty(new Property("array", new object[] { 1, 2, 3 }));

        Assert.Equal(nodeA.GetHashCode(), nodeB.GetHashCode());
    }

    [Fact]
    public void EqualityAndHashCodeIgnorePropertyInsertionOrder()
    {
        var nodeA = new Node();
        nodeA.Id = 100;
        nodeA.AddLabel("L1");
        nodeA.AddProperty("Hello", "World");
        nodeA.AddProperty(new Property("array", new object[] { 1, 2, 3 }));

        var nodeB = new Node();
        nodeB.Id = 100;
        nodeB.AddLabel("L1");
        nodeB.AddProperty(new Property("array", new object[] { 1, 2, 3 }));
        nodeB.AddProperty("Hello", "World");

        Assert.Equal(nodeA, nodeB);
        Assert.Equal(nodeA.GetHashCode(), nodeB.GetHashCode());
    }

    [Fact]
    public void EqualityAndHashCodeIgnoreLabelOrder()
    {
        var nodeA = new Node();
        nodeA.Id = 100;
        nodeA.AddLabel("L1");
        nodeA.AddLabel("L2");
        nodeA.AddProperty("Hello", "World");

        var nodeB = new Node();
        nodeB.Id = 100;
        nodeB.AddLabel("L2");
        nodeB.AddLabel("L1");
        nodeB.AddProperty("Hello", "World");

        Assert.Equal(nodeA, nodeB);
        Assert.Equal(nodeA.GetHashCode(), nodeB.GetHashCode());
    }

    [Fact]
    public void NodesWithDifferentLabelsAreNotEqual()
    {
        var nodeA = new Node();
        nodeA.Id = 100;
        nodeA.AddLabel("L1");
        nodeA.AddLabel("L1");

        var nodeB = new Node();
        nodeB.Id = 100;
        nodeB.AddLabel("L1");
        nodeB.AddLabel("L2");

        Assert.NotEqual(nodeA, nodeB);
    }

    [Fact]
    public void EqualityAndHashCodeToleratesNullPropertyValues()
    {
        var nodeA = new Node();
        nodeA.Id = 100;
        nodeA.AddLabel("L1");
        nodeA.AddProperty("nullValue", null);

        var nodeB = new Node();
        nodeB.Id = 100;
        nodeB.AddLabel("L1");
        nodeB.AddProperty("nullValue", null);

        Assert.Equal(nodeA, nodeB);
        Assert.Equal(nodeA.GetHashCode(), nodeB.GetHashCode());
    }

    [Fact]
    public void HashCodeToleratesNullPropertyEntries()
    {
        var node = new Node();
        node.Id = 100;
        node.PropertyMap["missing"] = null;

        var exception = Xunit.Record.Exception(() => node.GetHashCode());

        Assert.Null(exception);
    }

    [Fact]
    public void EqualityAndHashCodeIgnoreEntryOrderOfMapValuedProperties()
    {
        var nodeA = new Node();
        nodeA.Id = 100;
        nodeA.AddLabel("L1");
        nodeA.AddProperty("map", new Dictionary<string, object> { { "a", 1L }, { "b", 2L } });

        var nodeB = new Node();
        nodeB.Id = 100;
        nodeB.AddLabel("L1");
        nodeB.AddProperty("map", new Dictionary<string, object> { { "b", 2L }, { "a", 1L } });

        Assert.Equal(nodeA, nodeB);
        Assert.Equal(nodeA.GetHashCode(), nodeB.GetHashCode());
    }
}