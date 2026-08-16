using System.Collections.Generic;
using Xunit;

namespace NFalkorDB.Tests;

public class RecordTests
{
    [Fact]
    public void EqualRecordsProduceEqualHashCodes()
    {
        var recordA = new Record(
            new List<string> { "a", "b", "c" },
            new List<object> { "value", 1L, null });

        var recordB = new Record(
            new List<string> { "a", "b", "c" },
            new List<object> { "value", 1L, null });

        Assert.Equal(recordA, recordB);
        Assert.Equal(recordA.GetHashCode(), recordB.GetHashCode());
    }

    [Fact]
    public void RecordsAreUsableAsDictionaryKeys()
    {
        var recordA = new Record(new List<string> { "a" }, new List<object> { "value" });
        var recordB = new Record(new List<string> { "a" }, new List<object> { "value" });

        var set = new HashSet<Record> { recordA };

        Assert.Contains(recordB, set);
    }

    [Fact]
    public void EqualityUsesValueSemanticsForCollectionValues()
    {
        var recordA = new Record(new List<string> { "a" }, new List<object> { new[] { 1, 2, 3 } });
        var recordB = new Record(new List<string> { "a" }, new List<object> { new[] { 1, 2, 3 } });

        Assert.Equal(recordA, recordB);
        Assert.Equal(recordA.GetHashCode(), recordB.GetHashCode());
    }

    [Fact]
    public void EqualityUsesGraphEntitySemanticsForEntityValues()
    {
        var nodeA = new Node { Id = 1 };
        nodeA.AddLabel("L1");
        nodeA.AddProperty("Hello", "World");
        nodeA.AddProperty("Goodbye", "Moon");

        var nodeB = new Node { Id = 1 };
        nodeB.AddLabel("L1");
        nodeB.AddProperty("Goodbye", "Moon");
        nodeB.AddProperty("Hello", "World");

        var recordA = new Record(new List<string> { "n" }, new List<object> { nodeA });
        var recordB = new Record(new List<string> { "n" }, new List<object> { nodeB });

        Assert.Equal(recordA, recordB);
        Assert.Equal(recordA.GetHashCode(), recordB.GetHashCode());
    }

    [Fact]
    public void RecordsWithDifferentValuesAreNotEqual()
    {
        var recordA = new Record(new List<string> { "a" }, new List<object> { "value" });
        var recordB = new Record(new List<string> { "a" }, new List<object> { "other" });

        Assert.NotEqual(recordA, recordB);
    }

    [Fact]
    public void RecordsWithDifferentKeysAreNotEqual()
    {
        var recordA = new Record(new List<string> { "a" }, new List<object> { "value" });
        var recordB = new Record(new List<string> { "b" }, new List<object> { "value" });

        Assert.NotEqual(recordA, recordB);
    }

    [Fact]
    public void KeyOrderIsSignificant()
    {
        var recordA = new Record(new List<string> { "a", "b" }, new List<object> { 1L, 2L });
        var recordB = new Record(new List<string> { "b", "a" }, new List<object> { 1L, 2L });

        Assert.NotEqual(recordA, recordB);
    }

    [Fact]
    public void EqualityAndHashCodeTreatNaNValuesAsEqual()
    {
        var recordA = new Record(new List<string> { "a" }, new List<object> { double.NaN });
        var recordB = new Record(new List<string> { "a" }, new List<object> { double.NaN });

        Assert.Equal(recordA, recordB);
        Assert.Equal(recordA.GetHashCode(), recordB.GetHashCode());
        Assert.Contains(recordB, new HashSet<Record> { recordA });
    }
}
