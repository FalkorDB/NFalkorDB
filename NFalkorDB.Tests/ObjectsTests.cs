using System.Collections.Generic;
using Xunit;

namespace NFalkorDB.Tests
{
    public class ObjectsTests
    {
        [Theory]
        [MemberData(nameof(BoxedData))]
        public void CanCompareBoxedTypes(object obj1, object obj2)
        {
            Assert.True(Objects.AreEqual(obj1, obj2));
        }

        [Theory]
        [MemberData(nameof(BadBoxedData))]
        public void CanCompareNonEqualBoxedTypes(object obj1, object obj2)
        {
            Assert.False(Objects.AreEqual(obj1, obj2));
        }

        public static IEnumerable<object[]> BoxedData => new object[][]
        {
            [(byte)25, (byte)25],
            [(sbyte)100, (sbyte)100],
            [(short)30_000, (short)30_000],
            [(ushort)60_000, (ushort)60_000],
            [(int)75_000, (int)75_000],
            [850_000u, 850_000u],        
            [1_000_000L, 1_000_000L],
            [50_000_000UL, 50_000_000UL],
            [34_000f, 34_000f],
            [56_000d, 56_000d],
            [89_000m, 89_000m],
            ['t', 't'],
            [true, true],
            ["tom", "tom"],
            [null, null]
        };

        public static IEnumerable<object[]> BadBoxedData => new object[][]
        {
            [(byte)25, (byte)26],
            [(sbyte)100, (sbyte)101],
            [(short)30_000, (short)30_001],
            [(ushort)60_000, (ushort)60_001],
            [(int)75_000, (int)75_001],
            [850_000u, 850_001u],        
            [1_000_000L, 1_000_001L],
            [50_000_000UL, 50_000_001UL],
            [34_000f, 34_001f],
            [56_000d, 56_001d],
            [89_000m, 89_001m],
            ['t', 'u'],
            [true, false],
            ["tom", "hanks"],
            ["tom", null],
            ["1", 1]
        };        
    }
}