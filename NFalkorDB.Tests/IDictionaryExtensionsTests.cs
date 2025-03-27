using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace NFalkorDB.Tests;

public class IDictionaryExtensionsTests
{
    public class SequenceEqualCan
    {
        [Fact]
        public void DetermineIfTwoDictionariesAreEqual()
        {
            var testDict1 = new Dictionary<string, string>
            {
                { "hello", "world" },
                { "goodnight", "moon" }
            };

            var testDict2 = new Dictionary<string, string>
            {
                { "hello", "world" },
                { "goodnight", "moon" }
            };    

            Assert.True(testDict1.SequenceEqual(testDict2));            
        }

        [Fact]
        public void DetermineIfTwoDictionariesArentEqual()
        {
            var testDict1 = new Dictionary<string, string>
            {
                { "hello", "world" },
                { "goodnight", "moon" }
            };

            var testDict2 = new Dictionary<string, string>
            {
                { "hello", "moon" },
                { "goodnight", "world" }
            };    

            Assert.False(testDict1.SequenceEqual(testDict2)); 
        }
    }
}