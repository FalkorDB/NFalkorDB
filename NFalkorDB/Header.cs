using System;
using System.Collections.Generic;
using StackExchange.Redis;

namespace NFalkorDB
{
    /// <summary>
    /// Query response header interface. Represents the response schema (column names and types).
    /// </summary>
    public sealed class Header
    {
        /// <summary>
        /// The expected column types.
        /// </summary>
        public enum ResultSetColumnTypes
        {
            /// <summary>
            /// Who can say?
            /// </summary>
            COLUMN_UNKNOWN,

            /// <summary>
            /// A single value.
            /// </summary>
            COLUMN_SCALAR,

            /// <summary>
            /// Refers to an actual node.
            /// </summary>
            COLUMN_NODE,

            /// <summary>
            /// Refers to a relation.
            /// </summary>            
            COLUMN_RELATION
        }

        /// <summary>
        /// Collection of the schema names present in the header.
        /// </summary>
        /// <value></value>
        public List<string> SchemaNames { get; }

        internal Header(RedisResult result)
        {
            SchemaNames = [];

            foreach (RedisResult[] tuple in (RedisResult[])result)
            {
                SchemaNames.Add((string)tuple[1]);
            }
        }

        public override bool Equals(object obj)
        {
            if (this == obj)
            {
                return true;
            }

            var header = obj as Header;

            if (header is null)
            {
                return false;
            }

            return Objects.AreEqual(SchemaNames, header.SchemaNames);
        }

        public override string ToString() =>
            $"Header{{schemaNames=[{string.Join(", ", SchemaNames)}]}}";

        public override int GetHashCode()
        {
            var hash = new HashCode();

            foreach (var name in SchemaNames)
            {
                hash.Add(name);
            }

            return hash.ToHashCode();
        }
    }
}