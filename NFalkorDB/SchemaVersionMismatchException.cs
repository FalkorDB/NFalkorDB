using System;

namespace NFalkorDB;

/// <summary>
/// Indicates that the client's cached schema version is out of date with the server.
/// </summary>
[Serializable]
public sealed class SchemaVersionMismatchException : Exception
{
    public SchemaVersionMismatchException(string message) : base(message)
    {
    }

    public SchemaVersionMismatchException(string message, Exception inner) : base(message, inner)
    {
    }
}