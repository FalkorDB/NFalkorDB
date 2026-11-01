using System;

namespace NFalkorDB.Linq;

/// <summary>
/// Thrown when a CLR type cannot be mapped to a graph node or relationship, or when a value
/// returned by FalkorDB cannot be converted into the CLR type the query asked for.
/// </summary>
public class GraphMappingException : Exception
{
    /// <summary>
    /// Creates a new <see cref="GraphMappingException"/>.
    /// </summary>
    /// <param name="message">A description of the mapping problem.</param>
    public GraphMappingException(string message) : base(message)
    {
    }

    /// <summary>
    /// Creates a new <see cref="GraphMappingException"/>.
    /// </summary>
    /// <param name="message">A description of the mapping problem.</param>
    /// <param name="innerException">The underlying failure.</param>
    public GraphMappingException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
