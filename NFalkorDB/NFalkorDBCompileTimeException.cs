using System;

namespace NFalkorDB;

/// <summary>
/// FalkorDB compile time exception.
/// 
/// The intent here would be to throw the exception when there is an exception during the evaluation of a Cypher
/// query against FalkorDB, but I didn't see a way to discriminate between the exceptions that are throw by
/// StackExchange.Redis. So for now this isn't used.
/// </summary>
[Serializable]
public class NFalkorDBCompileTimeException : Exception
{
    /// <summary>
    /// Create an instance using an error message.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <returns></returns>
    public NFalkorDBCompileTimeException(string message) : base(message) { }

    /// <summary>
    /// Create an instance using an error message and an inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="inner">The inner exception.</param>
    /// <returns></returns>
    public NFalkorDBCompileTimeException(string message, Exception inner) : base(message, inner) { }
}