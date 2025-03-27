using System.ComponentModel;

namespace System.Runtime.CompilerServices
{
    [EditorBrowsable(EditorBrowsableState.Never)]
    internal class IsExternalInit{}
}

/// <summary>
/// Represents a geo point.
/// </summary>
/// <param name="X"></param>
/// <param name="Y"></param>
public record Point(double X, double Y);