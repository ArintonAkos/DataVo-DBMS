using Microsoft.EntityFrameworkCore;

namespace DataVo.EntityFrameworkCore;

/// <summary>
/// EF Core function shims for DataVo vector distance operations.
/// </summary>
/// <remarks>
/// These methods are intended for LINQ expression translation in DataVo native query translation preview.
/// They should be used inside LINQ expressions and are not intended for direct in-memory execution.
///
/// Example:
/// <code>
/// var q = new float[] { 1f, 0f, 0f };
/// var rows = ctx.QueryFromDataVo&lt;ItemEmbedding&gt;(s =&gt; s
///     .Where(x =&gt; DataVoVectorDbFunctions.CosineDistance(EF.Functions, x.Vector, q) &lt; 0.25)
///     .OrderBy(x =&gt; DataVoVectorDbFunctions.CosineDistance(EF.Functions, x.Vector, q))
///     .Take(5));
/// </code>
/// </remarks>
public static class DataVoVectorDbFunctions
{
    /// <summary>
    /// Computes cosine distance between two vectors.
    /// </summary>
    /// <param name="_">EF function scope (use <see cref="EF.Functions"/>).</param>
    /// <param name="left">Left vector expression.</param>
    /// <param name="right">Right vector expression.</param>
    /// <returns>Cosine distance scalar.</returns>
    /// <exception cref="NotSupportedException">
    /// Always thrown when executed client-side. The method exists only so DataVo can translate it to SQL.
    /// </exception>
    public static double CosineDistance(this DbFunctions _, float[] left, float[] right)
        => throw new NotSupportedException("Use inside a LINQ query. DataVo translates this method to COSINE_DISTANCE(left, right). Client-side execution is not supported.");

    /// <summary>
    /// Computes Euclidean (L2) distance between two vectors.
    /// </summary>
    /// <param name="_">EF function scope (use <see cref="EF.Functions"/>).</param>
    /// <param name="left">Left vector expression.</param>
    /// <param name="right">Right vector expression.</param>
    /// <returns>L2 distance scalar.</returns>
    /// <exception cref="NotSupportedException">
    /// Always thrown when executed client-side. The method exists only so DataVo can translate it to SQL.
    /// </exception>
    public static double L2Distance(this DbFunctions _, float[] left, float[] right)
        => throw new NotSupportedException("Use inside a LINQ query. DataVo translates this method to L2_DISTANCE(left, right). Client-side execution is not supported.");
}
