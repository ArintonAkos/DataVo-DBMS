using DataVo.Core.Contracts.Results;
namespace DataVo.Core.Contracts;

/// <summary>
/// Defines an executable database action produced by the parser/evaluator pipeline.
/// </summary>
internal interface IDbAction
{
    /// <summary>
    /// Executes the action for the supplied session.
    /// </summary>
    /// <param name="session">The session whose active database and transaction state should be used.</param>
    /// <returns>A standardized <see cref="QueryResult"/> describing the action outcome.</returns>
    public QueryResult Perform(Guid session);
}