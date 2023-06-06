using Server.Contracts;
using Server.Server.MongoDB;
using Server.Server.Responses;
using Server.Server.Responses.Parts;

namespace Server.Parser.Actions;

internal abstract class BaseDbAction : IDbAction
{
    protected DbContext Context;
    protected List<List<dynamic>> Data = new();
    protected List<FieldResponse> Fields = new();

    protected List<string> Messages = new();

    public BaseDbAction() => Context = DbContext.Instance;

    public ActionResponse Perform(Guid session)
    {
        PerformAction(session);

        return ActionResponse.FromRaw(Messages, Data, Fields);
    }

    /// <summary>
    ///     Do actions on the Messages, Fields, Data fields.
    ///     These values will be returned.
    /// </summary>
    public abstract void PerformAction(Guid session);
}