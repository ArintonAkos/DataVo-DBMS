using System.Text.RegularExpressions;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.Commands;

internal class ShowDatabases : BaseDbAction
{
    public ShowDatabases(Match match, string query)
    { }

    public ShowDatabases(ShowDatabasesStatement ast)
    { }

    public override void PerformAction(Guid session)
    {
        try
        {
            Catalog.GetDatabases()
            .ForEach(databaseName => Fields.Add(databaseName));
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}