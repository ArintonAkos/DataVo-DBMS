using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
﻿using System.Text.RegularExpressions;
using DataVo.Core.Models.DQL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.Commands;

internal class Use(UseStatement ast) : BaseDbAction
{
    private readonly UseModel _model = UseModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        // Switching to a database that does not exist would only surface later as misleading
        // "table doesn't exist in database X" errors; keep the current database instead.
        if (!Catalog.DatabaseExists(_model.DatabaseName))
        {
            throw new CatalogException($"Database {_model.DatabaseName} doesn't exist.")
            {
                ErrorCode = DataVoErrorCodes.UnknownDatabase,
            };
        }

        SetDatabaseName(session, _model.DatabaseName);
    }
}