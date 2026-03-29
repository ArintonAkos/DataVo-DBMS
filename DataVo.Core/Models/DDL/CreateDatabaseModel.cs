using System.Text.RegularExpressions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Utils;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Models.DDL;

/// <summary>
/// Represents the normalized model for a CREATE DATABASE statement.
/// </summary>
/// <param name="databaseName">The database name.</param>
public class CreateDatabaseModel(string databaseName)
{
    /// <summary>
    /// Gets or sets the database name.
    /// </summary>
    public string DatabaseName { get; set; } = databaseName;

    /// <summary>
    /// Builds a model from CREATE DATABASE AST.
    /// </summary>
    /// <param name="ast">The parsed CREATE DATABASE statement.</param>
    /// <returns>The normalized create-database model.</returns>
    public static CreateDatabaseModel FromAst(CreateDatabaseStatement ast) => new(ast.DatabaseName.Name);

    /// <summary>
    /// Converts the model into catalog database metadata.
    /// </summary>
    /// <returns>The catalog database definition.</returns>
    public Database ToDatabase() =>
        new()
        {
            DatabaseName = DatabaseName,
            Tables = [],
        };
}