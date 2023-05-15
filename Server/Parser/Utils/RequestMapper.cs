using System.Text.RegularExpressions;
using Server.Parser.Actions;
using Server.Parser.Commands;
using Server.Parser.DDL;
using Server.Parser.DML;
using Server.Parser.DQL;
using Server.Server.Requests.Controllers.Parser;

namespace Server.Parser.Utils;

internal class RequestMapper
{
    private static readonly Dictionary<string, Type> _commands = new()
    {
        { Patterns.CreateDatabase, typeof(CreateDatabase) },
        { Patterns.DropDatabase, typeof(DropDatabase) },
        { Patterns.CreateTable, typeof(CreateTable) },
        { Patterns.DropTable, typeof(DropTable) },
        { Patterns.InsertInto, typeof(InsertInto) },
        { Patterns.CreateIndex, typeof(CreateIndex) },
        { Patterns.DropIndex, typeof(DropIndex) },
        { Patterns.ShowDatabases, typeof(ShowDatabases) },
        { Patterns.ShowTables, typeof(ShowTables) },
        { Patterns.Describe, typeof(Describe) },
        { Patterns.DeleteFrom, typeof(DeleteFrom) },
        { Patterns.Select, typeof(Select) },
        { Patterns.Use, typeof(Use) },
    };

    private static readonly KeyValuePair<string, Type> _goCommand = new(Patterns.Go, typeof(Go));

    public static List<Queue<IDbAction>> ToRunnables(ParseRequest request)
    {
        List<Queue<IDbAction>> runnables = new();
        Queue<IDbAction> actions = new();

        string rawSqlCode = HandleRequestData(request.Data);
        int lineCount = 0;

        REPEAT:
        while (!string.IsNullOrEmpty(rawSqlCode.Trim()))
        {
            if (MatchCommand(_goCommand, ref rawSqlCode, ref lineCount) != null)
            {
                runnables.Add(actions);
                actions = new Queue<IDbAction>();
                continue;
            }

            foreach (KeyValuePair<string, Type> command in _commands)
            {
                try
                {
                    var action = MatchCommand(command, ref rawSqlCode, ref lineCount);

                    if (action != null)
                    {
                        actions.Enqueue(action);
                        goto REPEAT;
                    }
                }
                catch (Exception ex)
                {
                    while (ex.InnerException != null)
                    {
                        ex = ex.InnerException;
                    }

                    throw new Exception(
                        $"Exception thrown at: {FirstKeyWord(rawSqlCode)}\n" +
                        $"Line: {lineCount}\n" +
                        $"Message: {ex.Message}"
                    );
                }
            }

            throw new Exception($"Invalid Keyword: {FirstKeyWord(rawSqlCode)} at line: {lineCount}!");
        }

        if (actions.Count != 0)
        {
            runnables.Add(actions);
        }

        return runnables;
    }

    private static string HandleRequestData(string data) => RemoveSqlComments(data.Replace(";", ""));

    private static string RemoveSqlComments(string input)
    {
        string pattern = @"(--[^\r\n]*|/\*[\s\S]*?\*/)";
        string output = Regex.Replace(input, pattern, string.Empty, RegexOptions.Multiline);

        return output;
    }

    private static IDbAction? MatchCommand(KeyValuePair<string, Type> command, ref string rawSqlCode,
        ref int lineCount)
    {
        var match = Regex.Match(rawSqlCode, command.Key, RegexOptions.IgnoreCase);

        if (match.Success)
        {
            var action = (IDbAction)Activator.CreateInstance(command.Value, match)!;
            lineCount += Regex.Split(match.Value, "\r\n|\r|\n").Length;
            rawSqlCode = rawSqlCode.Substring(match.Index + match.Length);

            return action;
        }

        return null;
    }

    private static string FirstKeyWord(string rawSqlCode) =>
        rawSqlCode.Trim().Split(" |\t").FirstOrDefault() ?? string.Empty;
}