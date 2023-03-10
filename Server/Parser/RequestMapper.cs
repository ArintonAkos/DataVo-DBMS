using Server.Parser.Actions;
using Server.Parser.Commands;
using Server.Parser.DDL;
using Server.Server.Requests;
using System.Text.RegularExpressions;

namespace Server.Parser
{
    internal class RequestMapper
    {
        private static readonly Dictionary<String, Type> _commands = new()
        {
            { Patterns.CreateDatabase, typeof(CreateDatabase) },
            { Patterns.DropDatabase, typeof(DropDatabase) },
            { Patterns.CreateTable, typeof(CreateTable) },
            { Patterns.DropTable, typeof(DropTable) },
        };
        private static readonly KeyValuePair<String, Type> _goCommand = new(Patterns.Go, typeof(Go));

        public static List<Queue<DbAction>> ToRunnables(Request request)
        {
            List<Queue<DbAction>> runnables = new();
            Queue<DbAction> actions = new();

            var rawSqlCode = request.Data;
            int lineCount = 0;

            REPEAT:
            while (!String.IsNullOrEmpty(rawSqlCode.Trim()))
            {
                if (MatchCommand(_goCommand, ref rawSqlCode, ref lineCount) != null)
                {
                    runnables.Add(actions);
                    actions.Clear();
                }

                foreach (KeyValuePair<String, Type> command in _commands)
                {
                    var action = MatchCommand(command, ref rawSqlCode, ref lineCount);

                    if (action != null)
                    {
                        actions.Enqueue(action);
                        goto REPEAT;
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

        private static DbAction? MatchCommand(KeyValuePair<String, Type> command, ref String rawSqlCode, ref int lineCount)
        {
            Match match = Regex.Match(rawSqlCode, command.Key, RegexOptions.IgnoreCase | RegexOptions.Multiline);

            if (match.Success)
            {
                lineCount += match.Value.Split("\r\n|\r|\n").Length;
                rawSqlCode = rawSqlCode.Substring(match.Index + match.Length);
                return (DbAction)Activator.CreateInstance(command.Value, match);
            }

            return null;
        }

        private static String FirstKeyWord(String rawSqlCode)
        {
            return rawSqlCode.Trim().Split(" |\t").FirstOrDefault() ?? String.Empty;
        }
    }
}
