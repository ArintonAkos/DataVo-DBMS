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

            var sqlFile = request.Data;

            REPEAT:
            while (!String.IsNullOrEmpty(sqlFile.Trim()))
            {
                if (MatchCommand(_goCommand, ref sqlFile) != null)
                {
                    runnables.Add(actions);
                    actions.Clear();
                }

                foreach (KeyValuePair<String, Type> command in _commands)
                {
                    var action = MatchCommand(command, ref sqlFile);

                    if (action != null)
                    {
                        actions.Enqueue(action);
                        goto REPEAT;
                    }
                }

                throw new Exception($"Parse Error!");
            }
            
            if (actions.Count != 0)
            {
                runnables.Add(actions);
            }

            return runnables;
        }

        public static DbAction? MatchCommand(KeyValuePair<String, Type> command, ref String sqlFile)
        {
            Match match = Regex.Match(sqlFile, command.Key, RegexOptions.IgnoreCase | RegexOptions.Multiline);

            if (match.Success)
            {
                sqlFile = sqlFile.Substring(match.Index + match.Length);
                return (DbAction)Activator.CreateInstance(command.Value, match);
            }

            return null;
        }

        public static bool IsValid(String commandName)
        {
            return _commands.ContainsKey(commandName);
        }
    }
}
