using Server.Parser.Actions;
using Server.Parser.DDL;
using Server.Server.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser
{
    internal class RequestMapper
    {
        private static readonly Dictionary<String, Type> _commands = new()
        {
            { "CreateDatabase", typeof(CreateDatabase) },
            { "DropDatabase", typeof(DropDatabase) },
            { "CreateTable", typeof(CreateTable) },
            { "DropTable", typeof(DropTable) },
        };

        public static DbAction ToAction(Request request)
        {
            var type = _commands.GetValueOrDefault(request.CommandType);

            if (type == null)
            {
                var commandTypes = String.Join(", ", _commands.ToArray());
                throw new Exception($"Invalid Command Type: {request.CommandType}. \nSupported command types: {commandTypes}");
            }
            
            return (DbAction)Activator.CreateInstance(type, request.Data);
        }

        public static bool IsValid(String commandName)
        {
            return _commands.ContainsKey(commandName);
        }
    }
}
