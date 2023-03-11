using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.Responses;
using Server.Models;
using Server.Utils;
using System.Net;
using System.Text.RegularExpressions;

namespace Server.Parser.DDL
{
    internal class CreateTable : BaseDbAction
    {    
        public CreateTableModel Model { get; private set; }

        public CreateTable(Match match)
        {
            this.Model = CreateTableModel.FromMatch(match);
        }

        public override void PerformAction()
        {
            // TO-DO: Create Table in MongoDB

            Context.ListDbs();
            XML<CreateTableModel>.InsertObjIntoXML(Model, "Databases", "databases", $"{Model.TableName}.xml");

            return new Response()
            {
                Code = HttpStatusCode.OK,
                Meta = "Database successfully created!",
            };
        }
    }
}
