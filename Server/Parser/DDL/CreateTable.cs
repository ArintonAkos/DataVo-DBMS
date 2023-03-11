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

        public override Response Perform()
        {
            if (Model.TableName.ContainsAny("#", " ", ".", "/", "\\", "_"))
            {
                throw new Exception("Table Name Contains invalid characters!");
            }

            Context.ListDbs();
            XML<Table>.InsertObjIntoXML(Model.ToTable(), "Tables", "databases", "Catalog.xml");

            return new Response()
            {
                Code = HttpStatusCode.OK,
                Meta = "Table successfully created!",
            };
        }
    }
}
