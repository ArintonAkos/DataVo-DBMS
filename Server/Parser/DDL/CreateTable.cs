using Newtonsoft.Json;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.Responses;
using Server.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml.Linq;
using Server.Server.MongoDB;

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
                throw new Exception("Database Name Contains invalid characters!");
            }

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
