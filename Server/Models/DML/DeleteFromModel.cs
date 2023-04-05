using Server.Models.Statement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Server.Models.DML
{
    internal class DeleteFromModel
    {
        public String TableName { get; set; }
        public WhereModel WhereModel { get; set; }

        public static DeleteFromModel FromMatch(Match match)
        {
            string tableName = match.Groups["TableName"].Value;
            WhereModel whereModel = WhereModel.FromString(match.Groups["Where"].Value);

            return new DeleteFromModel
            {
                TableName = tableName,
                WhereModel = whereModel
            };
        }
    }
}
