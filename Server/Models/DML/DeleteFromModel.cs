using Server.Models.Statement;
using System.Text.RegularExpressions;

namespace Server.Models.DML
{
    internal class DeleteFromModel
    {
        public String TableName { get; set; }
        public WhereModel WhereModel { get; set; }

        public static DeleteFromModel FromMatch(Match match)
        {
            string tableName = match.Groups["TableName"].Value;
            WhereModel whereModel = WhereModel.FromString(match.Groups["WhereStatement"].Value);

            return new DeleteFromModel
            {
                TableName = tableName,
                WhereModel = whereModel
            };
        }
    }
}
