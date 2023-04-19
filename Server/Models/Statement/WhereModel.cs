using MongoDB.Bson;
using Server.Models.Catalog;
using Server.Parser.Statements;

namespace Server.Models.Statement
{
    internal class WhereModel
    {
        public Node Statement { get; set; }

        public static WhereModel FromString(string value)
        {
            value = value.Remove(0, 5);
            Node statement = StatementParser.Parse(value);

            return new WhereModel
            {
                Statement = statement
            };
        }
    }
}
