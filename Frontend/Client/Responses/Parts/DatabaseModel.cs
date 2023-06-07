using Frontend.Models;
using Server.Models.Catalog;

namespace Frontend.Client.Responses.Parts
{
    public class DatabaseModel
    {
        public class Table
        {
            public string Name { get; set; }
            public List<Column> Columns { get;set;}
            public List<string> PrimaryKeys { get; set; }
            public List<ForeignKey> ForeignKeys { get; set; }
            public List<string> UniqueKeys { get; set; }
            public List<IndexFile> IndexFiles { get; set; }
        }

        public string DatabaseName { get; set; }

        public List<Table> Tables { get; set; }
    }
}
