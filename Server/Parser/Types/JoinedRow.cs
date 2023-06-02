using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Types
{
    public class JoinedRow
    {
        private readonly Dictionary<string, Row> _row = new();

        public JoinedRow()
        {
            _row = new Dictionary<string, Row>();
        }

        public JoinedRow(Dictionary<string, Row> rows)
        {
            _row = rows;
        }

        public JoinedRow(string tableName, Row row)
        {
            _row = new()
            {
                { tableName, row }
            };
        }

        public int Count
        {
            get { return _row.Count; }
        }

        public IEnumerable<string> Keys
        {
            get { return _row.Keys; }
        }

        public bool ContainsKey(string tableName)
        {
            return _row.ContainsKey(tableName);
        }

        public Row this[string tableName]
        {
            get { return _row[tableName]; }
            set { _row[tableName] = value; }
        }

        public void Add(string tableName, Row row)
        {
            _row.Add(tableName, row);
        }

        public Row Get(string tableName)
        {
            return _row[tableName];
        }
    }
}