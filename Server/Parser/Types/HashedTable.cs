using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Server.Parser.Types
{
    public class HashedTable : IEnumerable<KeyValuePair<string, JoinedRow>>
    {
        private readonly Dictionary<string, JoinedRow> _rows = new();

        public HashedTable()
        {
            _rows = new Dictionary<string, JoinedRow>();
        }

        public HashedTable(Dictionary<string, JoinedRow> rows)
        {
            _rows = rows;
        }

        public void Add(string hash, JoinedRow row)
        {
            _rows.Add(hash, row);
        }

        public bool ContainsKey(string hash)
        {
            return _rows.ContainsKey(hash);
        }

        public IEnumerable<string> Keys
        {
            get { return _rows.Keys; }
        }

        public JoinedRow this[string hash]
        {
            get { return _rows[hash]; }
            set { _rows[hash] = value; }
        }

        public int Count
        {
            get { return _rows.Count; }
        }

        public KeyValuePair<string, JoinedRow> First()
        {
            return _rows.First();
        }

        public JoinedRow Get(string hash)
        {
            return _rows[hash];
        }

        public ListedTable ToListedTable()
        {
            return new ListedTable(_rows.Select(row => row.Value).ToList());
        }

        public IEnumerator<KeyValuePair<string, JoinedRow>> GetEnumerator()
        {
            return _rows.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
