using System.Collections;

namespace DataVo.Core.Parser.Types
{
    public class HashedTable : IEnumerable<KeyValuePair<JoinedRowId, JoinedRow>>
    {
        private readonly Dictionary<JoinedRowId, JoinedRow> _rows;

        public HashedTable()
        {
            _rows = [];
        }

        public HashedTable(Dictionary<JoinedRowId, JoinedRow> rows)
        {
            _rows = rows;
        }

        public void Add(JoinedRowId hash, JoinedRow row)
        {
            _rows.Add(hash, row);
        }

        public bool ContainsKey(JoinedRowId hash)
        {
            return _rows.ContainsKey(hash);
        }

        public IEnumerable<JoinedRowId> Keys
        {
            get { return _rows.Keys; }
        }

        public JoinedRow this[JoinedRowId hash]
        {
            get { return _rows[hash]; }
            set { _rows[hash] = value; }
        }

        public int Count
        {
            get { return _rows.Count; }
        }

        public KeyValuePair<JoinedRowId, JoinedRow> First()
        {
            return _rows.First();
        }

        public JoinedRow Get(JoinedRowId hash)
        {
            return _rows[hash];
        }

        public ListedTable ToListedTable()
        {
            return new ListedTable(_rows.Select(row => row.Value).ToList());
        }

        public IEnumerator<KeyValuePair<JoinedRowId, JoinedRow>> GetEnumerator()
        {
            return _rows.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
