using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Parser.Types
{
    public class ListedTable : IEnumerable<JoinedRow>
    {
        private readonly List<JoinedRow> _tables = new();

        public ListedTable()
        {
            _tables = new List<JoinedRow>();
        }

        public ListedTable(List<JoinedRow> tables)
        {
            _tables = tables;
        }

        public JoinedRow this[int index]
        {
            get { return _tables[index]; }
            set { _tables[index] = value; }
        }

        public int Count
        {
            get { return _tables.Count; }
        }

        public JoinedRow Get(int index)
        {
            return _tables[index];
        }

        public IEnumerator<JoinedRow> GetEnumerator()
        {
            return _tables.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
