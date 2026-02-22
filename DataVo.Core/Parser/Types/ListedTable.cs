using DataVo.Core.Parser.Statements;
using System.Collections;

namespace DataVo.Core.Parser.Types
{
    public class ListedTable : IEnumerable<JoinedRow>
    {
        private readonly List<JoinedRow> _tables = [];

        public ListedTable()
        {
            _tables = [];
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

        public void Add(JoinedRow row)
        {
            _tables.Add(row);
        }

        public IEnumerator<JoinedRow> GetEnumerator()
        {
            return _tables.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public GroupedTable ToGroupedTable()
        {
            GroupedTable groupedTable = new()
            {
                { GroupBy.HASH_VALUE, new() }
            };

            foreach (var row in _tables)
            {
                groupedTable[GroupBy.HASH_VALUE].Add(row);
            }

            return groupedTable;
        }
    }
}
