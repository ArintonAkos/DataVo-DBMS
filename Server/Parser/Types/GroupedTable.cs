using Amazon.Runtime.Internal.Transform;
using Server.Parser.Aggregations;
using System.Collections;

namespace Server.Parser.Types
{
    public class GroupedTable : IEnumerable<KeyValuePair<string, ListedTable>>
    {
        private readonly Dictionary<string, ListedTable> _tables = new();

        public IEnumerator<KeyValuePair<string, ListedTable>> GetEnumerator()
        {
            return _tables.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(string hash, ListedTable table)
        {
            _tables.Add(hash, table);
        }

        public bool ContainsKey(string hash)
        {
            return _tables.ContainsKey(hash);
        }

        public ListedTable this[string hash]
        {
            get { return _tables[hash]; }
            set { _tables[hash] = value; }
        }

        public ListedTable ApplyAggregations(List<Aggregation> aggregations)
        {
            ListedTable result = new();

            foreach (var group in _tables)
            {
                // It doesn't matter which one do we choose, because
                // the return value will only return one value / group
                // which have the same value.
                JoinedRow row = group.Value.First();
                Row groupedRow = new();

                foreach (var aggregation in aggregations)
                {
                    dynamic? value = aggregation.Execute(group.Value);
                    groupedRow.Add(aggregation.GetHeaderName(), value);
                }

                row.Add(Aggregation.HASH_VALUE, groupedRow);
                result.Add(row);
            }

            return result;
        }
    }
}
