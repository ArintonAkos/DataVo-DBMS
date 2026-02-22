namespace DataVo.Core.Parser.Types
{
    public class Row
    {
        private readonly Dictionary<string, dynamic> _cells = [];

        public Row()
        {
            _cells = [];
        }

        public Row(Dictionary<string, dynamic> cells)
        {
            _cells = cells;
        }

        public bool ContainsKey(string key)
        {
            return _cells.ContainsKey(key);
        }

        public IEnumerable<string> Keys
        {
            get { return _cells.Keys; }
        }

        public dynamic this[string key]
        {
            get { return _cells[key]; }
            set { _cells[key] = value; }
        }

        public dynamic Get(string key)
        {
            return _cells[key];
        }

        public void Add(string cellName, dynamic value)
        {
            _cells.Add(cellName, value);
        }
    }
}
