using System;
using System.Collections.Generic;

namespace DataVo.Core.Parser.Types
{
    public class JoinedRowId : IEquatable<JoinedRowId>
    {
        private readonly List<long> _rowIds;

        public IReadOnlyList<long> RowIds => _rowIds;

        public JoinedRowId(long singleRowId)
        {
            _rowIds = [singleRowId];
        }

        public JoinedRowId(List<long> rowIds)
        {
            _rowIds = rowIds;
        }

        public JoinedRowId(params long[] rowIds)
        {
            _rowIds = [..rowIds];
        }

        public JoinedRowId Append(long rowId)
        {
            var newIds = new List<long>(_rowIds) { rowId };
            return new JoinedRowId(newIds);
        }

        public JoinedRowId Prepend(long rowId)
        {
            var newIds = new List<long> { rowId };
            newIds.AddRange(_rowIds);
            return new JoinedRowId(newIds);
        }

        public bool Equals(JoinedRowId? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            if (_rowIds.Count != other._rowIds.Count) return false;

            for (int i = 0; i < _rowIds.Count; i++)
            {
                if (_rowIds[i] != other._rowIds[i]) return false;
            }
            return true;
        }

        public override bool Equals(object? obj) => Equals(obj as JoinedRowId);

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;
                foreach (var id in _rowIds)
                {
                    hash = hash * 31 + id.GetHashCode();
                }
                return hash;
            }
        }
    }
}
