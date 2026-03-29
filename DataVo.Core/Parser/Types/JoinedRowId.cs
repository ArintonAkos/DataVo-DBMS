using System;
using System.Collections.Generic;

namespace DataVo.Core.Parser.Types
{
    /// <summary>
    /// Represents a composite row identifier used for joined row keys.
    /// </summary>
    public class JoinedRowId : IEquatable<JoinedRowId>
    {
        private readonly List<long> _rowIds;

        /// <summary>
        /// Gets the ordered list of row identifiers that compose this key.
        /// </summary>
        public IReadOnlyList<long> RowIds => _rowIds;

        /// <summary>
        /// Initializes a key from a single row identifier.
        /// </summary>
        /// <param name="singleRowId">The single row identifier.</param>
        public JoinedRowId(long singleRowId)
        {
            _rowIds = [singleRowId];
        }

        /// <summary>
        /// Initializes a key from an existing identifier list.
        /// </summary>
        /// <param name="rowIds">Ordered row identifiers.</param>
        public JoinedRowId(List<long> rowIds)
        {
            _rowIds = rowIds;
        }

        /// <summary>
        /// Initializes a key from one or more identifiers.
        /// </summary>
        /// <param name="rowIds">Ordered row identifiers.</param>
        public JoinedRowId(params long[] rowIds)
        {
            _rowIds = [..rowIds];
        }

        /// <summary>
        /// Creates a new key by appending a row identifier.
        /// </summary>
        /// <param name="rowId">The row identifier to append.</param>
        /// <returns>A new composite key with the appended identifier.</returns>
        public JoinedRowId Append(long rowId)
        {
            var newIds = new List<long>(_rowIds) { rowId };
            return new JoinedRowId(newIds);
        }

        /// <summary>
        /// Creates a new key by prepending a row identifier.
        /// </summary>
        /// <param name="rowId">The row identifier to prepend.</param>
        /// <returns>A new composite key with the prepended identifier.</returns>
        public JoinedRowId Prepend(long rowId)
        {
            var newIds = new List<long> { rowId };
            newIds.AddRange(_rowIds);
            return new JoinedRowId(newIds);
        }

        /// <summary>
        /// Determines whether another <see cref="JoinedRowId"/> has identical ordered identifiers.
        /// </summary>
        /// <param name="other">The candidate key.</param>
        /// <returns><see langword="true"/> when equal; otherwise <see langword="false"/>.</returns>
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

        /// <inheritdoc/>
        public override bool Equals(object? obj) => Equals(obj as JoinedRowId);

        /// <inheritdoc/>
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
