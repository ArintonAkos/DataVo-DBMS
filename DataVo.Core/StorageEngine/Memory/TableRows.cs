using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.StorageEngine.Memory;

/// <summary>
/// The rows of one in-memory table, numbered from 0 in the order they were inserted. A row is either
/// <em>typed</em>, inserted as cells and kept packed in a <see cref="FlatRowStore"/>, or <em>serialized</em>,
/// inserted as bytes and kept as those bytes. Both kinds share the one sequence of positions, so a row's id
/// (its position plus one) does not depend on how it was inserted.
/// </summary>
/// <remarks>
/// Each kind costs memory only where it is used. A table of typed rows only has no storage for serialized
/// rows at all; a table of serialized rows only has no packed store; serialized rows are held in pages that
/// exist only where such a row is. The packed store begins at the table's first typed row, so serialized rows
/// inserted before it take no space in it, and each one inserted after it takes one empty record.
/// <para>
/// Writing is done under the table's write lock, and so is everything that reads serialized rows or counts.
/// Typed rows can be read without a lock: <see cref="TryReadTypedCells"/> and <see cref="TryCopyTyped"/> give
/// a row whole or not at all, whatever a writer is doing.
/// </para>
/// </remarks>
internal sealed class TableRows
{
    private RawRowPages? _raw;
    private volatile FlatRowStore? _flat;
    private int _flatBase;
    private int _count;

    /// <summary>The number of positions used, removed rows included.</summary>
    public int Count => Volatile.Read(ref _count);

    /// <summary>The column names of the table's typed rows; null until a typed row has been inserted.</summary>
    public ReactiveRowSchema? Schema { get; private set; }

    /// <summary>The layout of the table's typed rows; null until a typed row has been inserted.</summary>
    public RowLayout? Layout => _flat?.Layout;

    /// <summary>The number of typed rows that could not be packed and are kept as cells.</summary>
    public int KeptApartCount => _flat?.KeptApartCount ?? 0;

    /// <summary>Appends a serialized row. Called by the writer.</summary>
    /// <returns>The row's position.</returns>
    public int AddRaw(byte[] row)
    {
        int position = _count;
        (_raw ??= new RawRowPages()).Set(position, row);
        _flat?.AddEmpty();
        Volatile.Write(ref _count, position + 1);
        return position;
    }

    /// <summary>Appends a typed row. Called by the writer.</summary>
    /// <param name="columns">The table's columns in catalog order; they shape the table's first typed row and those after it.</param>
    /// <param name="row">The row. Its cells are copied.</param>
    /// <returns>The row's position.</returns>
    public int AddTyped(IReadOnlyList<Column> columns, StoredRow row)
    {
        FlatRowStore flat = _flat ?? StartTypedRows(columns, row.Schema);
        flat.Add(row.Cells);
        int position = _count;
        Volatile.Write(ref _count, position + 1);
        return position;
    }

    /// <summary>The serialized row at a position; null when the position holds a typed row or no row.</summary>
    public byte[]? RawAt(int position) => _raw?[position];

    /// <summary>Whether a position holds a typed row, as of now. Takes no lock.</summary>
    public bool IsTypedLive(int position) => _flat is { } flat && flat.IsLive(position - _flatBase);

    /// <summary>Whether a position holds a row of either kind.</summary>
    public bool IsLive(int position) => RawAt(position) is not null || IsTypedLive(position);

    /// <summary>Whether the table holds any typed row.</summary>
    public bool HasTypedRows()
    {
        if (_flat is not { } flat)
        {
            return false;
        }

        for (int index = 0; index < flat.Count; index++)
        {
            if (flat.IsLive(index))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Reads the typed row at a position as cells, without a lock and without allocating. The cells are valid
    /// until the reader is used again or disposed.
    /// </summary>
    /// <returns><see langword="false"/> when the position holds no typed row, or it was removed meanwhile.</returns>
    public bool TryReadTypedCells(int position, ref PackedRowReader reader, out ReadOnlySpan<CellValue> cells)
    {
        if (_flat is { } flat && flat.TryRead(position - _flatBase, out FlatRow row))
        {
            cells = reader.Cells(flat.Layout, row);

            // The row was read without a lock: it counts only if it was still there once it had been read.
            if (row.IsKeptApart || flat.IsLive(position - _flatBase))
            {
                return true;
            }
        }

        cells = default;
        return false;
    }

    /// <summary>Copies the typed row at a position out as a row object of its own, without a lock.</summary>
    /// <returns><see langword="false"/> when the position holds no typed row, or it was removed meanwhile.</returns>
    public bool TryCopyTyped(int position, out StoredRow? row)
    {
        var reader = new PackedRowReader();
        try
        {
            if (TryReadTypedCells(position, ref reader, out ReadOnlySpan<CellValue> cells))
            {
                row = StoredRow.FromOwnedCells(Schema!, cells.ToArray());
                return true;
            }
        }
        finally
        {
            reader.Dispose();
        }

        row = null;
        return false;
    }

    /// <summary>Removes the row at a position, leaving the position unused. Called by the writer.</summary>
    /// <returns><see langword="false"/> when the position holds no row.</returns>
    public bool Remove(int position)
    {
        if (_raw is { } raw && raw[position] is not null)
        {
            raw.Set(position, null);
            return true;
        }

        return _flat is { } flat && flat.Remove(position - _flatBase);
    }

    /// <summary>
    /// Copies the rows: the same rows at the same positions, of the same kinds. Later changes to either copy
    /// do not show in the other. Called with no writer at work.
    /// </summary>
    public TableRows Clone() => new()
    {
        _raw = _raw?.Clone(),
        _flat = _flat?.Clone(),
        _flatBase = _flatBase,
        _count = _count,
        Schema = Schema,
    };

    /// <summary>Creates the packed store at the table's first typed row; it begins at the current position.</summary>
    private FlatRowStore StartTypedRows(IReadOnlyList<Column> columns, ReactiveRowSchema schema)
    {
        var flat = new FlatRowStore(RowLayout.Create(ColumnCellTypes.ForLayout(columns)));
        Schema = schema;
        _flatBase = _count;
        _flat = flat; // published last: a reader that sees the store also sees where it begins
        return flat;
    }

    /// <summary>
    /// Serialized rows by position, in pages that are allocated only where a row is. Read and written under
    /// the table's lock.
    /// </summary>
    private sealed class RawRowPages
    {
        private const int PageShift = 10;
        private const int PageRows = 1 << PageShift;
        private const int PageMask = PageRows - 1;
        private const int FirstPageRows = 16;
        private const int FirstPageGrowth = 4;

        private byte[]?[]?[] _pages = new byte[]?[]?[4];

        /// <summary>The row at a position; null when there is none.</summary>
        public byte[]? this[int position]
        {
            get
            {
                int number = position >> PageShift;
                if ((uint)number >= (uint)_pages.Length || _pages[number] is not { } page)
                {
                    return null;
                }

                int inPage = position & PageMask;
                return inPage < page.Length ? page[inPage] : null;
            }
        }

        /// <summary>Stores a row at a position, or clears the position when <paramref name="row"/> is null.</summary>
        public void Set(int position, byte[]? row)
        {
            int number = position >> PageShift;
            int inPage = position & PageMask;
            if (row is null)
            {
                if (this[position] is not null)
                {
                    _pages[number]![inPage] = null;
                }

                return;
            }

            PageFor(number, inPage)[inPage] = row;
        }

        /// <summary>Copies the pages and the rows in them.</summary>
        public RawRowPages Clone()
        {
            var copy = new RawRowPages { _pages = new byte[]?[]?[_pages.Length] };
            for (int number = 0; number < _pages.Length; number++)
            {
                if (_pages[number] is { } page)
                {
                    copy._pages[number] = Array.ConvertAll(page, static row => row?.ToArray());
                }
            }

            return copy;
        }

        /// <summary>The page with a number, made or grown to hold the slot <paramref name="inPage"/>.</summary>
        private byte[]?[] PageFor(int number, int inPage)
        {
            if (number >= _pages.Length)
            {
                Array.Resize(ref _pages, Math.Max(number + 1, _pages.Length * 2));
            }

            // The first page starts small, so a table of a few rows does not pay for a whole page.
            byte[]?[] page = _pages[number] ??= new byte[]?[number == 0 ? FirstPageRows : PageRows];
            if (inPage >= page.Length)
            {
                int length = page.Length;
                while (length <= inPage)
                {
                    length = Math.Min(PageRows, length * FirstPageGrowth);
                }

                Array.Resize(ref page, length);
                _pages[number] = page;
            }

            return page;
        }
    }
}
