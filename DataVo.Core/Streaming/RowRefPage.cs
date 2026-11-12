using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core;

/// <summary>
/// The memory behind a <see cref="RowRefStream"/>: one page of rows copied out of storage while the read lock
/// is held, with every cell's offset found once. Pages are kept per thread and reused, so a stream allocates
/// nothing once its thread has streamed a table of that shape before.
/// </summary>
internal sealed class RowRefPage
{
    /// <summary>The most rows one page holds; locks are held only while one page is read.</summary>
    internal const int Rows = 1024;

    private const int PoolSize = 4;
    private const int InitialBytes = 64 * 1024;

    // Past this, a page goes back to its initial size when returned, so one wide scan does not pin memory.
    private const int RetainedBytes = 4 * 1024 * 1024;

    [ThreadStatic]
    private static RowRefPage?[]? t_pool;

    private byte[] _bytes = new byte[InitialBytes];
    private char[] _chars = [];
    private readonly int[] _starts = new int[Rows];
    private readonly int[] _lengths = new int[Rows];
    private readonly RowKey[] _keys = new RowKey[Rows];
    private int[] _cells = [];
    private StorageKind[] _kinds = [];
    private int _used;
    private bool _hasText;
    private int _keyOrdinal = -1;
    private bool _keyIsGuid;

    // Where each column's null flag sits in a row image record (see EncodeImage), and the record's length.
    private int[] _imageOffsets = [];
    private int _imageBytes;

    // Which table the cached layout describes. The engine is held by id, so a pooled page keeps no engine alive.
    private Guid _engineId;
    private string? _database;
    private string? _table;
    private int _schemaVersion = -1;

    public StorageContext.RowPageCursor Cursor { get; } = new();

    public int Count { get; private set; }

    public int SchemaVersion => _schemaVersion;

    public int ColumnCount => _kinds.Length;

    public StorageKind KindAt(int ordinal) => _kinds[ordinal];

    /// <summary>Takes a page from this thread's pool, or makes one.</summary>
    public static RowRefPage Rent()
    {
        RowRefPage?[]? pool = t_pool;
        if (pool is not null)
        {
            for (int i = 0; i < pool.Length; i++)
            {
                if (pool[i] is { } page)
                {
                    pool[i] = null;
                    return page;
                }
            }
        }

        return new RowRefPage();
    }

    /// <summary>Puts a page back in this thread's pool.</summary>
    public static void Return(RowRefPage page)
    {
        page.Count = 0;
        page._used = 0;
        if (page._bytes.Length > RetainedBytes)
        {
            page._bytes = new byte[InitialBytes];
            page._chars = [];
        }

        RowRefPage?[] pool = t_pool ??= new RowRefPage?[PoolSize];
        for (int i = 0; i < pool.Length; i++)
        {
            if (pool[i] is null)
            {
                pool[i] = page;
                return;
            }
        }
    }

    /// <summary>
    /// Points the page at a table and rewinds its cursor. The column layout is rebuilt only when the table, or
    /// its schema, differs from the last one this page streamed.
    /// </summary>
    /// <exception cref="InvalidOperationException">The table does not exist.</exception>
    public void Open(DataVoEngine engine, string databaseName, string tableName)
    {
        Cursor.After = null;
        Cursor.Generation = null;
        Cursor.Done = false;
        Count = 0;
        _used = 0;

        int version = engine.Catalog.GetTableSchemaVersion(tableName, databaseName);
        if (version == _schemaVersion
            && _engineId == engine.Id
            && string.Equals(_table, tableName, StringComparison.Ordinal)
            && string.Equals(_database, databaseName, StringComparison.Ordinal))
        {
            return;
        }

        if (!engine.Catalog.TableExists(tableName, databaseName))
        {
            throw new InvalidOperationException($"Table {tableName} does not exist in database {databaseName}.");
        }

        List<Column> columns = engine.Catalog.GetTableColumns(tableName, databaseName);
        var kinds = new StorageKind[columns.Count];
        bool hasText = false;
        for (int i = 0; i < kinds.Length; i++)
        {
            kinds[i] = RowSerializer.ClassifyColumnType(columns[i].Type);
            hasText |= kinds[i] == StorageKind.String;
        }

        ClusteredKeyLayout? clustered = engine.StorageContext.ResolveClusteredKey(databaseName, tableName);
        _kinds = kinds;
        _cells = new int[Rows * kinds.Length];
        _hasText = hasText;
        _keyOrdinal = clustered?.Ordinal ?? -1;
        _keyIsGuid = clustered is not null && kinds[clustered.Ordinal] == StorageKind.Guid;
        _imageOffsets = new int[kinds.Length];
        int imageBytes = 0;
        for (int i = 0; i < kinds.Length; i++)
        {
            _imageOffsets[i] = imageBytes;
            imageBytes += 1 + RowSerializer.FixedWidth(kinds[i]);
        }

        _imageBytes = imageBytes;
        _engineId = engine.Id;
        _database = databaseName;
        _table = tableName;
        _schemaVersion = version;
    }

    /// <summary>Empties the page before the next one is read into it.</summary>
    public void Clear()
    {
        Count = 0;
        _used = 0;
    }

    /// <summary>Copies one stored row into the page. Typed rows are written in the stored byte format.</summary>
    /// <exception cref="InvalidDataException">The row's bytes do not match the table's columns.</exception>
    public void Add(RowKey key, ReadOnlySpan<byte> stored, ReadOnlySpan<CellValue> typed)
    {
        int length = typed.IsEmpty ? stored.Length : Measure(typed);
        Span<byte> destination = Reserve(length);
        if (typed.IsEmpty)
        {
            stored.CopyTo(destination);
        }
        else
        {
            Write(typed, destination);
        }

        Commit(key, length);
    }

    /// <summary>Encodes one row of values (in column order) into the page, in the stored byte format.</summary>
    public void Add(DataVoRow row)
    {
        int length = 0;
        for (int i = 0; i < _kinds.Length; i++)
        {
            length += RowSerializer.MeasureCell(_kinds[i], ToCell(row[i]));
        }

        Span<byte> destination = Reserve(length);
        int offset = 0;
        for (int i = 0; i < _kinds.Length; i++)
        {
            offset += RowSerializer.WriteCell(_kinds[i], ToCell(row[i]), destination[offset..]);
        }

        Commit(default, length);
    }

    /// <summary>The row at a position of the page.</summary>
    public RowRef RowAt(int index)
    {
        int start = _starts[index];
        int length = _lengths[index];
        int columns = _kinds.Length;
        return new RowRef(
            new ReadOnlySpan<byte>(_bytes, start, length),
            new ReadOnlySpan<int>(_cells, index * columns, columns),
            _kinds,
            _hasText ? new Span<char>(_chars, start, length) : default,
            _keys[index],
            _keyOrdinal,
            _keyIsGuid);
    }

    /// <summary>
    /// A stored row read where it lies, for a condition that is evaluated during the storage scan. Only the first
    /// <paramref name="columns"/> cells are located; the row must not be asked for a later one, and is valid only
    /// until the scan moves on.
    /// </summary>
    /// <exception cref="InvalidDataException">The row's bytes do not match the table's columns.</exception>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public RowRef InPlace(RowKey key, ReadOnlySpan<byte> stored, int columns)
    {
        if (!RowSerializer.TryLocateCells(stored, new ReadOnlySpan<StorageKind>(_kinds, 0, columns), new Span<int>(_cells, 0, columns)))
        {
            throw new InvalidDataException($"A stored row of {_table} does not match the table's columns.");
        }

        return new RowRef(stored, new ReadOnlySpan<int>(_cells, 0, _kinds.Length), _kinds, default, key, _keyOrdinal, _keyIsGuid);
    }

    /// <summary>
    /// A typed row of an in-memory table, encoded into this page's buffer so a condition can read it. Fixed-width
    /// cells marked in <paramref name="needed"/> (bit <c>n</c> for column <c>n</c>; columns past 63 always) are
    /// encoded in full. Every other cell keeps only its null flag, which is all a condition asks of text. The row
    /// is valid only until the scan moves on.
    /// </summary>
    public RowRef InPlace(RowKey key, ReadOnlySpan<CellValue> typed, int columns, ulong needed)
    {
        // A flag and at most a GUID per cell.
        int capacity = columns * 17;
        if (_bytes.Length < capacity)
        {
            _bytes = new byte[Math.Max(capacity, InitialBytes)];
        }

        Span<byte> destination = _bytes;
        ReadOnlySpan<CellValue> cells = typed;
        int offset = 0;
        for (int i = 0; i < columns; i++)
        {
            _cells[i] = offset;
            if (i >= cells.Length)
            {
                destination[offset++] = 1;
                continue;
            }

            ref readonly CellValue cell = ref cells[i];
            StorageKind kind = _kinds[i];
            if (cell.IsNull || kind == StorageKind.String || kind == StorageKind.Vector || (i < 64 && ((needed >> i) & 1) == 0))
            {
                destination[offset++] = cell.IsNull ? (byte)1 : (byte)0;
                continue;
            }

            offset += WriteValue(kind, in cell, destination.Slice(offset));
        }

        return new RowRef(destination.Slice(0, offset), new ReadOnlySpan<int>(_cells, 0, _kinds.Length), _kinds, default, key, _keyOrdinal, _keyIsGuid);
    }

    /// <summary>
    /// Writes a cell that is not NULL as its flag and value, in the stored byte format, and returns the bytes
    /// written. The usual pairings of column kind and cell type are written here; anything else converts.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static int WriteValue(StorageKind kind, in CellValue cell, Span<byte> destination)
    {
        Span<byte> value = destination.Slice(1);
        switch (kind)
        {
            case StorageKind.Int when cell.Type == CellType.Int32:
                destination[0] = 0;
                System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(value, cell.AsInt32());
                return 1 + sizeof(int);
            case StorageKind.BigInt when cell.Type == CellType.Int64:
                destination[0] = 0;
                System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(value, cell.AsInt64());
                return 1 + sizeof(long);
            case StorageKind.Double when cell.Type == CellType.Double:
                destination[0] = 0;
                System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(value, BitConverter.DoubleToInt64Bits(cell.AsDouble()));
                return 1 + sizeof(long);
            case StorageKind.Float when cell.Type == CellType.Double:
                destination[0] = 0;
                System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(value, BitConverter.SingleToInt32Bits((float)cell.AsDouble()));
                return 1 + sizeof(int);
            case StorageKind.Bit when cell.Type == CellType.Boolean:
                destination[0] = 0;
                value[0] = cell.AsBoolean() ? (byte)1 : (byte)0;
                return 1 + sizeof(bool);
            default:
                return RowSerializer.WriteCell(kind, cell, destination);
        }
    }

    /// <summary>
    /// Whether rows of this table can be kept as images: every table whose rows carry their own key cell.
    /// A clustered table keeps its key outside the row, where an image has no place for it.
    /// </summary>
    public bool ImageSupported => _keyOrdinal < 0;

    /// <summary>The length of a row image record of this table.</summary>
    public int ImageBytes => _imageBytes;

    /// <summary>
    /// Writes a row as an image record: every column at a fixed offset, as a null flag followed by the value's
    /// stored bytes. A NULL keeps its column's width, and text and vector columns keep only their flag, so the
    /// record has the same length and layout for every row of the table. A compiled condition reads it at
    /// offsets worked out once per scan, whatever the row holds.
    /// </summary>
    /// <param name="stored">The row's stored bytes, when it has no typed form.</param>
    /// <param name="typed">The row's cells, when it is typed.</param>
    /// <param name="record">Receives the image; <see cref="ImageBytes"/> long.</param>
    /// <exception cref="InvalidDataException">The row's bytes do not match the table's columns.</exception>
    public void EncodeImage(ReadOnlySpan<byte> stored, ReadOnlySpan<CellValue> typed, Span<byte> record)
    {
        StorageKind[] kinds = _kinds;
        int[] offsets = _imageOffsets;
        if (!typed.IsEmpty)
        {
            ReadOnlySpan<CellValue> cells = typed;
            for (int i = 0; i < kinds.Length; i++)
            {
                int at = offsets[i];
                if (i >= cells.Length || cells[i].IsNull)
                {
                    record[at] = 1;
                }
                else if (RowSerializer.FixedWidth(kinds[i]) == 0)
                {
                    record[at] = 0;
                }
                else
                {
                    WriteValue(kinds[i], in cells[i], record.Slice(at));
                }
            }

            return;
        }

        Span<int> located = new(_cells, 0, kinds.Length);
        if (!RowSerializer.TryLocateCells(stored, kinds, located))
        {
            throw new InvalidDataException($"A stored row of {_table} does not match the table's columns.");
        }

        for (int i = 0; i < kinds.Length; i++)
        {
            int at = offsets[i];
            int flag = located[i];
            if (flag == RowSerializer.AbsentCell || stored[flag] != 0)
            {
                record[at] = 1;
                continue;
            }

            record[at] = 0;
            int width = RowSerializer.FixedWidth(kinds[i]);
            if (width != 0)
            {
                stored.Slice(flag + 1, width).CopyTo(record.Slice(at + 1));
            }
        }
    }

    /// <summary>
    /// A row image read through a <see cref="RowRef"/>, for a row the direct readers cannot take (one with a NULL
    /// in a column the scan reads). Text and vector cells can only be tested for NULL. Valid until the scan moves on.
    /// </summary>
    public RowRef InImage(RowKey key, ReadOnlySpan<byte> record) =>
        new(record, _imageOffsets, _kinds, default, key, -1, false);

    /// <summary>What <see cref="PlanImage"/> found: how a scan reads row images.</summary>
    internal struct ImageLayout
    {
        /// <summary>Whether the scan can read images: the table keeps them and its columns are of the kinds the condition was compiled for.</summary>
        public bool Usable;

        /// <summary>The flag offsets of the columns the scan reads; a row is read directly when all of them are 0.</summary>
        public int[] FlagOffsets;

        /// <summary>How many columns <see cref="FlagOffsets"/> lists.</summary>
        public int FlagCount;

        /// <summary>For each column the condition names, the offset of its value.</summary>
        public int[] ValueOffsets;

        /// <summary>The offset of the folded column's value.</summary>
        public int SumOffset;
    }

    /// <summary>Works out where a scan finds its columns in a row image.</summary>
    /// <param name="ordinals">The columns the condition reads.</param>
    /// <param name="kinds">One letter per column, as for <see cref="PlanDirect"/>.</param>
    /// <param name="sumOrdinal">The folded column, or -1.</param>
    public ImageLayout PlanImage(int[] ordinals, string kinds, int sumOrdinal)
    {
        ImageLayout layout = default;
        if (!ImageSupported || !Fits(ordinals, kinds))
        {
            return layout;
        }

        int read = ordinals.Length + 1;
        if (_imageFlagOffsets.Length < read)
        {
            _imageFlagOffsets = new int[read];
            _imageValueOffsets = new int[read];
        }

        for (int i = 0; i < ordinals.Length; i++)
        {
            _imageFlagOffsets[i] = _imageOffsets[ordinals[i]];
            _imageValueOffsets[i] = _imageOffsets[ordinals[i]] + 1;
        }

        layout.FlagCount = ordinals.Length;
        if (sumOrdinal >= 0)
        {
            _imageFlagOffsets[layout.FlagCount++] = _imageOffsets[sumOrdinal];
            layout.SumOffset = _imageOffsets[sumOrdinal] + 1;
        }

        layout.Usable = true;
        layout.FlagOffsets = _imageFlagOffsets;
        layout.ValueOffsets = _imageValueOffsets;
        return layout;
    }

    private int[] _imageFlagOffsets = [];
    private int[] _imageValueOffsets = [];

    /// <summary>Whether the table's columns are of the kinds a condition was compiled to read.</summary>
    private bool Fits(int[] ordinals, string kinds)
    {
        if (kinds.Length != ordinals.Length)
        {
            return false;
        }

        for (int i = 0; i < ordinals.Length; i++)
        {
            StorageKind actual = _kinds[ordinals[i]];
            bool fits = kinds[i] switch
            {
                'i' => actual == StorageKind.Int,
                'l' => actual == StorageKind.BigInt,
                'f' => actual == StorageKind.Float,
                'd' => actual == StorageKind.Double,
                'b' => actual == StorageKind.Bit,
                'g' => actual == StorageKind.Guid,
                'n' => true,
                _ => false,
            };
            if (!fits)
            {
                return false;
            }
        }

        return true;
    }

    // The layout a direct scan checks each row against; rebuilt per scan in arrays the page keeps.
    private int[] _directFlagOffsets = [];
    private byte[] _directFlagExpected = [];
    private int[] _directValueOffsets = [];
    private int[] _directCellOrdinals = [];
    private byte[] _directCellTypes = [];

    /// <summary>What <see cref="PlanDirect"/> found: how a scan can read rows without building a <see cref="RowRef"/>.</summary>
    internal struct DirectLayout
    {
        /// <summary>Whether stored bytes can be read at fixed offsets.</summary>
        public bool Bytes;

        /// <summary>Whether typed rows can be read from their cells.</summary>
        public bool Cells;

        /// <summary>For each of the first <see cref="FlagCount"/> columns, where its null flag is when every cell before it is present.</summary>
        public int[] FlagOffsets;

        /// <summary>For each of those columns, the flag a row must hold: 0, or 1 for a clustered key, which is stored as NULL.</summary>
        public byte[] FlagExpected;

        /// <summary>How many leading columns the layout covers.</summary>
        public int FlagCount;

        /// <summary>The length of a row whose leading cells are all present.</summary>
        public int MinLength;

        /// <summary>For each column the condition names, the offset of its value.</summary>
        public int[] ValueOffsets;

        /// <summary>The offset of the summed column's value.</summary>
        public int SumOffset;

        /// <summary>The columns whose cells a typed row must hold, with <see cref="CellTypes"/>.</summary>
        public int[] CellOrdinals;

        /// <summary>The <see cref="CellType"/> each of those cells must have.</summary>
        public byte[] CellTypes;

        /// <summary>How many columns <see cref="CellOrdinals"/> lists.</summary>
        public int CellCount;
    }

    /// <summary>
    /// Works out whether rows of this table can be tested without a <see cref="RowRef"/>. Stored bytes qualify when
    /// every column up to the last one read has a fixed width: then, in any row whose leading cells are all
    /// present, each value sits at the same offset, and a row is checked by looking at its leading null flags.
    /// Typed rows qualify when each column read has a cell type of its own. Neither qualifies when the table's
    /// columns are not of the kinds the condition was compiled for.
    /// </summary>
    /// <param name="ordinals">The columns the condition reads.</param>
    /// <param name="kinds">One letter per column: <c>i l f d b g</c> for the type the condition reads, <c>n</c> for a null test only.</param>
    /// <param name="columns">One more than the last column read, summed column included.</param>
    /// <param name="sumOrdinal">The summed column, or -1.</param>
    public DirectLayout PlanDirect(int[] ordinals, string kinds, int columns, int sumOrdinal)
    {
        DirectLayout layout = default;
        if (!Fits(ordinals, kinds))
        {
            return layout;
        }

        if (_directFlagOffsets.Length < columns)
        {
            _directFlagOffsets = new int[columns];
            _directFlagExpected = new byte[columns];
        }

        int read = ordinals.Length + 1;
        if (_directValueOffsets.Length < read)
        {
            _directValueOffsets = new int[read];
            _directCellOrdinals = new int[read];
            _directCellTypes = new byte[read];
        }

        // Stored bytes: walk the leading columns as they lie when none is NULL.
        layout.Bytes = true;
        int offset = 0;
        for (int i = 0; i < columns; i++)
        {
            int width = RowSerializer.FixedWidth(_kinds[i]);
            _directFlagOffsets[i] = offset;
            if (i == _keyOrdinal)
            {
                _directFlagExpected[i] = 1; // a clustered key lives in the row key; its cell is stored as NULL
                offset += 1;
            }
            else if (width == 0)
            {
                layout.Bytes = false;
                break;
            }
            else
            {
                _directFlagExpected[i] = 0;
                offset += 1 + width;
            }
        }

        layout.Cells = true;
        for (int i = 0; i < ordinals.Length; i++)
        {
            layout.Bytes &= ordinals[i] != _keyOrdinal;
            _directValueOffsets[i] = _directFlagOffsets[ordinals[i]] + 1;
            layout.Cells &= TryCellType(_kinds[ordinals[i]], out _directCellTypes[i]);
            _directCellOrdinals[i] = ordinals[i];
        }

        layout.CellCount = ordinals.Length;
        if (sumOrdinal >= 0)
        {
            layout.Bytes &= sumOrdinal != _keyOrdinal;
            layout.SumOffset = _directFlagOffsets[sumOrdinal] + 1;
            layout.Cells &= TryCellType(_kinds[sumOrdinal], out _directCellTypes[layout.CellCount]);
            _directCellOrdinals[layout.CellCount++] = sumOrdinal;
        }

        layout.FlagOffsets = _directFlagOffsets;
        layout.FlagExpected = _directFlagExpected;
        layout.FlagCount = columns;
        layout.MinLength = offset;
        layout.ValueOffsets = _directValueOffsets;
        layout.CellOrdinals = _directCellOrdinals;
        layout.CellTypes = _directCellTypes;
        return layout;
    }

    /// <summary>The cell type a typed row holds for a column of this kind, when there is exactly one.</summary>
    private static bool TryCellType(StorageKind kind, out byte type)
    {
        CellType? cell = kind switch
        {
            StorageKind.Int => CellType.Int32,
            StorageKind.BigInt => CellType.Int64,
            StorageKind.Float or StorageKind.Double => CellType.Double,
            StorageKind.Bit => CellType.Boolean,
            StorageKind.Guid => CellType.Guid,
            StorageKind.Date => CellType.Date,
            StorageKind.String => CellType.String,
            _ => null,
        };
        type = (byte)(cell ?? CellType.Null);
        return cell is not null;
    }

    private int Measure(ReadOnlySpan<CellValue> row)
    {
        int length = 0;
        for (int i = 0; i < _kinds.Length; i++)
        {
            length += RowSerializer.MeasureCell(_kinds[i], i < row.Length ? row[i] : CellValue.Null);
        }

        return length;
    }

    private void Write(ReadOnlySpan<CellValue> row, Span<byte> destination)
    {
        int offset = 0;
        for (int i = 0; i < _kinds.Length; i++)
        {
            offset += RowSerializer.WriteCell(_kinds[i], i < row.Length ? row[i] : CellValue.Null, destination[offset..]);
        }
    }

    private Span<byte> Reserve(int length)
    {
        int needed = _used + length;
        if (needed > _bytes.Length)
        {
            int size = Math.Max(needed, _bytes.Length * 2);
            byte[] grown = new byte[size];
            Buffer.BlockCopy(_bytes, 0, grown, 0, _used);
            _bytes = grown;
        }

        return new Span<byte>(_bytes, _used, length);
    }

    private void Commit(RowKey key, int length)
    {
        int index = Count;
        int columns = _kinds.Length;
        if (!RowSerializer.TryLocateCells(new ReadOnlySpan<byte>(_bytes, _used, length), _kinds, new Span<int>(_cells, index * columns, columns)))
        {
            throw new InvalidDataException($"A stored row of {_table} does not match the table's columns.");
        }

        _starts[index] = _used;
        _lengths[index] = length;
        _keys[index] = key;
        _used += length;
        Count = index + 1;
        if (_hasText && _chars.Length < _used)
        {
            // Text decodes into the characters that line up with its own bytes, so the two buffers match in size.
            _chars = new char[_bytes.Length];
        }
    }

    private static CellValue ToCell(object? value) => value switch
    {
        float single => CellValue.From((double)single),
        _ => CellValue.From(value),
    };
}
