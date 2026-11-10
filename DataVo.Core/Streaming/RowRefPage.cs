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
    public void Add(RowKey key, ReadOnlySpan<byte> stored, StoredRow? typed)
    {
        int length = typed is null ? stored.Length : Measure(typed);
        Span<byte> destination = Reserve(length);
        if (typed is null)
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

    private int Measure(StoredRow row)
    {
        int length = 0;
        for (int i = 0; i < _kinds.Length; i++)
        {
            length += RowSerializer.MeasureCell(_kinds[i], i < row.Count ? row[i] : CellValue.Null);
        }

        return length;
    }

    private void Write(StoredRow row, Span<byte> destination)
    {
        int offset = 0;
        for (int i = 0; i < _kinds.Length; i++)
        {
            offset += RowSerializer.WriteCell(_kinds[i], i < row.Count ? row[i] : CellValue.Null, destination[offset..]);
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
