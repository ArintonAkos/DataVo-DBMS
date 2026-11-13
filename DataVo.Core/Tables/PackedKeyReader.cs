using DataVo.Core.Indexing;
using DataVo.Core.Models.Catalog;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Memory;

namespace DataVo.Core;

/// <summary>
/// Reads one row of an in-memory table by its integer primary key and builds the model straight from the
/// row's packed record: primary-key map, record, model. No statement runs, and no result, dictionary or boxed
/// value is made on the way.
/// </summary>
/// <typeparam name="T">The model type.</typeparam>
/// <remarks>
/// This is a shortcut, not a second query engine. It answers only when it can read the row exactly as the
/// prepared statement for the same lookup would, and otherwise reports that it did not, so the caller runs the
/// statement. It steps aside when the session is in a transaction, a snapshot is being read, rows are write
/// locked, authorization or diagnostics are on, the table is not kept in memory, its primary key is not the
/// model's integer key, the stored columns are not the model's, the key is not in the primary-key map, or the
/// row is serialized or kept apart rather than packed.
/// <para>
/// The table and its key map are found by name once and kept. Nothing is locked for a read, so both can be
/// replaced under the reader, by a dropped or recreated table, a rebuilt index or a restored snapshot. The
/// reader does not try to hear of that; it checks every row it reads instead. A row counts only if, after
/// it was read, it is still live, it holds the key that was asked for, and its table is still the engine's
/// table. Whatever fails that check is looked up afresh once and otherwise left to the statement, so
/// something stale can cost a lookup its shortcut but cannot give it a wrong row. A row removed while it
/// is being read is discarded the same way, never returned half read; see <see cref="FlatRowStore"/>.
/// </para>
/// </remarks>
internal sealed class PackedKeyReader<T>
{
    private readonly DataVoEngine _engine;
    private readonly Guid _session;
    private readonly string _tableName;
    private readonly string[] _columnNames;
    private readonly string[] _columnTypes;
    private readonly string _keyColumn;
    private readonly DataVoPackedReader<T> _read;
    private volatile Source? _source;
    private volatile LayoutBinding? _layout;

    /// <summary>Creates a reader for one model in one session.</summary>
    /// <param name="engine">The engine that holds the table.</param>
    /// <param name="session">The session whose selected database and transaction state apply.</param>
    /// <param name="tableName">The table name as the model gives it.</param>
    /// <param name="columnNames">The model's columns, in the order the generated reader numbers them.</param>
    /// <param name="columnTypes">The SQL type of each of those columns.</param>
    /// <param name="keyColumn">The model's primary-key column.</param>
    /// <param name="read">The generated reader.</param>
    public PackedKeyReader(
        DataVoEngine engine,
        Guid session,
        string tableName,
        string[] columnNames,
        string[] columnTypes,
        string keyColumn,
        DataVoPackedReader<T> read)
    {
        _engine = engine;
        _session = session;
        _tableName = tableName;
        _columnNames = columnNames;
        _columnTypes = columnTypes;
        _keyColumn = keyColumn;
        _read = read;
    }

    /// <summary>Reads the row with a primary key.</summary>
    /// <param name="key">The key.</param>
    /// <param name="item">The model built from the row.</param>
    /// <returns>
    /// <see langword="false"/> when the shortcut does not apply or found no row; the caller then runs the
    /// statement, which decides whether the row exists.
    /// </returns>
    public bool TryRead(long key, out T item)
    {
        item = default!;
        if (MustUseStatement() || _engine.Sessions.Get(_session) is not { Length: > 0 } databaseName)
        {
            return false;
        }

        Source? source = _source;
        if (source is null || !string.Equals(source.DatabaseName, databaseName, StringComparison.Ordinal))
        {
            _source = source = FindSource(databaseName);
        }

        if (TryReadFrom(source, key, out item))
        {
            return true;
        }

        // What was kept did not answer. Usually the key is simply not there, or its row is not packed, and
        // the statement takes over. But the table or its key map may have appeared or been replaced: then
        // find both again and try once more.
        if (IsCurrent(source))
        {
            return false;
        }

        _source = source = FindSource(databaseName);
        return TryReadFrom(source, key, out item);
    }

    /// <summary>
    /// Whether what was found for a table still stands. For a table and key map that were found: they are
    /// still the ones the engine uses. For a table that could not be served: its schema has not changed, and
    /// if all it lacked was rows or a key map, it still lacks them. Looks names up but allocates nothing, so a
    /// lookup that keeps missing costs no more than this check before its statement.
    /// </summary>
    private bool IsCurrent(Source source)
    {
        if (source.Table is not null)
        {
            return source.Table.CurrentRows is not null
                && _engine.IndexManager.TryGetIntegerPrimaryKeyMap(source.IndexName, source.StoredTable, source.DatabaseName, out ConcurrentInt64Int64Map? keys)
                && ReferenceEquals(keys, source.Keys);
        }

        if (_engine.Catalog.GetTableSchemaVersion(_tableName, source.DatabaseName) != source.SchemaVersion)
        {
            return false;
        }

        return !source.HasModelKey
            || _engine.StorageContext.PackedTableOf(source.StoredTable, source.DatabaseName) is null
            || !_engine.IndexManager.TryGetIntegerPrimaryKeyMap(source.IndexName, source.StoredTable, source.DatabaseName, out _);
    }

    /// <summary>Whether something about the engine or the session calls for the statement's own handling.</summary>
    private bool MustUseStatement() =>
        _engine.Config.EnableAuthorization
        || _engine.Diagnostics.Enabled
        || _engine.TransactionManager.HasActiveTransaction(_session)
        || _engine.LockManager.HasActiveRowWriteLocks
        || MvccExecutionScope.CurrentSnapshot is not null;

    /// <summary>
    /// Looks the key up in a kept key map and builds the model from the packed row it names. The result is
    /// kept only when it is proven right after the fact: the row is still live, it holds the key that was
    /// asked for, and its table is still the engine's table. A stale map or a table replaced meanwhile
    /// therefore yields nothing rather than a wrong row.
    /// </summary>
    private bool TryReadFrom(Source source, long key, out T item)
    {
        item = default!;
        if (source.Table?.CurrentRows is not { Layout: { } layout } rows
            || FieldsFor(layout, rows.Schema!) is not { } binding
            || !source.Keys!.TryGetValue(key, out long rowId)
            || rowId < 1
            || rowId > rows.Count)
        {
            return false;
        }

        int position = (int)(rowId - 1);
        if (!rows.TryReadPacked(position, out FlatRow row) || binding.KeyOf(row.Record) != key)
        {
            return false;
        }

        try
        {
            item = _read(new DataVoPackedRow(row, binding.Fields!)); // FieldsFor returns only bindings that have fields
        }
        catch (Exception) when (!rows.IsTypedLive(position))
        {
            // The row was removed while it was read, so a string it had let go of looked like a NULL.
            return false;
        }

        return rows.IsTypedLive(position) && source.Table.CurrentRows is not null;
    }

    /// <summary>
    /// Finds the table and its key map by name. The result has neither when the table is not one this reader
    /// can serve: it does not exist, is not kept in memory, has no rows yet, or its primary key is not the
    /// model's integer key. It is kept all the same, so that is not worked out again for every lookup.
    /// </summary>
    private Source FindSource(string databaseName)
    {
        int schemaVersion = _engine.Catalog.GetTableSchemaVersion(_tableName, databaseName);
        string? storedTable = _engine.Catalog.FindTableName(_tableName, databaseName);
        if (storedTable is null || !HasModelKey(storedTable, databaseName))
        {
            return new Source(databaseName, storedTable ?? _tableName, schemaVersion, hasModelKey: false, null, null);
        }

        IPackedTable? table = _engine.StorageContext.PackedTableOf(storedTable, databaseName);
        if (table is null || !_engine.IndexManager.TryGetIntegerPrimaryKeyMap($"_PK_{storedTable}", storedTable, databaseName, out ConcurrentInt64Int64Map? keys))
        {
            return new Source(databaseName, storedTable, schemaVersion, hasModelKey: true, null, null);
        }

        return new Source(databaseName, storedTable, schemaVersion, hasModelKey: true, table, keys);
    }

    /// <summary>Whether the table's primary key is the model's key column alone, and an integer.</summary>
    private bool HasModelKey(string storedTable, string databaseName)
    {
        List<string> primaryKeys = _engine.Catalog.GetTablePrimaryKeys(storedTable, databaseName);
        if (primaryKeys.Count != 1 || !primaryKeys[0].Equals(_keyColumn, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        foreach (Column column in _engine.Catalog.GetTableColumns(storedTable, databaseName))
        {
            if (column.Name.Equals(_keyColumn, StringComparison.OrdinalIgnoreCase))
            {
                return ColumnCellTypes.TryGet(column.Type, out CellType type) && type is CellType.Int32 or CellType.Int64;
            }
        }

        return false;
    }

    /// <summary>
    /// Where each model column is in records of a layout; null when the stored rows do not hold the model's
    /// columns with the model's types. Worked out once per layout: a table gets a new one when it is recreated.
    /// </summary>
    private LayoutBinding? FieldsFor(RowLayout layout, ReactiveRowSchema schema)
    {
        LayoutBinding? binding = _layout;
        if (binding is null || !ReferenceEquals(binding.Layout, layout))
        {
            _layout = binding = new LayoutBinding(layout, BindFields(layout, schema), KeyFieldOf(layout, schema));
        }

        return binding.Fields is null ? null : binding;
    }

    private PackedField[]? BindFields(RowLayout layout, ReactiveRowSchema schema)
    {
        var fields = new PackedField[_columnNames.Length];
        for (int i = 0; i < fields.Length; i++)
        {
            if (!schema.TryGetOrdinal(_columnNames[i], out int ordinal)
                || ordinal >= layout.ColumnCount
                || !ColumnCellTypes.TryGet(BaseType(_columnTypes[i]), out CellType expected)
                || layout[ordinal].Type != expected)
            {
                return null;
            }

            fields[i] = new PackedField(layout[ordinal].Position, layout.NullsOffset + (ordinal >> 3), (byte)(1 << (ordinal & 7)));
        }

        return fields;
    }

    /// <summary>Where the key column is in records of a layout; the default when the layout has no such integer column.</summary>
    private RowField KeyFieldOf(RowLayout layout, ReactiveRowSchema schema) =>
        schema.TryGetOrdinal(_keyColumn, out int ordinal) && ordinal < layout.ColumnCount && layout[ordinal].Type is CellType.Int32 or CellType.Int64
            ? layout[ordinal]
            : default;

    /// <summary>A SQL type without its length: <c>VECTOR(3)</c> is <c>VECTOR</c>.</summary>
    private static string BaseType(string sqlType)
    {
        int parenthesis = sqlType.IndexOf('(');
        return parenthesis < 0 ? sqlType : sqlType.Substring(0, parenthesis);
    }

    /// <summary>
    /// What was found for the table in one database, kept for the lookups that follow: the table and its key
    /// map, or the fact that it cannot be served and why.
    /// </summary>
    private sealed class Source(string databaseName, string storedTable, int schemaVersion, bool hasModelKey, IPackedTable? table, ConcurrentInt64Int64Map? keys)
    {
        public readonly string DatabaseName = databaseName;
        public readonly string StoredTable = storedTable;
        public readonly string IndexName = $"_PK_{storedTable}";

        /// <summary>The version of the table's schema this was found for.</summary>
        public readonly int SchemaVersion = schemaVersion;

        /// <summary>Whether the table exists and its primary key is the model's integer key.</summary>
        public readonly bool HasModelKey = hasModelKey;

        /// <summary>The table's packed rows; null when it cannot be served.</summary>
        public readonly IPackedTable? Table = table;

        /// <summary>The table's key map; null exactly when <see cref="Table"/> is.</summary>
        public readonly ConcurrentInt64Int64Map? Keys = keys;
    }

    /// <summary>The model's fields in one layout; <see cref="Fields"/> is null when the model does not fit it.</summary>
    private sealed class LayoutBinding(RowLayout layout, PackedField[]? fields, RowField key)
    {
        public readonly RowLayout Layout = layout;
        public readonly PackedField[]? Fields = fields;

        /// <summary>The key a record holds; a value no key equals when the layout has no integer key column.</summary>
        public long KeyOf(ReadOnlySpan<byte> record) => key.Type switch
        {
            CellType.Int32 => FlatRowCodec.ReadInt32(record, key.Position),
            CellType.Int64 => FlatRowCodec.ReadInt64(record, key.Position),
            _ => throw new InvalidOperationException("The table's rows hold no integer key column."),
        };
    }
}
