using System.Globalization;
using DataVo.Core.BTree;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.Transactions;

/// <summary>
/// The key locks one statement holds on one table: values of the primary key and of unique columns that an INSERT
/// gives its row, or that an UPDATE gives a row in place of its old ones. A writer reserves a value before it
/// checks whether the value is taken and keeps it until its row is published, so two writers giving rows the same
/// value check one after the other, and the second sees the first's row.
/// </summary>
/// <remarks>
/// <para>
/// Lock identity is a function of the value's index key string (see <see cref="IndexKeyEncoder"/>), which INSERT and
/// UPDATE build the same way: an integer key (<c>[42]</c>) is locked by its number, any other key by its string. An
/// integer cell is locked by its number directly, without building the string.
/// </para>
/// <para>
/// A statement reserves its values in a fixed order (the primary key, then each unique column in catalog order), so
/// two single-row inserts never wait for each other in a cycle. A value is reserved once per statement: the locks
/// cannot be re-entered.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// KeyLockSet keys = KeyLockSet.Rent(locks, "Shop", "Users", takeTableIntent: true);
/// try
/// {
///     keys.ReserveInsertKeys(catalog, schema, cells);
///     // validate the keys, write and publish the row
/// }
/// finally
/// {
///     keys.Release();
/// }
/// </code>
/// </example>
internal sealed class KeyLockSet
{
    /// <summary>The lock scope of primary-key values; no column can have this name.</summary>
    internal const string PrimaryKeyScope = "\0primary-key";

    [ThreadStatic] private static KeyLockSet? t_spare;

    private readonly List<(string Scope, long Number, string? Text)> _held = [];
    private LockManager _locks = null!;
    private string _databaseName = string.Empty;
    private string _tableName = string.Empty;
    private bool _holdsTableIntent;
    private bool _reusable;

    private KeyLockSet()
    {
    }

    /// <summary>
    /// Starts a key lock set for a single-row INSERT, reusing the thread's spare set. With
    /// <paramref name="takeTableIntent"/> it first takes the table's intent lock, which keeps DDL and other exclusive
    /// table writers out for as long as the set is held.
    /// </summary>
    /// <param name="locks">The engine's lock manager.</param>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="takeTableIntent">Whether to take the table's intent lock.</param>
    /// <returns>The set; release it with <see cref="Release"/>.</returns>
    public static KeyLockSet Rent(LockManager locks, string databaseName, string tableName, bool takeTableIntent)
    {
        KeyLockSet set = t_spare ?? new KeyLockSet();
        t_spare = null;
        set._reusable = true;
        set.Start(locks, databaseName, tableName);
        if (takeTableIntent)
        {
            set._holdsTableIntent = locks.AcquireTableIntentLock(databaseName, tableName);
        }

        return set;
    }

    /// <summary>
    /// Creates a key lock set for an UPDATE that already holds row write locks (and through them the table's intent
    /// lock) on the table.
    /// </summary>
    /// <param name="locks">The engine's lock manager.</param>
    /// <param name="databaseName">The database name.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>The set; release it with <see cref="Release"/> once the update is published.</returns>
    public static KeyLockSet ForUpdate(LockManager locks, string databaseName, string tableName)
    {
        var set = new KeyLockSet();
        set.Start(locks, databaseName, tableName);
        return set;
    }

    /// <summary>
    /// Reserves the primary-key value and the non-NULL unique values of a typed row about to be inserted. A key with a
    /// NULL part is not reserved: the insert's validation rejects it, or a NULL unique value never clashes.
    /// </summary>
    /// <param name="catalog">The engine's catalog.</param>
    /// <param name="schema">The column names of <paramref name="cells"/>.</param>
    /// <param name="cells">The row's cells.</param>
    public void ReserveInsertKeys(EngineCatalog catalog, ReactiveRowSchema schema, ReadOnlySpan<CellValue> cells)
    {
        TableValidationMetadata metadata = catalog.GetTableValidationMetadata(_tableName, _databaseName);
        IReadOnlyList<string> primaryKeys = metadata.PrimaryKeys;
        if (primaryKeys.Count == 1)
        {
            if (schema.TryGetOrdinal(primaryKeys[0], out int ordinal) && !cells[ordinal].IsNull)
            {
                ReserveCell(PrimaryKeyScope, cells[ordinal]);
            }
        }
        else if (primaryKeys.Count > 1 && IndexKeyEncoder.HasCompleteKey(schema, cells, primaryKeys))
        {
            Reserve(PrimaryKeyScope, IndexKeyEncoder.BuildKeyString(schema, cells, primaryKeys));
        }

        IReadOnlyList<string> uniqueKeys = metadata.UniqueKeys;
        for (int i = 0; i < uniqueKeys.Count; i++)
        {
            if (schema.TryGetOrdinal(uniqueKeys[i], out int ordinal) && !cells[ordinal].IsNull)
            {
                ReserveCell(uniqueKeys[i], cells[ordinal]);
            }
        }
    }

    /// <summary>Reserves a key value given as its index key string.</summary>
    /// <param name="scope">The constraint: <see cref="PrimaryKeyScope"/> or a unique column's name.</param>
    /// <param name="encodedKey">The value's index key string.</param>
    public void Reserve(string scope, string encodedKey)
    {
        if (TryParseIntegerKey(encodedKey, out long number))
        {
            ReserveNumber(scope, number);
            return;
        }

        if (IsHeld(scope, 0, encodedKey))
        {
            return;
        }

        if (_locks.AcquireKeyLock(_databaseName, _tableName, scope, encodedKey))
        {
            _held.Add((scope, 0, encodedKey));
        }
    }

    /// <summary>
    /// Releases every key lock, newest first, then the table intent lock. Inside a durable write batch the releases
    /// wait for publication. The set must not be used afterwards.
    /// </summary>
    public void Release()
    {
        List<Exception>? errors = null;
        for (int i = _held.Count - 1; i >= 0; i--)
        {
            (string scope, long number, string? text) = _held[i];
            try
            {
                if (text is null)
                {
                    _locks.ReleaseKeyLock(_databaseName, _tableName, scope, number);
                }
                else
                {
                    _locks.ReleaseKeyLock(_databaseName, _tableName, scope, text);
                }
            }
            catch (Exception ex)
            {
                (errors ??= []).Add(ex);
            }
        }

        _held.Clear();
        if (_holdsTableIntent)
        {
            _holdsTableIntent = false;
            _locks.ReleaseTableIntentLock(_databaseName, _tableName, acquired: true);
        }

        if (_reusable)
        {
            _reusable = false;
            _locks = null!;
            t_spare = this;
        }

        if (errors is not null)
        {
            throw new AggregateException(errors);
        }
    }

    /// <summary>
    /// Reads an index key string as an integer key: <c>[n]</c>, the form integer, integral decimal and numeric
    /// string values take (see <see cref="IndexKeyEncoder"/>).
    /// </summary>
    internal static bool TryParseIntegerKey(string encodedKey, out long number)
    {
        number = 0;
        if (encodedKey.Length < 3 || encodedKey[0] != '[' || encodedKey[encodedKey.Length - 1] != ']')
        {
            return false;
        }

        // Only the canonical form an integer formats to ("[-12]", never "[012]", "[+12]" or "[-0]"), so one
        // value has exactly one lock whichever way it reached its key string.
        ReadOnlySpan<char> digits = encodedKey.AsSpan(1, encodedKey.Length - 2);
        ReadOnlySpan<char> magnitude = digits[0] == '-' ? digits[1..] : digits;
        if (magnitude.IsEmpty
            || (magnitude[0] == '0' && (magnitude.Length > 1 || digits.Length != magnitude.Length)))
        {
            return false;
        }

        foreach (char digit in magnitude)
        {
            if (digit is < '0' or > '9')
            {
                return false;
            }
        }

        return long.TryParse(digits, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out number);
    }

    private void Start(LockManager locks, string databaseName, string tableName)
    {
        _locks = locks;
        _databaseName = databaseName;
        _tableName = tableName;
        _holdsTableIntent = false;
        _held.Clear();
    }

    private void ReserveCell(string scope, CellValue cell)
    {
        switch (cell.Type)
        {
            case CellType.Int32:
                ReserveNumber(scope, cell.AsInt32()); // encodes as [n]: the same lock as the key string
                return;
            case CellType.Int64:
                ReserveNumber(scope, cell.AsInt64());
                return;
            default:
                Reserve(scope, IndexKeyEncoder.BuildCellKeyString(cell));
                return;
        }
    }

    private void ReserveNumber(string scope, long number)
    {
        if (IsHeld(scope, number, null))
        {
            return;
        }

        if (_locks.AcquireKeyLock(_databaseName, _tableName, scope, number))
        {
            _held.Add((scope, number, null));
        }
    }

    private bool IsHeld(string scope, long number, string? text)
    {
        foreach ((string heldScope, long heldNumber, string? heldText) in _held)
        {
            if (heldNumber == number
                && string.Equals(heldText, text, StringComparison.Ordinal)
                && string.Equals(heldScope, scope, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
}
