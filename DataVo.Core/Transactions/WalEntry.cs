using System.Buffers.Binary;

namespace DataVo.Core.Transactions;

/// <summary>
/// Identifies the type of physical data operation represented in the write-ahead log.
/// </summary>
public enum WalOperationType
{
    /// <summary>
    /// Represents an INSERT operation.
    /// </summary>
    Insert,

    /// <summary>
    /// Represents a DELETE operation.
    /// </summary>
    Delete,

    /// <summary>
    /// Represents an UPDATE operation.
    /// </summary>
    Update,
}

/// <summary>
/// Describes a single table-level data mutation stored inside a WAL transaction entry.
/// </summary>
/// <example>
/// <code>
/// var operation = new WalOperation
/// {
///     OperationType = WalOperationType.Insert,
///     TableName = "Users",
///     RowData = new Dictionary&lt;string, object?&gt;
///     {
///         ["Id"] = 1,
///         ["Name"] = "Alice"
///     }
/// };
/// </code>
/// </example>
public sealed class WalOperation
{
    /// <summary>
    /// Gets or sets the type of database change represented by this operation.
    /// </summary>
    public WalOperationType OperationType { get; set; }

    /// <summary>
    /// Gets or sets the target table name for the operation.
    /// </summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the affected row identifier for delete and update operations.
    /// </summary>
    public long? RowId { get; set; }

    /// <summary>
    /// Gets or sets the full row payload for insert operations.
    /// </summary>
    public Dictionary<string, object?>? RowData { get; set; }

    /// <summary>
    /// Gets or sets the partial column map for update operations.
    /// </summary>
    public Dictionary<string, object?>? UpdatedColumns { get; set; }
}

/// <summary>
/// Represents one committed transaction written to the write-ahead log.
/// </summary>
/// <remarks>
/// A <see cref="WalEntry"/> stores the database name, a durable transaction identifier,
/// the ordered list of operations to replay, and whether the entry has already been checkpointed.
/// </remarks>
/// <example>
/// <code>
/// WalEntry entry = WalEntry.FromTransactionContext("AppDb", transactionContext);
/// TransactionContext replayContext = entry.ToTransactionContext();
/// </code>
/// </example>
public sealed class WalEntry
{
    private const string VectorEnvelopeTypeKey = "__dvType";
    private const string VectorEnvelopeTypeValue = "vector-f32b64-v1";
    private const string VectorEnvelopeDimsKey = "dims";
    private const string VectorEnvelopeDataKey = "data";

    /// <summary>
    /// Gets or sets the unique identifier of the committed transaction.
    /// </summary>
    public Guid TransactionId { get; set; }

    /// <summary>
    /// Gets or sets the MVCC transaction identifier associated with the entry.
    /// </summary>
    public long MvccTransactionId { get; set; }

    /// <summary>
    /// Gets or sets the UTC timestamp of the transaction, expressed as <see cref="DateTime.Ticks"/>.
    /// </summary>
    public long Timestamp { get; set; }

    /// <summary>
    /// Gets or sets the database targeted by the transaction.
    /// </summary>
    public string DatabaseName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the ordered list of operations that make up the transaction.
    /// </summary>
    public List<WalOperation> Operations { get; set; } = [];

    /// <summary>
    /// Gets or sets a value indicating whether the transaction has already been checkpointed.
    /// </summary>
    public bool IsCheckpointed { get; set; }

    /// <summary>
    /// Creates a WAL entry from an in-memory transaction buffer.
    /// </summary>
    /// <param name="databaseName">The database associated with the transaction.</param>
    /// <param name="context">The buffered transaction context to serialize.</param>
    /// <returns>A new <see cref="WalEntry"/> containing all buffered operations.</returns>
    public static WalEntry FromTransactionContext(string databaseName, TransactionContext context)
    {
        var operations = new List<WalOperation>();

        AddInsertOperations(operations, context);
        AddDeleteOperations(operations, context);
        AddUpdateOperations(operations, context);

        return new WalEntry
        {
            TransactionId = Guid.NewGuid(),
            MvccTransactionId = context.TransactionId,
            Timestamp = DateTime.UtcNow.Ticks,
            DatabaseName = databaseName,
            Operations = operations,
            IsCheckpointed = false,
        };
    }

    /// <summary>
    /// Converts this WAL entry back into a replayable <see cref="TransactionContext"/>.
    /// </summary>
    /// <returns>A transaction buffer equivalent to the serialized operations.</returns>
    public TransactionContext ToTransactionContext()
    {
        var context = new TransactionContext
        {
            TransactionId = MvccTransactionId
        };

        foreach (var operation in Operations)
        {
            ApplyOperation(context, operation);
        }

        return context;
    }

    /// <summary>
    /// Adds insert operations from the buffered transaction into the WAL operation list.
    /// </summary>
    /// <param name="operations">The destination WAL operation list.</param>
    /// <param name="context">The source transaction context.</param>
    private static void AddInsertOperations(List<WalOperation> operations, TransactionContext context)
    {
        foreach (var (tableName, rows) in context.InsertedRows)
        {
            operations.AddRange(rows.Select(row => new WalOperation
            {
                OperationType = WalOperationType.Insert,
                TableName = tableName,
                RowData = CloneRow(row),
            }));
        }
    }

    /// <summary>
    /// Adds delete operations from the buffered transaction into the WAL operation list.
    /// </summary>
    /// <param name="operations">The destination WAL operation list.</param>
    /// <param name="context">The source transaction context.</param>
    private static void AddDeleteOperations(List<WalOperation> operations, TransactionContext context)
    {
        foreach (var (tableName, rowIds) in context.DeletedRowIds)
        {
            operations.AddRange(rowIds.Select(rowId => new WalOperation
            {
                OperationType = WalOperationType.Delete,
                TableName = tableName,
                RowId = rowId,
            }));
        }
    }

    /// <summary>
    /// Adds update operations from the buffered transaction into the WAL operation list.
    /// </summary>
    /// <param name="operations">The destination WAL operation list.</param>
    /// <param name="context">The source transaction context.</param>
    private static void AddUpdateOperations(List<WalOperation> operations, TransactionContext context)
    {
        foreach (var (tableName, updates) in context.UpdatedRows)
        {
            operations.AddRange(updates.Select(update => new WalOperation
            {
                OperationType = WalOperationType.Update,
                TableName = tableName,
                RowId = update.RowId,
                UpdatedColumns = CloneRow(update.UpdatedColumns),
            }));
        }
    }

    /// <summary>
    /// Applies a single serialized WAL operation back into a transaction context.
    /// </summary>
    /// <param name="context">The context receiving the replayed operation.</param>
    /// <param name="operation">The operation to apply.</param>
    private static void ApplyOperation(TransactionContext context, WalOperation operation)
    {
        switch (operation.OperationType)
        {
            case WalOperationType.Insert:
                context.BufferInsert(operation.TableName, NormalizeRow(operation.RowData));
                break;
            case WalOperationType.Delete:
                BufferDeleteIfPresent(context, operation);
                break;
            case WalOperationType.Update:
                BufferUpdateIfPresent(context, operation);
                break;
        }
    }

    /// <summary>
    /// Buffers a delete operation only when a row identifier is available.
    /// </summary>
    /// <param name="context">The context receiving the delete operation.</param>
    /// <param name="operation">The WAL operation to inspect.</param>
    private static void BufferDeleteIfPresent(TransactionContext context, WalOperation operation)
    {
        if (operation.RowId.HasValue)
        {
            context.BufferDelete(operation.TableName, operation.RowId.Value);
        }
    }

    /// <summary>
    /// Buffers an update operation only when a row identifier is available.
    /// </summary>
    /// <param name="context">The context receiving the update operation.</param>
    /// <param name="operation">The WAL operation to inspect.</param>
    private static void BufferUpdateIfPresent(TransactionContext context, WalOperation operation)
    {
        if (operation.RowId.HasValue)
        {
            context.BufferUpdate(operation.TableName, operation.RowId.Value, NormalizeRow(operation.UpdatedColumns));
        }
    }

    /// <summary>
    /// Creates a detached copy of a row dictionary suitable for JSON serialization.
    /// </summary>
    /// <param name="row">The row to clone.</param>
    /// <returns>A shallow copy of the row values.</returns>
    private static Dictionary<string, object?> CloneRow(Dictionary<string, object?> row)
    {
        return row.ToDictionary(pair => pair.Key, pair => PrepareValueForWalSerialization(pair.Value));
    }

    /// <summary>
    /// Converts a deserialized object dictionary into the dynamic shape expected by transaction replay.
    /// </summary>
    /// <param name="row">The raw row values read from JSON.</param>
    /// <returns>A normalized case-insensitive row dictionary.</returns>
    private static Dictionary<string, object?> NormalizeRow(Dictionary<string, object?>? row)
    {
        if (row == null)
        {
            return [];
        }

        var normalized = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, value) in row)
        {
            normalized[key] = NormalizeValue(value)!;
        }

        return normalized;
    }

    /// <summary>
    /// Normalizes JSON token types into plain CLR objects.
    /// </summary>
    /// <param name="value">The raw value to normalize.</param>
    /// <returns>A CLR value that can be replayed safely into storage logic.</returns>
    private static object? NormalizeValue(object? value)
    {
        // WalObjectConverter reads JSON into plain CLR shapes: primitives, Dictionary<string, object?>
        // (objects) and List<object?> (arrays). Decode the vector envelope / numeric sequences here.
        return value switch
        {
            IReadOnlyDictionary<string, object?> map => TryDecodeVectorEnvelope(map, out float[]? vector)
                ? vector
                : map.ToDictionary(pair => pair.Key, pair => NormalizeValue(pair.Value)),
            IReadOnlyList<object?> array => NormalizeArray(array),
            _ => value,
        };
    }

    private static object? NormalizeArray(IReadOnlyList<object?> array)
    {
        List<object?> values = array.Select(NormalizeValue).ToList();
        return TryConvertNumericSequenceToVector(values, out float[] vector)
            ? vector
            : values;
    }

    private static object? PrepareValueForWalSerialization(object? value)
    {
        if (TryCoerceRuntimeVector(value, out float[] vector))
        {
            return CreateVectorEnvelope(vector);
        }

        return value;
    }

    private static bool TryCoerceRuntimeVector(object? value, out float[] vector)
    {
        vector = [];

        switch (value)
        {
            case null:
                return false;
            case float[] floatArray:
                vector = [.. floatArray];
                return vector.Length > 0;
            case double[] doubleArray:
                vector = doubleArray.Select(item => (float)item).ToArray();
                return vector.Length > 0;
            case IEnumerable<float> floatEnumerable:
                vector = floatEnumerable.ToArray();
                return vector.Length > 0;
            case IEnumerable<double> doubleEnumerable:
                vector = doubleEnumerable.Select(item => (float)item).ToArray();
                return vector.Length > 0;
            default:
                return false;
        }
    }

    private static Dictionary<string, object> CreateVectorEnvelope(float[] vector)
    {
        byte[] payload = new byte[vector.Length * sizeof(int)];
        for (int i = 0; i < vector.Length; i++)
        {
            int bits = BitConverter.SingleToInt32Bits(vector[i]);
            BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(i * sizeof(int), sizeof(int)), bits);
        }

        return new Dictionary<string, object>(StringComparer.Ordinal)
        {
            [VectorEnvelopeTypeKey] = VectorEnvelopeTypeValue,
            [VectorEnvelopeDimsKey] = vector.Length,
            [VectorEnvelopeDataKey] = Convert.ToBase64String(payload)
        };
    }

    private static bool TryDecodeVectorEnvelope(IReadOnlyDictionary<string, object?> node, out float[]? vector)
    {
        vector = null;

        if (!node.TryGetValue(VectorEnvelopeTypeKey, out object? typeValue)
            || typeValue is not string type
            || !string.Equals(type, VectorEnvelopeTypeValue, StringComparison.Ordinal))
        {
            return false;
        }

        if (!node.TryGetValue(VectorEnvelopeDimsKey, out object? dimsValue)
            || !TryConvertToInt32(dimsValue, out int dims)
            || dims <= 0)
        {
            return false;
        }

        if (!node.TryGetValue(VectorEnvelopeDataKey, out object? dataValue)
            || dataValue is not string encoded
            || string.IsNullOrWhiteSpace(encoded))
        {
            return false;
        }

        byte[] payload;
        try
        {
            payload = Convert.FromBase64String(encoded);
        }
        catch
        {
            return false;
        }

        if (payload.Length != dims * sizeof(int))
        {
            return false;
        }

        float[] decoded = new float[dims];
        for (int i = 0; i < decoded.Length; i++)
        {
            int bits = BinaryPrimitives.ReadInt32LittleEndian(payload.AsSpan(i * sizeof(int), sizeof(int)));
            decoded[i] = BitConverter.Int32BitsToSingle(bits);
        }

        vector = decoded;
        return true;
    }

    private static bool TryConvertToInt32(object? value, out int result)
    {
        switch (value)
        {
            case int i:
                result = i;
                return true;
            case long l when l is >= int.MinValue and <= int.MaxValue:
                result = (int)l;
                return true;
            case double d when d == Math.Floor(d) && d is >= int.MinValue and <= int.MaxValue:
                result = (int)d;
                return true;
            default:
                result = 0;
                return false;
        }
    }

    private static bool TryConvertNumericSequenceToVector(IEnumerable<object?> values, out float[] vector)
    {
        List<float> parsed = [];
        foreach (object? item in values)
        {
            if (!TryConvertToFloat(item, out float value))
            {
                vector = [];
                return false;
            }

            parsed.Add(value);
        }

        vector = [.. parsed];
        return vector.Length > 0;
    }

    private static bool TryConvertToFloat(object? value, out float result)
    {
        switch (value)
        {
            case null:
                result = 0;
                return false;
            case float f:
                result = f;
                return true;
            case double d:
                result = (float)d;
                return true;
            case decimal m:
                result = (float)m;
                return true;
            case int i:
                result = i;
                return true;
            case long l:
                result = l;
                return true;
            case short s:
                result = s;
                return true;
            case byte b:
                result = b;
                return true;
            case uint ui:
                result = ui;
                return true;
            case ulong ul when ul <= float.MaxValue:
                result = ul;
                return true;
            case string text when float.TryParse(text, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out float parsed):
                result = parsed;
                return true;
            default:
                result = 0;
                return false;
        }
    }
}
