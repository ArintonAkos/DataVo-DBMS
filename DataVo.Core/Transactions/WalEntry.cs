using Newtonsoft.Json.Linq;

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
    /// <summary>
    /// Gets or sets the unique identifier of the committed transaction.
    /// </summary>
    public Guid TransactionId { get; set; }

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
        var context = new TransactionContext();

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
    private static Dictionary<string, object?> CloneRow(Dictionary<string, dynamic> row)
    {
        return row.ToDictionary(pair => pair.Key, pair => (object?)pair.Value);
    }

    /// <summary>
    /// Converts a deserialized object dictionary into the dynamic shape expected by transaction replay.
    /// </summary>
    /// <param name="row">The raw row values read from JSON.</param>
    /// <returns>A normalized case-insensitive row dictionary.</returns>
    private static Dictionary<string, dynamic> NormalizeRow(Dictionary<string, object?>? row)
    {
        if (row == null)
        {
            return [];
        }

        var normalized = new Dictionary<string, dynamic>(StringComparer.OrdinalIgnoreCase);
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
        return value switch
        {
            JValue jValue => jValue.Value,
            JObject jObject => jObject.Properties().ToDictionary(
                property => property.Name,
                property => NormalizeValue(property.Value)),
            JArray jArray => jArray.Select(NormalizeValue).ToList(),
            _ => value,
        };
    }
}
