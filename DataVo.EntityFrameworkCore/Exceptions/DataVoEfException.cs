namespace DataVo.EntityFrameworkCore;

/// <summary>
/// Exception thrown by the DataVo EF Core integration layer when a bridge operation fails.
/// </summary>
public class DataVoEfException : Exception
{
    /// <summary>The operation that was being performed when the failure occurred.</summary>
    public DataVoEfOperation Operation { get; }

    public DataVoEfException(DataVoEfOperation operation, string message)
        : base(message)
    {
        Operation = operation;
    }

    public DataVoEfException(DataVoEfOperation operation, string message, Exception inner)
        : base(message, inner)
    {
        Operation = operation;
    }
}

/// <summary>Identifies which EF bridge operation raised a <see cref="DataVoEfException"/>.</summary>
public enum DataVoEfOperation
{
    /// <summary>Schema creation via <c>EnsureCreated</c>.</summary>
    SchemaCreation,

    /// <summary>Schema deletion via <c>EnsureDeleted</c>.</summary>
    SchemaDeletion,

    /// <summary>Bulk data loading from DataVo into the EF change tracker.</summary>
    DataLoad,

    /// <summary>An <c>INSERT</c> generated from a tracked <c>Added</c> entity.</summary>
    Insert,

    /// <summary>An <c>UPDATE</c> generated from a tracked <c>Modified</c> entity.</summary>
    Update,

    /// <summary>A <c>DELETE</c> generated from a tracked <c>Deleted</c> entity.</summary>
    Delete,

    /// <summary>A raw SQL command routed to DataVo through the bridge.</summary>
    RawSql,

    /// <summary>Connection validation (<c>CanConnect</c>).</summary>
    Connection,
}
