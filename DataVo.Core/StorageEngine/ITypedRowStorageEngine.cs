using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.StorageEngine;

/// <summary>
/// Receives the cells of one typed row. The cells are borrowed for the call: read them at once and do not keep
/// the span.
/// </summary>
/// <returns><see langword="false"/> to stop a visit of several rows; ignored for a single row.</returns>
internal delegate bool TypedCellsVisitor<TState>(ref TState state, long rowId, ReadOnlySpan<CellValue> cells);

/// <summary>
/// A storage engine that keeps rows inserted as typed cells in a typed form of its own, rather than as
/// serialized bytes, and reads them back as cells.
/// </summary>
internal interface ITypedRowStorageEngine
{
    /// <summary>Inserts one typed row.</summary>
    /// <param name="databaseName">The owning database.</param>
    /// <param name="tableName">The table.</param>
    /// <param name="columns">The table's columns in catalog order, which give the rows their stored shape.</param>
    /// <param name="row">The row. Its cells are copied; the engine keeps no reference to it.</param>
    /// <returns>The row's 1-based id.</returns>
    long InsertTypedRow(string databaseName, string tableName, IReadOnlyList<Column> columns, StoredRow row);

    /// <summary>Inserts typed rows; see <see cref="InsertTypedRow"/>.</summary>
    /// <returns>The rows' 1-based ids, in the order given.</returns>
    List<long> InsertTypedRows(string databaseName, string tableName, IReadOnlyList<Column> columns, IReadOnlyList<StoredRow> rows);

    /// <summary>Copies a live typed row out as an object of its own. Allocates; prefer <see cref="TryVisitTypedCells"/>.</summary>
    bool TryReadTypedRow(string databaseName, string tableName, long rowId, out StoredRow? row);

    /// <summary>Whether a row id names a live typed row.</summary>
    bool HasTypedRow(string databaseName, string tableName, long rowId);

    /// <summary>Hands the cells of one live typed row to a visitor, without a lock and without allocating.</summary>
    /// <returns><see langword="false"/>, without calling the visitor, when the id names no live typed row.</returns>
    bool TryVisitTypedCells<TState>(string databaseName, string tableName, long rowId, ref TState state, TypedCellsVisitor<TState> visit);

    /// <summary>
    /// Hands the cells of each live typed row to a visitor in row id order, without a lock and without
    /// allocating, until the visitor returns <see langword="false"/>. Rows added after the visit began are
    /// not visited.
    /// </summary>
    void VisitTypedRows<TState>(string databaseName, string tableName, ref TState state, TypedCellsVisitor<TState> visit);

    /// <summary>Copies every live typed row out as an object of its own, in row id order. Allocates per row.</summary>
    IEnumerable<(long RowId, StoredRow Row)> ReadAllTypedRows(string databaseName, string tableName);

    /// <summary>Whether the table holds any live row, typed or serialized.</summary>
    bool HasAnyRows(string databaseName, string tableName);
}
