using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.StorageEngine;

/// <summary>
/// The one mapping from a catalog column type to the type of the cells a typed row holds for it. Typed inserts
/// validate cells against it and typed storage lays rows out by it, so the two always agree.
/// </summary>
internal static class ColumnCellTypes
{
    /// <summary>Finds the cell type of a column type's non-null values.</summary>
    /// <param name="columnType">The catalog type name, such as <c>INT</c> or <c>VARCHAR</c>; case is ignored.</param>
    /// <param name="cellType">The cell type, when the column type can be held in typed rows.</param>
    /// <returns><see langword="false"/> for a column type typed rows do not support.</returns>
    public static bool TryGet(string columnType, out CellType cellType)
    {
        // Compared name by name rather than through an upper-cased copy: this runs for every cell of a typed
        // insert and must not allocate.
        cellType =
            Is(columnType, "INT") ? CellType.Int32 :
            Is(columnType, "BIGINT") ? CellType.Int64 :
            Is(columnType, "FLOAT") || Is(columnType, "DOUBLE") ? CellType.Double :
            Is(columnType, "BIT") ? CellType.Boolean :
            Is(columnType, "DATE") ? CellType.Date :
            Is(columnType, "GUID") ? CellType.Guid :
            Is(columnType, "VARCHAR") ? CellType.String :
            Is(columnType, "VECTOR") ? CellType.Vector :
            CellType.Null;
        return cellType != CellType.Null;
    }

    private static bool Is(string columnType, string name) => columnType.Equals(name, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The cell type of each column, for laying out a table's typed rows. A column of a type typed rows do not
    /// support is treated as a string column: no cell of such a column passes typed-insert validation, and a
    /// row that reached storage anyway is kept as it is rather than packed.
    /// </summary>
    public static CellType[] ForLayout(IReadOnlyList<Column> columns)
    {
        var types = new CellType[columns.Count];
        for (int i = 0; i < types.Length; i++)
        {
            types[i] = TryGet(columns[i].Type, out CellType type) ? type : CellType.String;
        }

        return types;
    }
}
