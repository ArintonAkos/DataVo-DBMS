using System.Text;
using DataVo.Core.Models.Catalog;

namespace DataVo.Core.StorageEngine.Serialization;

public static class RowSerializer
{
    /// <summary>
    /// Serializes a dictionary of column names and values into a tight binary format
    /// based on the schema order defined in the table's Catalog.
    /// </summary>
    public static byte[] Serialize(string databaseName, string tableName, Dictionary<string, dynamic> row)
    {
        var columns = Catalog.GetTableColumns(tableName, databaseName);
        using var memoryStream = new MemoryStream();
        using var writer = new BinaryWriter(memoryStream, Encoding.UTF8, leaveOpen: true);

        foreach (var column in columns)
        {
            // Null check (implementing basic null bitmap or prefix later if necessary, 
            // for now assume required matching the old mongodb behavior)
            if (!row.TryGetValue(column.Name, out var value) || value == null)
            {
                // Write a boolean "is null" flag for every field
                writer.Write(true);
                continue;
            }

            writer.Write(false); // Not null flag

            string type = column.Type.ToUpperInvariant();
            if (type == "INT")
            {
                writer.Write(Convert.ToInt32(value));
            }
            else if (type == "FLOAT")
            {
                writer.Write(Convert.ToSingle(value));
            }
            else if (type == "BIT")
            {
                writer.Write(Convert.ToBoolean(value));
            }
            else if (type == "DATE" || type == "DATETIME")
            {
                if (value is DateOnly dateOnly)
                {
                    writer.Write(dateOnly.ToDateTime(TimeOnly.MinValue).ToBinary());
                }
                else if (value is DateTime dateTime)
                {
                    writer.Write(dateTime.ToBinary());
                }
                else
                {
                    writer.Write(Convert.ToDateTime(value).ToBinary());
                }
            }
            else // VARCHAR etc
            {
                writer.Write(value!.ToString() ?? "");
            }
        }

        writer.Flush();
        return memoryStream.ToArray();
    }

    /// <summary>
    /// Deserializes a raw binary payload back into a dictionary of column names and typed values
    /// using the schema defined in the table's Catalog.
    /// </summary>
    public static Dictionary<string, dynamic> Deserialize(string databaseName, string tableName, byte[] data)
    {
        var columns = Catalog.GetTableColumns(tableName, databaseName);
        var row = new Dictionary<string, dynamic>();

        using var memoryStream = new MemoryStream(data);
        using var reader = new BinaryReader(memoryStream, Encoding.UTF8, leaveOpen: true);

        foreach (var column in columns)
        {
            bool isNull = reader.ReadBoolean();
            if (isNull)
            {
                row[column.Name] = null!;
                continue;
            }

            string type = column.Type.ToUpperInvariant();
            if (type == "INT")
            {
                row[column.Name] = reader.ReadInt32();
            }
            else if (type == "FLOAT")
            {
                row[column.Name] = reader.ReadSingle();
            }
            else if (type == "BIT")
            {
                row[column.Name] = reader.ReadBoolean();
            }
            else if (type == "DATE")
            {
                row[column.Name] = DateOnly.FromDateTime(DateTime.FromBinary(reader.ReadInt64()));
            }
            else if (type == "DATETIME")
            {
                row[column.Name] = DateTime.FromBinary(reader.ReadInt64());
            }
            else // VARCHAR
            {
                row[column.Name] = reader.ReadString();
            }
        }

        return row;
    }
}
