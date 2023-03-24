using Server.Enums;
using Server.Models.Catalog;

namespace Server.Models.Factories
{
    internal class DataFactory
    {
        private Field Field { get; set; }
        private String? ColumnName { get; set; }
        private String? RawValue { get; set; }

        public static DataFactory Create()
        {
            return new DataFactory();
        }

        public DataFactory SetRawValue(String rawValue)
        {
            this.RawValue = rawValue;

            return this;
        }

        public DataFactory SetColumnName(String columnName)
        {
            ColumnName = columnName;

            return this;
        }

        public DataFactory LinkTable(string tableName, string databaseName)
        {
            if (ColumnName == null)
            {
                throw new Exception("Column name must be defined before ");
            }

            return this;
        }

        public Column Build()
        {
            if (Field == null)
            {
                throw new Exception("The table wasn't linked with this data!");
            }

            return new Column()
            {
                Value = ParseRawValue()
            };
        }

        private dynamic ParseRawValue()
        {
            try
            {
                switch (Field!.Type)
                {
                    case DataTypes.Varchar:
                        return RawValue![..Field.Length];
                    case DataTypes.Int:
                        return int.Parse(RawValue!);
                    case DataTypes.Float:
                        return float.Parse(RawValue!);
                    default:
                        throw new FormatException("Unsupported type!");
                }
            }
            catch (Exception)
            {
                throw new Exception($"Invalid datatype in column {ColumnName}");
            }
        }
    }
}