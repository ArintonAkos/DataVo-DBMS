using DataVo.Core.Contracts;

namespace DataVo.Core.Services
{
    internal class TableColumnService
    {
        public static bool IsNumeric(IColumn column)
        {
            return IsInteger(column) || IsFloat(column);
        }

        public static bool IsInteger(IColumn column)
        {
            string type = column.RawType().ToLower();

            return type is "int" or "bigint";
        }

        public static bool IsFloat(IColumn column)
        {
            string type = column.RawType().ToLower();

            return type is "float" or "double";
        }

        public static bool IsBoolean(IColumn column)
        {
            string type = column.RawType().ToLower();

            return (type == "bit");
        }

        public static bool IsString(IColumn column)
        {
            string type = column.RawType().ToLower();

            return (type == "varchar");
        }
        
        public static bool IsDate(IColumn column)
        {
            string type = column.RawType().ToLower();

            return (type == "date");
        }
    }
}
