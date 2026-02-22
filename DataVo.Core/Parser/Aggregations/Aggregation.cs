using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.Types;
using DataVo.Core.Services;

namespace DataVo.Core.Parser.Aggregations
{
    public abstract class Aggregation(Column field)
    {
        protected readonly Column _field = field;

        public string ColumnName
        {
            get
            {
                return $"{_field.TableName}.{_field.ColumnName}";
            }
        }

        public virtual string ClassName
        {
            get
            {
                return GetType().Name.ToUpper();
            }
        }

        public static string HASH_VALUE
        {
            get
            {
                return string.Empty;
            }
        }

        public dynamic? Execute(ListedTable rows)
        {
            Validate();

            return Apply(rows);
        }

        protected virtual void Validate()
        {
            // By default we do not validate anything
        }

        protected abstract dynamic? Apply(ListedTable rows);

        public virtual string GetHeaderName()
        {
            return $"{ClassName}({ColumnName})";
        }

        protected dynamic? SelectColumn(JoinedRow row)
        {
            return row[_field.TableName][_field.ColumnName];
        }

        protected T SelectColumn<T>(JoinedRow row)
        {
            try
            {
                return row[_field.TableName][_field.ColumnName];
            }
            catch (InvalidCastException)
            {
                throw new Exception($"Wrong aggregation ({ClassName}) called on {ColumnName} fields data type!");
            }
        }

        protected void ValidateNumericColumn()
        {
            if (!TableColumnService.IsNumeric(_field))
            {
                throw new Exception($"Cannot apply {ClassName} aggregation on non numeric column!");
            }
        }

        protected void ValidateStringColumn()
        {
            if (!TableColumnService.IsString(_field))
            {
                throw new Exception($"Cannot apply {ClassName} aggregation on non string column!");
            }
        }

        protected void ValidateDateColumn()
        {
            if (!TableColumnService.IsDate(_field))
            {
                throw new Exception($"Cannot apply {ClassName} aggregation on non date column!");
            }
        }
    }
}
