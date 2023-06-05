using Database = Server.Models.Catalog;
using Server.Models.Statement.Utils;
using Server.Parser.Types;
using Server.Services;

namespace Server.Parser.Aggregations
{
    internal abstract class Aggregation
    {
        protected readonly Column _field;

        protected Aggregation(Column field)
        {
            _field = field;
        }

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
