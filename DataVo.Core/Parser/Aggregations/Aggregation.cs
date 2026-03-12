using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;
using DataVo.Core.Services;

namespace DataVo.Core.Parser.Aggregations
{
    public abstract class Aggregation(Column? field, ExpressionNode? expression, Func<JoinedRow, object?> valueSelector, string? headerName)
    {
        protected readonly Column? _field = field;
        protected readonly ExpressionNode? _expression = expression;
        private readonly Func<JoinedRow, object?> _valueSelector = valueSelector;
        private readonly string? _headerName = headerName;

        public string ColumnName
        {
            get
            {
                if (_field is null)
                {
                    return "<expression>";
                }

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
            if (!string.IsNullOrWhiteSpace(_headerName))
            {
                return _headerName;
            }

            return $"{ClassName}({ColumnName})";
        }

        protected dynamic? SelectColumn(JoinedRow row)
        {
            return _valueSelector(row);
        }

        protected T SelectColumn<T>(JoinedRow row)
        {
            try
            {
                object? value = _valueSelector(row);
                if (value is null)
                {
                    return default!;
                }

                if (value is T direct)
                {
                    return direct;
                }

                return (T)Convert.ChangeType(value, typeof(T));
            }
            catch (Exception ex) when (ex is InvalidCastException || ex is FormatException || ex is OverflowException)
            {
                throw new Exception($"Wrong aggregation ({ClassName}) called on {ColumnName} fields data type!");
            }
        }

        protected void ValidateNumericColumn()
        {
            if (_field is null)
            {
                return;
            }

            if (!TableColumnService.IsNumeric(_field))
            {
                throw new Exception($"Cannot apply {ClassName} aggregation on non numeric column!");
            }
        }

        protected void ValidateStringColumn()
        {
            if (_field is null)
            {
                return;
            }

            if (!TableColumnService.IsString(_field))
            {
                throw new Exception($"Cannot apply {ClassName} aggregation on non string column!");
            }
        }

        protected void ValidateDateColumn()
        {
            if (_field is null)
            {
                return;
            }

            if (!TableColumnService.IsDate(_field))
            {
                throw new Exception($"Cannot apply {ClassName} aggregation on non date column!");
            }
        }
    }
}
