using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Types;
using DataVo.Core.Services;

namespace DataVo.Core.Parser.Aggregations
{
    /// <summary>
    /// Base abstraction for SQL aggregate operations over grouped rows.
    /// </summary>
    /// <param name="field">The bound source column, when aggregation targets a direct column.</param>
    /// <param name="expression">The source expression, when aggregation targets an expression.</param>
    /// <param name="valueSelector">Selector used to extract aggregation input values from a joined row.</param>
    /// <param name="headerName">Optional explicit output header name.</param>
    public abstract class Aggregation(Column? field, ExpressionNode? expression, Func<JoinedRow, object?> valueSelector, string? headerName)
    {
        /// <summary>
        /// The bound source column for this aggregation, if available.
        /// </summary>
        protected readonly Column? _field = field;

        /// <summary>
        /// The source expression for this aggregation, if available.
        /// </summary>
        protected readonly ExpressionNode? _expression = expression;
        private readonly Func<JoinedRow, object?> _valueSelector = valueSelector;
        private readonly string? _headerName = headerName;

        /// <summary>
        /// Gets the canonical input column name used for header generation.
        /// </summary>
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

        /// <summary>
        /// Gets the aggregation class name used in default header formatting.
        /// </summary>
        public virtual string ClassName
        {
            get
            {
                return GetType().Name.ToUpper();
            }
        }

        /// <summary>
        /// Gets the synthetic hash-value key used for grouped aggregate payloads.
        /// </summary>
        public static string HASH_VALUE
        {
            get
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Validates and executes the aggregation over grouped rows.
        /// </summary>
        /// <param name="rows">The grouped rows.</param>
        /// <returns>The aggregate result value.</returns>
        public object? Execute(ListedTable rows)
        {
            Validate();

            return Apply(rows);
        }

        /// <summary>
        /// Validates aggregation prerequisites before execution.
        /// </summary>
        protected virtual void Validate()
        {
            // By default we do not validate anything
        }

        /// <summary>
        /// Computes the aggregate value for the provided rows.
        /// </summary>
        /// <param name="rows">The grouped rows.</param>
        /// <returns>The aggregate result value.</returns>
        protected abstract object? Apply(ListedTable rows);

        /// <summary>
        /// Gets the output header name for this aggregation.
        /// </summary>
        /// <returns>The configured header name or a generated default header.</returns>
        public virtual string GetHeaderName()
        {
            if (!string.IsNullOrWhiteSpace(_headerName))
            {
                return _headerName;
            }

            return $"{ClassName}({ColumnName})";
        }

        /// <summary>
        /// Selects a raw input value from a joined row.
        /// </summary>
        /// <param name="row">The joined row.</param>
        /// <returns>The selected value.</returns>
        protected object? SelectColumn(JoinedRow row)
        {
            return _valueSelector(row);
        }

        /// <summary>
        /// Selects and converts an input value from a joined row.
        /// </summary>
        /// <typeparam name="T">The requested target value type.</typeparam>
        /// <param name="row">The joined row.</param>
        /// <returns>The converted value.</returns>
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
                throw new EvaluationException($"Wrong aggregation ({ClassName}) called on {ColumnName} fields data type!");
            }
        }

        /// <summary>
        /// Validates that the bound source column is numeric.
        /// </summary>
        protected void ValidateNumericColumn()
        {
            if (_field is null)
            {
                return;
            }

            if (!TableColumnService.IsNumeric(_field))
            {
                throw new EvaluationException($"Cannot apply {ClassName} aggregation on non numeric column!");
            }
        }

        /// <summary>
        /// Validates that the bound source column is string-like.
        /// </summary>
        protected void ValidateStringColumn()
        {
            if (_field is null)
            {
                return;
            }

            if (!TableColumnService.IsString(_field))
            {
                throw new EvaluationException($"Cannot apply {ClassName} aggregation on non string column!");
            }
        }

        /// <summary>
        /// Validates that the bound source column is date-like.
        /// </summary>
        protected void ValidateDateColumn()
        {
            if (_field is null)
            {
                return;
            }

            if (!TableColumnService.IsDate(_field))
            {
                throw new EvaluationException($"Cannot apply {ClassName} aggregation on non date column!");
            }
        }

        /// <summary>
        /// Validates that the bound source column can be ordered by MIN/MAX.
        /// </summary>
        protected void ValidateOrderableColumn()
        {
            if (_field is null)
            {
                return;
            }

            if (!TableColumnService.IsNumeric(_field)
                && !TableColumnService.IsString(_field)
                && !TableColumnService.IsDate(_field))
            {
                throw new EvaluationException($"Cannot apply {ClassName} aggregation on non orderable column!");
            }
        }
    }
}
