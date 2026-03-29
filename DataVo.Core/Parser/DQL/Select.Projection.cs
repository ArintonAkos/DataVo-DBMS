using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Statements;
using DataVo.Core.Parser.Types;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.DQL;

internal partial class Select
{
    /// <summary>
    /// Constructs the output field list based on the columns specified in the SELECT clause.
    /// In a JOIN context, field names are prefixed with the table name or alias (e.g., <c>Users.Name</c>).
    /// If aggregation results are present (identified by <see cref="GroupBy.HASH_VALUE"/>),
    /// the aggregated column names are appended to the field list.
    /// </summary>
    /// <param name="filteredTable">The fully evaluated result set, used to inspect aggregation metadata.</param>
    /// <returns>A list of qualified field names representing the output schema.</returns>
    private List<string> CreateFieldsFromColumns(ListedTable filteredTable)
    {
        List<string> selectedColumns = _model.GetSelectedColumns();
        List<string> fields = [];

        foreach (string column in selectedColumns)
        {
            string[] splittedColumn = column.Split('.');
            string tableName = splittedColumn[0];
            string columnName = splittedColumn[1];

            if (_model.JoinStatement.ContainsJoin())
            {
                string inUseNameOfTable = _model.TableService!.GetTableDetailByAliasOrName(tableName).GetTableNameInUse();
                fields.Add($"{inUseNameOfTable}.{columnName}");
            }
            else
            {
                fields.Add(columnName);
            }
        }

        JoinedRow? firstRow = filteredTable.FirstOrDefault();
        if (firstRow != null)
        {
            foreach (var expressionColumn in _model.GetComputedExpressionColumns())
            {
                fields.Add(expressionColumn.Alias ?? expressionColumn.RawExpression);
            }

            foreach (var windowColumn in _model.GetWindowFunctionColumns())
            {
                fields.Add(windowColumn.Alias ?? windowColumn.RawExpression);
            }
        }

        if (firstRow != null && firstRow.ContainsKey(GroupBy.HASH_VALUE))
        {
            foreach (var aggregateColumn in _model.GetAggregateColumns())
            {
                if (aggregateColumn.Alias != null)
                {
                    fields.Add(aggregateColumn.Alias);
                    continue;
                }

                if (aggregateColumn.Expression is AggregateExpressionNode aggregateExpression)
                {
                    string canonicalKey = AggregateExpressionFormatter.BuildHeader(aggregateExpression);
                    string outputName = firstRow[GroupBy.HASH_VALUE].ContainsKey(canonicalKey)
                        ? canonicalKey
                        : ResolveAggregateKey(aggregateExpression, firstRow);

                    fields.Add(outputName);
                }
            }
        }

        return fields;
    }

    /// <summary>
    /// Projects each result row into a dictionary keyed by field name, matching the output schema.
    /// </summary>
    /// <param name="filteredTable">The fully evaluated and filtered result set.</param>
    /// <param name="fieldsList">The ordered list of output field names.</param>
    /// <returns>A list of dictionaries, each representing one output row mapped by field name to its value.</returns>
    private List<Dictionary<string, object?>> CreateDataFromResult(ListedTable filteredTable, List<string> fieldsList)
    {
        List<Dictionary<string, object?>> result = new();

        foreach (var row in filteredTable)
        {
            result.Add(ExtractRowData(row, fieldsList));
        }

        return result;
    }

    /// <summary>
    /// Extracts column values from a single <see cref="JoinedRow"/> according to the output field list.
    /// Handles column aliases (split on <c>" AS "</c>) and includes aggregation results when present.
    /// </summary>
    /// <param name="row">The joined row containing per-table column dictionaries.</param>
    /// <param name="fieldsList">The ordered list of output field names.</param>
    /// <returns>A dictionary mapping each field name to its value for this row.</returns>
    private Dictionary<string, object?> ExtractRowData(JoinedRow row, List<string> fieldsList)
    {
        Dictionary<string, object?> data = new();
        int fieldIndex = 0;

        foreach (string nameAssembly in _model.GetSelectedColumns())
        {
            string extractedOriginalName = nameAssembly;
            if (extractedOriginalName.Contains(" AS "))
            {
                extractedOriginalName = extractedOriginalName.Split(" AS ")[0];
            }

            string[] splittedAssembly = extractedOriginalName.Split('.');
            string tableName = splittedAssembly[0];
            string columnName = splittedAssembly[1];

            string currentFieldName = fieldsList[fieldIndex++];
            data[currentFieldName] = row[tableName][columnName];
        }

        if (row.ContainsKey(GroupBy.HASH_VALUE))
        {
            foreach (var expressionColumn in _model.GetComputedExpressionColumns())
            {
                string currentFieldName = fieldsList[fieldIndex++];
                if (expressionColumn.Expression != null)
                {
                    data[currentFieldName] = ResolveNodeValue(expressionColumn.Expression, row);
                }
            }

            foreach (var aggregateColumn in _model.GetAggregateColumns())
            {
                string currentFieldName = fieldsList[fieldIndex++];

                if (aggregateColumn.Expression is AggregateExpressionNode aggregateExpression)
                {
                    data[currentFieldName] = ResolveNodeValue(aggregateExpression, row);
                }
            }

            foreach (var windowColumn in _model.GetWindowFunctionColumns())
            {
                string currentFieldName = fieldsList[fieldIndex++];
                data[currentFieldName] = ResolveWindowValue(row, currentFieldName);
            }
        }
        else
        {
            foreach (var expressionColumn in _model.GetComputedExpressionColumns())
            {
                string currentFieldName = fieldsList[fieldIndex++];
                if (expressionColumn.Expression != null)
                {
                    data[currentFieldName] = ResolveNodeValue(expressionColumn.Expression, row);
                }
            }

            foreach (var windowColumn in _model.GetWindowFunctionColumns())
            {
                string currentFieldName = fieldsList[fieldIndex++];
                data[currentFieldName] = ResolveWindowValue(row, currentFieldName);
            }
        }

        return data;
    }
}
