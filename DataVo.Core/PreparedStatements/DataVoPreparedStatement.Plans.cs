using System.Collections.Concurrent;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using DataVo.Core.BTree;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Constants;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.MVCC;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Utils;
using DataVo.Core.PreparedStatements;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Utils;

namespace DataVo.Core;

public sealed partial class DataVoPreparedStatement
{
    private static readonly List<string> RowsSelected0Messages = ["Rows selected: 0"];
    private static readonly List<string> RowsSelected1Messages = ["Rows selected: 1"];
    private static readonly List<Dictionary<string, object?>> EmptyDataRows = [];
    private static readonly QueryResult Affected0Result = new() { Messages = ["Rows affected: 0"] };
    private static readonly QueryResult Affected1Result = new() { Messages = ["Rows affected: 1"] };
    private static readonly object BoxedTrue = true;
    private static readonly object BoxedFalse = false;

    private SelectStatement? _candidateSelectAst;
    private UpdateCandidate? _candidateUpdate;
    private DeleteCandidate? _candidateDelete;

    private volatile CompiledSelectPlan? _cachedSelectPlan;
    private volatile CompiledUpdatePlan? _cachedUpdatePlan;
    private volatile CompiledDeletePlan? _cachedDeletePlan;

    private void InitializeFastPathCandidates()
    {
        if (_statements is not { Count: 1 })
        {
            return;
        }

        switch (_statements[0])
        {
            case SelectStatement select when IsEligibleSingleTableSelectAst(select):
                _candidateSelectAst = select;
                break;
            case UpdateStatement update:
                _candidateUpdate = TryBuildUpdateCandidate(update);
                break;
            case DeleteFromStatement delete:
                _candidateDelete = TryBuildDeleteCandidate(delete);
                break;
        }
    }

    private bool TryExecuteFastPath(
        ReadOnlySpan<DataVoQueryParameter> parameters,
        Guid sessionId,
        out QueryResult? result)
    {
        result = null;

        // The general path authorizes every statement; the fast paths have no equivalent, so a session
        // subject to authorization never takes them.
        if (_engine.Config.EnableAuthorization)
        {
            return false;
        }

        string? databaseName = _engine.Sessions.Get(sessionId);
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            return false;
        }

        if (_candidateSelectAst is not null)
        {
            return TryExecuteSelectFastPath(databaseName, parameters, out result);
        }

        if (_candidateUpdate is not null)
        {
            return TryExecuteUpdateFastPath(databaseName, parameters, out result);
        }

        if (_candidateDelete is not null)
        {
            return TryExecuteDeleteFastPath(databaseName, parameters, out result);
        }

        return false;
    }

    private static bool IsEligibleSingleTableSelectAst(SelectStatement select)
    {
        if (select.IsDistinct
            || select.Ctes.Count > 0
            || select.Joins.Count > 0
            || select.FromTable is null
            || string.IsNullOrWhiteSpace(select.FromTable.Name)
            || select.GroupByExpression is not null
            || select.HavingExpression is not null
            || select.OrderByExpression is { Columns.Count: > 0 }
            || select.LimitExpression is { HasParameters: true }
            || select.Columns.Count == 0)
        {
            return false;
        }

        for (int i = 0; i < select.Columns.Count; i++)
        {
            SelectColumnNode col = select.Columns[i];
            if (col.Expression is AggregateExpressionNode or WindowFunctionExpressionNode or ScalarFunctionExpressionNode or BinaryExpressionNode)
            {
                return false;
            }
        }

        return true;
    }

    private bool TryExecuteSelectFastPath(
        string databaseName,
        ReadOnlySpan<DataVoQueryParameter> parameters,
        out QueryResult? result)
    {
        result = null;
        SelectStatement select = _candidateSelectAst!;
        string rawTable = select.FromTable!.Name;
        int spaceIdx = rawTable.IndexOf(' ');
        string tableName = spaceIdx > 0 ? rawTable[..spaceIdx] : rawTable;

        int schemaVersion = _engine.Catalog.GetTableSchemaVersion(tableName, databaseName);
        CompiledSelectPlan? plan = _cachedSelectPlan;
        if (plan is null
            || plan.SchemaVersion != schemaVersion
            || !string.Equals(plan.DatabaseName, databaseName, StringComparison.Ordinal))
        {
            plan = BuildCompiledSelectPlan(select, StoredTableName(tableName, databaseName), databaseName, schemaVersion);
            _cachedSelectPlan = plan;
        }

        if (!plan.Eligible)
        {
            return false;
        }

        return plan.AccessKind switch
        {
            SelectAccessKind.ClusteredPoint => TryExecuteClusteredPointSelect(plan, parameters, out result),
            SelectAccessKind.SecondaryIntIndex => TryExecuteSecondaryIntIndexSelect(plan, parameters, out result),
            SelectAccessKind.RangeOrFilteredScan => TryExecuteScanSelect(plan, parameters, out result),
            _ => false,
        };
    }

    private CompiledSelectPlan BuildCompiledSelectPlan(
        SelectStatement select,
        string tableName,
        string databaseName,
        int schemaVersion)
    {
        CompiledSelectPlan Ineligible() => new()
        {
            DatabaseName = databaseName,
            TableName = tableName,
            SchemaVersion = schemaVersion,
            Eligible = false,
        };

        if (!_engine.Catalog.TableExists(tableName, databaseName))
        {
            return Ineligible();
        }

        IReadOnlyList<Column> columns = _engine.Catalog.GetTableColumns(tableName, databaseName);
        if (columns.Count == 0)
        {
            return Ineligible();
        }

        string? tableAlias = select.FromAlias?.Name;
        if (!TryResolveProjection(select.Columns, tableName, tableAlias, columns, out List<string> fields, out (string Field, int Ordinal)[] directOutput))
        {
            return Ineligible();
        }

        var predicateSlots = new List<CompiledPredicateSlot>();
        if (select.WhereExpression is not null
            && !TryCompilePredicates(select.WhereExpression, tableName, tableAlias, columns, predicateSlots))
        {
            return Ineligible();
        }

        ClusteredKeyLayout? clustered = _engine.StorageContext.ResolveClusteredKey(databaseName, tableName);
        int clusteredOrdinal = clustered?.Ordinal ?? -1;
        bool clusteredIsGuid = clustered is not null
            && string.Equals(columns[clusteredOrdinal].Type, "GUID", StringComparison.OrdinalIgnoreCase);

        var colTypes = new FastColumnType[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            colTypes[i] = ClassifyColumnType(columns[i].Type);
        }

        var outputMask = new bool[columns.Count];
        for (int i = 0; i < directOutput.Length; i++)
        {
            outputMask[directOutput[i].Ordinal] = true;
        }

        var filterMask = new bool[columns.Count];
        for (int i = 0; i < predicateSlots.Count; i++)
        {
            filterMask[predicateSlots[i].Ordinal] = true;
        }

        var restMask = new bool[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            restMask[i] = outputMask[i] && !filterMask[i];
        }

        var decodeMask = new bool[columns.Count];
        int lastDecodedOrdinal = -1;
        for (int i = 0; i < columns.Count; i++)
        {
            if ((outputMask[i] || filterMask[i]) && i != clusteredOrdinal)
            {
                decodeMask[i] = true;
                lastDecodedOrdinal = i;
            }
        }

        int limitSkip = select.LimitExpression?.SkipTarget ?? 0;
        int limitTake = select.LimitExpression?.TakeTarget ?? -1;

        // Check for clustered primary-key point lookup: single equality predicate on ClusteredOrdinal.
        if (clusteredOrdinal >= 0
            && predicateSlots.Count == 1
            && predicateSlots[0].Ordinal == clusteredOrdinal
            && predicateSlots[0].Operator == Operators.EQUALS
            && limitSkip == 0
            && limitTake != 0)
        {
            return new CompiledSelectPlan
            {
                DatabaseName = databaseName,
                TableName = tableName,
                SchemaVersion = schemaVersion,
                Eligible = true,
                AccessKind = SelectAccessKind.ClusteredPoint,
                Columns = columns,
                ColumnTypes = colTypes,
                Fields = fields,
                DirectOutput = directOutput,
                DecodeMask = decodeMask,
                FilterMask = filterMask,
                RestMask = restMask,
                OutputMask = outputMask,
                LastDecodedOrdinal = lastDecodedOrdinal,
                ClusteredOrdinal = clusteredOrdinal,
                ClusteredIsGuid = clusteredIsGuid,
                Predicates = [.. predicateSlots],
                IndexName = $"_PK_{tableName}",
                LimitSkip = limitSkip,
                LimitTake = limitTake,
                EmptyResult = new QueryResult
                {
                    Fields = fields,
                    Data = EmptyDataRows,
                    Messages = RowsSelected0Messages,
                },
            };
        }

        // Check for single-column INT secondary index (or non-clustered INT PK) equality lookup.
        if (predicateSlots.Count == 1
            && predicateSlots[0].Operator == Operators.EQUALS
            && colTypes[predicateSlots[0].Ordinal] is FastColumnType.Int or FastColumnType.BigInt
            && limitSkip == 0
            && limitTake < 0)
        {
            string colName = columns[predicateSlots[0].Ordinal].Name;
            string? resolvedIndex = TryResolveSingleColumnIntBTreeIndex(tableName, databaseName, colName);
            if (resolvedIndex is not null)
            {
                return new CompiledSelectPlan
                {
                    DatabaseName = databaseName,
                    TableName = tableName,
                    SchemaVersion = schemaVersion,
                    Eligible = true,
                    AccessKind = SelectAccessKind.SecondaryIntIndex,
                    Columns = columns,
                    ColumnTypes = colTypes,
                    Fields = fields,
                    DirectOutput = directOutput,
                    DecodeMask = decodeMask,
                    FilterMask = filterMask,
                    RestMask = restMask,
                    OutputMask = outputMask,
                    LastDecodedOrdinal = lastDecodedOrdinal,
                    ClusteredOrdinal = clusteredOrdinal,
                    ClusteredIsGuid = clusteredIsGuid,
                    Predicates = [.. predicateSlots],
                    IndexName = resolvedIndex,
                    LimitSkip = limitSkip,
                    LimitTake = limitTake,
                    EmptyResult = new QueryResult
                    {
                        Fields = fields,
                        Data = EmptyDataRows,
                        Messages = RowsSelected0Messages,
                    },
                };
            }
        }

        // Check for clustered range or filtered table scan (when no predicate touches an indexed column via '=').
        if (!HasEqualityOnIndexedOrKeyColumn(tableName, databaseName, columns, predicateSlots))
        {
            return new CompiledSelectPlan
            {
                DatabaseName = databaseName,
                TableName = tableName,
                SchemaVersion = schemaVersion,
                Eligible = true,
                AccessKind = SelectAccessKind.RangeOrFilteredScan,
                Columns = columns,
                ColumnTypes = colTypes,
                Fields = fields,
                DirectOutput = directOutput,
                DecodeMask = decodeMask,
                FilterMask = filterMask,
                RestMask = restMask,
                OutputMask = outputMask,
                LastDecodedOrdinal = lastDecodedOrdinal,
                ClusteredOrdinal = clusteredOrdinal,
                ClusteredIsGuid = clusteredIsGuid,
                Predicates = [.. predicateSlots],
                LimitSkip = limitSkip,
                LimitTake = limitTake,
                EmptyResult = new QueryResult
                {
                    Fields = fields,
                    Data = EmptyDataRows,
                    Messages = RowsSelected0Messages,
                },
            };
        }

        return Ineligible();
    }

    private string StoredTableName(string tableName, string databaseName) =>
        _engine.Catalog.FindTableName(tableName, databaseName) ?? tableName;

    private string? TryResolveSingleColumnIntBTreeIndex(string tableName, string databaseName, string columnName)
    {
        if (_engine.Config.StorageMode is not (StorageMode.InMemory or StorageMode.Lsm))
        {
            return null;
        }

        List<string> primaryKeys = _engine.Catalog.GetTablePrimaryKeys(tableName, databaseName);
        if (primaryKeys.Count == 1 && string.Equals(primaryKeys[0], columnName, StringComparison.OrdinalIgnoreCase))
        {
            return $"_PK_{tableName}";
        }

        foreach (IndexFile index in _engine.Catalog.GetTableIndexesReadOnly(tableName, databaseName))
        {
            if (index.AttributeNames.Count == 1
                && string.Equals(index.AttributeNames[0], columnName, StringComparison.OrdinalIgnoreCase)
                && string.Equals(index.IndexKind, "BTREE", StringComparison.OrdinalIgnoreCase))
            {
                return index.IndexFileName;
            }
        }

        return null;
    }

    private bool HasEqualityOnIndexedOrKeyColumn(
        string tableName,
        string databaseName,
        IReadOnlyList<Column> columns,
        List<CompiledPredicateSlot> predicates)
    {
        List<string> primaryKeys = _engine.Catalog.GetTablePrimaryKeys(tableName, databaseName);
        IReadOnlyList<IndexFile> indexes = _engine.Catalog.GetTableIndexesReadOnly(tableName, databaseName);

        for (int i = 0; i < predicates.Count; i++)
        {
            if (predicates[i].Operator != Operators.EQUALS)
            {
                continue;
            }

            string colName = columns[predicates[i].Ordinal].Name;
            if (primaryKeys.Contains(colName, StringComparer.OrdinalIgnoreCase))
            {
                return true;
            }

            for (int j = 0; j < indexes.Count; j++)
            {
                if (indexes[j].AttributeNames.Contains(colName, StringComparer.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static bool TryResolveProjection(
        List<SelectColumnNode> selectColumns,
        string tableName,
        string? tableAlias,
        IReadOnlyList<Column> tableColumns,
        out List<string> fields,
        out (string Field, int Ordinal)[] directOutput)
    {
        fields = [];
        var outputList = new List<(string Field, int Ordinal)>(selectColumns.Count);

        for (int i = 0; i < selectColumns.Count; i++)
        {
            SelectColumnNode node = selectColumns[i];
            string raw = (node.RawExpression ?? string.Empty).Trim();
            string normalized = raw.Replace(" . ", ".").Replace(" .", ".").Replace(". ", ".");

            if (normalized == "*" || normalized.EndsWith(".*", StringComparison.Ordinal))
            {
                if (normalized.EndsWith(".*", StringComparison.Ordinal))
                {
                    string prefix = normalized[..^2];
                    if (!MatchesTableOrAlias(prefix, tableName, tableAlias))
                    {
                        directOutput = [];
                        return false;
                    }
                }

                for (int c = 0; c < tableColumns.Count; c++)
                {
                    string name = tableColumns[c].Name;
                    fields.Add(name);
                    outputList.Add((name, c));
                }

                continue;
            }

            string colName;
            if (node.Expression is ColumnRefNode colRef)
            {
                if (!string.IsNullOrEmpty(colRef.TableOrAlias)
                    && !MatchesTableOrAlias(colRef.TableOrAlias, tableName, tableAlias))
                {
                    directOutput = [];
                    return false;
                }

                colName = colRef.Column;
            }
            else
            {
                int dot = normalized.LastIndexOf('.');
                if (dot > 0)
                {
                    string prefix = normalized[..dot];
                    if (!MatchesTableOrAlias(prefix, tableName, tableAlias))
                    {
                        directOutput = [];
                        return false;
                    }

                    colName = normalized[(dot + 1)..];
                }
                else
                {
                    colName = normalized;
                }
            }

            int ordinal = FindColumnOrdinal(tableColumns, colName);
            if (ordinal < 0)
            {
                directOutput = [];
                return false;
            }

            string canonicalName = tableColumns[ordinal].Name;
            string fieldName = !string.IsNullOrWhiteSpace(node.Alias) ? node.Alias! : canonicalName;
            fields.Add(fieldName);
            outputList.Add((fieldName, ordinal));
        }

        directOutput = [.. outputList];
        return directOutput.Length > 0;
    }

    private static bool TryCompilePredicates(
        ExpressionNode expression,
        string tableName,
        string? tableAlias,
        IReadOnlyList<Column> columns,
        List<CompiledPredicateSlot> slots)
    {
        if (expression is LiteralNode constant && constant is not NullLiteralNode && constant is not BoundValueLiteralNode)
        {
            return constant.Value is true
                || (constant.Value is string s && s == SqlLiterals.TrueExpression);
        }

        if (expression is not BinaryExpressionNode binary)
        {
            return false;
        }

        if (binary.Operator == Operators.AND)
        {
            return TryCompilePredicates(binary.Left, tableName, tableAlias, columns, slots)
                && TryCompilePredicates(binary.Right, tableName, tableAlias, columns, slots);
        }

        if (binary.Operator is not (Operators.EQUALS
            or Operators.NOT_EQUALS
            or Operators.LESS_THAN
            or Operators.LESS_THAN_OR_EQUAL_TO
            or Operators.GREATER_THAN
            or Operators.GREATER_THAN_OR_EQUAL_TO))
        {
            return false;
        }

        if (TryExtractColumnAndOperand(binary.Left, binary.Right, binary.Operator, tableName, tableAlias, columns, out CompiledPredicateSlot slot))
        {
            slots.Add(slot);
            return true;
        }

        if (TryExtractColumnAndOperand(binary.Right, binary.Left, InvertComparisonOperator(binary.Operator), tableName, tableAlias, columns, out slot))
        {
            slots.Add(slot);
            return true;
        }

        return false;
    }

    private static bool TryExtractColumnAndOperand(
        ExpressionNode columnSide,
        ExpressionNode valueSide,
        string op,
        string tableName,
        string? tableAlias,
        IReadOnlyList<Column> columns,
        out CompiledPredicateSlot slot)
    {
        slot = default;
        string? colName = columnSide switch
        {
            ColumnRefNode colRef when string.IsNullOrEmpty(colRef.TableOrAlias) || MatchesTableOrAlias(colRef.TableOrAlias, tableName, tableAlias)
                => colRef.Column,
            ResolvedColumnRefNode resolved when string.Equals(resolved.TableName, tableName, StringComparison.OrdinalIgnoreCase)
                => resolved.Column,
            _ => null,
        };

        if (colName is null)
        {
            return false;
        }

        int ordinal = FindColumnOrdinal(columns, colName);
        if (ordinal < 0)
        {
            return false;
        }

        if (valueSide is ParameterNode param)
        {
            slot = new CompiledPredicateSlot(ordinal, op, param.Name, null);
            return true;
        }

        if (valueSide is LiteralNode literal && literal is not NullLiteralNode && literal.Value is not null)
        {
            slot = new CompiledPredicateSlot(ordinal, op, null, literal.Value);
            return true;
        }

        return false;
    }

    private static string InvertComparisonOperator(string op) => op switch
    {
        Operators.LESS_THAN => Operators.GREATER_THAN,
        Operators.LESS_THAN_OR_EQUAL_TO => Operators.GREATER_THAN_OR_EQUAL_TO,
        Operators.GREATER_THAN => Operators.LESS_THAN,
        Operators.GREATER_THAN_OR_EQUAL_TO => Operators.LESS_THAN_OR_EQUAL_TO,
        _ => op,
    };

    private bool TryExecuteClusteredPointSelect(
        CompiledSelectPlan plan,
        ReadOnlySpan<DataVoQueryParameter> parameters,
        out QueryResult? result)
    {
        result = null;
        CompiledPredicateSlot pred = plan.Predicates[0];

        RowKey rowId;
        if (pred.ParameterName is not null)
        {
            DataVoQueryParameter param;
            if (parameters.Length == 1 && parameters[0].NameMatches(pred.ParameterName))
            {
                param = parameters[0];
            }
            else if (!SqlParameterBinder.TryFindParameter(pred.ParameterName, parameters, out param))
            {
                throw new BindingException($"Bind Error: Parameter '@{pred.ParameterName}' was not provided.");
            }

            if (param.IsNull)
            {
                result = plan.EmptyResult;
                return true;
            }

            if (!TryResolveClusteredRowKey(plan, param, out rowId, out bool unmatchedRowKey))
            {
                return false;
            }

            if (unmatchedRowKey)
            {
                result = plan.EmptyResult;
                return true;
            }
        }
        else
        {
            DataVoQueryParameter constParam = DataVoQueryParameter.FromObject(string.Empty, pred.ConstantLiteral);
            if (!TryResolveClusteredRowKey(plan, constParam, out rowId, out bool unmatchedRowKey))
            {
                return false;
            }

            if (unmatchedRowKey)
            {
                result = plan.EmptyResult;
                return true;
            }
        }

        if (_engine.LockManager.HasActiveRowWriteLocks)
        {
            return false;
        }

        ThreadSelectWorkspace workspace = plan.GetThreadWorkspace();
        var visitState = new ClusteredPointVisitState
        {
            Plan = plan,
            Workspace = workspace,
        };

        if (!_engine.StorageContext.TryVisitRowByKey(plan.TableName, plan.DatabaseName, rowId, ref visitState, VisitClusteredPointRow, out _))
        {
            return false;
        }

        result = visitState.Result ?? plan.EmptyResult;
        return true;
    }

    private static bool TryResolveClusteredRowKey(
        CompiledSelectPlan plan,
        DataVoQueryParameter param,
        out RowKey rowId,
        out bool unmatched)
    {
        rowId = default;
        unmatched = false;

        if (!plan.ClusteredIsGuid)
        {
            FastColumnType keyType = plan.ColumnTypes[plan.ClusteredOrdinal];
            switch (param.Kind)
            {
                case DataVoQueryParameterKind.Int64:
                {
                    long val = param.AsInt64();
                    if (OutsideKeyRange(keyType, val))
                    {
                        unmatched = true;
                        return true;
                    }

                    rowId = RowKey.FromInt64(val);
                    return true;
                }
                case DataVoQueryParameterKind.Double:
                {
                    double d = param.AsDouble();
                    if (double.IsNaN(d) || d != Math.Floor(d) || OutsideKeyRange(keyType, d))
                    {
                        unmatched = true;
                        return true;
                    }

                    rowId = RowKey.FromInt64((long)d);
                    return true;
                }
                case DataVoQueryParameterKind.String:
                {
                    string text = param.AsString()!;
                    string trimmed = UnquoteSingleQuoted(text).Trim();
                    if (long.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsedLong))
                    {
                        if (OutsideKeyRange(keyType, parsedLong))
                        {
                            unmatched = true;
                            return true;
                        }

                        rowId = RowKey.FromInt64(parsedLong);
                        return true;
                    }

                    if (double.TryParse(trimmed, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double parsedDouble))
                    {
                        if (parsedDouble != Math.Floor(parsedDouble) || OutsideKeyRange(keyType, parsedDouble))
                        {
                            unmatched = true;
                            return true;
                        }

                        rowId = RowKey.FromInt64((long)parsedDouble);
                        return true;
                    }

                    Column col = plan.Columns[plan.ClusteredOrdinal];
                    throw new BindingException(
                        $"Type mismatch: column '{col.Name}' is of type {col.Type}, cannot compare with text literal '{text}'.")
                    {
                        ErrorCode = DataVoErrorCodes.TypeMismatch,
                    };
                }
                default:
                    return false;
            }
        }
        else
        {
            switch (param.Kind)
            {
                case DataVoQueryParameterKind.Guid:
                    rowId = RowKey.FromGuid(param.AsGuid());
                    return true;
                case DataVoQueryParameterKind.String:
                {
                    string text = UnquoteSingleQuoted(param.AsString()!).Trim();
                    if (Guid.TryParse(text, out Guid parsed))
                    {
                        rowId = RowKey.FromGuid(parsed);
                        return true;
                    }

                    unmatched = true;
                    return true;
                }
                default:
                    return false;
            }
        }
    }

    private static bool VisitClusteredPointRow(
        ref ClusteredPointVisitState state,
        RowKey rowId,
        ReadOnlySpan<byte> rowBytes,
        ReadOnlySpan<CellValue> typedCells)
    {
        CompiledSelectPlan plan = state.Plan;
        ThreadSelectWorkspace workspace = state.Workspace;
        RowResultSlot slot = workspace.NextSlot();
        Dictionary<string, object?> row = slot.Row;
        object?[] decodedByOrdinal = workspace.DecodedValuesByOrdinal;

        if (!typedCells.IsEmpty)
        {
            bool[] decodeMask = plan.DecodeMask;
            for (int i = 0; i <= plan.LastDecodedOrdinal; i++)
            {
                if (decodeMask[i])
                {
                    decodedByOrdinal[i] = BoxCellIntoSlot(plan, slot, i, in typedCells[i]);
                }
            }
        }
        else if (plan.LastDecodedOrdinal >= 0)
        {
            DecodeRowBytesIntoWorkspace(rowBytes, plan, workspace, slot);
        }

        if (plan.ClusteredOrdinal >= 0 && plan.OutputMask[plan.ClusteredOrdinal])
        {
            decodedByOrdinal[plan.ClusteredOrdinal] = plan.ClusteredIsGuid
                ? slot.BoxGuid(plan.ClusteredOrdinal, rowId.AsGuid())
                : plan.ColumnTypes[plan.ClusteredOrdinal] == FastColumnType.BigInt
                    ? slot.BoxInt64(plan.ClusteredOrdinal, rowId.AsInt64())
                    : slot.BoxInt32(plan.ClusteredOrdinal, (int)rowId.AsInt64());
        }

        (string Field, int Ordinal)[] directOutput = plan.DirectOutput;
        for (int i = 0; i < directOutput.Length; i++)
        {
            (string field, int ordinal) = directOutput[i];
            row[field] = decodedByOrdinal[ordinal];
        }

        state.Result = slot.HitResult;
        return false;
    }

    /// <summary>
    /// The value of a typed row's cell as the row dictionary holds it, without allocating for the common
    /// types: numbers and GUIDs go into the slot's reused boxes, booleans into the two shared ones, and a
    /// string is the stored instance. The result equals <see cref="StorageContext.ConvertCellForLegacyDictionary"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static object? BoxCellIntoSlot(CompiledSelectPlan plan, RowResultSlot slot, int ordinal, in CellValue cell)
    {
        // A FLOAT column keeps its value in a double cell and hands it out in single precision.
        if (plan.ColumnTypes[ordinal] == FastColumnType.Float)
        {
            return cell.Type == CellType.Double
                ? slot.BoxFloat(ordinal, (float)cell.AsDouble())
                : StorageContext.ConvertCellForLegacyDictionary(plan.Columns[ordinal], cell);
        }

        return cell.Type switch
        {
            CellType.Null => null,
            CellType.Int32 => slot.BoxInt32(ordinal, cell.AsInt32()),
            CellType.Int64 => slot.BoxInt64(ordinal, cell.AsInt64()),
            CellType.Double => slot.BoxDouble(ordinal, cell.AsDouble()),
            CellType.Boolean => cell.AsBoolean() ? BoxedTrue : BoxedFalse,
            CellType.Guid => slot.BoxGuid(ordinal, cell.AsGuid()),
            CellType.String => cell.AsString(),
            _ => cell.ToObject(),
        };
    }

    private static void DecodeRowBytesIntoWorkspace(
        ReadOnlySpan<byte> rowBytes,
        CompiledSelectPlan plan,
        ThreadSelectWorkspace workspace,
        RowResultSlot slot)
    {
        var reader = new ByteSpanReader(rowBytes);
        bool[] decodeMask = plan.DecodeMask;
        FastColumnType[] colTypes = plan.ColumnTypes;
        object?[] decoded = workspace.DecodedValuesByOrdinal;
        int last = plan.LastDecodedOrdinal;

        for (int i = 0; i <= last; i++)
        {
            bool isNull = reader.ReadBoolean();
            if (!decodeMask[i])
            {
                if (!isNull)
                {
                    SkipSerializedCell(ref reader, colTypes[i]);
                }

                continue;
            }

            if (isNull)
            {
                decoded[i] = null;
                continue;
            }

            decoded[i] = colTypes[i] switch
            {
                FastColumnType.Int => slot.BoxInt32(i, reader.ReadInt32()),
                FastColumnType.Float => slot.BoxFloat(i, BitConverter.Int32BitsToSingle(reader.ReadInt32())),
                FastColumnType.BigInt => slot.BoxInt64(i, reader.ReadInt64()),
                FastColumnType.Double => slot.BoxDouble(i, BitConverter.Int64BitsToDouble(reader.ReadInt64())),
                FastColumnType.Bit => reader.ReadBoolean() ? BoxedTrue : BoxedFalse,
                FastColumnType.Guid => slot.BoxGuid(i, reader.ReadGuid()),
                FastColumnType.String => workspace.DecodeUtf8String(i, reader.ReadStringSpan()),
                FastColumnType.Date => DataVoDate.FromDateTime(DateTime.FromBinary(reader.ReadInt64())),
                FastColumnType.DateTime => DateTime.FromBinary(reader.ReadInt64()),
                FastColumnType.Vector => ReadVectorArray(ref reader),
                _ => reader.ReadString(),
            };
        }
    }

    private static float[] ReadVectorArray(ref ByteSpanReader reader)
    {
        int count = reader.ReadInt32();
        var vector = new float[count];
        for (int i = 0; i < count; i++)
        {
            vector[i] = BitConverter.Int32BitsToSingle(reader.ReadInt32());
        }

        return vector;
    }

    private static void SkipSerializedCell(ref ByteSpanReader reader, FastColumnType type)
    {
        switch (type)
        {
            case FastColumnType.Int:
            case FastColumnType.Float:
                reader.Skip(sizeof(int));
                return;
            case FastColumnType.Bit:
                reader.Skip(sizeof(bool));
                return;
            case FastColumnType.BigInt:
            case FastColumnType.Double:
            case FastColumnType.Date:
            case FastColumnType.DateTime:
                reader.Skip(sizeof(long));
                return;
            case FastColumnType.Guid:
                reader.Skip(16);
                return;
            case FastColumnType.Vector:
                reader.Skip(reader.ReadInt32() * sizeof(int));
                return;
            default:
                reader.SkipString();
                return;
        }
    }

    private bool TryExecuteSecondaryIntIndexSelect(
        CompiledSelectPlan plan,
        ReadOnlySpan<DataVoQueryParameter> parameters,
        out QueryResult? result)
    {
        result = null;
        long writeLockEpoch = _engine.LockManager.RowWriteLockEpoch;
        if (_engine.LockManager.HasActiveRowWriteLocks)
        {
            return false;
        }

        CompiledPredicateSlot pred = plan.Predicates[0];
        if (!TryBindIntegerEqualityValue(plan, pred, parameters, out long key, out bool unmatched))
        {
            return false;
        }

        if (unmatched)
        {
            result = plan.EmptyResult;
            return true;
        }

        // A hit in the integer primary-key map names the one matching row without building a list of ids.
        IReadOnlyList<RowKey>? rawIds = null;
        if (_engine.IndexManager.IsLsmBacked
            || !_engine.IndexManager.TryLookupIntegerPrimaryKey(key, plan.IndexName!, plan.TableName, plan.DatabaseName, out RowKey singleId))
        {
            rawIds = _engine.IndexManager.FilterUsingIntegerIndex(
                key,
                plan.IndexName!,
                plan.TableName,
                plan.DatabaseName);

            if (rawIds.Count == 0)
            {
                result = plan.EmptyResult;
                return true;
            }

            singleId = rawIds[0];
        }

        // One matching row is returned in the thread's reused result slot, as a clustered point lookup is:
        // no list, dictionary or boxed number is allocated for it. Where the storage cannot serve the row
        // that way (staged writes, a snapshot), the row is read as part of the multi-row path below.
        if (rawIds is null || rawIds.Count == 1)
        {
            var visitState = new ClusteredPointVisitState
            {
                Plan = plan,
                Workspace = plan.GetThreadWorkspace(),
            };

            if (_engine.StorageContext.TryVisitRowByKey(plan.TableName, plan.DatabaseName, singleId, ref visitState, VisitClusteredPointRow, out _))
            {
                result = visitState.Result ?? plan.EmptyResult;
                return true;
            }

            rawIds ??= [singleId];
        }

        // The index may hand back its own list, which other readers share: sort a copy.
        IReadOnlyList<RowKey> rowIds = rawIds;
        if (rawIds.Count > 1)
        {
            List<RowKey> sorted = [.. rawIds];
            sorted.Sort();
            rowIds = sorted;
        }

        var rows = new List<Dictionary<string, object?>>(rowIds.Count);
        var state = new MultiRowMaterializeState
        {
            Plan = plan,
            Workspace = plan.GetThreadWorkspace(),
            Rows = rows,
            StopAfter = -1,
        };

        if (!_engine.StorageContext.VisitRowsByKey(plan.TableName, plan.DatabaseName, rowIds, ref state, VisitMaterializedRow))
        {
            return false;
        }

        // A writer that started during a multi-row read could be seen half applied; the general path
        // re-reads those rows under row locks.
        if (rowIds.Count > 1 && _engine.LockManager.RowWriteLockEpoch != writeLockEpoch)
        {
            return false;
        }

        result = new QueryResult
        {
            Fields = [.. plan.Fields],
            Data = rows,
            Messages = [$"Rows selected: {rows.Count}"],
        };
        return true;
    }

    private bool TryExecuteScanSelect(
        CompiledSelectPlan plan,
        ReadOnlySpan<DataVoQueryParameter> parameters,
        out QueryResult? result)
    {
        result = null;
        if (_engine.LockManager.HasActiveRowWriteLocks)
        {
            return false;
        }

        ThreadSelectWorkspace workspace = plan.GetThreadWorkspace();
        BoundScanPredicate[] boundPredicates = workspace.BoundPredicates;
        for (int i = 0; i < plan.Predicates.Length; i++)
        {
            if (!TryBindScanPredicate(plan, plan.Predicates[i], parameters, out boundPredicates[i], out bool alwaysFalse))
            {
                return false;
            }

            if (alwaysFalse)
            {
                result = plan.EmptyResult;
                return true;
            }
        }

        (RowKey? lower, RowKey? upper) = plan.ClusteredOrdinal >= 0 && !plan.ClusteredIsGuid
            ? ComputeClusteredKeyRange(boundPredicates.AsSpan(0, plan.Predicates.Length), plan.ClusteredOrdinal)
            : (null, null);

        if (lower is { } l && upper is { } u && l >= u)
        {
            result = plan.EmptyResult;
            return true;
        }

        // OFFSET without LIMIT parses as a take of int.MaxValue, so the sum can exceed an int: no early stop then.
        long stopAtRow = plan.LimitTake >= 0 ? (long)plan.LimitTake + Math.Max(0, plan.LimitSkip) : -1;
        int stopAfter = stopAtRow > int.MaxValue ? -1 : (int)stopAtRow;
        if (stopAfter == 0)
        {
            result = plan.EmptyResult;
            return true;
        }

        var rows = new List<Dictionary<string, object?>>();
        var state = new MultiRowMaterializeState
        {
            Plan = plan,
            Workspace = workspace,
            Rows = rows,
            StopAfter = stopAfter,
            HasPredicates = plan.Predicates.Length > 0,
        };

        if (!_engine.StorageContext.ScanTableRows(plan.TableName, plan.DatabaseName, lower, upper, ref state, VisitScanMaterializedRow))
        {
            return false;
        }

        if (plan.LimitSkip > 0)
        {
            rows = rows.Skip(plan.LimitSkip).ToList();
        }

        if (plan.LimitTake >= 0 && rows.Count > plan.LimitTake)
        {
            rows = rows.Take(plan.LimitTake).ToList();
        }

        result = new QueryResult
        {
            Fields = [.. plan.Fields],
            Data = rows,
            Messages = [$"Rows selected: {rows.Count}"],
        };
        return true;
    }

    private static bool TryBindIntegerEqualityValue(
        CompiledSelectPlan plan,
        CompiledPredicateSlot slot,
        ReadOnlySpan<DataVoQueryParameter> parameters,
        out long key,
        out bool unmatched)
    {
        key = 0;
        unmatched = false;

        DataVoQueryParameter param;
        if (slot.ParameterName is not null)
        {
            if (parameters.Length == 1 && parameters[0].NameMatches(slot.ParameterName))
            {
                param = parameters[0];
            }
            else if (!SqlParameterBinder.TryFindParameter(slot.ParameterName, parameters, out param))
            {
                throw new BindingException($"Bind Error: Parameter '@{slot.ParameterName}' was not provided.");
            }
        }
        else
        {
            param = DataVoQueryParameter.FromObject(string.Empty, slot.ConstantLiteral);
        }

        if (param.IsNull)
        {
            unmatched = true;
            return true;
        }

        FastColumnType keyType = plan.ColumnTypes[slot.Ordinal];
        switch (param.Kind)
        {
            case DataVoQueryParameterKind.Int64:
                key = param.AsInt64();
                if (OutsideKeyRange(keyType, key))
                {
                    unmatched = true;
                }

                return true;
            case DataVoQueryParameterKind.Double:
            {
                double d = param.AsDouble();
                if (double.IsNaN(d) || d != Math.Floor(d) || OutsideKeyRange(keyType, d))
                {
                    unmatched = true;
                    return true;
                }

                key = (long)d;
                return true;
            }
            case DataVoQueryParameterKind.String:
            {
                string text = param.AsString()!;
                string trimmed = UnquoteSingleQuoted(text).Trim();
                if (long.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsedLong))
                {
                    if (OutsideKeyRange(keyType, parsedLong))
                    {
                        unmatched = true;
                    }
                    else
                    {
                        key = parsedLong;
                    }

                    return true;
                }

                Column col = plan.Columns[slot.Ordinal];
                throw new BindingException(
                    $"Type mismatch: column '{col.Name}' is of type {col.Type}, cannot compare with text literal '{text}'.")
                {
                    ErrorCode = DataVoErrorCodes.TypeMismatch,
                };
            }
            default:
                return false;
        }
    }

    private static bool OutsideKeyRange(FastColumnType keyType, long value) =>
        keyType != FastColumnType.BigInt && (value < int.MinValue || value > int.MaxValue);

    private static bool OutsideKeyRange(FastColumnType keyType, double value) =>
        keyType == FastColumnType.BigInt
            ? value < -9223372036854775808d || value >= 9223372036854775808d
            : value < int.MinValue || value > int.MaxValue;

    private static bool TryBindScanPredicate(
        CompiledSelectPlan plan,
        CompiledPredicateSlot slot,
        ReadOnlySpan<DataVoQueryParameter> parameters,
        out BoundScanPredicate bound,
        out bool alwaysFalse)
    {
        bound = default;
        alwaysFalse = false;

        DataVoQueryParameter param;
        if (slot.ParameterName is not null)
        {
            if (!SqlParameterBinder.TryFindParameter(slot.ParameterName, parameters, out param))
            {
                throw new BindingException($"Bind Error: Parameter '@{slot.ParameterName}' was not provided.");
            }
        }
        else
        {
            param = DataVoQueryParameter.FromObject(string.Empty, slot.ConstantLiteral);
        }

        if (param.IsNull)
        {
            alwaysFalse = true;
            return true;
        }

        FastColumnType colType = plan.ColumnTypes[slot.Ordinal];
        if (colType == FastColumnType.BigInt && TryGetExactInt64(param, out long exactInteger))
        {
            bound = new BoundScanPredicate(slot.Ordinal, slot.Operator, BoundPredicateKind.Integer, 0, null, exactInteger);
            return true;
        }

        if (colType is FastColumnType.Int or FastColumnType.BigInt or FastColumnType.Float or FastColumnType.Double)
        {
            double number;
            switch (param.Kind)
            {
                case DataVoQueryParameterKind.Int64:
                    number = param.AsInt64();
                    break;
                case DataVoQueryParameterKind.Double:
                    number = param.AsDouble();
                    break;
                case DataVoQueryParameterKind.String:
                {
                    string text = param.AsString()!;
                    string trimmed = UnquoteSingleQuoted(text).Trim();
                    if (!double.TryParse(trimmed, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out number))
                    {
                        Column col = plan.Columns[slot.Ordinal];
                        throw new BindingException(
                            $"Type mismatch: column '{col.Name}' is of type {col.Type}, cannot compare with text literal '{text}'.")
                        {
                            ErrorCode = DataVoErrorCodes.TypeMismatch,
                        };
                    }

                    break;
                }
                default:
                    return false;
            }

            bound = new BoundScanPredicate(slot.Ordinal, slot.Operator, BoundPredicateKind.Numeric, number, null);
            return true;
        }

        if (colType == FastColumnType.String
            && (slot.Operator == Operators.EQUALS || slot.Operator == Operators.NOT_EQUALS)
            && param.Kind == DataVoQueryParameterKind.String)
        {
            string rawText = param.AsString()!;
            string unquoted = slot.ParameterName is not null ? rawText : UnquoteSingleQuoted(rawText);
            if (!double.TryParse(unquoted, NumberStyles.Any, CultureInfo.InvariantCulture, out _))
            {
                BoundPredicateKind textKind = slot.ParameterName is not null ? BoundPredicateKind.ExactText : BoundPredicateKind.Text;
                bound = new BoundScanPredicate(slot.Ordinal, slot.Operator, textKind, 0, unquoted);
                return true;
            }
        }

        bound = new BoundScanPredicate(slot.Ordinal, slot.Operator, BoundPredicateKind.General, 0, param.ToBoxedValue());
        return true;
    }

    private static bool TryGetExactInt64(DataVoQueryParameter param, out long value)
    {
        switch (param.Kind)
        {
            case DataVoQueryParameterKind.Int64:
                value = param.AsInt64();
                return true;
            case DataVoQueryParameterKind.String:
                return long.TryParse(UnquoteSingleQuoted(param.AsString()!).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out value);
            default:
                value = 0;
                return false;
        }
    }

    private static (RowKey? Lower, RowKey? Upper) ComputeClusteredKeyRange(
        ReadOnlySpan<BoundScanPredicate> predicates,
        int keyOrdinal)
    {
        long? low = null;
        long? high = null;
        for (int i = 0; i < predicates.Length; i++)
        {
            BoundScanPredicate predicate = predicates[i];
            if (predicate.Ordinal != keyOrdinal || predicate.Kind is not (BoundPredicateKind.Numeric or BoundPredicateKind.Integer))
            {
                continue;
            }

            long floor;
            long ceiling;
            if (predicate.Kind == BoundPredicateKind.Integer)
            {
                floor = ceiling = predicate.Integer;
            }
            else
            {
                double n = predicate.Number;
                if (double.IsNaN(n) || Math.Abs(n) > 9007199254740992d)
                {
                    continue; // beyond 2^53 a double no longer names one integer, so it cannot bound the range
                }

                floor = (long)Math.Floor(n);
                ceiling = (long)Math.Ceiling(n);
            }

            if (floor == long.MaxValue && predicate.Operator is Operators.GREATER_THAN or Operators.LESS_THAN_OR_EQUAL_TO)
            {
                continue; // floor + 1 would wrap; the predicate itself still filters
            }

            switch (predicate.Operator)
            {
                case Operators.GREATER_THAN_OR_EQUAL_TO:
                    low = low is { } curLow1 ? Math.Max(curLow1, ceiling) : ceiling;
                    break;
                case Operators.GREATER_THAN:
                    low = low is { } curLow2 ? Math.Max(curLow2, floor + 1) : floor + 1;
                    break;
                case Operators.LESS_THAN:
                    high = high is { } curHigh1 ? Math.Min(curHigh1, ceiling) : ceiling;
                    break;
                case Operators.LESS_THAN_OR_EQUAL_TO:
                    high = high is { } curHigh2 ? Math.Min(curHigh2, floor + 1) : floor + 1;
                    break;
            }
        }

        return (low is { } a ? RowKey.FromInt64(a) : null, high is { } b ? RowKey.FromInt64(b) : null);
    }

    private static bool VisitMaterializedRow(
        ref MultiRowMaterializeState state,
        RowKey rowId,
        ReadOnlySpan<byte> rowBytes,
        StoredRow? typedRow)
    {
        CompiledSelectPlan plan = state.Plan;
        ThreadSelectWorkspace workspace = state.Workspace;
        CellValue[] cells = workspace.Cells;

        LoadCells(plan, cells, rowId, rowBytes, typedRow, plan.OutputMask);
        state.Rows.Add(BuildRowDictionary(plan, cells));
        return state.StopAfter < 0 || state.Rows.Count < state.StopAfter;
    }

    private static bool VisitScanMaterializedRow(
        ref MultiRowMaterializeState state,
        RowKey rowId,
        ReadOnlySpan<byte> rowBytes,
        StoredRow? typedRow)
    {
        CompiledSelectPlan plan = state.Plan;
        ThreadSelectWorkspace workspace = state.Workspace;
        CellValue[] cells = workspace.Cells;

        if (state.HasPredicates)
        {
            LoadCells(plan, cells, rowId, rowBytes, typedRow, plan.FilterMask);
            BoundScanPredicate[] predicates = workspace.BoundPredicates;
            for (int i = 0; i < plan.Predicates.Length; i++)
            {
                BoundScanPredicate pred = predicates[i];
                if (!MatchesBoundPredicate(pred, cells[pred.Ordinal], plan.Columns[pred.Ordinal]))
                {
                    return true;
                }
            }

            LoadCells(plan, cells, rowId, rowBytes, typedRow, plan.RestMask);
        }
        else
        {
            LoadCells(plan, cells, rowId, rowBytes, typedRow, plan.OutputMask);
        }

        state.Rows.Add(BuildRowDictionary(plan, cells));
        return state.StopAfter < 0 || state.Rows.Count < state.StopAfter;
    }

    private static void LoadCells(
        CompiledSelectPlan plan,
        CellValue[] cells,
        RowKey rowId,
        ReadOnlySpan<byte> rowBytes,
        StoredRow? typedRow,
        bool[] mask)
    {
        if (typedRow is not null)
        {
            for (int i = 0; i < mask.Length; i++)
            {
                if (mask[i])
                {
                    cells[i] = typedRow[i];
                }
            }
        }
        else
        {
            RowSerializer.DecodeCellsByOrdinal(rowBytes, plan.Columns, mask, cells);
        }

        int key = plan.ClusteredOrdinal;
        if (key >= 0 && mask[key] && cells[key].IsNull)
        {
            cells[key] = plan.ClusteredIsGuid
                ? CellValue.From(rowId.AsGuid())
                : plan.ColumnTypes[key] == FastColumnType.BigInt
                    ? CellValue.From(rowId.AsInt64())
                    : CellValue.From(checked((int)rowId.AsInt64()));
        }
    }

    private static Dictionary<string, object?> BuildRowDictionary(CompiledSelectPlan plan, CellValue[] cells)
    {
        (string Field, int Ordinal)[] output = plan.DirectOutput;
        var row = new Dictionary<string, object?>(output.Length);
        for (int i = 0; i < output.Length; i++)
        {
            (string field, int ordinal) = output[i];
            CellValue cell = cells[ordinal];
            if (cell.IsNull)
            {
                row[field] = null;
            }
            else if (cell.Type == CellType.Int32)
            {
                row[field] = BoxedIntegerCache.Get(cell.AsInt32());
            }
            else
            {
                row[field] = StorageContext.ConvertCellForLegacyDictionary(plan.Columns[ordinal], cell);
            }
        }

        return row;
    }

    private static bool MatchesBoundPredicate(BoundScanPredicate predicate, CellValue cell, Column column)
    {
        if (cell.IsNull)
        {
            return false;
        }

        if (predicate.Kind is BoundPredicateKind.Text or BoundPredicateKind.ExactText && cell.Type == CellType.String)
        {
            string stored = predicate.Kind == BoundPredicateKind.Text ? UnquoteSingleQuoted(cell.AsString()!) : cell.AsString()!;
            bool equal = string.Equals(stored, (string)predicate.Literal!, StringComparison.Ordinal);
            return predicate.Operator == Operators.EQUALS ? equal : !equal;
        }

        if (predicate.Kind == BoundPredicateKind.Integer)
        {
            long exact = cell.Type == CellType.Int32 ? cell.AsInt32() : cell.AsInt64();
            return SatisfiesComparison(predicate.Operator, exact.CompareTo(predicate.Integer));
        }

        if (predicate.Kind == BoundPredicateKind.Numeric)
        {
            double value = cell.Type switch
            {
                CellType.Int32 => cell.AsInt32(),
                CellType.Int64 => cell.AsInt64(),
                CellType.Double => StorageContext.FloatingValue(column, cell),
                _ => double.NaN,
            };

            if (!double.IsNaN(value))
            {
                int order = value.CompareTo(predicate.Number);
                return SatisfiesComparison(predicate.Operator, order);
            }
        }

        object? boxed = StorageContext.ConvertCellForLegacyDictionary(column, cell);
        return predicate.Operator switch
        {
            Operators.EQUALS => ExpressionValueComparer.AreEqual(boxed, predicate.Literal, trimQuotedStrings: false),
            Operators.NOT_EQUALS => ExpressionValueComparer.SqlNotEquals(boxed, predicate.Literal, trimQuotedStrings: false),
            _ => SatisfiesComparison(predicate.Operator, ExpressionValueComparer.Compare(boxed, predicate.Literal, trimQuotedStrings: false)),
        };
    }

    private static bool SatisfiesComparison(string op, int order) => op switch
    {
        Operators.EQUALS => order == 0,
        Operators.NOT_EQUALS => order != 0,
        Operators.LESS_THAN => order < 0,
        Operators.LESS_THAN_OR_EQUAL_TO => order <= 0,
        Operators.GREATER_THAN => order > 0,
        Operators.GREATER_THAN_OR_EQUAL_TO => order >= 0,
        _ => false,
    };

    // --- UPDATE Fast Path ---

    private static UpdateCandidate? TryBuildUpdateCandidate(UpdateStatement update)
    {
        if (update.TableName is null
            || string.IsNullOrWhiteSpace(update.TableName.Name)
            || update.SetClauses.Count == 0
            || update.WhereClause is not BinaryExpressionNode whereBinary
            || whereBinary.Operator != Operators.EQUALS)
        {
            return null;
        }

        if (!TryExtractSimpleEquality(whereBinary, out string whereCol, out string? whereParam, out object? whereConst))
        {
            return null;
        }

        var assignColumns = new string[update.SetClauses.Count];
        var assignParams = new string?[update.SetClauses.Count];
        var assignConsts = new object?[update.SetClauses.Count];

        for (int i = 0; i < update.SetClauses.Count; i++)
        {
            SetClauseNode clause = update.SetClauses[i];
            assignColumns[i] = clause.ColumnName.Name;
            if (clause.Value is ParameterNode param)
            {
                assignParams[i] = param.Name;
            }
            else if (clause.Value is LiteralNode lit && lit is not NullLiteralNode && lit.Value is not null)
            {
                assignConsts[i] = lit.Value;
            }
            else
            {
                return null;
            }
        }

        return new UpdateCandidate
        {
            TableName = update.TableName.Name,
            WhereColumn = whereCol,
            WhereParameterName = whereParam,
            WhereConstant = whereConst,
            AssignColumns = assignColumns,
            AssignParameterNames = assignParams,
            AssignConstants = assignConsts,
        };
    }

    private bool TryExecuteUpdateFastPath(
        string databaseName,
        ReadOnlySpan<DataVoQueryParameter> parameters,
        out QueryResult? result)
    {
        result = null;
        if (_engine.Changes.Enabled || _engine.TransactionManager.HasAnyActiveTransaction())
        {
            return false;
        }

        UpdateCandidate candidate = _candidateUpdate!;
        int schemaVersion = _engine.Catalog.GetTableSchemaVersion(candidate.TableName, databaseName);
        CompiledUpdatePlan? plan = _cachedUpdatePlan;
        if (plan is null
            || plan.SchemaVersion != schemaVersion
            || !string.Equals(plan.DatabaseName, databaseName, StringComparison.Ordinal))
        {
            plan = BuildCompiledUpdatePlan(candidate, databaseName, schemaVersion);
            _cachedUpdatePlan = plan;
        }

        if (!plan.Eligible)
        {
            return false;
        }

        DataVoFixedWidthValue pkValue;
        if (candidate.WhereParameterName is not null)
        {
            if (!SqlParameterBinder.TryFindParameter(candidate.WhereParameterName, parameters, out DataVoQueryParameter pkParam))
            {
                throw new BindingException($"Bind Error: Parameter '@{candidate.WhereParameterName}' was not provided.");
            }

            if (!TryToFixedWidthValue(pkParam, plan.PrimaryKeyType, out pkValue))
            {
                return false;
            }
        }
        else if (candidate.WhereConstant is null || !DataVoFixedWidthValue.TryFromObject(candidate.WhereConstant, out pkValue))
        {
            return false;
        }

        int count = candidate.AssignColumns.Length;
        Span<DataVoFixedWidthValue> assignValues = count <= 8
            ? stackalloc DataVoFixedWidthValue[count]
            : new DataVoFixedWidthValue[count];

        for (int i = 0; i < count; i++)
        {
            string? paramName = candidate.AssignParameterNames[i];
            if (paramName is not null)
            {
                if (!SqlParameterBinder.TryFindParameter(paramName, parameters, out DataVoQueryParameter assignParam))
                {
                    throw new BindingException($"Bind Error: Parameter '@{paramName}' was not provided.");
                }

                if (!TryToFixedWidthValue(assignParam, plan.AssignColumnTypes[i], out assignValues[i]))
                {
                    return false;
                }
            }
            else if (candidate.AssignConstants[i] is not { } constVal || !DataVoFixedWidthValue.TryFromObject(constVal, out assignValues[i]))
            {
                return false;
            }
        }

        if (!DataVoCompiledQuery.TryUpdateFixedWidthByPrimaryKeyFastPath(
            _engine,
            databaseName,
            plan.CompiledPlan!,
            pkValue,
            assignValues,
            out int affected))
        {
            return false;
        }

        result = affected == 1 ? Affected1Result : Affected0Result;
        return true;
    }

    private CompiledUpdatePlan BuildCompiledUpdatePlan(
        UpdateCandidate candidate,
        string databaseName,
        int schemaVersion)
    {
        string storedTable = StoredTableName(candidate.TableName, databaseName);
        CompiledUpdatePlan Ineligible() => new()
        {
            DatabaseName = databaseName,
            SchemaVersion = schemaVersion,
            Eligible = false,
        };

        if (!_engine.Catalog.TableExists(storedTable, databaseName))
        {
            return Ineligible();
        }

        IReadOnlyList<Column> columns = _engine.Catalog.GetTableColumns(storedTable, databaseName);
        List<string> primaryKeys = _engine.Catalog.GetTablePrimaryKeys(storedTable, databaseName);
        if (primaryKeys.Count != 1
            || !string.Equals(primaryKeys[0], candidate.WhereColumn, StringComparison.OrdinalIgnoreCase))
        {
            return Ineligible();
        }

        int pkOrdinal = FindColumnOrdinal(columns, primaryKeys[0]);
        if (pkOrdinal < 0 || !RowSerializer.IsIntegerType(columns[pkOrdinal].Type))
        {
            return Ineligible();
        }

        var assignments = new Dictionary<string, string>(candidate.AssignColumns.Length, StringComparer.OrdinalIgnoreCase);
        var assignColTypes = new FastColumnType[candidate.AssignColumns.Length];
        for (int i = 0; i < candidate.AssignColumns.Length; i++)
        {
            int ordinal = FindColumnOrdinal(columns, candidate.AssignColumns[i]);
            if (ordinal < 0 || ordinal == pkOrdinal || !RowSerializer.IsFixedWidthType(columns[ordinal].Type))
            {
                return Ineligible();
            }

            assignColTypes[i] = ClassifyColumnType(columns[ordinal].Type);
            assignments[columns[ordinal].Name] = candidate.AssignParameterNames[i] ?? $"__const_{i}";
        }

        var compiledPlan = DataVoCompiledQueryPlan.Update(
            storedTable,
            assignments,
            columns[pkOrdinal].Name,
            candidate.WhereParameterName ?? "__pk_const");

        return new CompiledUpdatePlan
        {
            DatabaseName = databaseName,
            SchemaVersion = schemaVersion,
            Eligible = true,
            CompiledPlan = compiledPlan,
            AssignColumnTypes = assignColTypes,
            PrimaryKeyType = ClassifyColumnType(columns[pkOrdinal].Type),
        };
    }

    private static bool TryToFixedWidthValue(
        DataVoQueryParameter param,
        FastColumnType targetType,
        out DataVoFixedWidthValue value)
    {
        value = default;
        if (param.IsNull)
        {
            return false;
        }

        switch (targetType)
        {
            case FastColumnType.Int:
                if (param.Kind == DataVoQueryParameterKind.Int64)
                {
                    long l = param.AsInt64();
                    if (l is >= int.MinValue and <= int.MaxValue)
                    {
                        value = DataVoFixedWidthValue.From((int)l);
                        return true;
                    }
                }

                return false;
            case FastColumnType.BigInt:
                if (param.Kind == DataVoQueryParameterKind.Int64)
                {
                    value = DataVoFixedWidthValue.From(param.AsInt64());
                    return true;
                }

                return false;
            case FastColumnType.Float:
            case FastColumnType.Double:
                if (param.Kind == DataVoQueryParameterKind.Double)
                {
                    value = DataVoFixedWidthValue.From(param.AsDouble());
                    return true;
                }

                if (param.Kind == DataVoQueryParameterKind.Int64)
                {
                    value = DataVoFixedWidthValue.From((double)param.AsInt64());
                    return true;
                }

                return false;
            case FastColumnType.Bit:
                if (param.Kind == DataVoQueryParameterKind.Boolean)
                {
                    value = DataVoFixedWidthValue.From(param.AsBoolean());
                    return true;
                }

                return false;
            default:
                return false;
        }
    }

    // --- DELETE Fast Path ---

    private static DeleteCandidate? TryBuildDeleteCandidate(DeleteFromStatement delete)
    {
        if (delete.TableName is null
            || string.IsNullOrWhiteSpace(delete.TableName.Name)
            || delete.WhereExpression is not BinaryExpressionNode whereBinary
            || whereBinary.Operator != Operators.EQUALS)
        {
            return null;
        }

        if (!TryExtractSimpleEquality(whereBinary, out string whereCol, out string? whereParam, out object? whereConst))
        {
            return null;
        }

        return new DeleteCandidate
        {
            TableName = delete.TableName.Name,
            WhereColumn = whereCol,
            WhereParameterName = whereParam,
            WhereConstant = whereConst,
        };
    }

    private bool TryExecuteDeleteFastPath(
        string databaseName,
        ReadOnlySpan<DataVoQueryParameter> parameters,
        out QueryResult? result)
    {
        result = null;
        if (_engine.Changes.Enabled || _engine.TransactionManager.HasAnyActiveTransaction())
        {
            return false;
        }

        DeleteCandidate candidate = _candidateDelete!;
        int schemaVersion = _engine.Catalog.GetTableSchemaVersion(candidate.TableName, databaseName);
        CompiledDeletePlan? plan = _cachedDeletePlan;
        if (plan is null
            || plan.SchemaVersion != schemaVersion
            || !string.Equals(plan.DatabaseName, databaseName, StringComparison.Ordinal))
        {
            plan = BuildCompiledDeletePlan(candidate, databaseName, schemaVersion);
            _cachedDeletePlan = plan;
        }

        if (!plan.Eligible)
        {
            return false;
        }

        long pk;
        if (candidate.WhereParameterName is not null)
        {
            DataVoQueryParameter param;
            if (parameters.Length == 1 && parameters[0].NameMatches(candidate.WhereParameterName))
            {
                param = parameters[0];
            }
            else if (!SqlParameterBinder.TryFindParameter(candidate.WhereParameterName, parameters, out param))
            {
                throw new BindingException($"Bind Error: Parameter '@{candidate.WhereParameterName}' was not provided.");
            }

            if (param.IsNull)
            {
                result = Affected0Result;
                return true;
            }

            if (param.Kind != DataVoQueryParameterKind.Int64)
            {
                return false;
            }

            pk = param.AsInt64();
            if (OutsideKeyRange(plan.PrimaryKeyType, pk))
            {
                result = Affected0Result;
                return true;
            }
        }
        else if (candidate.WhereConstant is int constInt)
        {
            pk = constInt;
        }
        else if (candidate.WhereConstant is long constLong && !OutsideKeyRange(plan.PrimaryKeyType, constLong))
        {
            pk = constLong;
        }
        else
        {
            return false;
        }

        RowKey rowId = RowKey.FromInt64(pk);
        string tableName = plan.TableName;
        using IWriteBatchScope batch = _engine.StorageContext.BeginWriteBatch();
        _engine.LockManager.AcquireRowWriteLock(databaseName, tableName, rowId);
        try
        {
            if (plan.SecondaryIndexes.Length == 0)
            {
                if (!_engine.StorageContext.TableContainsRow(rowId, tableName, databaseName))
                {
                    result = Affected0Result;
                    return true;
                }

                MvccCoordinator.ValidateCanModifyRow(_engine, databaseName, tableName, rowId, null, "DELETE");
                long txId = MvccCoordinator.ResolveStatementTransactionId(_engine, null);
                MvccCoordinator.RegisterDeleteVersion(_engine, databaseName, tableName, rowId, txId);
                _engine.StorageContext.DeleteFromTable([rowId], tableName, databaseName);
                batch.Complete();
                result = Affected1Result;
                return true;
            }

            byte[]? rawRow = _engine.StorageContext.TryReadRowBytes(tableName, databaseName, rowId);
            if (rawRow is null || !_engine.StorageContext.IsRowVisible(tableName, databaseName, rowId))
            {
                result = Affected0Result;
                return true;
            }

            Dictionary<string, object?> oldRow = RowSerializer.Deserialize(
                databaseName,
                tableName,
                rawRow,
                selectedColumns: null,
                _engine.Catalog,
                _engine.Id.ToString("N"));

            MvccCoordinator.ValidateCanModifyRow(_engine, databaseName, tableName, rowId, null, "DELETE");
            long statementTxId = MvccCoordinator.ResolveStatementTransactionId(_engine, null);
            MvccCoordinator.RegisterDeleteVersion(_engine, databaseName, tableName, rowId, statementTxId);

            _engine.StorageContext.DeleteFromTable([rowId], tableName, databaseName);

            for (int i = 0; i < plan.SecondaryIndexes.Length; i++)
            {
                IndexFile indexFile = plan.SecondaryIndexes[i];
                string key = IndexKeyEncoder.BuildKeyString(oldRow, indexFile.AttributeNames);
                _engine.IndexManager.DeleteIndexEntries([(key, rowId)], indexFile.IndexFileName, tableName, databaseName);
            }

            batch.Complete();
            result = Affected1Result;
            return true;
        }
        finally
        {
            _engine.LockManager.ReleaseRowWriteLock(databaseName, tableName, rowId);
        }
    }

    private CompiledDeletePlan BuildCompiledDeletePlan(
        DeleteCandidate candidate,
        string databaseName,
        int schemaVersion)
    {
        string storedTable = StoredTableName(candidate.TableName, databaseName);
        CompiledDeletePlan Ineligible() => new()
        {
            DatabaseName = databaseName,
            TableName = storedTable,
            SchemaVersion = schemaVersion,
            Eligible = false,
        };

        if (_engine.Config.StorageMode != StorageMode.Lsm
            || !_engine.Catalog.TableExists(storedTable, databaseName)
            || !_engine.Catalog.IsTableClustered(storedTable, databaseName)
            || _engine.Catalog.GetChildForeignKeys(storedTable, databaseName).Count > 0)
        {
            return Ineligible();
        }

        IReadOnlyList<Column> columns = _engine.Catalog.GetTableColumns(storedTable, databaseName);
        List<string> primaryKeys = _engine.Catalog.GetTablePrimaryKeys(storedTable, databaseName);
        if (primaryKeys.Count != 1
            || !string.Equals(primaryKeys[0], candidate.WhereColumn, StringComparison.OrdinalIgnoreCase))
        {
            return Ineligible();
        }

        int pkOrdinal = FindColumnOrdinal(columns, primaryKeys[0]);
        if (pkOrdinal < 0 || !RowSerializer.IsIntegerType(columns[pkOrdinal].Type))
        {
            return Ineligible();
        }

        string pkIndexName = $"_PK_{storedTable}";
        var secondary = new List<IndexFile>();
        foreach (IndexFile index in _engine.Catalog.GetTableIndexesReadOnly(storedTable, databaseName))
        {
            if (string.Equals(index.IndexFileName, pkIndexName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (_engine.IndexManager.SupportsVectorIndexType(index.IndexKind ?? string.Empty))
            {
                return Ineligible();
            }

            secondary.Add(index);
        }

        return new CompiledDeletePlan
        {
            PrimaryKeyType = ClassifyColumnType(columns[pkOrdinal].Type),
            DatabaseName = databaseName,
            TableName = storedTable,
            SchemaVersion = schemaVersion,
            Eligible = true,
            SecondaryIndexes = [.. secondary],
        };
    }

    private static bool TryExtractSimpleEquality(
        BinaryExpressionNode binary,
        out string columnName,
        out string? parameterName,
        out object? constantLiteral)
    {
        columnName = string.Empty;
        parameterName = null;
        constantLiteral = null;

        if (binary.Left is ColumnRefNode leftCol && string.IsNullOrEmpty(leftCol.TableOrAlias))
        {
            if (binary.Right is ParameterNode rightParam)
            {
                columnName = leftCol.Column;
                parameterName = rightParam.Name;
                return true;
            }

            if (binary.Right is LiteralNode rightLit && rightLit is not NullLiteralNode && rightLit.Value is not null)
            {
                columnName = leftCol.Column;
                constantLiteral = rightLit.Value;
                return true;
            }
        }

        if (binary.Right is ColumnRefNode rightCol && string.IsNullOrEmpty(rightCol.TableOrAlias))
        {
            if (binary.Left is ParameterNode leftParam)
            {
                columnName = rightCol.Column;
                parameterName = leftParam.Name;
                return true;
            }

            if (binary.Left is LiteralNode leftLit && leftLit is not NullLiteralNode && leftLit.Value is not null)
            {
                columnName = rightCol.Column;
                constantLiteral = leftLit.Value;
                return true;
            }
        }

        return false;
    }

    private static bool MatchesTableOrAlias(string candidate, string tableName, string? tableAlias) =>
        string.Equals(candidate, tableName, StringComparison.OrdinalIgnoreCase)
        || (tableAlias is not null && string.Equals(candidate, tableAlias, StringComparison.OrdinalIgnoreCase));

    private static int FindColumnOrdinal(IReadOnlyList<Column> columns, string columnName)
    {
        for (int i = 0; i < columns.Count; i++)
        {
            if (string.Equals(columns[i].Name, columnName, StringComparison.Ordinal))
            {
                return i;
            }
        }

        return -1;
    }

    private static string UnquoteSingleQuoted(string value) =>
        value.Length >= 2 && value[0] == '\'' && value[^1] == '\'' ? value[1..^1] : value;

    private static FastColumnType ClassifyColumnType(string type) =>
        string.Equals(type, "INT", StringComparison.OrdinalIgnoreCase) ? FastColumnType.Int :
        string.Equals(type, "BIGINT", StringComparison.OrdinalIgnoreCase) ? FastColumnType.BigInt :
        string.Equals(type, "FLOAT", StringComparison.OrdinalIgnoreCase) ? FastColumnType.Float :
        string.Equals(type, "DOUBLE", StringComparison.OrdinalIgnoreCase) ? FastColumnType.Double :
        string.Equals(type, "BIT", StringComparison.OrdinalIgnoreCase) ? FastColumnType.Bit :
        string.Equals(type, "DATE", StringComparison.OrdinalIgnoreCase) ? FastColumnType.Date :
        string.Equals(type, "DATETIME", StringComparison.OrdinalIgnoreCase) ? FastColumnType.DateTime :
        string.Equals(type, "GUID", StringComparison.OrdinalIgnoreCase) ? FastColumnType.Guid :
        string.Equals(type, "VECTOR", StringComparison.OrdinalIgnoreCase) ? FastColumnType.Vector :
        FastColumnType.String;

    private enum FastColumnType : byte
    {
        Int,
        Float,
        Bit,
        Date,
        DateTime,
        Guid,
        Vector,
        String,
        BigInt,
        Double,
    }

    private enum SelectAccessKind : byte
    {
        None = 0,
        ClusteredPoint = 1,
        SecondaryIntIndex = 2,
        RangeOrFilteredScan = 3,
    }

    private enum BoundPredicateKind : byte
    {
        Numeric = 0,
        Text = 1,
        ExactText = 2,
        General = 3,
        Integer = 4,
    }

    private readonly record struct CompiledPredicateSlot(
        int Ordinal,
        string Operator,
        string? ParameterName,
        object? ConstantLiteral);

    private readonly record struct BoundScanPredicate(
        int Ordinal,
        string Operator,
        BoundPredicateKind Kind,
        double Number,
        object? Literal,
        long Integer = 0);

    private struct ClusteredPointVisitState
    {
        public required CompiledSelectPlan Plan;
        public required ThreadSelectWorkspace Workspace;
        public QueryResult? Result;
    }

    private struct MultiRowMaterializeState
    {
        public required CompiledSelectPlan Plan;
        public required ThreadSelectWorkspace Workspace;
        public required List<Dictionary<string, object?>> Rows;
        public required int StopAfter;
        public bool HasPredicates;
    }

    private sealed class CompiledSelectPlan
    {
        private readonly ThreadLocal<ThreadSelectWorkspace> _workspaces;

        public CompiledSelectPlan()
        {
            _workspaces = new ThreadLocal<ThreadSelectWorkspace>(() => new ThreadSelectWorkspace(this), trackAllValues: false);
        }

        public required string DatabaseName { get; init; }
        public required string TableName { get; init; }
        public required int SchemaVersion { get; init; }
        public required bool Eligible { get; init; }
        public SelectAccessKind AccessKind { get; init; }
        public IReadOnlyList<Column> Columns { get; init; } = [];
        public FastColumnType[] ColumnTypes { get; init; } = [];
        public List<string> Fields { get; init; } = [];
        public (string Field, int Ordinal)[] DirectOutput { get; init; } = [];
        public bool[] DecodeMask { get; init; } = [];
        public bool[] FilterMask { get; init; } = [];
        public bool[] RestMask { get; init; } = [];
        public bool[] OutputMask { get; init; } = [];
        public int LastDecodedOrdinal { get; init; } = -1;
        public int ClusteredOrdinal { get; init; } = -1;
        public bool ClusteredIsGuid { get; init; }
        public CompiledPredicateSlot[] Predicates { get; init; } = [];
        public string? IndexName { get; init; }
        public int LimitSkip { get; init; }
        public int LimitTake { get; init; } = -1;
        public QueryResult EmptyResult { get; init; } = null!;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ThreadSelectWorkspace GetThreadWorkspace() => _workspaces.Value!;
    }

    private sealed class ThreadSelectWorkspace
    {
        private const int RingSize = 8;
        private const int RingMask = RingSize - 1;
        private const int StringCacheSize = 256;
        private const int StringCacheMask = StringCacheSize - 1;
        private const int MaxCachedStringBytes = 64;

        private readonly RowResultSlot[] _slots = new RowResultSlot[RingSize];
        private readonly Utf8CacheEntry[]?[] _stringCachesByColumn;
        private int _ringCursor;

        public readonly object?[] DecodedValuesByOrdinal;
        public readonly CellValue[] Cells;
        public readonly BoundScanPredicate[] BoundPredicates;

        public ThreadSelectWorkspace(CompiledSelectPlan plan)
        {
            int colCount = plan.Columns.Count;
            DecodedValuesByOrdinal = new object?[colCount];
            Cells = new CellValue[colCount];
            BoundPredicates = new BoundScanPredicate[plan.Predicates.Length];
            _stringCachesByColumn = new Utf8CacheEntry[]?[colCount];

            for (int i = 0; i < RingSize; i++)
            {
                _slots[i] = new RowResultSlot(plan);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RowResultSlot NextSlot()
        {
            int idx = _ringCursor++ & RingMask;
            return _slots[idx];
        }

        public string DecodeUtf8String(int columnOrdinal, ReadOnlySpan<byte> utf8)
        {
            if (utf8.IsEmpty)
            {
                return string.Empty;
            }

            if (utf8.Length > MaxCachedStringBytes)
            {
                return Encoding.UTF8.GetString(utf8);
            }

            Utf8CacheEntry[] cache = _stringCachesByColumn[columnOrdinal] ??= new Utf8CacheEntry[StringCacheSize];
            uint hash = ComputeFnv1a(utf8);
            ref Utf8CacheEntry entry = ref cache[hash & StringCacheMask];
            if (entry.Bytes is not null && entry.Hash == hash && utf8.SequenceEqual(entry.Bytes))
            {
                return entry.Decoded;
            }

            string decoded = Encoding.UTF8.GetString(utf8);
            entry.Hash = hash;
            entry.Bytes = utf8.ToArray();
            entry.Decoded = decoded;
            return decoded;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint ComputeFnv1a(ReadOnlySpan<byte> bytes)
        {
            uint hash = 2166136261u;
            for (int i = 0; i < bytes.Length; i++)
            {
                hash = (hash ^ bytes[i]) * 16777619u;
            }

            return hash;
        }

        private struct Utf8CacheEntry
        {
            public uint Hash;
            public byte[]? Bytes;
            public string Decoded;
        }
    }

    private sealed class RowResultSlot
    {
        public readonly Dictionary<string, object?> Row;
        public readonly QueryResult HitResult;
        private readonly object[] _boxedInts;
        private readonly object[] _boxedFloats;
        private readonly object[] _boxedLongs;
        private readonly object[] _boxedDoubles;
        private readonly object[] _boxedGuids;

        public RowResultSlot(CompiledSelectPlan plan)
        {
            int colCount = plan.Columns.Count;
            Row = new Dictionary<string, object?>(plan.DirectOutput.Length);
            for (int i = 0; i < plan.DirectOutput.Length; i++)
            {
                Row[plan.DirectOutput[i].Field] = null;
            }

            HitResult = new QueryResult
            {
                Fields = plan.Fields,
                Data = [Row],
                Messages = RowsSelected1Messages,
            };

            _boxedInts = new object[colCount];
            _boxedFloats = new object[colCount];
            _boxedLongs = new object[colCount];
            _boxedDoubles = new object[colCount];
            _boxedGuids = new object[colCount];

            for (int i = 0; i < colCount; i++)
            {
                _boxedInts[i] = BoxFreshInt(int.MinValue + i);
                _boxedFloats[i] = BoxFreshFloat(float.NaN);
                _boxedLongs[i] = long.MinValue + i;
                _boxedDoubles[i] = double.NaN;
                _boxedGuids[i] = BoxFreshGuid(Guid.Empty);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public object BoxInt32(int ordinal, int value)
        {
            if (value is >= -1 and <= 32767)
            {
                return BoxedIntegerCache.Get(value);
            }

            object box = _boxedInts[ordinal];
            Unsafe.Unbox<int>(box) = value;
            return box;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public object BoxInt64(int ordinal, long value)
        {
            object box = _boxedLongs[ordinal];
            Unsafe.Unbox<long>(box) = value;
            return box;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public object BoxDouble(int ordinal, double value)
        {
            object box = _boxedDoubles[ordinal];
            Unsafe.Unbox<double>(box) = value;
            return box;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public object BoxFloat(int ordinal, float value)
        {
            object box = _boxedFloats[ordinal];
            Unsafe.Unbox<float>(box) = value;
            return box;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public object BoxGuid(int ordinal, Guid value)
        {
            object box = _boxedGuids[ordinal];
            Unsafe.Unbox<Guid>(box) = value;
            return box;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static object BoxFreshInt(int seed) => seed;

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static object BoxFreshFloat(float seed) => seed;

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static object BoxFreshGuid(Guid seed) => seed;
    }

    private sealed class UpdateCandidate
    {
        public required string TableName { get; init; }
        public required string WhereColumn { get; init; }
        public string? WhereParameterName { get; init; }
        public object? WhereConstant { get; init; }
        public required string[] AssignColumns { get; init; }
        public required string?[] AssignParameterNames { get; init; }
        public required object?[] AssignConstants { get; init; }
    }

    private sealed class CompiledUpdatePlan
    {
        public required string DatabaseName { get; init; }
        public required int SchemaVersion { get; init; }
        public required bool Eligible { get; init; }
        public DataVoCompiledQueryPlan? CompiledPlan { get; init; }
        public FastColumnType[] AssignColumnTypes { get; init; } = [];
        public FastColumnType PrimaryKeyType { get; init; }
    }

    private sealed class DeleteCandidate
    {
        public required string TableName { get; init; }
        public required string WhereColumn { get; init; }
        public string? WhereParameterName { get; init; }
        public object? WhereConstant { get; init; }
    }

    private sealed class CompiledDeletePlan
    {
        public FastColumnType PrimaryKeyType { get; init; }

        public required string DatabaseName { get; init; }
        public required string TableName { get; init; }
        public required int SchemaVersion { get; init; }
        public required bool Eligible { get; init; }
        public IndexFile[] SecondaryIndexes { get; init; } = [];
    }
}
