using DataVo.Core.Exceptions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.PreparedStatements;

/// <summary>
/// Clones parsed <see cref="SqlStatement"/> abstract syntax trees while substituting <see cref="ParameterNode"/>
/// placeholders with immutable bound literal nodes (<see cref="BoundValueLiteralNode"/> or <see cref="NullLiteralNode"/>).
/// </summary>
/// <remarks>
/// <para>
/// Because the original template AST inside a <see cref="DataVoPreparedStatement"/> is never mutated in place,
/// multiple threads can concurrently bind and execute the same prepared statement with independent parameter sets.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// List&lt;SqlStatement&gt; bound = SqlParameterBinder.BindStatements(templateStatements, [("id", 42)]);
/// </code>
/// </example>
internal static class SqlParameterBinder
{
    /// <summary>
    /// Clones a sequence of <see cref="SqlStatement"/> nodes and binds any <see cref="ParameterNode"/> placeholders
    /// using the supplied <paramref name="parameters"/>.
    /// </summary>
    /// <param name="statements">The template SQL statements parsed at prepare time.</param>
    /// <param name="parameters">The parameter values for the current execution.</param>
    /// <returns>A new list of bound SQL statements ready for execution.</returns>
    /// <exception cref="BindingException">Thrown when a referenced SQL parameter is missing from <paramref name="parameters"/>.</exception>
    public static List<SqlStatement> BindStatements(
        IReadOnlyList<SqlStatement> statements,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        var bound = new List<SqlStatement>(statements.Count);
        for (int i = 0; i < statements.Count; i++)
        {
            bound.Add(BindStatement(statements[i], parameters));
        }

        return bound;
    }

    /// <summary>
    /// Clones a single <see cref="SqlStatement"/> and substitutes any <see cref="ParameterNode"/> placeholders.
    /// </summary>
    /// <param name="statement">The template statement to bind.</param>
    /// <param name="parameters">The parameter values for the current execution.</param>
    /// <returns>A bound copy of <paramref name="statement"/>.</returns>
    public static SqlStatement BindStatement(
        SqlStatement statement,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        return statement switch
        {
            SelectStatement select => BindSelectStatement(select, parameters),
            UnionSelectStatement union => BindUnionSelectStatement(union, parameters),
            ExplainStatement explain => new ExplainStatement
            {
                Select = BindSelectStatement(explain.Select, parameters),
            },
            InsertIntoStatement insert => BindInsertStatement(insert, parameters),
            UpdateStatement update => BindUpdateStatement(update, parameters),
            DeleteFromStatement delete => BindDeleteStatement(delete, parameters),
            CreateTableStatement createTable => BindCreateTableStatement(createTable, parameters),
            AlterTableAddColumnStatement addCol => new AlterTableAddColumnStatement
            {
                TableName = new IdentifierNode(addCol.TableName.Name),
                Column = BindColumnDefinition(addCol.Column, parameters),
            },
            AlterTableModifyColumnStatement modCol => new AlterTableModifyColumnStatement
            {
                TableName = new IdentifierNode(modCol.TableName.Name),
                Column = BindColumnDefinition(modCol.Column, parameters),
            },
            _ => statement,
        };
    }

    /// <summary>
    /// Clones a <see cref="SelectStatement"/> and binds all parameter placeholders across its clauses and CTEs.
    /// </summary>
    public static SelectStatement BindSelectStatement(
        SelectStatement select,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        var columns = new List<SelectColumnNode>(select.Columns.Count);
        for (int i = 0; i < select.Columns.Count; i++)
        {
            SelectColumnNode col = select.Columns[i];
            columns.Add(new SelectColumnNode
            {
                RawExpression = col.RawExpression,
                Alias = col.Alias,
                Expression = col.Expression is not null ? BindExpression(col.Expression, parameters) : null,
            });
        }

        var ctes = new List<CteDefinitionNode>(select.Ctes.Count);
        for (int i = 0; i < select.Ctes.Count; i++)
        {
            CteDefinitionNode cte = select.Ctes[i];
            ctes.Add(new CteDefinitionNode
            {
                Name = new IdentifierNode(cte.Name.Name),
                Select = BindSelectStatement(cte.Select, parameters),
            });
        }

        var joins = new List<JoinDetailNode>(select.Joins.Count);
        for (int i = 0; i < select.Joins.Count; i++)
        {
            JoinDetailNode join = select.Joins[i];
            joins.Add(new JoinDetailNode
            {
                JoinType = join.JoinType,
                TableName = new IdentifierNode(join.TableName.Name),
                Alias = join.Alias is not null ? new IdentifierNode(join.Alias.Name) : null,
                Condition = join.Condition is not null
                    ? new JoinConditionNode
                    {
                        Left = new ColumnRefNode { TableOrAlias = join.Condition.Left.TableOrAlias, Column = join.Condition.Left.Column },
                        Right = new ColumnRefNode { TableOrAlias = join.Condition.Right.TableOrAlias, Column = join.Condition.Right.Column },
                    }
                    : null,
            });
        }

        return new SelectStatement
        {
            IsDistinct = select.IsDistinct,
            Columns = columns,
            Ctes = ctes,
            FromTable = select.FromTable is not null ? new IdentifierNode(select.FromTable.Name) : null,
            FromAlias = select.FromAlias is not null ? new IdentifierNode(select.FromAlias.Name) : null,
            Joins = joins,
            WhereExpression = select.WhereExpression is not null ? BindExpression(select.WhereExpression, parameters) : null,
            GroupByExpression = select.GroupByExpression,
            HavingExpression = select.HavingExpression is not null ? BindExpression(select.HavingExpression, parameters) : null,
            OrderByExpression = select.OrderByExpression,
            LimitExpression = BindLimit(select.LimitExpression, parameters),
        };
    }

    private static LimitNode? BindLimit(LimitNode? limit, ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        if (limit is null || !limit.HasParameters)
        {
            return limit;
        }

        return new LimitNode
        {
            TakeTarget = limit.TakeParameter is { } take ? ResolveLimitValue(take, parameters) : limit.TakeTarget,
            SkipTarget = limit.SkipParameter is { } skip ? ResolveLimitValue(skip, parameters) : limit.SkipTarget,
        };
    }

    private static int ResolveLimitValue(string parameterName, ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        if (!TryFindParameter(parameterName, parameters, out DataVoQueryParameter found))
        {
            throw new BindingException($"Bind Error: Parameter '@{parameterName}' was not provided.");
        }

        if (found.Kind != DataVoQueryParameterKind.Int64 || found.AsInt64() is < 0 or > int.MaxValue)
        {
            throw new BindingException($"Bind Error: Parameter '@{parameterName}' must be a non-negative 32-bit integer in LIMIT or OFFSET.");
        }

        return (int)found.AsInt64();
    }

    private static UnionSelectStatement BindUnionSelectStatement(
        UnionSelectStatement union,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        var branches = new List<UnionBranchNode>(union.Branches.Count);
        for (int i = 0; i < union.Branches.Count; i++)
        {
            UnionBranchNode branch = union.Branches[i];
            branches.Add(new UnionBranchNode
            {
                IsAll = branch.IsAll,
                Select = BindSelectStatement(branch.Select, parameters),
            });
        }

        return new UnionSelectStatement
        {
            Left = BindSelectStatement(union.Left, parameters),
            Branches = branches,
            OrderByExpression = union.OrderByExpression,
            LimitExpression = BindLimit(union.LimitExpression, parameters),
        };
    }

    private static InsertIntoStatement BindInsertStatement(
        InsertIntoStatement insert,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        var valuesLists = new List<List<SqlNode>>(insert.ValuesLists.Count);
        for (int r = 0; r < insert.ValuesLists.Count; r++)
        {
            List<SqlNode> sourceRow = insert.ValuesLists[r];
            var boundRow = new List<SqlNode>(sourceRow.Count);
            for (int c = 0; c < sourceRow.Count; c++)
            {
                SqlNode cell = sourceRow[c];
                boundRow.Add(cell switch
                {
                    ParameterNode param => ResolveParameterLiteral(param.Name, parameters),
                    ExpressionNode expr => BindExpression(expr, parameters),
                    _ => cell,
                });
            }

            valuesLists.Add(boundRow);
        }

        return new InsertIntoStatement
        {
            TableName = new IdentifierNode(insert.TableName.Name),
            Columns = insert.Columns,
            ValuesLists = valuesLists,
        };
    }

    private static UpdateStatement BindUpdateStatement(
        UpdateStatement update,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        var clauses = new List<SetClauseNode>(update.SetClauses.Count);
        for (int i = 0; i < update.SetClauses.Count; i++)
        {
            SetClauseNode clause = update.SetClauses[i];
            clauses.Add(new SetClauseNode
            {
                ColumnName = new IdentifierNode(clause.ColumnName.Name),
                Value = BindExpression(clause.Value, parameters),
            });
        }

        return new UpdateStatement
        {
            TableName = new IdentifierNode(update.TableName.Name),
            SetClauses = clauses,
            WhereClause = update.WhereClause is not null ? BindExpression(update.WhereClause, parameters) : null,
        };
    }

    private static DeleteFromStatement BindDeleteStatement(
        DeleteFromStatement delete,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        return new DeleteFromStatement
        {
            TableName = new IdentifierNode(delete.TableName.Name),
            WhereExpression = delete.WhereExpression is not null ? BindExpression(delete.WhereExpression, parameters) : null,
        };
    }

    private static CreateTableStatement BindCreateTableStatement(
        CreateTableStatement createTable,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        var columns = new List<ColumnDefinitionNode>(createTable.Columns.Count);
        for (int i = 0; i < createTable.Columns.Count; i++)
        {
            columns.Add(BindColumnDefinition(createTable.Columns[i], parameters));
        }

        return new CreateTableStatement
        {
            IfNotExists = createTable.IfNotExists,
            TableName = new IdentifierNode(createTable.TableName.Name),
            Columns = columns,
        };
    }

    private static ColumnDefinitionNode BindColumnDefinition(
        ColumnDefinitionNode column,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        return new ColumnDefinitionNode
        {
            ColumnName = new IdentifierNode(column.ColumnName.Name),
            DataType = column.DataType,
            IsPrimaryKey = column.IsPrimaryKey,
            IsUnique = column.IsUnique,
            IsNotNull = column.IsNotNull,
            ReferencesTable = column.ReferencesTable,
            ReferencesColumn = column.ReferencesColumn,
            OnDeleteAction = column.OnDeleteAction,
            DefaultExpression = column.DefaultExpression is not null
                ? BindExpression(column.DefaultExpression, parameters)
                : null,
        };
    }

    /// <summary>
    /// Recursively clones an <see cref="ExpressionNode"/> tree, replacing every <see cref="ParameterNode"/>
    /// with its bound literal node.
    /// </summary>
    public static ExpressionNode BindExpression(
        ExpressionNode expression,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        return expression switch
        {
            ParameterNode param => ResolveParameterLiteral(param.Name, parameters),
            NullLiteralNode => new NullLiteralNode(),
            BoundValueLiteralNode bound => new BoundValueLiteralNode(bound.Value),
            LiteralNode literal => new LiteralNode { Value = literal.Value },
            ColumnRefNode col => new ColumnRefNode { TableOrAlias = col.TableOrAlias, Column = col.Column },
            ResolvedColumnRefNode resolved => new ResolvedColumnRefNode { TableName = resolved.TableName, Column = resolved.Column },
            BinaryExpressionNode binary => BindBinaryExpression(binary, parameters),
            AggregateExpressionNode agg => new AggregateExpressionNode
            {
                FunctionName = agg.FunctionName,
                IsStar = agg.IsStar,
                Argument = agg.Argument is not null ? BindExpression(agg.Argument, parameters) : null,
            },
            ScalarFunctionExpressionNode scalar => BindScalarFunctionExpression(scalar, parameters),
            WindowFunctionExpressionNode window => new WindowFunctionExpressionNode
            {
                FunctionName = window.FunctionName,
                PartitionByColumns = window.PartitionByColumns
                    .Select(col => new ColumnRefNode { TableOrAlias = col.TableOrAlias, Column = col.Column })
                    .ToList(),
                OrderByColumn = new ColumnRefNode
                {
                    TableOrAlias = window.OrderByColumn.TableOrAlias,
                    Column = window.OrderByColumn.Column,
                },
                IsOrderAscending = window.IsOrderAscending,
            },
            InSubqueryExpressionNode inSubquery => new InSubqueryExpressionNode
            {
                IsNegated = inSubquery.IsNegated,
                Left = BindExpression(inSubquery.Left, parameters),
                Subquery = BindStatement(inSubquery.Subquery, parameters),
            },
            ExistsSubqueryExpressionNode existsSubquery => new ExistsSubqueryExpressionNode
            {
                IsNegated = existsSubquery.IsNegated,
                Subquery = BindStatement(existsSubquery.Subquery, parameters),
            },
            ScalarSubqueryExpressionNode scalarSubquery => new ScalarSubqueryExpressionNode
            {
                Subquery = BindStatement(scalarSubquery.Subquery, parameters),
            },
            _ => expression,
        };
    }

    private static BinaryExpressionNode BindBinaryExpression(
        BinaryExpressionNode binary,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        return new BinaryExpressionNode
        {
            Operator = binary.Operator,
            Left = BindExpression(binary.Left, parameters),
            Right = BindExpression(binary.Right, parameters),
        };
    }

    private static ScalarFunctionExpressionNode BindScalarFunctionExpression(
        ScalarFunctionExpressionNode scalar,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        var args = new List<ExpressionNode>(scalar.Arguments.Count);
        for (int i = 0; i < scalar.Arguments.Count; i++)
        {
            args.Add(BindExpression(scalar.Arguments[i], parameters));
        }

        return new ScalarFunctionExpressionNode
        {
            FunctionName = scalar.FunctionName,
            Arguments = args,
        };
    }

    /// <summary>
    /// Looks up a parameter by name in <paramref name="parameters"/> and converts it into a bound AST literal node.
    /// </summary>
    public static LiteralNode ResolveParameterLiteral(
        string parameterName,
        ReadOnlySpan<DataVoQueryParameter> parameters)
    {
        if (!TryFindParameter(parameterName, parameters, out DataVoQueryParameter found))
        {
            throw new BindingException($"Bind Error: Parameter '@{parameterName}' was not provided.");
        }

        if (found.IsNull)
        {
            return new NullLiteralNode();
        }

        return new BoundValueLiteralNode(found.ToBoxedValue());
    }

    /// <summary>
    /// Searches <paramref name="parameters"/> for a parameter matching <paramref name="parameterName"/> (without leading <c>@</c>).
    /// </summary>
    public static bool TryFindParameter(
        ReadOnlySpan<char> parameterName,
        ReadOnlySpan<DataVoQueryParameter> parameters,
        out DataVoQueryParameter matched)
    {
        if (!parameterName.IsEmpty && parameterName[0] is ('@' or '$' or ':'))
        {
            parameterName = parameterName[1..];
        }

        for (int i = 0; i < parameters.Length; i++)
        {
            if (parameters[i].NameMatches(parameterName))
            {
                matched = parameters[i];
                return true;
            }
        }

        matched = default;
        return false;
    }
}
