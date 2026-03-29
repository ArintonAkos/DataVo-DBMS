using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Enums;

namespace DataVo.Core.Parser.AST;

/// <summary>
/// Base type for all SQL abstract syntax tree nodes.
/// </summary>
public abstract class SqlNode
{
}

/// <summary>
/// Base type for top-level SQL statements.
/// </summary>
public abstract class SqlStatement : SqlNode
{
}

/// <summary>
/// Represents an identifier token (database, table, column, alias, and so on).
/// </summary>
/// <param name="name">The identifier text.</param>
public class IdentifierNode(string name) : SqlNode
{
    /// <summary>
    /// Gets or sets the identifier text.
    /// </summary>
    public string Name { get; set; } = name;
}

/// <summary>
/// Represents a projected column or expression in a SELECT list.
/// </summary>
public class SelectColumnNode : SqlNode
{
    /// <summary>
    /// Gets or sets the original textual expression (for example <c>A.Id</c> or <c>*</c>).
    /// </summary>
    public string RawExpression { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the parsed expression tree.
    /// </summary>
    public ExpressionNode? Expression { get; set; }

    /// <summary>
    /// Gets or sets the optional output alias.
    /// </summary>
    public string? Alias { get; set; }
}

/// <summary>
/// Base type for scalar and predicate expressions.
/// </summary>
public abstract class ExpressionNode : SqlNode
{
}

/// <summary>
/// Represents an aggregate function expression such as <c>SUM(x)</c> or <c>COUNT(*)</c>.
/// </summary>
public class AggregateExpressionNode : ExpressionNode
{
    /// <summary>
    /// Gets or sets the aggregate function name.
    /// </summary>
    public string FunctionName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the optional aggregate argument.
    /// </summary>
    public ExpressionNode? Argument { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether this expression is the star form (for example <c>COUNT(*)</c>).
    /// </summary>
    public bool IsStar { get; set; }
}

/// <summary>
/// Represents a scalar function call such as <c>LOWER(name)</c>.
/// </summary>
public class ScalarFunctionExpressionNode : ExpressionNode
{
    /// <summary>
    /// Gets or sets the scalar function name.
    /// </summary>
    public string FunctionName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets function arguments.
    /// </summary>
    public List<ExpressionNode> Arguments { get; set; } = [];
}

/// <summary>
/// Represents a window function expression.
/// </summary>
public class WindowFunctionExpressionNode : ExpressionNode
{
    /// <summary>
    /// Gets or sets the window function name.
    /// </summary>
    public string FunctionName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets columns used in <c>PARTITION BY</c>.
    /// </summary>
    public List<ColumnRefNode> PartitionByColumns { get; set; } = [];

    /// <summary>
    /// Gets or sets the column used for window ordering.
    /// </summary>
    public ColumnRefNode OrderByColumn { get; set; } = null!;

    /// <summary>
    /// Gets or sets a value indicating whether window ordering is ascending.
    /// </summary>
    public bool IsOrderAscending { get; set; } = true;
}

/// <summary>
/// Represents a binary expression (comparison, logical, arithmetic, and so on).
/// </summary>
public class BinaryExpressionNode : ExpressionNode
{
    /// <summary>
    /// Gets or sets the binary operator token.
    /// </summary>
    public string Operator { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the left operand.
    /// </summary>
    public ExpressionNode Left { get; set; } = null!;

    /// <summary>
    /// Gets or sets the right operand.
    /// </summary>
    public ExpressionNode Right { get; set; } = null!;
}

/// <summary>
/// Represents a literal value.
/// </summary>
public class LiteralNode : ExpressionNode
{
    /// <summary>
    /// Gets or sets the literal value.
    /// </summary>
    public object? Value { get; set; }
}

/// <summary>
/// Represents the SQL <c>NULL</c> literal.
/// </summary>
public class NullLiteralNode : LiteralNode
{
    /// <summary>
    /// Initializes a null literal.
    /// </summary>
    public NullLiteralNode()
    {
        Value = null;
    }
}

/// <summary>
/// Represents a possibly-qualified column reference.
/// </summary>
public class ColumnRefNode : ExpressionNode
{
    /// <summary>
    /// Gets or sets the optional table name or alias.
    /// </summary>
    public string? TableOrAlias { get; set; }

    /// <summary>
    /// Gets or sets the referenced column name.
    /// </summary>
    public string Column { get; set; } = string.Empty;
}

/// <summary>
/// Represents a column reference after binding has resolved table ownership.
/// </summary>
public class ResolvedColumnRefNode : ExpressionNode
{
    /// <summary>
    /// Gets or sets the resolved table name.
    /// </summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the resolved column name.
    /// </summary>
    public string Column { get; set; } = string.Empty;
}

/// <summary>
/// Represents an <c>expr IN (subquery)</c> predicate.
/// </summary>
public class InSubqueryExpressionNode : ExpressionNode
{
    /// <summary>
    /// Gets or sets the left expression tested for membership.
    /// </summary>
    public ExpressionNode Left { get; set; } = null!;

    /// <summary>
    /// Gets or sets the subquery operand.
    /// </summary>
    public SqlStatement Subquery { get; set; } = null!;
}

/// <summary>
/// Represents an <c>EXISTS (subquery)</c> predicate.
/// </summary>
public class ExistsSubqueryExpressionNode : ExpressionNode
{
    /// <summary>
    /// Gets or sets a value indicating whether the predicate is negated (<c>NOT EXISTS</c>).
    /// </summary>
    public bool IsNegated { get; set; }

    /// <summary>
    /// Gets or sets the subquery operand.
    /// </summary>
    public SqlStatement Subquery { get; set; } = null!;
}

/// <summary>
/// Represents a scalar subquery expression.
/// </summary>
public class ScalarSubqueryExpressionNode : ExpressionNode
{
    /// <summary>
    /// Gets or sets the scalar subquery.
    /// </summary>
    public SqlStatement Subquery { get; set; } = null!;
}

/// <summary>
/// Represents a join predicate between two columns.
/// </summary>
public class JoinConditionNode : SqlNode
{
    /// <summary>
    /// Gets or sets the left join key.
    /// </summary>
    public ColumnRefNode Left { get; set; } = null!;

    /// <summary>
    /// Gets or sets the right join key.
    /// </summary>
    public ColumnRefNode Right { get; set; } = null!;
}

/// <summary>
/// Represents one join clause in a SELECT query.
/// </summary>
public class JoinDetailNode : SqlNode
{
    /// <summary>
    /// Gets or sets the canonical join type (<c>INNER</c>, <c>LEFT</c>, <c>RIGHT</c>, <c>FULL</c>, <c>CROSS</c>).
    /// </summary>
    public string JoinType { get; set; } = JoinTypes.INNER;

    /// <summary>
    /// Gets or sets the joined table name.
    /// </summary>
    public IdentifierNode TableName { get; set; } = null!;

    /// <summary>
    /// Gets or sets the optional joined table alias.
    /// </summary>
    public IdentifierNode? Alias { get; set; }

    /// <summary>
    /// Gets or sets the optional join condition.
    /// </summary>
    public JoinConditionNode? Condition { get; set; }
}

/// <summary>
/// Represents a <c>GROUP BY</c> clause.
/// </summary>
public class GroupByNode : SqlNode
{
    /// <summary>
    /// Gets or sets grouped column identifiers.
    /// </summary>
    public List<IdentifierNode> Columns { get; set; } = [];
}

/// <summary>
/// Represents one ordered column in an <c>ORDER BY</c> clause.
/// </summary>
public class OrderByColumnNode : SqlNode
{
    /// <summary>
    /// Gets or sets the ordered column.
    /// </summary>
    public IdentifierNode Column { get; set; } = null!;

    /// <summary>
    /// Gets or sets a value indicating whether order direction is ascending.
    /// </summary>
    public bool IsAscending { get; set; } = true;
}

/// <summary>
/// Represents an <c>ORDER BY</c> clause.
/// </summary>
public class OrderByNode : SqlNode
{
    /// <summary>
    /// Gets or sets ordered columns.
    /// </summary>
    public List<OrderByColumnNode> Columns { get; set; } = [];
}

/// <summary>
/// Represents a <c>LIMIT/OFFSET</c> clause.
/// </summary>
public class LimitNode : SqlNode
{
    /// <summary>
    /// Gets or sets the row count to return.
    /// </summary>
    public int TakeTarget { get; set; }

    /// <summary>
    /// Gets or sets the row count to skip.
    /// </summary>
    public int SkipTarget { get; set; }
}

/// <summary>
/// Represents a SELECT query.
/// </summary>
public class SelectStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets a value indicating whether DISTINCT is requested.
    /// </summary>
    public bool IsDistinct { get; set; } = false;

    /// <summary>
    /// Gets or sets projected columns and expressions.
    /// </summary>
    public List<SelectColumnNode> Columns { get; set; } = [];

    /// <summary>
    /// Gets or sets common table expression definitions.
    /// </summary>
    public List<CteDefinitionNode> Ctes { get; set; } = [];

    /// <summary>
    /// Gets or sets the source table name.
    /// </summary>
    public IdentifierNode? FromTable { get; set; }

    /// <summary>
    /// Gets or sets the optional source table alias.
    /// </summary>
    public IdentifierNode? FromAlias { get; set; }

    /// <summary>
    /// Gets or sets join clauses.
    /// </summary>
    public List<JoinDetailNode> Joins { get; set; } = [];

    /// <summary>
    /// Gets or sets the WHERE expression.
    /// </summary>
    public ExpressionNode? WhereExpression { get; set; }

    /// <summary>
    /// Gets or sets the GROUP BY clause.
    /// </summary>
    public GroupByNode? GroupByExpression { get; set; }

    /// <summary>
    /// Gets or sets the HAVING expression.
    /// </summary>
    public ExpressionNode? HavingExpression { get; set; }

    /// <summary>
    /// Gets or sets the ORDER BY clause.
    /// </summary>
    public OrderByNode? OrderByExpression { get; set; }

    /// <summary>
    /// Gets or sets the LIMIT/OFFSET clause.
    /// </summary>
    public LimitNode? LimitExpression { get; set; }
}

/// <summary>
/// Represents one UNION branch.
/// </summary>
public class UnionBranchNode : SqlNode
{
    /// <summary>
    /// Gets or sets a value indicating whether this branch is <c>UNION ALL</c>.
    /// </summary>
    public bool IsAll { get; set; }

    /// <summary>
    /// Gets or sets the branch SELECT statement.
    /// </summary>
    public SelectStatement Select { get; set; } = null!;
}

/// <summary>
/// Represents a UNION query rooted at a left SELECT plus additional branches.
/// </summary>
public class UnionSelectStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the left-most SELECT.
    /// </summary>
    public SelectStatement Left { get; set; } = null!;

    /// <summary>
    /// Gets or sets UNION branches.
    /// </summary>
    public List<UnionBranchNode> Branches { get; set; } = [];

    /// <summary>
    /// Gets or sets optional final ORDER BY.
    /// </summary>
    public OrderByNode? OrderByExpression { get; set; }

    /// <summary>
    /// Gets or sets optional final LIMIT/OFFSET.
    /// </summary>
    public LimitNode? LimitExpression { get; set; }
}

/// <summary>
/// Represents one common table expression definition.
/// </summary>
public class CteDefinitionNode : SqlNode
{
    /// <summary>
    /// Gets or sets the CTE name.
    /// </summary>
    public IdentifierNode Name { get; set; } = null!;

    /// <summary>
    /// Gets or sets the SELECT query defining the CTE.
    /// </summary>
    public SelectStatement Select { get; set; } = null!;
}

/// <summary>
/// Represents a <c>USE database</c> command.
/// </summary>
public class UseStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the target database name.
    /// </summary>
    public IdentifierNode DatabaseName { get; set; } = null!;
}

/// <summary>
/// Represents a <c>SHOW DATABASES</c> command.
/// </summary>
public class ShowDatabasesStatement : SqlStatement { }

/// <summary>
/// Represents a <c>SHOW TABLES</c> command.
/// </summary>
public class ShowTablesStatement : SqlStatement { }

/// <summary>
/// Represents a <c>SHOW USERS</c> command.
/// </summary>
public class ShowUsersStatement : SqlStatement { }

/// <summary>
/// Represents a <c>SHOW ROLES</c> command.
/// </summary>
public class ShowRolesStatement : SqlStatement { }

/// <summary>
/// Represents a <c>SHOW GRANTS</c> command.
/// </summary>
public class ShowGrantsStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets a value indicating that grants should be filtered to a specific user.
    /// </summary>
    public bool FilterByUser { get; set; }

    /// <summary>
    /// Gets or sets a value indicating that grants should be filtered to a specific role.
    /// </summary>
    public bool FilterByRole { get; set; }

    /// <summary>
    /// Gets or sets the user or role name referenced by the filter.
    /// </summary>
    public IdentifierNode? PrincipalName { get; set; }
}

/// <summary>
/// Represents a <c>DESCRIBE table</c> command.
/// </summary>
public class DescribeStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the described table name.
    /// </summary>
    public IdentifierNode TableName { get; set; } = null!;
}

/// <summary>
/// Represents a <c>GO</c> batch separator command.
/// </summary>
public class GoStatement : SqlStatement { }

/// <summary>
/// Represents a <c>CREATE DATABASE</c> statement.
/// </summary>
public class CreateDatabaseStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets a value indicating whether <c>IF NOT EXISTS</c> was specified.
    /// </summary>
    public bool IfNotExists { get; set; } = false;

    /// <summary>
    /// Gets or sets the database name.
    /// </summary>
    public IdentifierNode DatabaseName { get; set; } = null!;
}

/// <summary>
/// Represents a <c>DROP DATABASE</c> statement.
/// </summary>
public class DropDatabaseStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets a value indicating whether <c>IF EXISTS</c> was specified.
    /// </summary>
    public bool IfExists { get; set; } = false;

    /// <summary>
    /// Gets or sets the database name.
    /// </summary>
    public IdentifierNode DatabaseName { get; set; } = null!;
}

/// <summary>
/// Represents one column definition in DDL.
/// </summary>
public class ColumnDefinitionNode : SqlNode
{
    /// <summary>
    /// Gets or sets the column name.
    /// </summary>
    public IdentifierNode ColumnName { get; set; } = null!;

    /// <summary>
    /// Gets or sets the SQL data type.
    /// </summary>
    public string DataType { get; set; } = null!;

    /// <summary>
    /// Gets or sets a value indicating whether this column is part of the primary key.
    /// </summary>
    public bool IsPrimaryKey { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether this column is unique.
    /// </summary>
    public bool IsUnique { get; set; }

    /// <summary>
    /// Gets or sets the referenced table for a foreign key.
    /// </summary>
    public IdentifierNode? ReferencesTable { get; set; }

    /// <summary>
    /// Gets or sets the referenced column for a foreign key.
    /// </summary>
    public IdentifierNode? ReferencesColumn { get; set; }

    /// <summary>
    /// Gets or sets the foreign-key delete action (<c>RESTRICT</c> or <c>CASCADE</c>).
    /// </summary>
    public string OnDeleteAction { get; set; } = "RESTRICT";

    /// <summary>
    /// Gets or sets the default value expression.
    /// </summary>
    public ExpressionNode? DefaultExpression { get; set; }
}

/// <summary>
/// Represents a <c>CREATE TABLE</c> statement.
/// </summary>
public class CreateTableStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets a value indicating whether <c>IF NOT EXISTS</c> was specified.
    /// </summary>
    public bool IfNotExists { get; set; } = false;

    /// <summary>
    /// Gets or sets the table name.
    /// </summary>
    public IdentifierNode TableName { get; set; } = null!;

    /// <summary>
    /// Gets or sets declared columns.
    /// </summary>
    public List<ColumnDefinitionNode> Columns { get; set; } = [];
}

/// <summary>
/// Represents a <c>DROP TABLE</c> statement.
/// </summary>
public class DropTableStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets a value indicating whether <c>IF EXISTS</c> was specified.
    /// </summary>
    public bool IfExists { get; set; } = false;

    /// <summary>
    /// Gets or sets the table name.
    /// </summary>
    public IdentifierNode TableName { get; set; } = null!;
}

/// <summary>
/// Base type for <c>ALTER TABLE</c> statements.
/// </summary>
public class AlterTableStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the target table name.
    /// </summary>
    public IdentifierNode TableName { get; set; } = null!;
}

/// <summary>
/// Represents <c>ALTER TABLE ... ADD COLUMN ...</c>.
/// </summary>
public class AlterTableAddColumnStatement : AlterTableStatement
{
    /// <summary>
    /// Gets or sets the added column definition.
    /// </summary>
    public ColumnDefinitionNode Column { get; set; } = null!;
}

/// <summary>
/// Represents <c>ALTER TABLE ... DROP COLUMN ...</c>.
/// </summary>
public class AlterTableDropColumnStatement : AlterTableStatement
{
    /// <summary>
    /// Gets or sets the dropped column name.
    /// </summary>
    public IdentifierNode ColumnName { get; set; } = null!;
}

/// <summary>
/// Represents <c>ALTER TABLE ... MODIFY COLUMN ...</c>.
/// </summary>
public class AlterTableModifyColumnStatement : AlterTableStatement
{
    /// <summary>
    /// Gets or sets the modified column definition.
    /// </summary>
    public ColumnDefinitionNode Column { get; set; } = null!;
}

/// <summary>
/// Represents a <c>CREATE INDEX</c> statement.
/// </summary>
public class CreateIndexStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the index name.
    /// </summary>
    public IdentifierNode IndexName { get; set; } = null!;

    /// <summary>
    /// Gets or sets the target table name.
    /// </summary>
    public IdentifierNode TableName { get; set; } = null!;

    /// <summary>
    /// Gets or sets indexed column names.
    /// </summary>
    public List<IdentifierNode> ColumnNames { get; set; } = [];

    /// <summary>
    /// Gets or sets an optional index method name.
    /// </summary>
    public IdentifierNode? UsingMethod { get; set; }
}

/// <summary>
/// Represents a <c>DROP INDEX</c> statement.
/// </summary>
public class DropIndexStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the index name.
    /// </summary>
    public IdentifierNode IndexName { get; set; } = null!;

    /// <summary>
    /// Gets or sets the target table name.
    /// </summary>
    public IdentifierNode TableName { get; set; } = null!;
}

/// <summary>
/// Represents a <c>DELETE FROM</c> statement.
/// </summary>
public class DeleteFromStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the target table name.
    /// </summary>
    public IdentifierNode TableName { get; set; } = null!;

    /// <summary>
    /// Gets or sets the optional WHERE predicate.
    /// </summary>
    public ExpressionNode? WhereExpression { get; set; }
}

/// <summary>
/// Represents one assignment in an UPDATE SET clause.
/// </summary>
public class SetClauseNode : SqlNode
{
    /// <summary>
    /// Gets or sets the target column.
    /// </summary>
    public IdentifierNode ColumnName { get; set; } = null!;

    /// <summary>
    /// Gets or sets the assigned expression value.
    /// </summary>
    public ExpressionNode Value { get; set; } = null!;
}

/// <summary>
/// Represents an <c>UPDATE</c> statement.
/// </summary>
public class UpdateStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the target table name.
    /// </summary>
    public IdentifierNode TableName { get; set; } = null!;

    /// <summary>
    /// Gets or sets SET clause assignments.
    /// </summary>
    public List<SetClauseNode> SetClauses { get; set; } = [];

    /// <summary>
    /// Gets or sets the optional WHERE predicate.
    /// </summary>
    public ExpressionNode? WhereClause { get; set; }
}

/// <summary>
/// Represents an <c>INSERT INTO</c> statement.
/// </summary>
public class InsertIntoStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the target table name.
    /// </summary>
    public IdentifierNode TableName { get; set; } = null!;

    /// <summary>
    /// Gets or sets optional target columns.
    /// </summary>
    public List<IdentifierNode> Columns { get; set; } = [];

    /// <summary>
    /// Gets or sets values row groups (supports multi-row insert).
    /// </summary>
    public List<List<SqlNode>> ValuesLists { get; set; } = [];
}

/// <summary>
/// Represents a <c>VACUUM</c> statement.
/// </summary>
public class VacuumStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the target table name.
    /// </summary>
    public IdentifierNode TableName { get; set; } = null!;
}

// --- Transaction Control ---

/// <summary>
/// Represents a <c>BEGIN [TRANSACTION]</c> statement that opens an explicit transaction scope.
/// All subsequent DML operations are buffered in memory until a <see cref="CommitStatement"/>
/// or <see cref="RollbackStatement"/> is encountered.
/// </summary>
public class BeginTransactionStatement : SqlStatement { }

/// <summary>
/// Represents a <c>COMMIT</c> statement that finalizes the active transaction,
/// flushing all buffered changes to the storage engine.
/// </summary>
public class CommitStatement : SqlStatement { }

/// <summary>
/// Represents a <c>ROLLBACK</c> statement that discards all buffered changes
/// in the active transaction and restores the session to auto-commit mode.
/// </summary>
public class RollbackStatement : SqlStatement { }

/// <summary>
/// Represents a <c>SAVEPOINT name</c> statement that captures the current
/// transaction buffer state under the provided savepoint name.
/// </summary>
public class SavepointStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the savepoint name.
    /// </summary>
    public string SavepointName { get; set; } = string.Empty;
}

/// <summary>
/// Represents a <c>ROLLBACK TO [SAVEPOINT] name</c> statement that restores
/// buffered transaction state to a named savepoint while keeping the
/// transaction active.
/// </summary>
public class RollbackToSavepointStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the savepoint name.
    /// </summary>
    public string SavepointName { get; set; } = string.Empty;
}

/// <summary>
/// Represents a <c>RELEASE [SAVEPOINT] name</c> statement that removes a named
/// savepoint from the active transaction.
/// </summary>
public class ReleaseSavepointStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the savepoint name.
    /// </summary>
    public string SavepointName { get; set; } = string.Empty;
}

/// <summary>
/// Represents a <c>CREATE USER</c> statement.
/// </summary>
public class CreateUserStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the created username.
    /// </summary>
    public IdentifierNode Username { get; set; } = null!;

    /// <summary>
    /// Gets or sets the quoted password literal.
    /// </summary>
    public string PasswordLiteral { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the optional initial role name.
    /// </summary>
    public IdentifierNode? RoleName { get; set; }
}

/// <summary>
/// Represents a <c>DROP USER</c> statement.
/// </summary>
public class DropUserStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the dropped username.
    /// </summary>
    public IdentifierNode Username { get; set; } = null!;
}

/// <summary>
/// Represents a <c>CREATE ROLE</c> statement.
/// </summary>
public class CreateRoleStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the created role name.
    /// </summary>
    public IdentifierNode RoleName { get; set; } = null!;
}

/// <summary>
/// Represents a <c>DROP ROLE</c> statement.
/// </summary>
public class DropRoleStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the dropped role name.
    /// </summary>
    public IdentifierNode RoleName { get; set; } = null!;
}

/// <summary>
/// Represents a <c>GRANT</c> statement.
/// </summary>
public class GrantStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets a value indicating whether this is a role membership grant.
    /// </summary>
    public bool IsRoleGrant { get; set; }

    /// <summary>
    /// Gets or sets the granted role when <see cref="IsRoleGrant"/> is true.
    /// </summary>
    public IdentifierNode? GrantedRole { get; set; }

    /// <summary>
    /// Gets or sets permission tokens when this is a permission grant.
    /// </summary>
    public List<string> Permissions { get; set; } = [];

    /// <summary>
    /// Gets or sets a value indicating whether target principal is a role.
    /// </summary>
    public bool TargetIsRole { get; set; }

    /// <summary>
    /// Gets or sets the target user or role name.
    /// </summary>
    public IdentifierNode TargetName { get; set; } = null!;
}

/// <summary>
/// Represents a <c>REVOKE</c> statement.
/// </summary>
public class RevokeStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets a value indicating whether this is a role membership revoke.
    /// </summary>
    public bool IsRoleRevoke { get; set; }

    /// <summary>
    /// Gets or sets the revoked role when <see cref="IsRoleRevoke"/> is true.
    /// </summary>
    public IdentifierNode? RevokedRole { get; set; }

    /// <summary>
    /// Gets or sets permission tokens when this is a permission revoke.
    /// </summary>
    public List<string> Permissions { get; set; } = [];

    /// <summary>
    /// Gets or sets a value indicating whether target principal is a role.
    /// </summary>
    public bool TargetIsRole { get; set; }

    /// <summary>
    /// Gets or sets the target user or role name.
    /// </summary>
    public IdentifierNode TargetName { get; set; } = null!;
}

/// <summary>
/// Represents a <c>LOGIN</c> statement.
/// </summary>
public class LoginStatement : SqlStatement
{
    /// <summary>
    /// Gets or sets the username.
    /// </summary>
    public IdentifierNode Username { get; set; } = null!;

    /// <summary>
    /// Gets or sets the quoted password literal.
    /// </summary>
    public string PasswordLiteral { get; set; } = string.Empty;
}

/// <summary>
/// Represents a <c>LOGOUT</c> statement.
/// </summary>
public class LogoutStatement : SqlStatement { }
