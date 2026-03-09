using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Enums;

namespace DataVo.Core.Parser.AST;

public abstract class SqlNode
{
    // Base class for all AST nodes
}

public abstract class SqlStatement : SqlNode
{
    // Base class for top-level statements (SELECT, INSERT, CREATE)
}

public class IdentifierNode(string name) : SqlNode
{
    public string Name { get; set; } = name;
}

public class SelectColumnNode : SqlNode
{
    public string Expression { get; set; } = string.Empty; // e.g. "A.Id" or "*"
    public string? Alias { get; set; } // e.g. "AId"
}

// --- Expressions ---
public abstract class ExpressionNode : SqlNode
{
    // Base class for expressions (used in WHERE, HAVING, etc.)
}

public class BinaryExpressionNode : ExpressionNode
{
    public string Operator { get; set; } = string.Empty;
    public ExpressionNode Left { get; set; } = null!;
    public ExpressionNode Right { get; set; } = null!;
}

public class LiteralNode : ExpressionNode
{
    public object? Value { get; set; } // Can be string, int, double, bool, DateOnly, etc.
}

public class NullLiteralNode : LiteralNode
{
    public NullLiteralNode()
    {
        Value = null;
    }
}

public class ColumnRefNode : ExpressionNode
{
    public string? TableOrAlias { get; set; } // Optional table name or alias for disambiguation
    public string Column { get; set; } = string.Empty;
}

public class ResolvedColumnRefNode : ExpressionNode
{
    public string TableName { get; set; } = string.Empty; // After resolution, the actual table name
    public string Column { get; set; } = string.Empty; // After resolution, the actual column name
}

public class JoinConditionNode : SqlNode
{
    public ColumnRefNode Left { get; set; } = null!;
    public ColumnRefNode Right { get; set; } = null!;
}

public class JoinDetailNode : SqlNode
{
    public string JoinType { get; set; } = JoinTypes.INNER; // Canonical type: INNER, LEFT, RIGHT, FULL, CROSS
    public IdentifierNode TableName { get; set; } = null!;
    public IdentifierNode? Alias { get; set; }
    public JoinConditionNode? Condition { get; set; }
}

public class GroupByNode : SqlNode
{
    public List<IdentifierNode> Columns { get; set; } = [];
}

public class OrderByColumnNode : SqlNode
{
    public IdentifierNode Column { get; set; } = null!;
    public bool IsAscending { get; set; } = true;
}

public class OrderByNode : SqlNode
{
    public List<OrderByColumnNode> Columns { get; set; } = [];
}

public class LimitNode : SqlNode
{
    public int TakeTarget { get; set; }
    public int SkipTarget { get; set; }
}

public class SelectStatement : SqlStatement
{
    public bool IsDistinct { get; set; } = false;
    public List<SelectColumnNode> Columns { get; set; } = []; // Could be IdentifierNode, Asterisk, or Aggregate
    public IdentifierNode? FromTable { get; set; }
    public IdentifierNode? FromAlias { get; set; }
    public List<JoinDetailNode> Joins { get; set; } = [];
    public ExpressionNode? WhereExpression { get; set; }
    public GroupByNode? GroupByExpression { get; set; }
    public ExpressionNode? HavingExpression { get; set; }
    public OrderByNode? OrderByExpression { get; set; }
    public LimitNode? LimitExpression { get; set; }
}

public class UnionBranchNode : SqlNode
{
    public bool IsAll { get; set; }
    public SelectStatement Select { get; set; } = null!;
}

public class UnionSelectStatement : SqlStatement
{
    public SelectStatement Left { get; set; } = null!;
    public List<UnionBranchNode> Branches { get; set; } = [];
}

// --- Commands ---
public class UseStatement : SqlStatement
{
    public IdentifierNode DatabaseName { get; set; } = null!;
}

public class ShowDatabasesStatement : SqlStatement { }
public class ShowTablesStatement : SqlStatement { }

public class DescribeStatement : SqlStatement
{
    public IdentifierNode TableName { get; set; } = null!;
}

public class GoStatement : SqlStatement { }

// --- DDL ---
public class CreateDatabaseStatement : SqlStatement
{
    public bool IfNotExists { get; set; } = false;
    public IdentifierNode DatabaseName { get; set; } = null!;
}

public class DropDatabaseStatement : SqlStatement
{
    public bool IfExists { get; set; } = false;
    public IdentifierNode DatabaseName { get; set; } = null!;
}

public class ColumnDefinitionNode : SqlNode
{
    public IdentifierNode ColumnName { get; set; } = null!;
    public string DataType { get; set; } = null!; // INT, VARCHAR(255), etc.
    public bool IsPrimaryKey { get; set; }
    public bool IsUnique { get; set; }
    public IdentifierNode? ReferencesTable { get; set; }
    public IdentifierNode? ReferencesColumn { get; set; }
    public string OnDeleteAction { get; set; } = "RESTRICT"; // RESTRICT | CASCADE
    public ExpressionNode? DefaultExpression { get; set; } // e.g. DEFAULT 10, DEFAULT 'active'
}

public class CreateTableStatement : SqlStatement
{
    public bool IfNotExists { get; set; } = false;
    public IdentifierNode TableName { get; set; } = null!;
    public List<ColumnDefinitionNode> Columns { get; set; } = [];
}

public class DropTableStatement : SqlStatement
{
    public bool IfExists { get; set; } = false;
    public IdentifierNode TableName { get; set; } = null!;
}

public class AlterTableStatement : SqlStatement
{
    public IdentifierNode TableName { get; set; } = null!;
    // Sub-operations will be defined here (Add, Drop, Modify etc)
}

public class AlterTableAddColumnStatement : AlterTableStatement
{
    public ColumnDefinitionNode Column { get; set; } = null!;
}

public class AlterTableDropColumnStatement : AlterTableStatement
{
    public IdentifierNode ColumnName { get; set; } = null!;
}

public class CreateIndexStatement : SqlStatement
{
    public IdentifierNode IndexName { get; set; } = null!;
    public IdentifierNode TableName { get; set; } = null!;
    public IdentifierNode ColumnName { get; set; } = null!;
}

public class DropIndexStatement : SqlStatement
{
    public IdentifierNode IndexName { get; set; } = null!;
    public IdentifierNode TableName { get; set; } = null!;
}

// --- DML ---
public class DeleteFromStatement : SqlStatement
{
    public IdentifierNode TableName { get; set; } = null!;
    public ExpressionNode? WhereExpression { get; set; }
}

public class SetClauseNode : SqlNode
{
    public IdentifierNode ColumnName { get; set; } = null!;
    public ExpressionNode Value { get; set; } = null!;
}

public class UpdateStatement : SqlStatement
{
    public IdentifierNode TableName { get; set; } = null!;
    public List<SetClauseNode> SetClauses { get; set; } = [];
    public ExpressionNode? WhereClause { get; set; }
}

public class InsertIntoStatement : SqlStatement
{
    public IdentifierNode TableName { get; set; } = null!;
    public List<IdentifierNode> Columns { get; set; } = [];
    public List<List<SqlNode>> ValuesLists { get; set; } = []; // Supports multiple rows of values
}

public class VacuumStatement : SqlStatement
{
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
