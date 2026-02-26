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

public class SelectStatement : SqlStatement
{
    public List<SqlNode> Columns { get; set; } = []; // Could be IdentifierNode, Asterisk, or Aggregate
    public IdentifierNode? FromTable { get; set; }
    public IdentifierNode? FromAlias { get; set; }
    public List<JoinDetailNode> Joins { get; set; } = [];
    public ExpressionNode? WhereExpression { get; set; }
    public GroupByNode? GroupByExpression { get; set; }
    public ExpressionNode? HavingExpression { get; set; }
    public OrderByNode? OrderByExpression { get; set; }
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
    public IdentifierNode DatabaseName { get; set; } = null!;
}

public class DropDatabaseStatement : SqlStatement
{
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
}

public class CreateTableStatement : SqlStatement
{
    public IdentifierNode TableName { get; set; } = null!;
    public List<ColumnDefinitionNode> Columns { get; set; } = [];
}

public class DropTableStatement : SqlStatement
{
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

public class InsertIntoStatement : SqlStatement
{
    public IdentifierNode TableName { get; set; } = null!;
    public List<IdentifierNode> Columns { get; set; } = [];
    public List<List<SqlNode>> ValuesLists { get; set; } = []; // Supports multiple rows of values
}
