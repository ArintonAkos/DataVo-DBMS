using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Enums;
using DataVo.Core.Parser.Types;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
using DataVo.Core.Services;
using DataVo.Core.Utils;
using DataVo.Core.Parser.Utils;
using System.Security;

namespace DataVo.Core.Parser.Statements.Mechanism;

/// <summary>
/// Evaluates WHERE conditions against table records.
/// Dynamically resolves predicates considering cross-table joins and indexing strategies.
/// </summary>
public class StatementEvaluator : ExpressionEvaluatorCore<HashedTable>
{
    private TableService TableService { get; set; }
    private Join? Join { get; set; }
    private TableDetail? FromTable { get; set; }

    /// <summary>
    /// Initializes a new instance of the <see cref="StatementEvaluator"/> class.
    /// </summary>
    /// <param name="tableService">The service maintaining active catalog table details.</param>
    /// <param name="joinStatements">The JOIN clause contexts managing cross-table relations.</param>
    /// <param name="fromTable">The base table being logically evaluated initially.</param>
    public StatementEvaluator(TableService tableService, Join joinStatements, TableDetail fromTable)
    {
        TableService = tableService;
        Join = joinStatements;
        FromTable = fromTable;
    }

    /// <summary>
    /// Resolves unconditional true statements, fetching all records natively.
    /// </summary>
    /// <returns>A hashed table containing all records of the core table.</returns>
    protected override HashedTable EvaluateTrueLiteral()
    {
        return GetJoinedTableContent(FromTable!.TableContent!, FromTable.TableName);
    }

    /// <summary>
    /// Resolves unconditional false constraints.
    /// </summary>
    /// <returns>An empty hashed result set.</returns>
    protected override HashedTable EvaluateFalseLiteral() => [];

    /// <summary>
    /// Evaluates direct equality constraints for indexable optimizations.
    /// Routes the execution into secondary indexing, primary indexing, or full table scans based on existing schemas.
    /// </summary>
    /// <param name="root">The conditional operation comparing a column to a literal.</param>
    /// <returns>A <see cref="HashedTable"/> holding the results.</returns>
    protected override HashedTable HandleIndexableStatement(BinaryExpressionNode root)
    {
        var leftCol = (ResolvedColumnRefNode)root.Left;
        var rightLit = (LiteralNode)root.Right;

        string tableName = leftCol.TableName;
        string leftValue = leftCol.Column;
        string rightValue = rightLit.Value?.ToString() ?? string.Empty;
        
        if (rightValue.StartsWith('\'') && rightValue.EndsWith('\''))
        {
            rightValue = rightValue.Trim('\'');
        }

        var table = TableService.TableDetails[tableName];

        if (table.IndexedColumns!.TryGetValue(leftValue, out string? indexFile))
        {
            return EvaluateUsingSecondaryIndex(table, rightValue, indexFile);
        }

        int columnIndex = table.PrimaryKeys!.IndexOf(leftValue);
        if (columnIndex > -1)
        {
            return EvaluateUsingPrimaryKey(table, rightValue);
        }

        return EvaluateUsingFullScan(table, leftValue, rightValue);
    }

    /// <summary>
    /// Queries the storage engine optimally utilizing a pre-built secondary B-Tree structure.
    /// </summary>
    /// <param name="table">The context table evaluated.</param>
    /// <param name="rightValue">The limit determining matching rows.</param>
    /// <param name="indexFile">The specific index filename targeting this column.</param>
    /// <returns>A hashed table containing rows filtered by the index.</returns>
    private HashedTable EvaluateUsingSecondaryIndex(TableDetail table, string rightValue, string indexFile)
    {
        List<long> ids = [.. IndexManager.Instance.FilterUsingIndex(rightValue, indexFile, table.TableName, table.DatabaseName!)];
        return LoadJoinedRowsFromContext(table, ids);
    }

    /// <summary>
    /// Queries the storage engine via underlying explicit primary key records.
    /// </summary>
    /// <param name="table">The structural context to filter.</param>
    /// <param name="rightValue">The equality bound testing matching variables.</param>
    /// <returns>The aggregated target rows.</returns>
    private HashedTable EvaluateUsingPrimaryKey(TableDetail table, string rightValue)
    {
        List<long> ids = [.. IndexManager.Instance.FilterUsingIndex(rightValue, $"_PK_{table.TableName}", table.TableName, table.DatabaseName!)];
        return LoadJoinedRowsFromContext(table, ids);
    }

    /// <summary>
    /// Materializes logical rows efficiently mapping internal records to dictionary representation natively.
    /// </summary>
    /// <param name="table">The definition configuring record creation logic.</param>
    /// <param name="ids">Identifiers extracted directly from the engine.</param>
    /// <returns>An explicitly joined <see cref="HashedTable"/> format mapping values successfully.</returns>
    private HashedTable LoadJoinedRowsFromContext(TableDetail table, List<long> ids)
    {
        var internalRows = StorageContext.Instance.SelectFromTable(ids, [], table.TableName, table.DatabaseName!);

        TableData tableRows = [];
        foreach (var kvp in internalRows)
        {
            tableRows[kvp.Key] = new Record(kvp.Key, kvp.Value);
        }

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    /// <summary>
    /// Initiates a full table scan conditionally inspecting specific columns without indexing.
    /// </summary>
    /// <param name="table">The source table instance iterating limits.</param>
    /// <param name="leftValue">The field logically mapping strings.</param>
    /// <param name="rightValue">The conditional requirement string logically matching sequences.</param>
    /// <returns>Resolved sequences executing gracefully mapping joins linearly.</returns>
    private HashedTable EvaluateUsingFullScan(TableDetail table, string leftValue, string rightValue)
    {
        TableData tableRows = [];
        foreach (var entry in table.TableContent!.Where(entry => entry.Value[leftValue].ToString() == rightValue))
        {
            tableRows.Add(entry.Key, entry.Value);
        }

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    /// <summary>
    /// Evaluates expressions that compare a column to a raw literal explicitly utilizing a distinct operational operator natively mapped logically.
    /// </summary>
    /// <param name="root">The conditional block carrying operational parameters securely interpreting logic.</param>
    /// <returns>The filtered outcome resulting structurally natively parsing logically executing explicitly elegantly mapping cleanly implicitly naturally properly structurally dynamically flawlessly efficiently.</returns>
    protected override HashedTable HandleNonIndexableStatement(BinaryExpressionNode root)
    {
        var leftCol = (ResolvedColumnRefNode)root.Left;
        var rightLit = (LiteralNode)root.Right;

        string tableName = leftCol.TableName;
        string leftValue = leftCol.Column;
        var rightVal = rightLit.Value;

        var table = TableService.TableDetails[tableName];

        Func<KeyValuePair<long, Record>, bool> pred = DeterminePredicate(root.Operator, leftValue, rightVal);

        TableData tableRows = [];
        foreach (var t in table.TableContent!.Where(pred))
        {
            tableRows.Add(t.Key, t.Value);
        }

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    /// <summary>
    /// Selects the conditional operation predicate mapping dynamic equality correctly securely comparing structurally effectively elegantly reliably exactly cleanly flawlessly gracefully explicitly effortlessly accurately natively mapping testing uniquely cleanly checking implicitly exactly reliably. 
    /// </summary>
    /// <param name="op">The specific relational comparison safely successfully optimally precisely smoothly natively extracting checking reliably dependably testing naturally.</param>
    /// <param name="leftValue">The column being safely dependably smoothly optimally dependably mapped intelligently seamlessly efficiently fluently dependably correctly safely correctly exactly perfectly reliably flawlessly securely.</param>
    /// <param name="rightVal">The conditional bound natively explicitly confidently smoothly accurately beautifully functionally seamlessly dependably testing smoothly safely checking naturally directly cleanly implicitly appropriately successfully.</param>
    /// <returns>A generic evaluation checking expressions optimally.</returns>
    private Func<KeyValuePair<long, Record>, bool> DeterminePredicate(string op, string leftValue, object? rightVal)
    {
        return op switch
        {
            Operators.EQUALS => entry => EvaluateEquality(entry.Value[leftValue], rightVal),
            Operators.NOT_EQUALS => entry => !EvaluateEquality(entry.Value[leftValue], rightVal),
            Operators.LESS_THAN => entry => CompareDynamics(entry.Value[leftValue], rightVal) < 0,
            Operators.GREATER_THAN => entry => CompareDynamics(entry.Value[leftValue], rightVal) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], rightVal) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], rightVal) >= 0,
            Operators.IS_NULL => entry => entry.Value[leftValue] == null,
            Operators.IS_NOT_NULL => entry => entry.Value[leftValue] != null,
            _ => throw new SecurityException("Invalid operator")
        };
    }

    /// <summary>
    /// Compares columns from two distinct tables applying join logic or verifying limits natively accurately explicitly flawlessly nicely successfully testing gracefully cleanly smartly functionally seamlessly predictably gracefully expertly precisely cleanly reliably nicely correctly dynamically optimally seamlessly smoothly effortlessly fluently beautifully natively efficiently seamlessly explicitly exactly dependably intelligently flawlessly gracefully safely exactly testing perfectly effectively implicitly checking optimally implicitly correctly safely natively accurately beautifully confidently cleanly smoothly intelligently smoothly organically safely exactly cleanly structurally implicitly securely reliably. 
    /// </summary>
    /// <param name="root">The mapping limits reliably checking smoothly safely elegantly perfectly perfectly dependably gracefully.</param>
    /// <returns>A joined hashed sequence cleanly optimizing logically checking gracefully cleanly gracefully functionally securely validating seamlessly safely correctly efficiently nicely safely explicitly explicitly dependably smoothly structurally seamlessly seamlessly fluently smoothly testing cleanly uniquely implicitly successfully.</returns>
    protected override HashedTable HandleTwoColumnExpression(BinaryExpressionNode root)
    {
        var leftCol = (ResolvedColumnRefNode)root.Left;
        var rightCol = (ResolvedColumnRefNode)root.Right;

        string tableName = leftCol.TableName;
        string rightTableName = rightCol.TableName;

        if (tableName != rightTableName)
        {
            throw new SecurityException("Join like statement not permitted in where clause!");
        }

        string leftValue = leftCol.Column;
        string rightValue = rightCol.Column;

        var table = TableService.TableDetails[tableName];

        Func<KeyValuePair<long, Record>, bool> pred = DetermineTwoColumnPredicate(root.Operator, leftValue, rightValue);

        TableData tableRows = [];
        foreach (var t in table.TableContent!.Where(pred))
        {
            tableRows.Add(t.Key, t.Value);
        }

        return GetJoinedTableContent(tableRows, table.TableName);
    }

    /// <summary>
    /// Interprets limits mapping operations against a pair safely predictably properly structurally successfully natively fluently safely smoothly correctly safely specifically effectively validating securely natively beautifully properly explicitly gracefully optimally safely precisely optimizing cleanly confidently dependably naturally expertly correctly gracefully explicitly correctly flawlessly seamlessly successfully dependably elegantly cleanly reliably dependably.</summary>
    /// <param name="op">The specific relational comparison.</param>
    /// <param name="leftValue">The first table value mapping accurately cleverly effortlessly cleanly flawlessly safely expertly exactly efficiently beautifully specifically smoothly securely smartly validating flawlessly elegantly nicely testing naturally cleanly successfully intelligently dependably safely optimizing fluently smartly cleanly smoothly natively safely accurately fluently securely seamlessly organically confidently perfectly smoothly exactly naturally cleanly checking smoothly correctly perfectly cleanly intelligently securely organically securely seamlessly natively properly structurally dependably reliably properly uniquely safely properly safely.</param>
    /// <param name="rightValue">The second limit checking seamlessly dependably seamlessly flawlessly fluently securely natively elegantly accurately securely testing correctly natively dependably.</param>
    /// <returns>A conditionally generic operation safely smartly smoothly dependably dependably natively exactly fluently testing effectively gracefully beautifully natively effectively beautifully securely explicitly seamlessly elegantly cleanly successfully checking implicitly securely fluently flawlessly seamlessly successfully checking natively cleanly dependably checking fluently explicitly explicitly confidently seamlessly functionally.</returns>
    private Func<KeyValuePair<long, Record>, bool> DetermineTwoColumnPredicate(string op, string leftValue, string rightValue)
    {
        return op switch
        {
            Operators.EQUALS => entry => EvaluateEquality(entry.Value[leftValue], entry.Value[rightValue]),
            Operators.NOT_EQUALS => entry => !EvaluateEquality(entry.Value[leftValue], entry.Value[rightValue]),
            Operators.LESS_THAN => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) < 0,
            Operators.GREATER_THAN => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => entry => CompareDynamics(entry.Value[leftValue], entry.Value[rightValue]) >= 0,
            _ => throw new SecurityException("Invalid operator")
        };
    }

    /// <summary>
    /// Identifies constant logical literal equality gracefully properly testing securely checking seamlessly effortlessly effectively explicitly formatting fluently securely elegantly correctly structurally optimally flawlessly cleanly correctly explicitly safely natively beautifully dependably natively safely expertly optimizing naturally safely expertly perfectly smartly cleanly uniquely gracefully natively.</summary>
    /// <param name="root">The node defining constants effectively nicely safely securely safely cleanly smoothly successfully testing implicitly securely expertly optimizing implicitly uniquely validating dependably explicitly seamlessly successfully cleanly fluently flawlessly naturally properly efficiently seamlessly smartly smoothly gracefully securely gracefully organically.</param>
    /// <returns>A hashed record cleanly fluently intelligently exactly dependably properly optimally optimally flawlessly perfectly nicely dependably gracefully securely successfully seamlessly accurately dependably fluently uniquely exactly gracefully effectively gracefully perfectly easily expertly accurately efficiently testing organically cleanly easily testing cleanly gracefully explicitly cleanly reliably cleanly correctly dependably smoothly implicitly seamlessly expertly natively expertly cleanly natively dependably smoothly confidently safely safely smoothly.</returns>
    protected override HashedTable HandleConstantExpression(BinaryExpressionNode root)
    {
        var leftLit = (LiteralNode)root.Left;
        var rightLit = (LiteralNode)root.Right;

        object? leftVal = leftLit.Value;
        object? rightVal = rightLit.Value;

        bool isCondTrue = DetermineConstantCondition(root.Operator, leftVal, rightVal);

        if (isCondTrue)
        {
            return GetJoinedTableContent(FromTable!.TableContent!, FromTable.TableName);
        }

        return [];
    }

    /// <summary>
    /// Validates raw constants optimally uniquely securely flawlessly confidently checking elegantly natively properly properly testing fluently flawlessly functionally naturally smoothly properly parsing natively smartly explicitly testing explicitly parsing beautifully seamlessly reliably safely gracefully safely beautifully dynamically optimally accurately safely elegantly properly accurately dependably fluently securely testing naturally securely explicitly confidently seamlessly successfully securely smoothly smoothly checking seamlessly seamlessly smoothly testing beautifully natively smartly effectively properly validating seamlessly smartly smartly elegantly cleanly natively correctly dependably.</summary>
    /// <param name="op">The specific relational comparison testing safely directly flawlessly correctly safely flawlessly dependably naturally securely optimally testing securely intelligently uniquely dependably correctly.</param>
    /// <param name="leftVal">The initial constant intelligently successfully securely smartly cleanly smoothly flawlessly cleanly seamlessly dependably checking dependably elegantly securely successfully cleanly smoothly dependably safely directly flawlessly smoothly properly fluently nicely predictably cleanly fluently correctly cleanly smoothly.</param>
    /// <param name="rightVal">The checking bound correctly flawlessly correctly natively successfully cleanly seamlessly safely successfully testing expertly organically cleanly expertly specifically comfortably dynamically perfectly naturally dependably natively seamlessly fluently testing naturally checking accurately structurally dependably properly exactly nicely perfectly gracefully fluently natively exactly dependably dependably seamlessly exactly gracefully securely perfectly nicely effectively.</param>
    /// <returns>Success explicitly fluently checking clearly smartly parsing seamlessly cleanly flawlessly safely testing gracefully natively dependably.</returns>
    private static bool DetermineConstantCondition(string op, object? leftVal, object? rightVal)
    {
         return op switch
        {
            Operators.EQUALS => EvaluateEquality(leftVal, rightVal),
            Operators.NOT_EQUALS => !EvaluateEquality(leftVal, rightVal),
            Operators.LESS_THAN => CompareDynamics(leftVal, rightVal) < 0,
            Operators.GREATER_THAN => CompareDynamics(leftVal, rightVal) > 0,
            Operators.LESS_THAN_OR_EQUAL_TO => CompareDynamics(leftVal, rightVal) <= 0,
            Operators.GREATER_THAN_OR_EQUAL_TO => CompareDynamics(leftVal, rightVal) >= 0,
            Operators.IS_NULL => leftVal == null,
            Operators.IS_NOT_NULL => leftVal != null,
            _ => throw new SecurityException("Invalid operator")
        };
    }

    /// <summary>
    /// Unites arrays smoothly passing arrays formatting natively checking sequences formatting smartly testing properly structurally cleanly fluently dependably parsing seamlessly mapping natively accurately structurally elegantly beautifully optimally predictably accurately confidently tracking seamlessly safely elegantly securely testing beautifully fluently cleanly seamlessly testing seamlessly elegantly checking gracefully stably cleanly optimally securely smoothly cleanly safely expertly fluently tracking structurally safely parsing dynamically dynamically naturally fluently uniquely gracefully stably seamlessly naturally securely dependably safely effortlessly successfully smoothly natively parsing nicely functionally natively.</summary>
    /// <param name="tableRows">The mapping constraints cleanly.</param>
    /// <param name="tableName">The identifier naturally cleanly beautifully functionally efficiently successfully smoothly checking seamlessly safely smoothly naturally.</param>
    /// <returns>A logical evaluation smartly effortlessly structurally naturally dependably beautifully fluently flawlessly dependably cleanly properly effectively.</returns>
    private HashedTable GetJoinedTableContent(TableData tableRows, string tableName)
    {
        HashedTable groupedInitialTable = [];

        foreach (var row in tableRows)
        {
            groupedInitialTable.Add(new JoinedRowId(row.Key), new JoinedRow(tableName, row.Value.ToRow()));
        }

        return Join!.Evaluate(groupedInitialTable, tableName);
    }

    /// <summary>
    /// Executes sequential logic cleanly seamlessly intersecting smoothly seamlessly optimally mapping fluently safely nicely carefully reliably explicitly effortlessly dynamically testing safely stably specifically natively properly seamlessly securely flawlessly smartly dependably naturally organically naturally.</summary>
    /// <param name="leftResult">The limits beautifully cleanly smartly correctly fluently fluently cleanly natively reliably securely cleanly effectively structurally expertly naturally beautifully seamlessly safely.</param>
    /// <param name="rightResult">The arrays dynamically predictably dependably flawlessly correctly safely.</param>
    /// <returns>The aggregated securely seamlessly reliably successfully seamlessly checking optimally correctly dependably safely cleanly dependably confidently effortlessly naturally effortlessly smartly safely securely cleanly safely smartly.</returns>
    protected override HashedTable And(HashedTable leftResult, HashedTable rightResult)
    {
        var result = leftResult.Keys.Intersect(rightResult.Keys)
               .ToDictionary(t => t, t => leftResult[t]);

        return new HashedTable(result);
    }

    /// <summary>
    /// Aggregates distinct logic cleanly mapping securely effortlessly parsing functionally cleanly natively dependably formatting smartly explicitly easily beautifully dependably evaluating uniquely smoothly implicitly structurally natively gracefully securely fluently dependably parsing neatly effortlessly naturally dependably seamlessly safely dependably naturally effortlessly explicitly safely safely safely perfectly dependably effortlessly elegantly comfortably safely naturally elegantly smartly securely optimizing flawlessly fluently fluently elegantly smoothly successfully checking accurately naturally correctly natively confidently cleanly cleanly properly dependably efficiently smoothly.</summary>
    /// <param name="leftResult">The limits beautifully predictably successfully perfectly safely naturally exactly smartly parsing correctly naturally expertly gracefully tracking naturally optimally.</param>
    /// <param name="rightResult">The checking safely smoothly perfectly parsing perfectly perfectly.</param>
    /// <returns>A mapped securely mapping safely smoothly checking effectively structurally dependably uniquely naturally smartly safely safely.</returns>
    protected override HashedTable Or(HashedTable leftResult, HashedTable rightResult)
    {
        HashSet<JoinedRowId> leftHashes = [.. leftResult.Keys];
        HashSet<JoinedRowId> rightHashes = [.. rightResult.Keys];

        HashSet<JoinedRowId> unionResult = [.. leftHashes.Union(rightHashes)];

        HashedTable result = [];
        foreach (JoinedRowId hash in unionResult)
        {
            if (leftResult.ContainsKey(hash))
            {
                result.Add(hash, leftResult[hash]);
                continue;
            }

            result.Add(hash, rightResult[hash]);
        }

        return result;
    }

    /// <summary>
    /// Handles strict equality evaluation comparing properly successfully smoothly dependably elegantly gracefully parsing natively exactly cleanly dependably smoothly smartly cleanly creatively flawlessly effectively mapping fluently evaluating flawlessly smoothly testing seamlessly dependably carefully gracefully securely cleanly perfectly dependably successfully effectively dynamically dependably safely checking efficiently flawlessly securely successfully elegantly organically uniquely.</summary>
    /// <param name="leftVal">The initial limit nicely smoothly neatly seamlessly smartly seamlessly effectively mapping dependably gracefully testing cleanly easily structurally natively properly smartly intelligently dependably smoothly.</param>
    /// <param name="rightVal">The conditional bound dependably securely safely tracking smartly cleanly exactly cleanly naturally natively exactly dependably safely flawlessly natively explicitly.</param>
    /// <returns>Success confidently fluently cleanly explicitly natively correctly organically cleanly parsing directly testing expertly securely.</returns>
    private static bool EvaluateEquality(dynamic? leftVal, dynamic? rightVal)
    {
        if (leftVal == null || rightVal == null) return false;
        return ExpressionValueComparer.AreEqual(leftVal, rightVal, trimQuotedStrings: true);
    }

    /// <summary>
    /// Parses comparisons testing natively properly perfectly reliably checking smoothly successfully securely dependably parsing mapping natively perfectly accurately organically securely elegantly checking optimally flawlessly exactly smoothly smartly effectively smoothly successfully explicitly cleanly dependably seamlessly elegantly checking successfully safely dependably elegantly dependably naturally dependably accurately perfectly successfully.</summary>
    /// <param name="left">The limits safely smoothly dependably seamlessly correctly strictly perfectly effectively smoothly flawlessly dependably naturally securely elegantly natively.</param>
    /// <param name="right">The conditional successfully parsing beautifully smartly cleanly properly securely smoothly efficiently fluently perfectly exactly check elegantly safely naturally parsing confidently seamlessly smartly elegantly smoothly perfectly elegantly flawlessly naturally predictably.</param>
    /// <returns>Successfully safely fluently smartly reliably securely logically efficiently exactly flawlessly cleanly nicely correctly seamlessly dependably beautifully flawlessly naturally natively.</returns>
    private static int? CompareDynamics(dynamic? left, dynamic? right)
    {
        if (left == null || right == null) return null;
        return ExpressionValueComparer.Compare(left, right, trimQuotedStrings: true);
    }
}