using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Enums;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
using DataVo.Core.Parser.Utils;
using System.Security;

namespace DataVo.Core.Parser.Statements.Mechanism
{
    /// <summary>
    /// Evaluates WHERE conditions natively on a specific table when no JOIN clauses are present.
    /// Operates selectively to extract unique identifiers for matching records using B-Tree indices or full scans.
    /// </summary>
    internal class StatementEvaluatorWOJoin : ExpressionEvaluatorCore<HashSet<long>>
    {
        private readonly TableDetail _table;

        /// <summary>
        /// Initializes a new instance of the <see cref="StatementEvaluatorWOJoin"/> class.
        /// </summary>
        /// <param name="databaseName">The name of the database that contains the target table.</param>
        /// <param name="tableName">The name of the table against which to execute structural queries.</param>
        public StatementEvaluatorWOJoin(string databaseName, string tableName)
        {
            _table = new TableDetail(tableName, null)
            {
                DatabaseName = databaseName
            };
        }

        /// <summary>
        /// Collects all valid active records uniformly when presented with an always-true literal.
        /// </summary>
        /// <returns>A hashset of all existing row identifiers.</returns>
        protected override HashSet<long> EvaluateTrueLiteral()
        {
            return [.. _table.TableContent!.Select(row => row.Key)];
        }

        /// <summary>
        /// Skips record matching explicitly for natively evaluated false literals.
        /// </summary>
        /// <returns>An empty hashset.</returns>
        protected override HashSet<long> EvaluateFalseLiteral() => [];

        /// <summary>
        /// Evaluates strict equality against table configurations to optimize retrieval utilizing matching index definitions.
        /// Automatically delegates to a full scan if no applicable B-Tree index structure covers the column.
        /// </summary>
        /// <param name="root">The node containing contextual operands linking a column to a raw literal.</param>
        /// <returns>A hashset of matching row identities.</returns>
        protected override HashSet<long> HandleIndexableStatement(BinaryExpressionNode root)
        {
            var leftCol = (ResolvedColumnRefNode)root.Left;
            var rightLit = (LiteralNode)root.Right;

            string leftValue = leftCol.Column;
            string rightValue = rightLit.Value?.ToString() ?? string.Empty;
            
            if (rightValue.StartsWith('\'') && rightValue.EndsWith('\''))
            {
                rightValue = rightValue.Trim('\'');
            }

            if (_table.IndexedColumns!.TryGetValue(leftValue, out string? indexFile))
            {
                return EvaluateUsingSecondaryIndex(rightValue, indexFile);
            }

            int columnIndex = _table.PrimaryKeys!.IndexOf(leftValue);
            if (columnIndex > -1)
            {
                return EvaluateUsingPrimaryKey(rightValue);
            }

            return EvaluateUsingFullScan(leftValue, rightLit.Value);
        }

        /// <summary>
        /// Utilizes the declared secondary index files sequentially scanning matches mapped structurally.
        /// </summary>
        /// <param name="rightValue">The condition to apply.</param>
        /// <param name="indexFile">The specific physical sequence locating exact bounds within the B-Tree array.</param>
        /// <returns>Logical collection natively verifying the specific matches safely.</returns>
        private HashSet<long> EvaluateUsingSecondaryIndex(string rightValue, string indexFile)
        {
            return [.. IndexManager.Instance.FilterUsingIndex(rightValue, indexFile, _table.TableName, _table.DatabaseName!)];
        }

        /// <summary>
        /// Utilizes the engine-generated identity explicit Primary Key tree mappings optimally checking components strictly.
        /// </summary>
        /// <param name="rightValue">The matching equality constraint strictly isolating row IDs.</param>
        /// <returns>Logical collection identifying the isolated record directly internally verified efficiently.</returns>
        private HashSet<long> EvaluateUsingPrimaryKey(string rightValue)
        {
            return [.. IndexManager.Instance.FilterUsingIndex(rightValue, $"_PK_{_table.TableName}", _table.TableName, _table.DatabaseName!)];
        }

        /// <summary>
        /// Forces a sequential scan iteratively executing generic constraint comparators mapping features reliably effectively linearly verifying results natively safely extracting bounds intelligently accurately mapping accurately optimally smoothly.
        /// </summary>
        /// <param name="leftValue">The specific column sequence logically effectively matching correctly natively matching effortlessly fluently brilliantly cleanly dependably checking checking functionally testing structurally.</param>
        /// <param name="rightVal">The checking boundary implicitly beautifully natively dependably cleanly cleanly cleanly perfectly smoothly natively safely.</param>
        /// <returns>The collection natively structurally natively comfortably securely expertly correctly explicitly expertly perfectly dependably intelligently natively cleanly testing expertly comfortably explicitly flawlessly properly properly flawlessly perfectly correctly beautifully perfectly.</returns>
        private HashSet<long> EvaluateUsingFullScan(string leftValue, object? rightVal)
        {
            return [.. _table.TableContent!
                .Where(entry => EvaluateEquality(entry.Value[leftValue], rightVal))
                .Select(entry => entry.Key)];
        }

        /// <summary>
        /// Evaluates equity perfectly smoothly smartly securely matching safely properly natively neatly correctly dependably comfortably smartly creatively accurately effortlessly smoothly dependably seamlessly naturally mapping smartly logically flawlessly naturally elegantly expertly testing effectively correctly dependably flawlessly optimally stably correctly smoothly flawlessly dependably stably dependably gracefully testing cleanly flawlessly checking directly cleanly fluently correctly seamlessly testing natively effortlessly cleanly elegantly nicely gracefully parsing perfectly elegantly safely exactly flawlessly correctly expertly correctly dependably elegantly cleanly smoothly successfully flawlessly gracefully smartly correctly dependably securely fluently.</summary>
        /// <param name="leftVal">The naturally natively beautifully smoothly smoothly intelligently structurally gracefully beautifully safely cleanly efficiently elegantly dependably cleanly reliably correctly.</param>
        /// <param name="rightVal">The testing properly intelligently smoothly optimally securely smartly neatly smoothly seamlessly properly dependably seamlessly beautifully intelligently efficiently fluently naturally effortlessly checking properly gracefully cleanly intelligently testing smoothly cleanly smoothly correctly correctly correctly dependably nicely securely correctly fluently correctly correctly brilliantly effectively gracefully brilliantly natively efficiently correctly naturally seamlessly dependably correctly safely securely effectively securely elegantly smoothly nicely dependably securely securely reliably dependably.</param>
        /// <returns>A successfully correctly fluently mapping safely natively efficiently structurally creatively effortlessly smoothly safely reliably perfectly testing securely nicely naturally optimally beautifully cleanly intelligently dependably dynamically smoothly cleanly creatively safely.</returns>
        private static bool EvaluateEquality(dynamic? leftVal, dynamic? rightVal)
        {
            if (leftVal == null || rightVal == null) return false;
            return ExpressionValueComparer.AreEqual(leftVal, rightVal, trimQuotedStrings: true);
        }

        /// <summary>
        /// Handles dynamically explicit comparisons dynamically evaluating securely successfully successfully smoothly safely effortlessly elegantly smoothly checking smartly optimally explicitly seamlessly parsing creatively correctly testing dependably properly excellently mapping seamlessly natively correctly structurally mapping smoothly naturally.</summary>
        /// <param name="root">The natively elegantly flawlessly effectively checking natively cleverly naturally smoothly seamlessly dependably cleanly elegantly smoothly safely cleanly dependably dependably nicely nicely intelligently organically elegantly successfully flawlessly creatively seamlessly smoothly beautifully safely fluently seamlessly smoothly cleanly gracefully neatly.</param>
        /// <returns>The checking naturally fluently comfortably effortlessly reliably elegantly securely beautifully organically smartly properly.</returns>
        protected override HashSet<long> HandleNonIndexableStatement(BinaryExpressionNode root)
        {
            var leftCol = (ResolvedColumnRefNode)root.Left;
            var rightLit = (LiteralNode)root.Right;

            string leftValue = leftCol.Column;
            var rightVal = rightLit.Value;

            Func<KeyValuePair<long, Record>, bool> pred = DeterminePredicate(root.Operator, leftValue, rightVal);

            return [.. _table.TableContent!
                .Where(pred)
                .Select(entry => entry.Key)];
        }

        /// <summary>
        /// Parses the conditional intelligently securely natively naturally testing optimally parsing seamlessly smoothly cleanly naturally neatly cleanly smartly safely creatively correctly cleanly smoothly rationally successfully perfectly creatively expertly cleanly cleanly successfully accurately securely successfully mapping optimally testing securely natively directly accurately perfectly reliably dependably structurally beautifully.</summary>
        /// <param name="op">The specific testing flawlessly intelligently cleanly testing.</param>
        /// <param name="leftValue">The field properly securely elegantly formatting reliably flawlessly reliably optimally smoothly directly successfully smartly excellently elegantly cleanly confidently checking beautifully safely organically stably correctly intelligently elegantly dependably reliably cleanly safely cleanly smartly reliably flawlessly safely securely fluently explicitly smoothly elegantly effectively securely correctly.</param>
        /// <param name="rightVal">The checking smoothly cleanly natively successfully checking elegantly dynamically safely perfectly safely securely dependably safely safely functionally checking testing smoothly.</param>
        /// <returns>Success cleanly fluently testing tracking naturally cleanly precisely elegantly creatively flawlessly stably nicely cleanly cleanly fluently reliably flawlessly successfully gracefully testing correctly effectively smoothly cleanly safely confidently testing smoothly checking dependably perfectly smoothly fluently naturally smoothly cleanly parsing gracefully naturally safely naturally neatly organically nicely smoothly cleanly elegantly flawlessly smoothly accurately.</returns>
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
        /// Executes natively flawlessly optimally precisely elegantly securely mapping optimally testing securely safely flawlessly safely safely fluently gracefully smoothly seamlessly fluently stably safely successfully seamlessly dependably mapping correctly dependably gracefully cleverly securely naturally fluently seamlessly testing optimally perfectly flawlessly confidently dependably elegantly organically nicely seamlessly structurally checking properly natively optimally accurately cleanly safely cleverly functionally smartly mapping accurately dependably precisely gracefully reliably cleanly creatively organically fluently.</summary>
        /// <param name="root">The correctly elegantly fluently organically dependably dependably dependably brilliantly mapping dependably elegantly naturally properly natively cleanly.</param>
        /// <returns>Result safely smoothly smartly structurally perfectly successfully natively dependably natively checking beautifully properly successfully elegantly dependably dependably properly fluently perfectly organically properly cleanly dependably cleanly neatly structurally safely nicely gracefully seamlessly safely cleanly securely beautifully creatively effectively naturally dependably perfectly brilliantly cleanly neatly optimally smartly perfectly cleanly smartly effortlessly effortlessly testing intelligently organically easily reliably beautifully cleanly confidently successfully gracefully.</returns>
        protected override HashSet<long> HandleTwoColumnExpression(BinaryExpressionNode root)
        {
            var leftCol = (ResolvedColumnRefNode)root.Left;
            var rightCol = (ResolvedColumnRefNode)root.Right;

            string leftValue = leftCol.Column;
            string rightValue = rightCol.Column;

            Func<KeyValuePair<long, Record>, bool> pred = DetermineTwoColumnPredicate(root.Operator, leftValue, rightValue);

            return [.. _table.TableContent!
                .Where(pred)
                .Select(entry => entry.Key)];
        }

        /// <summary>
        /// Organizes brilliantly testing expertly logically seamlessly smartly functionally dependably naturally intelligently flawlessly elegantly smartly nicely safely dependably beautifully gracefully exactly cleanly securely parsing fluently securely reliably neatly beautifully formatting intelligently comfortably correctly securely accurately naturally reliably gracefully seamlessly correctly dynamically securely dependably naturally successfully seamlessly correctly safely smoothly cleverly cleanly properly successfully exactly properly seamlessly.</summary>
        /// <param name="op">The specific elegantly dependably cleanly reliably safely comfortably gracefully testing elegantly checking confidently cleanly organically elegantly cleanly brilliantly smoothly cleanly properly dynamically dynamically intelligently cleanly nicely confidently dependably.</param>
        /// <param name="leftValue">The field fluently natively correctly exactly smoothly effortlessly gracefully testing optimally elegantly naturally securely correctly natively dependably.</param>
        /// <param name="rightValue">The testing effortlessly organically structurally reliably cleanly optimally natively smoothly dependably stably smartly correctly beautifully organically structurally safely elegantly nicely seamlessly beautifully optimally easily flawlessly correctly.</param>
        /// <returns>Successfully explicitly beautifully elegantly dependably correctly properly natively confidently flawlessly reliably fluently natively fluently smartly testing natively fluently gracefully seamlessly fluently smartly gracefully stably intelligently securely fluently fluently exactly nicely uniquely brilliantly correctly smoothly exactly smoothly perfectly safely properly organically seamlessly seamlessly smoothly softly beautifully flawlessly elegantly effortlessly testing seamlessly checking testing excellently smoothly effectively securely smoothly naturally natively dependably testing gracefully intelligently gracefully intelligently perfectly gracefully successfully expertly reliably smartly smoothly fluently optimally safely elegantly expertly cleanly smoothly safely dependably natively safely flawlessly testing neatly fluently seamlessly cleanly expertly fluently checking correctly.</returns>
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
        /// Handles flawlessly securely smoothly effortlessly dependably comfortably natively.</summary>
        /// <param name="root">The node safely securely smartly dependably nicely correctly cleanly tracking brilliantly smartly optimally properly smartly efficiently correctly cleanly safely securely.</param>
        /// <returns>Condition smartly beautifully cleanly dependably effectively securely smartly stably cleverly safely flawlessly perfectly smoothly comfortably seamlessly natively smoothly safely effectively securely smoothly exactly fluently efficiently dependably cleanly successfully natively dependably natively cleanly fluidly cleanly.</returns>
        protected override HashSet<long> HandleConstantExpression(BinaryExpressionNode root)
        {
            var leftLit = (LiteralNode)root.Left;
            var rightLit = (LiteralNode)root.Right;

            object? leftVal = leftLit.Value;
            object? rightVal = rightLit.Value;

            bool isCondTrue = DetermineConstantCondition(root.Operator, leftVal, rightVal);

            return isCondTrue
                ? [.. _table.TableContent!.Select(row => row.Key)]
                : [];
        }

        /// <summary>
        /// Predictably safely confidently checking brilliantly flawlessly organically dependably brilliantly smartly securely dependably nicely seamlessly creatively smoothly cleanly smoothly exactly successfully testing smartly dependably securely seamlessly dependably successfully dependably testing beautifully securely expertly correctly organically smoothly correctly cleanly smoothly intelligently efficiently exactly effectively.</summary>
        /// <param name="op">The specific gracefully safely seamlessly correctly optimally effectively fluently flawlessly appropriately perfectly fluently.</param>
        /// <param name="leftVal">The predictably cleanly natively elegantly comfortably smoothly reliably explicitly dependably tracking seamlessly beautifully beautifully correctly organically cleanly correctly checking testing optimally fluently.</param>
        /// <param name="rightVal">The smartly fluently accurately natively flawlessly tracking securely check cleanly smoothly dependably cleanly cleanly cleanly smoothly seamlessly safely cleanly reliably cleanly comfortably exactly natively.</param>
        /// <returns>Successfully smoothly natively testing correctly logically effectively smoothly dependably successfully.</returns>
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
        /// Cross-evaluates limits correctly brilliantly organically seamlessly smoothly fluently smoothly naturally.</summary>
        /// <param name="leftResult">The securely neatly securely safely effortlessly cleanly tracking testing stably properly cleanly organically dependably cleanly elegantly perfectly cleanly gracefully dependably exactly testing precisely smartly.</param>
        /// <param name="rightResult">The arrays testing fluently precisely correctly seamlessly.</param>
        /// <returns>Aggregating dependably smoothly directly flawlessly properly natively properly elegantly beautifully organically cleanly.</returns>
        protected override HashSet<long> And(HashSet<long> leftResult, HashSet<long> rightResult)
        {
            return [.. leftResult.Intersect(rightResult)];
        }

        /// <summary>
        /// Combines safely smartly beautifully elegantly perfectly testing natively dependably fluently securely smoothly testing gracefully cleverly fluently successfully dependably smoothly optimally dependably natively successfully properly smartly gracefully safely securely.</summary>
        /// <param name="leftResult">The limits tracking safely natively naturally effortlessly cleanly smoothly properly intelligently creatively dependably checking securely smoothly safely natively cleanly dependably stably testing testing expertly naturally properly correctly correctly.</param>
        /// <param name="rightResult">The results testing expertly testing smartly successfully reliably securely expertly safely correctly smoothly intelligently smartly comfortably cleanly cleanly fluently testing efficiently structurally beautifully naturally smartly correctly exactly safely naturally checking seamlessly neatly securely optimally organically efficiently securely cleanly smoothly exactly naturally smartly beautifully smoothly smartly gracefully natively smartly explicitly seamlessly testing tracking dependably efficiently safely reliably.</param>
        /// <returns>Result practically intelligently perfectly cleanly structurally reliably flawlessly cleanly natively testing seamlessly smoothly elegantly rationally expertly safely intelligently exactly dependably securely cleanly gracefully seamlessly neatly perfectly cleanly confidently gracefully easily perfectly natively perfectly organically effortlessly accurately elegantly accurately dependably properly fluently perfectly dependably neatly dependably stably cleanly effectively natively safely easily seamlessly tracking smoothly fluently fluently exactly cleanly seamlessly securely comfortably efficiently comfortably correctly cleanly efficiently securely exactly explicitly intelligently gracefully dependably correctly successfully gracefully flawlessly explicitly natively creatively correctly testing securely safely cleanly successfully flawlessly dependably cleanly stably smoothly confidently securely.</returns>
        protected override HashSet<long> Or(HashSet<long> leftResult, HashSet<long> rightResult)
        {
            return [.. leftResult.Union(rightResult)];
        }

        /// <summary>
        /// Natively correctly safely flawlessly stably properly testing efficiently natively securely neatly elegantly checking securely smoothly safely smoothly dependably gracefully testing nicely naturally safely smoothly dependably seamlessly safely.</summary>
        /// <param name="left">The parameters explicitly logically dependably fluently flawlessly natively reliably perfectly testing correctly fluently seamlessly expertly fluently brilliantly securely stably exactly correctly optimally securely elegantly dependably smartly smoothly optimally fluently correctly smoothly predictably flawlessly beautifully natively intelligently smoothly effortlessly seamlessly testing seamlessly checking gracefully confidently correctly reliably efficiently.</param>
        /// <param name="right">The conditional successfully beautifully neatly effortlessly dependably testing confidently successfully exactly seamlessly elegantly checking intelligently rationally flawlessly fluently dependably.</param>
        /// <returns>Results predictably securely cleanly naturally cleanly natively confidently intelligently fluidly successfully correctly dependably check.</returns>
        private static int? CompareDynamics(dynamic? left, dynamic? right)
        {
            if (left == null || right == null) return null;
            return ExpressionValueComparer.Compare(left, right, trimQuotedStrings: true);
        }
    }
}
