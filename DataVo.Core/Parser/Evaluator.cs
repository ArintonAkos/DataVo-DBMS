using DataVo.Core.Contracts;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.DQL;
using DataVo.Core.Parser.Transactions;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Runtime;

namespace DataVo.Core.Parser;

internal class Evaluator(List<SqlStatement> statements, DataVoEngine? engine = null)
{
    private readonly DataVoEngine _engine = engine ?? DataVoEngine.Current();

    public List<Queue<IDbAction>> ToRunnables()
    {
        List<Queue<IDbAction>> runnables = [];
        Queue<IDbAction> currentQueue = new();

        foreach (var statement in statements)
        {
            if (statement is SelectStatement selectAst)
            {
                currentQueue.Enqueue(BindEngine(new Select(selectAst)));
            }
            else if (statement is UnionSelectStatement unionSelectAst)
            {
                currentQueue.Enqueue(BindEngine(new UnionSelect(unionSelectAst)));
            }
            else if (statement is InsertIntoStatement insertAst)
            {
                currentQueue.Enqueue(BindEngine(new DML.InsertInto(insertAst)));
            }
            else if (statement is DeleteFromStatement deleteAst)
            {
                currentQueue.Enqueue(BindEngine(new DML.DeleteFrom(deleteAst)));
            }
            else if (statement is UpdateStatement updateAst)
            {
                currentQueue.Enqueue(BindEngine(new DML.Update(updateAst)));
            }
            else if (statement is CreateTableStatement createTableAst)
            {
                currentQueue.Enqueue(BindEngine(new DDL.CreateTable(createTableAst)));
            }
            else if (statement is DropTableStatement dropTableAst)
            {
                currentQueue.Enqueue(BindEngine(new DDL.DropTable(dropTableAst)));
            }
            else if (statement is AlterTableAddColumnStatement alterTableAddColumnAst)
            {
                currentQueue.Enqueue(BindEngine(new DDL.AlterTableAddColumn(alterTableAddColumnAst)));
            }
            else if (statement is AlterTableDropColumnStatement alterTableDropColumnAst)
            {
                currentQueue.Enqueue(BindEngine(new DDL.AlterTableDropColumn(alterTableDropColumnAst)));
            }
            else if (statement is CreateIndexStatement createIndexAst)
            {
                currentQueue.Enqueue(BindEngine(new DDL.CreateIndex(createIndexAst)));
            }
            else if (statement is DropIndexStatement dropIndexAst)
            {
                currentQueue.Enqueue(BindEngine(new DDL.DropIndex(dropIndexAst)));
            }
            else if (statement is CreateDatabaseStatement createDbAst)
            {
                currentQueue.Enqueue(BindEngine(new DDL.CreateDatabase(createDbAst)));
            }
            else if (statement is DropDatabaseStatement dropDbAst)
            {
                currentQueue.Enqueue(BindEngine(new DDL.DropDatabase(dropDbAst)));
            }
            else if (statement is UseStatement useAst)
            {
                currentQueue.Enqueue(BindEngine(new Commands.Use(useAst)));
            }
            else if (statement is ShowDatabasesStatement showDbAst)
            {
                currentQueue.Enqueue(BindEngine(new Commands.ShowDatabases(showDbAst)));
            }
            else if (statement is ShowTablesStatement showTablesAst)
            {
                currentQueue.Enqueue(BindEngine(new Commands.ShowTables(showTablesAst)));
            }
            else if (statement is DescribeStatement describeAst)
            {
                currentQueue.Enqueue(BindEngine(new Commands.Describe(describeAst)));
            }
            else if (statement is GoStatement goAst)
            {
                currentQueue.Enqueue(BindEngine(new Commands.Go(goAst)));
            }
            else if (statement is VacuumStatement vacuumAst)
            {
                currentQueue.Enqueue(BindEngine(new DML.Vacuum(vacuumAst)));
            }
            else if (statement is BeginTransactionStatement)
            {
                currentQueue.Enqueue(BindEngine(new BeginTransaction()));
            }
            else if (statement is CommitStatement)
            {
                currentQueue.Enqueue(BindEngine(new Commit()));
            }
            else if (statement is RollbackStatement)
            {
                currentQueue.Enqueue(BindEngine(new Rollback()));
            }
            else
            {
                throw new Exception($"Evaluator Error: Unsupported AST Node type '{statement.GetType().Name}'.");
            }
        }

        if (currentQueue.Count > 0)
        {
            runnables.Add(currentQueue);
        }

        return runnables;
    }

    private IDbAction BindEngine(IDbAction action)
    {
        if (action is BaseDbAction baseDbAction)
        {
            baseDbAction.UseEngine(_engine);
        }

        return action;
    }
}
