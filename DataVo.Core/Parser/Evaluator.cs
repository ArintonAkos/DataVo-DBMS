using DataVo.Core.Contracts;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.DQL;

namespace DataVo.Core.Parser;

internal class Evaluator
{
    private readonly List<SqlStatement> _statements;

    public Evaluator(List<SqlStatement> statements)
    {
        _statements = statements;
    }

    public List<Queue<IDbAction>> ToRunnables()
    {
        List<Queue<IDbAction>> runnables = new();
        Queue<IDbAction> currentQueue = new();

        foreach (var statement in _statements)
        {
            if (statement is SelectStatement selectAst)
            {
                currentQueue.Enqueue(new Select(selectAst));
            }
            else if (statement is InsertIntoStatement insertAst)
            {
                currentQueue.Enqueue(new DML.InsertInto(insertAst));
            }
            else if (statement is DeleteFromStatement deleteAst)
            {
                currentQueue.Enqueue(new DML.DeleteFrom(deleteAst));
            }
            else if (statement is CreateTableStatement createTableAst)
            {
                currentQueue.Enqueue(new DDL.CreateTable(createTableAst));
            }
            else if (statement is DropTableStatement dropTableAst)
            {
                currentQueue.Enqueue(new DDL.DropTable(dropTableAst));
            }
            else if (statement is CreateIndexStatement createIndexAst)
            {
                currentQueue.Enqueue(new DDL.CreateIndex(createIndexAst));
            }
            else if (statement is DropIndexStatement dropIndexAst)
            {
                currentQueue.Enqueue(new DDL.DropIndex(dropIndexAst));
            }
            else if (statement is CreateDatabaseStatement createDbAst)
            {
                currentQueue.Enqueue(new DDL.CreateDatabase(createDbAst));
            }
            else if (statement is DropDatabaseStatement dropDbAst)
            {
                currentQueue.Enqueue(new DDL.DropDatabase(dropDbAst));
            }
            else if (statement is UseStatement useAst)
            {
                currentQueue.Enqueue(new Commands.Use(useAst));
            }
            else if (statement is ShowDatabasesStatement showDbAst)
            {
                currentQueue.Enqueue(new Commands.ShowDatabases(showDbAst));
            }
            else if (statement is ShowTablesStatement showTablesAst)
            {
                currentQueue.Enqueue(new Commands.ShowTables(showTablesAst));
            }
            else if (statement is DescribeStatement describeAst)
            {
                currentQueue.Enqueue(new Commands.Describe(describeAst));
            }
            else if (statement is GoStatement goAst)
            {
                currentQueue.Enqueue(new Commands.Go(goAst));
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
}
