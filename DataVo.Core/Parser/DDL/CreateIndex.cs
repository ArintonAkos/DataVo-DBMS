using DataVo.Core.Indexing;
﻿using System.Text.RegularExpressions;
using DataVo.Core.Exceptions;
using DataVo.Core.Logging;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.StorageEngine;
using DataVo.Core.Parser.AST;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.DDL;

/// <summary>
/// Handles the <c>CREATE INDEX</c> DDL statement.
/// <para>
/// Registers a new index in the system catalog and populates it with data from
/// all existing rows in the target table. The index is backed by a B-Tree structure
/// managed by <see cref="DataVo.Core.Indexing.IndexManager"/>.
/// </para>
/// <para>
/// For composite indexes (multiple columns), the key is formed by concatenating
/// column values with <c>"##"</c> as a delimiter.
/// </para>
/// </summary>
/// <param name="ast">The parsed <see cref="CreateIndexStatement"/> AST node.</param>
/// <example>
/// <code>
/// var ast = new CreateIndexStatement { IndexName = "Idx_LastName", TableName = "Users" };
/// var action = new CreateIndex(ast);
/// action.PerformAction(sessionId);
/// // Output message: "New index file Idx_LastName successfully created!"
/// </code>
/// </example>
internal class CreateIndex(CreateIndexStatement ast) : BaseDbAction
{
    /// <summary>
    /// The parsed model containing the index name, target table, and indexed column names.
    /// </summary>
    private readonly CreateIndexModel _model = CreateIndexModel.FromAst(ast);

    /// <summary>
    /// Executes the index creation pipeline:
    /// <list type="number">
    ///   <item><description>Resolves the active database from the session cache.</description></item>
    ///   <item><description>Registers the index definition in the system catalog.</description></item>
    ///   <item><description>Reads all existing rows from the target table via the storage engine.</description></item>
    ///   <item><description>Builds the index key-to-rowID mapping from the existing data.</description></item>
    ///   <item><description>Creates the B-Tree index file via <see cref="DataVo.Core.Indexing.IndexManager"/>.</description></item>
    /// </list>
    /// </summary>
    /// <param name="session">The session identifier used to resolve the active database from the cache.</param>
    /// <remarks>
    /// On failure, the error message is logged and appended to <see cref="BaseDbAction.Messages"/>.
    /// </remarks>
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = GetDatabaseName(session);
            if (_model.Metric is not null && !Indexes.SupportsVectorIndexType(_model.IndexKind))
            {
                throw new BindingException($"Index option metric applies only to vector indexes; {_model.IndexKind} has no distance metric.");
            }

            if (_model.Hnsw is not null && !string.Equals(_model.IndexKind, "HNSW", StringComparison.OrdinalIgnoreCase))
            {
                throw new BindingException($"Index options {Models.Catalog.HnswIndexOptions.OptionNames} apply only to HNSW indexes, not {_model.IndexKind}.");
            }

            if (string.Equals(_model.IndexKind, Models.Catalog.IndexFile.ColumnarKind, StringComparison.OrdinalIgnoreCase))
            {
                CreateColumnar(databaseName);
                return;
            }

            Catalog.CreateIndex(_model.ToIndexFile(), _model.TableName, databaseName);

            var tableDataRows = Context.GetTableContents(_model.TableName, databaseName);
            if (Indexes.SupportsVectorIndexType(_model.IndexKind))
            {
                if (_model.Attributes.Count != 1)
                {
                    throw new BindingException($"Vector index type '{_model.IndexKind}' currently supports exactly one VECTOR column.");
                }

                string vectorColumnName = _model.Attributes[0];
                string columnType = Catalog.GetTableColumnType(_model.TableName, databaseName, vectorColumnName);
                if (!columnType.StartsWith("VECTOR", StringComparison.OrdinalIgnoreCase))
                {
                    throw new BindingException("HNSW index can only be created on VECTOR columns.");
                }

                List<(RowKey RowId, float[] Vector)> vectors = [];
                foreach (var row in tableDataRows)
                {
                    if (!row.Value.TryGetValue(vectorColumnName, out object? value) || value == null)
                    {
                        continue;
                    }

                    if (VectorParser.TryCoerceToVector(value, out float[] vector))
                    {
                        vectors.Add((row.Key, vector));
                    }
                }

                Indexes.CreateVectorIndex(
                    vectors,
                    _model.IndexName,
                    _model.TableName,
                    databaseName,
                    metric: _model.Metric ?? Models.Catalog.IndexFile.CosineMetric,
                    indexType: _model.IndexKind,
                    hnswOptions: _model.Hnsw);
            }
            else
            {
                ScalarIndexContents indexValues = CreateIndexContents(tableDataRows);
                Indexes.CreateIndex(indexValues, _model.IndexName, _model.TableName, databaseName);
            }

            Logger.Info($"New index file {_model.IndexName} successfully created!");
            Messages.Add($"New index file {_model.IndexName} successfully created!");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            AddError(ex);
        }
    }

    /// <summary>
    /// Records a columnar index and builds it from the table's rows. It holds no keys, so nothing else that
    /// maintains or consults indexes hears of it; the storage engine keeps it current as rows change.
    /// </summary>
    private void CreateColumnar(string databaseName)
    {
        Engine.Columnar.Validate(databaseName, _model.TableName, _model.Attributes);
        Catalog.CreateIndex(_model.ToIndexFile(), _model.TableName, databaseName);
        Engine.Columnar.Find(databaseName, _model.TableName);

        Logger.Info($"New columnar index {_model.IndexName} successfully created!");
        Messages.Add($"New columnar index {_model.IndexName} successfully created!");
    }

    /// <summary>
    /// Gathers the index's contents from every row of the table: row ids per key (column values joined by
    /// <c>"##"</c>), and the rows whose key has a NULL part, which follow the rule every write path uses.
    /// </summary>
    private ScalarIndexContents CreateIndexContents(Dictionary<RowKey, Dictionary<string, object?>> tableData)
    {
        var contents = new ScalarIndexContents();
        foreach (KeyValuePair<RowKey, Dictionary<string, object?>> row in tableData)
        {
            contents.Add(row.Value, row.Key, _model.Attributes);
        }

        return contents;
    }
}
