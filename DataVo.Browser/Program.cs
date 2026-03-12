using System.Runtime.InteropServices.JavaScript;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Constants;
using DataVo.Core.Enums;
using DataVo.Core.Models.Catalog;
using DataVo.Core.BTree;

namespace DataVo.Browser;

internal sealed class ExceptionDetails
{
    public string Error { get; init; } = string.Empty;

    public string? Type { get; init; }

    public string? Stack { get; init; }

    public string? RootType { get; init; }

    public string? RootError { get; init; }

    public string? RootStack { get; init; }

    public List<object> InnerExceptions { get; init; } = [];
}

public partial class DataVoInterop
{
    public static void Main() { }

    private static DataVoEngine? _engine;
    private static readonly Guid _session = Guid.NewGuid();

    [JSImport("globalThis.DataVoStorage.readCatalog")]
    internal static partial string? ReadCatalogState();

    [JSImport("globalThis.DataVoStorage.writeCatalog")]
    internal static partial void WriteCatalogState(string xml);

    [JSImport("globalThis.DataVoStorage.readSelectedDatabase")]
    internal static partial string? ReadSelectedDatabase();

    [JSImport("globalThis.DataVoStorage.writeSelectedDatabase")]
    internal static partial void WriteSelectedDatabase(string databaseName);

    [JSImport("globalThis.DataVoStorage.clearAllStorage")]
    internal static partial void ClearAllStorage();

    [JSExport]
    public static void Initialize()
    {
        if (_engine != null) return;

        var config = new DataVoConfig
        {
            StorageMode = StorageMode.Custom,
            CustomStorageEngine = new BrowserStorageEngine()
        };
        _engine = DataVoEngine.Initialize(config);

        var catalogState = ReadCatalogState();
        if (!string.IsNullOrWhiteSpace(catalogState))
        {
            _engine.Catalog.LoadState(catalogState);
        }

        RebuildIndexesFromStorage();

        var selectedDatabase = ReadSelectedDatabase();
        if (!string.IsNullOrWhiteSpace(selectedDatabase))
        {
            _engine.Sessions.Set(_session, selectedDatabase);
        }

        PersistBrowserState();
    }

    [JSExport]
    public static string ExecuteSql(string sql)
    {
        if (_engine == null) return JsonConvert.SerializeObject(new { error = "Engine not initialized. Call Initialize() first." });

        try
        {
            var queryEngine = new QueryEngine(sql, _session, _engine);
            var results = queryEngine.Parse();

            // Serialize the List<QueryResult> back to JS
            return JsonConvert.SerializeObject(results);
        }
        catch (Exception ex)
        {
            var details = BuildExceptionDetails(ex);
            return JsonConvert.SerializeObject(details);
        }
        finally
        {
            PersistBrowserState();
        }
    }

    [JSExport]
    public static string DiagnoseLexer(string sql)
    {
        try
        {
            RuntimeHelpers.RunClassConstructor(typeof(Lexer).TypeHandle);

            var lexer = new Lexer(sql);
            var tokens = lexer.Tokenize();

            return JsonConvert.SerializeObject(new
            {
                ok = true,
                tokenCount = tokens.Count,
                tokens = tokens.Select(token => token.ToString()).ToList(),
                diagnostics = BuildLexerEnvironment()
            });
        }
        catch (Exception ex)
        {
            var details = BuildExceptionDetails(ex);
            return JsonConvert.SerializeObject(new
            {
                ok = false,
                stage = "LexerDiagnostics",
                diagnostics = BuildLexerEnvironment(),
                error = details.Error,
                type = details.Type,
                stack = details.Stack,
                rootType = details.RootType,
                rootError = details.RootError,
                rootStack = details.RootStack,
                innerExceptions = details.InnerExceptions
            });
        }
    }

    [JSExport]
    public static void ResetStorage()
    {
        ClearAllStorage();
        _engine?.Dispose();
        _engine = null;
    }

    private static object BuildLexerEnvironment()
    {
        return new
        {
            lexerAssembly = typeof(Lexer).Assembly.FullName,
            keywordsAssembly = typeof(SqlKeywords).Assembly.FullName,
            operatorsAssembly = typeof(Operators).Assembly.FullName,
            keywordCount = SqlKeywords.All.Length,
            hasCreate = SqlKeywords.All.Contains(SqlKeywords.CREATE),
            hasDatabase = SqlKeywords.All.Contains(SqlKeywords.DATABASE),
            andOperator = Operators.AND,
            orOperator = Operators.OR
        };
    }

    private static void PersistBrowserState()
    {
        if (_engine == null)
        {
            return;
        }

        WriteCatalogState(_engine.Catalog.ExportState());
        WriteSelectedDatabase(_engine.Sessions.Get(_session) ?? string.Empty);
    }

    private static void RebuildIndexesFromStorage()
    {
        if (_engine == null)
        {
            return;
        }

        foreach (string databaseName in _engine.Catalog.GetDatabases())
        {
            foreach (string tableName in _engine.Catalog.GetTables(databaseName))
            {
                var rows = _engine.StorageContext.GetTableContents(tableName, databaseName);
                List<IndexFile> indexes = _engine.Catalog.GetTableIndexes(tableName, databaseName);

                foreach (IndexFile index in indexes)
                {
                    var values = new Dictionary<string, List<long>>();

                    foreach (var rowEntry in rows)
                    {
                        var row = rowEntry.Value;
                        if (index.AttributeNames.Any(attribute => !row.ContainsKey(attribute) || row[attribute] == null))
                        {
                            continue;
                        }

                        string key = IndexKeyEncoder.BuildKeyString(row, index.AttributeNames);
                        if (!values.TryGetValue(key, out List<long>? rowIds))
                        {
                            rowIds = [];
                            values[key] = rowIds;
                        }

                        rowIds.Add(rowEntry.Key);
                    }

                    _engine.IndexManager.CreateIndex(values, index.IndexFileName, tableName, databaseName);
                }
            }
        }
    }

    private static ExceptionDetails BuildExceptionDetails(Exception ex)
    {
        var current = ex;
        var depth = 0;
        var messages = new List<object>();

        while (current != null && depth < 10)
        {
            messages.Add(new
            {
                type = current.GetType().FullName,
                message = current.Message,
                stack = current.StackTrace
            });

            current = current.InnerException;
            depth++;
        }

        var root = ex.GetBaseException();

        return new ExceptionDetails
        {
            Error = ex.Message,
            Type = ex.GetType().FullName,
            Stack = ex.StackTrace,
            RootType = root.GetType().FullName,
            RootError = root.Message,
            RootStack = root.StackTrace,
            InnerExceptions = messages
        };
    }
}
