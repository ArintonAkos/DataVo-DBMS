using System.Globalization;
using System.Text;
using Microsoft.CodeAnalysis.CSharp;

namespace DataVo.Generators.Linq;

/// <summary>
/// Writes the interceptors for sort keys, <c>Select</c> projections and <c>ExecuteUpdate</c> calls. Each routes the
/// call to the runtime method that takes what the generator worked out: a compiled column, a projection over named
/// columns, or a SET list with its bound values. The members stay within C# 9.
/// </summary>
internal static class DataVoLinqShapeEmitter
{
    private const string Core = "global::DataVo.Core.";
    private const string Parameter = Core + "DataVoQueryParameter";

    /// <summary>Writes an <c>OrderBy</c>-family interceptor and the compiled key it passes.</summary>
    /// <example>
    /// <c>OrderByDescending(p =&gt; p.Level)</c> becomes <c>source.OrderByColumn(OrderKey0, keySelector)</c> with
    /// <c>OrderKey0 = new DataVoOrderKey("Level", true)</c>.
    /// </example>
    public static void EmitOrder(StringBuilder code, LinqOrderSite site, string id)
    {
        string runtime = site.Method.StartsWith("Then", StringComparison.Ordinal) ? "ThenByColumn" : "OrderByColumn";
        code.AppendLine($"        private static readonly {Core}DataVoOrderKey OrderKey{id} = new {Core}DataVoOrderKey({Literal(site.Column)}, {(site.Descending ? "true" : "false")});");
        code.AppendLine();
        AppendSignature(code, site.Call, site.Call.ReturnType, site.Method + id, $"global::System.Func<{site.Call.ModelType}, {site.KeyType}> keySelector");
        code.AppendLine($"            return source.{runtime}(OrderKey{id}, keySelector);");
        code.AppendLine("        }").AppendLine();
    }

    /// <summary>
    /// Writes a <c>Select</c> interceptor and its projection. A projection whose result type generated code can name
    /// gets a reader method; one that builds an anonymous type gets a generic interceptor, whose type arguments the
    /// compiler fills in, and a generic holder for the projection.
    /// </summary>
    public static void EmitProjection(StringBuilder code, LinqProjectionSite site, string id)
    {
        string columns = "new string[] { " + string.Join(", ", site.Columns.Select(Literal)) + " }";
        if (site.ResultType is { } result)
        {
            string projection = $"{Core}DataVoProjection<{site.Call.ModelType}, {result}>";
            code.AppendLine($"        private static readonly {projection} Projection{id} = new {projection}({columns}, Project{id});");
            code.AppendLine();
            code.AppendLine($"        private static {result} Project{id}({Core}DataVoRow row)");
            code.AppendLine("        {");
            code.AppendLine($"            return {site.Body};");
            code.AppendLine("        }").AppendLine();
            AppendSignature(code, site.Call, site.Call.ReturnType, "Select" + id, $"global::System.Func<{site.Call.ModelType}, {result}> selector");
            code.AppendLine($"            return source.SelectColumns(Projection{id}, selector);");
            code.AppendLine("        }").AppendLine();
            return;
        }

        bool table = site.Call.Kind == LinqReceiver.Table;
        string receiver = table ? $"{Core}DataVoTable<T>" : $"{Core}{ReceiverName(site.Call.Kind)}<T, TState>";
        string returns = $"{Core}DataVoProjectedQuery<T, {(table ? "global::System.Func<T, bool>" : "TState")}, TResult>";
        string parameters = table ? "<T, TResult>" : "<T, TState, TResult>";
        code.AppendLine($"        private static class Projection{id}<T, TResult>");
        code.AppendLine("        {");
        code.AppendLine($"            public static readonly {Core}DataVoProjection<T, TResult> Instance = new {Core}DataVoProjection<T, TResult>({columns}, static row => (TResult)(object)({site.Body}));");
        code.AppendLine("        }").AppendLine();
        code.AppendLine($"        [global::System.Runtime.CompilerServices.InterceptsLocation({DataVoLinqEmitter.LocationText(site.Call.Location)})]");
        code.AppendLine($"        public static {returns} Select{id}{parameters}(this {(table ? string.Empty : "in ")}{receiver} source, global::System.Func<T, TResult> selector)");
        code.AppendLine("        {");
        code.AppendLine($"            return source.SelectColumns(Projection{id}<T, TResult>.Instance, selector);");
        code.AppendLine("        }").AppendLine();
    }

    /// <summary>Writes an <c>ExecuteUpdate</c> interceptor that binds the SET list's values and runs the update.</summary>
    /// <example>
    /// <c>ExecuteUpdate(bonus, static (set, b) =&gt; set.Set(p =&gt; p.Gold, p =&gt; p.Gold + b))</c> becomes
    /// <c>source.ExecuteUpdateSet("Gold = Gold + @u0", new[] { new DataVoQueryParameter("u0", (long)(state)) })</c>.
    /// </example>
    public static void EmitUpdate(StringBuilder code, LinqUpdateSite site, string id)
    {
        string setters = $"{Core}DataVoSetters<{site.Call.ModelType}>";
        string parameters = site.StateType is { } state
            ? $"{state} state, global::System.Func<{setters}, {state}, {setters}> setters"
            : $"global::System.Func<{setters}, {setters}> setters";
        AppendSignature(code, site.Call, "int", "ExecuteUpdate" + id, parameters);
        string[] values = [.. site.Values];
        string array = values.Length == 0
            ? $"global::System.Array.Empty<{Parameter}>()"
            : $"new {Parameter}[] {{ {string.Join(", ", values)} }}";
        code.AppendLine($"            return source.ExecuteUpdateSet({Literal(site.Assignments)}, {array});");
        code.AppendLine("        }").AppendLine();
    }

    /// <summary>Writes the attribute and the opening of a non-generic interceptor over the call's own receiver type.</summary>
    private static void AppendSignature(StringBuilder code, LinqCall call, string returns, string name, string parameters)
    {
        string receiver = call.Kind == LinqReceiver.Table ? call.ReceiverType : "in " + call.ReceiverType;
        code.AppendLine($"        [global::System.Runtime.CompilerServices.InterceptsLocation({DataVoLinqEmitter.LocationText(call.Location)})]");
        code.AppendLine($"        public static {returns} {name}(this {receiver} source, {parameters})");
        code.AppendLine("        {");
    }

    private static string ReceiverName(LinqReceiver kind) => kind switch
    {
        LinqReceiver.Query => "DataVoQuery",
        LinqReceiver.OrderedQuery => "DataVoOrderedQuery",
        LinqReceiver.PagedQuery => "DataVoPagedQuery",
        _ => "DataVoTable",
    };

    private static string Literal(string text) => SymbolDisplay.FormatLiteral(text, quote: true);

    /// <summary>Numbers the sites of one kind so each interceptor and its helpers get distinct names.</summary>
    public static string Id(int index) => index.ToString(CultureInfo.InvariantCulture);
}
