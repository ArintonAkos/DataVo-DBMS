using System.Text.Json.Serialization;
using DataVo.Core.Execution.Volcano;

namespace DataVo.Core.Serialization;

/// <summary>
/// System.Text.Json source-generation context for the engine's persisted DTOs. Source generation emits
/// reflection-free (Native-AOT-safe) (de)serialization metadata, replacing the previous Newtonsoft.Json
/// usage. Each persisted type is registered with <see cref="JsonSerializableAttribute"/>; serialize/
/// deserialize through <c>DataVoJsonContext.Default.&lt;Type&gt;</c>.
/// </summary>
[JsonSourceGenerationOptions(WriteIndented = false)]
[JsonSerializable(typeof(TypedExecutionRow))]
[JsonSerializable(typeof(Dictionary<string, double>), TypeInfoPropertyName = "StringDoubleMap")]
internal partial class DataVoJsonContext : JsonSerializerContext;
