using System.Text.Json.Serialization;
using DataVo.Core.BTree;
using DataVo.Core.Indexing.HNSW;
using DataVo.Core.Transactions;

namespace DataVo.Core.Serialization;

/// <summary>
/// System.Text.Json source-generation context for the engine's persisted DTOs. Source generation emits
/// reflection-free (Native-AOT-safe) (de)serialization metadata, replacing the previous Newtonsoft.Json
/// usage. Each persisted type is registered with <see cref="JsonSerializableAttribute"/>; serialize/
/// deserialize through <c>DataVoJsonContext.Default.&lt;Type&gt;</c>.
/// </summary>
[JsonSourceGenerationOptions(WriteIndented = false)]
[JsonSerializable(typeof(HNSWIndexPersistence.HnswSnapshot))]
[JsonSerializable(typeof(HNSWIndexPersistence.FallbackSnapshot))]
[JsonSerializable(typeof(JsonBTreeIndex))]
[JsonSerializable(typeof(WalEntry))]
[JsonSerializable(typeof(WalFileStore.WalRecordEnvelope))]
internal partial class DataVoJsonContext : JsonSerializerContext;
