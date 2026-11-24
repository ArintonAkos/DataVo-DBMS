namespace HybridVectorRagAot;

/// <summary>One chunk of a document: the unit that is embedded, filtered and retrieved.</summary>
public sealed record Chunk(int Id, int DocumentId, string Category, DateTime CreatedAt, string Text);

/// <summary>A small built-in knowledge base of engineering notes, so the demo needs no files or downloads.</summary>
public static class Corpus
{
    private static readonly (string Category, string Title, DateTime Created, string[] Chunks)[] Documents =
    [
        ("security", "Key management runbook", Utc(2025, 1, 6),
        [
            "Rotate encryption keys every ninety days and keep the previous key active for decryption only.",
            "Store master keys in a hardware security module and never write key material to application logs.",
            "When a key is compromised, revoke it immediately, re-encrypt affected data and rotate dependent secrets.",
        ]),
        ("security", "Authentication hardening", Utc(2025, 9, 15),
        [
            "Require multi-factor authentication for administrator accounts and for any access to production databases.",
            "Hash passwords with a memory-hard function such as Argon2 and a unique salt for every user.",
            "Lock an account after repeated failed sign-in attempts and alert the security team about brute force patterns.",
        ]),
        ("security", "TLS certificate operations", Utc(2026, 3, 2),
        [
            "Renew TLS certificates automatically thirty days before expiry and monitor the expiry date of every endpoint.",
            "Pin the certificate authority for internal services and reject self-signed certificates in production.",
        ]),
        ("security", "Key rotation policy 2026", Utc(2026, 9, 14),
        [
            "New policy: rotate encryption keys every thirty days through the managed key service, which rotates them automatically.",
            "Old ninety-day key rotation schedules are retired; every service must adopt automatic thirty-day rotation by year end.",
        ]),
        ("database", "LSM storage tuning", Utc(2025, 2, 10),
        [
            "Size the memtable so a flush writes SSTables of a few megabytes; tiny flushes multiply compaction work.",
            "Leveled compaction keeps read amplification low; tiered compaction favours write-heavy ingestion.",
            "Bloom filters on every SSTable let point lookups skip files that cannot contain the key.",
        ]),
        ("database", "Backups and recovery", Utc(2025, 11, 20),
        [
            "Take a consistent snapshot, copy it off the machine, and test restoring it at least once a month.",
            "Write-ahead logs let the database replay committed transactions after a crash and lose nothing durable.",
            "Encrypt backups at rest and rotate the backup encryption keys on the same schedule as production keys.",
        ]),
        ("database", "Indexing guide", Utc(2026, 6, 1),
        [
            "Create a secondary index on columns used in equality filters; range filters benefit from a B-tree index.",
            "Columnar indexes accelerate aggregates such as SUM and COUNT over millions of rows with SIMD scans.",
            "HNSW indexes find approximate nearest neighbours for vector embeddings in logarithmic time.",
        ]),
        ("networking", "Load balancer setup", Utc(2025, 4, 14),
        [
            "Health checks should probe a lightweight endpoint and remove unhealthy instances within seconds.",
            "Use connection draining during deployments so in-flight requests finish before an instance stops.",
            "Terminate TLS at the load balancer and forward traffic to services over an encrypted internal network.",
        ]),
        ("networking", "Latency troubleshooting", Utc(2026, 8, 3),
        [
            "Measure tail latency at the 99th percentile; averages hide the slow requests users actually notice.",
            "Packet loss on a congested link causes TCP retransmissions and sudden latency spikes.",
            "Place caches close to users with a content delivery network to cut round trips for static assets.",
        ]),
        ("ml", "Embedding pipelines", Utc(2025, 7, 22),
        [
            "Split documents into overlapping chunks of a few hundred tokens before computing sentence embeddings.",
            "Normalize embedding vectors to unit length so cosine distance and dot product rank results identically.",
            "Re-embed the corpus when you change the embedding model; vectors from different models are not comparable.",
        ]),
        ("ml", "Retrieval augmented generation", Utc(2026, 10, 5),
        [
            "Retrieve the most similar chunks, filter them by metadata such as date and category, then pass them to the model.",
            "Hybrid search combines a structured SQL filter with vector similarity to keep stale documents out of answers.",
            "Evaluate retrieval with recall at k on a labelled question set before tuning the generation prompt.",
        ]),
    ];

    private static DateTime Utc(int year, int month, int day) => new(year, month, day, 9, 0, 0, DateTimeKind.Utc);

    /// <summary>Every chunk, with its document's creation time (2025 to 2026).</summary>
    public static IReadOnlyList<Chunk> Chunks()
    {
        var chunks = new List<Chunk>();
        int id = 1;
        for (int document = 0; document < Documents.Length; document++)
        {
            (string category, string title, DateTime created, string[] texts) = Documents[document];
            for (int part = 0; part < texts.Length; part++)
            {
                chunks.Add(new Chunk(id++, document + 1, category, created.AddMinutes(part * 17), $"{title}: {texts[part]}"));
            }
        }

        return chunks;
    }
}
