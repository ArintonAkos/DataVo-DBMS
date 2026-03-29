# DataVo — Embedded SQL Database Engine for .NET

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![NuGet: DataVo.Core](https://img.shields.io/badge/NuGet-DataVo.Core-blue.svg)](https://www.nuget.org/packages/DataVo.Core)
[![Status: Active](https://img.shields.io/badge/Status-Active%20Development-green.svg)](#status)

DataVo is a lightweight, embeddable SQL database engine written in C#. Run full SQL workflows—queries, inserts, transactions, and more—directly in your .NET application without external database dependencies.

**Designed for:**
- Desktop and mobile applications needing deterministic, fast local data
- SaaS backends embedding SQL capabilities per-tenant or per-user
- Testing and CI/CD pipelines (no container overhead)
- Browser-based SQL playgrounds and educational tools via WebAssembly

---

## Key Features

✅ **Full SQL Support**  
Standard `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `CREATE INDEX`, and transaction commands with full ACID guarantees.

✅ **In-Memory and Disk Storage**  
Choose between fast in-memory mode for testing or persistent disk-backed storage for production workloads.

✅ **Zero External Dependencies**  
Single C# DLL; no server processes, no containers, no external database installations required.

✅ **Transaction Support**  
Explicit `BEGIN`, `COMMIT`, `ROLLBACK` with full MVCC isolation and recovery.

✅ **User and Role Management**  
Built-in SQL commands for creating users, roles, and managing query-level permissions.

✅ **Indexes and Performance**  
B-Tree indexes for fast lookups; vector indexing infrastructure for similarity search and semantic applications.

✅ **Browser/WASM Support**  
Run SQL queries directly in the browser. Try the interactive playground at [**docs/features/getting-started**](https://datavo.dev) (coming soon).

---

## Quick Start

### Install via NuGet (Local Packages)

```bash
dotnet add package DataVo.Core --source /path/to/artifacts/packages
```

Or build packages locally:
```bash
dotnet pack DataVo.sln -c Release
```

### Your First SQL Query

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

// Create an in-memory database
using var context = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

// Run SQL
context.Execute("CREATE DATABASE Workspace");
context.Execute("USE Workspace");
context.Execute("CREATE TABLE Products (Id INT PRIMARY KEY, Name VARCHAR(100), Price FLOAT)");

context.Execute("INSERT INTO Products VALUES (1, 'Widget', 19.99)");
context.Execute("INSERT INTO Products VALUES (2, 'Gadget', 29.99)");

// Query results
var result = context.Execute(@"
    SELECT Name, Price 
    FROM Products 
    WHERE Price < 25.00 
    ORDER BY Name
");

// Access rows
foreach (var row in result.Data)
{
    Console.WriteLine($"{row[0]}: ${row[1]}");
}
```

### With Persistence (Disk Storage)

```csharp
using var context = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DatabasePath = "./my-app-data"
});
```

---

## Common Use Cases

### 1. **Local-First SaaS**
Each tenant or user has their own isolated SQL database instance in memory or on disk—no shared backend database needed for small deployments.

```csharp
// Per-tenant isolation
var tenantDb = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DatabasePath = $"./tenants/{tenantId}"
});
```

### 2. **Testing and CI/CD**
Spin up a fresh SQL database in milliseconds for each test run. No Docker containers, no cleanup headaches.

```csharp
[SetUp]
public void SetupTestDatabase()
{
    _db = new DataVoContext(new DataVoConfig 
    { 
        StorageMode = StorageMode.InMemory 
    });
}
```

### 3. **Educational Tools & SQL Playgrounds**
Teach SQL in the browser with instant feedback. The DataVo browser build lets anyone write and execute SQL queries without a backend.

### 4. **Edge and Offline-First Apps**
Embed DataVo in mobile or edge applications that need SQL without external network calls.

---

## Supported SQL

- **DQL:** `SELECT` with `WHERE`, `JOIN`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `UNION`, subqueries  
- **DML:** `INSERT`, `UPDATE`, `DELETE`, `VACUUM`  
- **DDL:** `CREATE TABLE`, `CREATE INDEX`, `ALTER TABLE`, `DROP TABLE`  
- **DCL:** `CREATE USER`, `CREATE ROLE`, `GRANT`, `REVOKE`, `LOGIN`, `LOGOUT`  
- **TCL:** `BEGIN`, `COMMIT`, `ROLLBACK`  
- **Transactions:** Full ACID compliance with MVCC isolation  

See [SQL Features Guide](https://datavo.dev/features/select-and-querying) for more.

---

## Browser & Interactive SQL Playground

Try DataVo online without installing anything:

```bash
cd docs
npm install
npm run docs:dev
```

Open `http://localhost:5173` and use the interactive SQL editor to experiment with queries, tables, and transactions in your browser.

---

## Performance Notes

- **Lightweight:** Single DLL, ~500 KB, minimal memory footprint  
- **Fast:** In-memory mode achieves microsecond-level query latency for small datasets  
- **Disk Storage:** B-Tree indexes provide O(log n) lookups even on large tables  
- **Concurrency:** MVCC allows multiple readers without blocking  

Benchmark your workload: see `scripts/test-hnsw-perf.sh` and performance reports under `artifacts/perf/`.

---

## When to Use DataVo

| Use Case                        | Verdict |
| ------------------------------- | ------- |
| Small application data          | ✅ Ideal |
| Single-user/tenant databases    | ✅ Perfect |
| Testing & CI/CD                 | ✅ Perfect |
| Educational SQL playgrounds     | ✅ Perfect |
| Mobile/offline-first apps       | ✅ Good |
| Data warehousing (100M+ rows)   | ⚠️ Not recommended |
| High-concurrency multi-tenant   | ⚠️ Consider alternatives |
| Real-time analytics on petabytes| ⚠️ Not recommended |

---

## How It Works

Unlike embedded SQLite, DataVo is a full SQL engine written entirely in C# with:

1. **Lexer** → tokenizes SQL text  
2. **Parser** → builds abstract syntax tree (AST)  
3. **Binder** → resolves symbols and validates semantics  
4. **Optimizer** → chooses execution strategy (join order, indexes, etc.)  
5. **Executor** → runs SQL using in-memory or disk storage engines  

Everything runs in-process with no IPC, no network overhead, and full debugger support.

---

## Status & Roadmap

**Now:**
- ✅ Core SQL support (queries, mutations, transactions)
- ✅ In-memory and disk storage  
- ✅ Local packaging and embedding  
- ✅ Browser/WASM playground  
- ✅ User and role management  

**Coming Soon:**
- 📦 Public NuGet distribution  
- 🔗 ADO.NET provider (standardized .NET database interface)  
- 🎯 Vector similarity search (semantic applications)  
- 📊 Query optimizer enhancements  
- 🔐 Advanced cryptography and audit logging  

---

## Contributing

We welcome contributions! Start with:

1. **Report a bug** or suggest a feature via [GitHub Issues](https://github.com/ArintonAkos/DataVo-DBMS/issues)  
2. **Fork, branch, and submit a PR** with a test case  
3. **Update docs** if your change affects user-facing behavior  

See [CONTRIBUTING](#) (link to come) for detailed guidelines.

---

## Documentation

- **[Getting Started](https://datavo.dev/features/getting-started)** — First steps with code examples  
- **[SQL Feature Guide](https://datavo.dev/features/select-and-querying)** — What SQL is supported  
- **[Setup and Packaging](https://datavo.dev/features/setup-and-packaging)** — How to install and use locally  
- **[Security & Auth](https://datavo.dev/features/security-and-authentication)** — User/role/grant examples  
- **[Architecture Reference](https://datavo.dev/DataVo.Core/)** — For contributors and deep dives  

---

## License

MIT License. Free for commercial and personal use.  
See [LICENSE](LICENSE) for details.

---

## Community

- **Have questions?** Open an issue or discussion on GitHub  
- **Want to chat?** Contributions and community feedback are always welcome  
- **Found a bug?** Let us know with a minimal reproduction case  

---

**DataVo — Embedded SQL. No containers. No servers. Pure .NET.**
