# Server Demo Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `Server` project explicitly demo/local-only by default and reduce unsafe request handling footguns.

**Architecture:** Do not turn the server into a full production API in this slice. Add conservative guardrails: bounded request body reads, configurable CORS origin, no nested `Task.Run` for synchronous router work, and README status clarity.

**Tech Stack:** C#, `HttpListener`, Newtonsoft.Json, docs.

---

## File Structure

- Modify: `Server/Server/HttpServer.cs`
  - Add request body size configuration and safer CORS origin.
  - Remove unnecessary nested `Task.Run` around `Router.HandleRequest`.
- Modify: `Server/Server/Http/Router.cs`
  - Add bounded body read.
- Modify: `Server/README.md`
  - Mark server as local demo/dev server, not production API.
- Create/Modify: `DataVo.Tests` server tests only if current test project can reference `Server`; otherwise document manual verification.

## Task 1: Add Router Body Limit

**Files:**
- Modify: `Server/Server/Http/Router.cs`

- [ ] **Step 1: Add constant and bounded read**

Add:

```csharp
private const int MaxRequestBodyBytes = 1_048_576;
```

Replace `GetRequestContent` with:

```csharp
private static string GetRequestContent(HttpListenerRequest request)
{
    using var memory = new MemoryStream();
    request.InputStream.CopyTo(memory);
    if (memory.Length > MaxRequestBodyBytes)
    {
        throw new InvalidOperationException($"Request body exceeds {MaxRequestBodyBytes} bytes.");
    }

    return request.ContentEncoding.GetString(memory.ToArray());
}
```

- [ ] **Step 2: Build server**

Run: `dotnet build Server/Server.csproj --no-restore`

Expected: build succeeds.

## Task 2: Remove Nested Router Task.Run

**Files:**
- Modify: `Server/Server/HttpServer.cs`

- [ ] **Step 1: Replace nested `Task.Run`**

Change:

```csharp
var response = await Task.Run(() => Router.HandleRequest(context));
```

to:

```csharp
var response = Router.HandleRequest(context);
```

Keep the outer per-request task for now to preserve concurrency behavior.

- [ ] **Step 2: Build server**

Run: `dotnet build Server/Server.csproj --no-restore`

Expected: build succeeds.

## Task 3: Make CORS Origin Configurable

**Files:**
- Modify: `Server/Server/HttpServer.cs`

- [ ] **Step 1: Add origin resolver**

Add:

```csharp
private static string ResolveCorsOrigin()
{
    return Environment.GetEnvironmentVariable("DATAVO_SERVER_CORS_ORIGIN") ?? "http://localhost:5173";
}
```

Change:

```csharp
context.Response.Headers.Add("Access-Control-Allow-Origin", "*");
```

to:

```csharp
context.Response.Headers.Add("Access-Control-Allow-Origin", ResolveCorsOrigin());
```

- [ ] **Step 2: Build server**

Run: `dotnet build Server/Server.csproj --no-restore`

Expected: build succeeds.

## Task 4: Clarify Server README

**Files:**
- Modify: `Server/README.md`

- [ ] **Step 1: Add support scope note near top**

Add:

```markdown
> Support scope: this server is a local development/demo host for DataVo APIs. It is not a hardened multi-tenant production API. Keep it bound to localhost unless you add authentication, deployment-grade CORS policy, request limits, logging, and operational controls.
```

- [ ] **Step 2: Verify server docs mention local scope**

Run: `rg -n "local development/demo|not a hardened" Server/README.md`

Expected: both phrases found.

## Task 5: Full Verification

- [ ] **Step 1: Build server**

Run: `dotnet build Server/Server.csproj --no-restore`

Expected: build succeeds.

- [ ] **Step 2: Run full tests**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore`

Expected: 0 failed.

