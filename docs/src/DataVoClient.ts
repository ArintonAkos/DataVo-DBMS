/**
 * DataVo WebAssembly Client
 * Provides a strongly typed TypeScript interface over the DataVo C# WebAssembly engine.
 */

/**
 * Result contract returned by SQL execution.
 *
 * @example
 * const [result] = await client.execute("SELECT 1 AS One");
 * if (!result.IsError) {
 *   console.log(result.Fields, result.Data);
 * }
 */
export interface QueryResult {
  Messages: string[];
  Data: Record<string, any>[];
  Fields: string[];
  ExecutionTime: string;
  IsError: boolean;
  ErrorLine?: number;
  ErrorColumn?: number;
}

/** Supported methods dispatched to the runtime worker. */
type WorkerMethod = "initialize" | "execute" | "reset" | "runtimeCapabilities";

/** Request envelope sent to the runtime worker. */
type WorkerRequest = {
  id: number;
  method: WorkerMethod;
  payload?: any;
};

/** Response envelope received from the runtime worker. */
type WorkerResponse = {
  id: number;
  ok: boolean;
  result?: any;
  error?: string;
};

/**
 * Browser-side singleton client for initializing and executing SQL against DataVo WASM.
 */
export class DataVoClient {
  private static instance: DataVoClient;
  private isInitialized = false;
  private dotnetRuntime: any;
  private initializePromise: Promise<void> | null = null;
  private runtimeWorker: Worker | null = null;
  private workerMode = false;
  private workerRequestId = 0;
  private pendingWorkerRequests = new Map<
    number,
    {
      resolve: (result: any) => void;
      reject: (error: Error) => void;
    }
  >();

  private constructor() {}

  /**
   * Determines whether runtime debug tracing is enabled via query string.
   *
   * @returns True when `datavoDebug=1|true` is present.
   */
  private isDebugEnabled(): boolean {
    const debugValue = new URLSearchParams(window.location.search).get(
      "datavoDebug",
    );
    return debugValue === "1" || debugValue === "true";
  }

  /**
   * Determines whether runtime worker mode should be used.
   *
   * @returns True when worker mode is enabled or unspecified.
   */
  private shouldUseRuntimeWorker(): boolean {
    const runtimeWorkerValue = new URLSearchParams(window.location.search).get(
      "datavoRuntimeWorker",
    );

    if (!runtimeWorkerValue) {
      return true;
    }

    const normalized = runtimeWorkerValue.toLowerCase();
    return !(
      normalized === "0" ||
      normalized === "false" ||
      normalized === "off"
    );
  }

  /**
   * Gets the process-wide client singleton.
   *
   * @returns Shared client instance.
   *
   * @example
   * const client = DataVoClient.getInstance();
   * await client.initialize();
   */
  public static getInstance(): DataVoClient {
    if (!DataVoClient.instance) {
      DataVoClient.instance = new DataVoClient();
    }
    return DataVoClient.instance;
  }

  /**
   * Bootstraps the .NET WASM runtime and initializes the DataVo Engine.
   *
   * The approach here is simple: we load the dotnet.js entry point from the
   * _framework directory and let it automatically discover its own boot config
   * (dotnet.boot.js) which contains assembly references, integrity hashes, etc.
   * We only need to tell it the correct base URL for resolving assets.
   *
   * @returns Promise that resolves when runtime is ready.
   *
   * @example
   * const client = DataVoClient.getInstance();
   * await client.initialize();
   */
  public async initialize(): Promise<void> {
    if (this.isInitialized) return;
    if (this.initializePromise) {
      return this.initializePromise;
    }

    this.initializePromise = (async () => {
      const workerInitialized = this.shouldUseRuntimeWorker()
        ? await this.tryInitializeWorkerRuntime()
        : false;
      if (workerInitialized) {
        this.isInitialized = true;
        return;
      }

      const debugEnabled = this.isDebugEnabled();

      // 1. Load the DataVoStorage interop module into globalThis so [JSImport] can find it
      const storageUrl =
        window.location.origin + "/datavo-wasm/datavo.interop.js";
      const storageModule = await import(/* @vite-ignore */ storageUrl);
      (globalThis as any).DataVoStorage = storageModule;

      // 2. Load the dotnet.js entry point from the _framework directory
      const frameworkBase = window.location.origin + "/datavo-wasm/_framework";
      const dotnetUrl = frameworkBase + "/dotnet.js";
      const { dotnet } = await import(/* @vite-ignore */ dotnetUrl);

      // 3. Create the runtime. dotnet.js will automatically load dotnet.boot.js
      //    from a sibling path which contains all assembly + resource references.
      const { getAssemblyExports, getConfig } = await dotnet
        .withDiagnosticTracing(debugEnabled)
        .withApplicationArgumentsFromQuery()
        .create();

      const config = getConfig();
      this.dotnetRuntime = await getAssemblyExports(config.mainAssemblyName);

      // 4. Call the C# Initialize method
      const interop = this.dotnetRuntime.DataVo.Browser.DataVoInterop;
      if (typeof interop.Initialize === "function") {
        interop.Initialize();
      } else if (typeof interop.InitializeAsync === "function") {
        await interop.InitializeAsync();
      } else {
        throw new Error("DataVoInterop initialization export was not found.");
      }

      this.isInitialized = true;
      console.log(
        `DataVo WASM Engine Initialized Successfully.${debugEnabled ? " Debug tracing enabled." : ""}`,
      );
    })();

    try {
      await this.initializePromise;
    } catch (error) {
      this.initializePromise = null;
      this.isInitialized = false;
      this.dotnetRuntime = null;
      console.error("Failed to initialize DataVo WASM Engine:", error);
      throw error;
    }
  }

  /**
   * Attempts to bootstrap DataVo runtime in a dedicated Web Worker.
   * Falls back to main-thread runtime when worker startup fails.
   *
   * @returns True when worker runtime initialization succeeds.
   */
  private async tryInitializeWorkerRuntime(): Promise<boolean> {
    if (typeof Worker === "undefined") {
      return false;
    }

    let worker: Worker;
    try {
      worker = new Worker(
        new URL("./DataVoRuntimeWorker.ts", import.meta.url),
        {
          type: "module",
        },
      );
    } catch {
      return false;
    }

    this.runtimeWorker = worker;

    worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
      const message = event.data;
      const pending = this.pendingWorkerRequests.get(message.id);
      if (!pending) {
        return;
      }

      this.pendingWorkerRequests.delete(message.id);
      if (message.ok) {
        pending.resolve(message.result);
      } else {
        pending.reject(
          new Error(message.error || "Runtime worker request failed."),
        );
      }
    };

    worker.onerror = (event: ErrorEvent) => {
      const error = new Error(
        event.message || "Runtime worker encountered an error.",
      );
      for (const pending of this.pendingWorkerRequests.values()) {
        pending.reject(error);
      }
      this.pendingWorkerRequests.clear();
    };

    try {
      await this.callRuntimeWorker("initialize");
      this.workerMode = true;
      return true;
    } catch (error) {
      console.warn("Falling back to main-thread DataVo runtime.", error);
      worker.terminate();
      this.runtimeWorker = null;
      this.workerMode = false;
      return false;
    }
  }

  /**
   * Sends a request to the runtime worker and awaits the corresponding response.
   *
   * @param method Worker operation to execute.
   * @param payload Optional request payload.
   * @returns Worker result payload.
   *
   * @example
   * const capabilities = await this.callRuntimeWorker("runtimeCapabilities");
   */
  private callRuntimeWorker(method: WorkerMethod, payload?: any): Promise<any> {
    if (!this.runtimeWorker) {
      return Promise.reject(new Error("Runtime worker is not available."));
    }

    const id = ++this.workerRequestId;
    const request: WorkerRequest = { id, method, payload };

    return new Promise((resolve, reject) => {
      this.pendingWorkerRequests.set(id, { resolve, reject });
      this.runtimeWorker!.postMessage(request);
    });
  }

  /**
   * Executes a SQL command or query against the DataVo database.
   *
   * @param sql SQL command text.
   * @returns Query result batch returned by the runtime.
   *
   * @example
   * const results = await client.execute("CREATE TABLE T (Id INT)");
   * if (results[0]?.IsError) {
   *   console.error(results[0].Messages);
   * }
   */
  public async execute(sql: string): Promise<QueryResult[]> {
    if (!this.isInitialized) {
      throw new Error(
        "DataVo WASM Engine is not initialized. Call initialize() first.",
      );
    }

    try {
      const parsed = this.workerMode
        ? await this.callRuntimeWorker("execute", { sql })
        : JSON.parse(
            this.dotnetRuntime.DataVo.Browser.DataVoInterop.ExecuteSql(sql),
          );

      if (
        parsed &&
        typeof parsed === "object" &&
        !Array.isArray(parsed) &&
        parsed.error
      ) {
        const messages = [parsed.error];

        if (parsed.rootType || parsed.rootError) {
          messages.push(
            `Root cause${parsed.rootType ? ` (${parsed.rootType})` : ""}: ${parsed.rootError ?? parsed.error}`,
          );
        }

        return [
          {
            IsError: true,
            Messages: messages,
            Data: [],
            Fields: [],
            ExecutionTime: "",
          },
        ];
      }

      return this.attachErrorLocations(parsed as QueryResult[]);
    } catch (error: any) {
      console.error("Error executing SQL:", error);

      try {
        if (this.workerMode) {
          return [
            this.attachErrorLocationToResult({
              IsError: true,
              Messages: [error.message || "Unknown execution error"],
              Data: [],
              Fields: [],
              ExecutionTime: "",
            }),
          ];
        }

        const interop = this.dotnetRuntime?.DataVo?.Browser?.DataVoInterop;
        if (interop && typeof interop.DiagnoseLexer === "function") {
          const diagnosticJson = interop.DiagnoseLexer(sql);
          const diagnostic = JSON.parse(diagnosticJson);
          console.error("Lexer diagnostics:", diagnostic);

          const messages = [error.message || "Unknown execution error"];

          if (diagnostic?.error) {
            messages.push(`Diagnostic error: ${diagnostic.error}`);
          }

          if (diagnostic?.rootType || diagnostic?.rootError) {
            messages.push(
              `Root cause${diagnostic.rootType ? ` (${diagnostic.rootType})` : ""}: ${diagnostic.rootError ?? diagnostic.error}`,
            );
          }

          if (diagnostic?.stage) {
            messages.push(`Diagnostic stage: ${diagnostic.stage}`);
          }

          return [
            this.attachErrorLocationToResult({
              IsError: true,
              Messages: messages,
              Data: [],
              Fields: [],
              ExecutionTime: "",
            }),
          ];
        }
      } catch (diagnosticError) {
        console.error("Failed to collect lexer diagnostics:", diagnosticError);
      }

      return [
        this.attachErrorLocationToResult({
          IsError: true,
          Messages: [error.message || "Unknown execution error"],
          Data: [],
          Fields: [],
          ExecutionTime: "",
        }),
      ];
    }
  }

  /**
   * Adds parsed line/column metadata to each failed result when available.
   *
   * @param results Raw query results.
   * @returns Results annotated with line/column when parseable.
   */
  private attachErrorLocations(results: QueryResult[]): QueryResult[] {
    return results.map((result) => this.attachErrorLocationToResult(result));
  }

  /**
   * Adds parsed line/column metadata to a single failed result when available.
   *
   * @param result Single query result.
   * @returns Annotated result.
   */
  private attachErrorLocationToResult(result: QueryResult): QueryResult {
    if (!result.IsError || !result.Messages || result.Messages.length === 0) {
      return result;
    }

    const parsedLocation = this.parseErrorLocation(result.Messages);
    if (!parsedLocation) {
      return result;
    }

    return {
      ...result,
      ErrorLine: parsedLocation.line,
      ErrorColumn: parsedLocation.column,
    };
  }

  /**
   * Extracts <line,column> coordinates from result messages when formatted by engine diagnostics.
   *
   * @param messages Diagnostic messages.
   * @returns Parsed location or null.
   */
  private parseErrorLocation(
    messages: string[],
  ): { line: number; column: number } | null {
    const combined = messages.join("\n");
    const match = combined.match(/line\s+(\d+)\s*,\s*column\s+(\d+)/i);
    if (!match) {
      return null;
    }

    const line = Number.parseInt(match[1], 10);
    const column = Number.parseInt(match[2], 10);

    if (Number.isNaN(line) || Number.isNaN(column)) {
      return null;
    }

    return { line, column };
  }

  /**
   * Returns runtime capability metadata for the active backend.
   *
   * @returns Capability object including `storageBackend`.
   *
   * @example
   * const caps = await client.runtimeCapabilities();
   * console.log(caps.storageBackend);
   */
  public async runtimeCapabilities(): Promise<Record<string, any>> {
    if (this.workerMode) {
      const workerCapabilities =
        (await this.callRuntimeWorker("runtimeCapabilities")) || {};
      if (typeof workerCapabilities.storageBackend !== "string") {
        return {
          ...workerCapabilities,
          storageBackend: "unknown",
        };
      }

      return workerCapabilities;
    }

    const interop = this.dotnetRuntime?.DataVo?.Browser?.DataVoInterop;
    if (interop && typeof interop.RuntimeCapabilities === "function") {
      const raw = interop.RuntimeCapabilities();
      try {
        return JSON.parse(raw);
      } catch {
        return { storageBackend: "unknown", raw };
      }
    }

    const backendKindFn = (globalThis as any).DataVoStorage
      ?.getStorageBackendKind;
    if (typeof backendKindFn === "function") {
      try {
        return {
          storageBackend: backendKindFn(),
        };
      } catch {
        // Continue to unknown fallback.
      }
    }

    return { storageBackend: "unknown" };
  }

  /**
   * Resets runtime state and storage, then clears all initialization state.
   *
   * @returns Promise that resolves when reset is complete.
   *
   * @example
   * await client.reset();
   * await client.initialize();
   */
  public async reset(): Promise<void> {
    if (this.workerMode) {
      await this.callRuntimeWorker("reset");
      this.runtimeWorker?.terminate();
      this.runtimeWorker = null;
      this.workerMode = false;
    } else {
      const interop = this.dotnetRuntime?.DataVo?.Browser?.DataVoInterop;
      if (interop && typeof interop.ResetStorage === "function") {
        interop.ResetStorage();
      }
    }

    for (const pending of this.pendingWorkerRequests.values()) {
      pending.reject(
        new Error("DataVo client reset interrupted pending worker request."),
      );
    }
    this.pendingWorkerRequests.clear();

    this.isInitialized = false;
    this.initializePromise = null;
    this.dotnetRuntime = null;
  }
}
