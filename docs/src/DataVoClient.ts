/**
 * DataVo WebAssembly Client
 * Provides a strongly typed TypeScript interface over the DataVo C# WebAssembly engine.
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

export class DataVoClient {
  private static instance: DataVoClient;
  private isInitialized = false;
  private dotnetRuntime: any;
  private initializePromise: Promise<void> | null = null;

  private constructor() {}

  private isDebugEnabled(): boolean {
    const debugValue = new URLSearchParams(window.location.search).get(
      "datavoDebug",
    );
    return debugValue === "1" || debugValue === "true";
  }

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
   */
  public async initialize(): Promise<void> {
    if (this.isInitialized) return;
    if (this.initializePromise) {
      return this.initializePromise;
    }

    this.initializePromise = (async () => {
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
   * Executes a SQL command or query against the DataVo database.
   */
  public execute(sql: string): QueryResult[] {
    if (!this.isInitialized) {
      throw new Error(
        "DataVo WASM Engine is not initialized. Call initialize() first.",
      );
    }

    try {
      const rawJson =
        this.dotnetRuntime.DataVo.Browser.DataVoInterop.ExecuteSql(sql);
      const parsed = JSON.parse(rawJson);

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

  private attachErrorLocations(results: QueryResult[]): QueryResult[] {
    return results.map((result) => this.attachErrorLocationToResult(result));
  }

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

  public reset(): void {
    const interop = this.dotnetRuntime?.DataVo?.Browser?.DataVoInterop;
    if (interop && typeof interop.ResetStorage === "function") {
      interop.ResetStorage();
    }

    this.isInitialized = false;
    this.initializePromise = null;
    this.dotnetRuntime = null;
  }
}
