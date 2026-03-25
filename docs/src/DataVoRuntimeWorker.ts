type RuntimeRequest = {
  id: number;
  method: "initialize" | "execute" | "reset" | "runtimeCapabilities";
  payload?: any;
};

type RuntimeResponse = {
  id: number;
  ok: boolean;
  result?: any;
  error?: string;
};

let dotnetRuntime: any = null;
let initialized = false;
let initPromise: Promise<void> | null = null;

function isDebugEnabled(): boolean {
  try {
    const locationUrl = typeof self !== "undefined" ? (self as any).location?.href : "";
    const debugValue = new URL(locationUrl).searchParams.get("datavoDebug");
    return debugValue === "1" || debugValue === "true";
  } catch {
    return false;
  }
}

async function initializeRuntime(): Promise<void> {
  if (initialized) {
    return;
  }

  if (initPromise) {
    return initPromise;
  }

  initPromise = (async () => {
    const debugEnabled = isDebugEnabled();
    const storageModuleUrl = `${self.location.origin}/datavo-wasm/datavo.interop.js`;
    const storageModule = await import(/* @vite-ignore */ storageModuleUrl);
    (globalThis as any).DataVoStorage = storageModule;

    const frameworkBase = `${self.location.origin}/datavo-wasm/_framework`;
    const dotnetUrl = `${frameworkBase}/dotnet.js`;
    const { dotnet } = await import(/* @vite-ignore */ dotnetUrl);

    const { getAssemblyExports, getConfig } = await dotnet
      .withDiagnosticTracing(debugEnabled)
      .withApplicationArgumentsFromQuery()
      .create();

    const config = getConfig();
    dotnetRuntime = await getAssemblyExports(config.mainAssemblyName);

    const interop = dotnetRuntime.DataVo.Browser.DataVoInterop;
    if (typeof interop.Initialize === "function") {
      interop.Initialize();
    } else if (typeof interop.InitializeAsync === "function") {
      await interop.InitializeAsync();
    } else {
      throw new Error("DataVoInterop initialization export was not found.");
    }

    initialized = true;
  })();

  try {
    await initPromise;
  } catch (error) {
    initPromise = null;
    initialized = false;
    dotnetRuntime = null;
    throw error;
  }
}

function getInterop(): any {
  const interop = dotnetRuntime?.DataVo?.Browser?.DataVoInterop;
  if (!interop) {
    throw new Error("DataVoInterop export is unavailable.");
  }

  return interop;
}

async function handleRequest(request: RuntimeRequest): Promise<any> {
  switch (request.method) {
    case "initialize": {
      await initializeRuntime();
      return { workerMode: true };
    }
    case "execute": {
      await initializeRuntime();
      const sql = typeof request.payload?.sql === "string" ? request.payload.sql : "";
      const raw = getInterop().ExecuteSql(sql);
      return JSON.parse(raw);
    }
    case "reset": {
      if (!initialized) {
        return null;
      }

      const interop = getInterop();
      if (typeof interop.ResetStorage === "function") {
        interop.ResetStorage();
      }

      initialized = false;
      initPromise = null;
      dotnetRuntime = null;
      return null;
    }
    case "runtimeCapabilities": {
      await initializeRuntime();
      const interop = getInterop();
      if (typeof interop.RuntimeCapabilities === "function") {
        const raw = interop.RuntimeCapabilities();
        try {
          return JSON.parse(raw);
        } catch {
          return { storageBackend: "unknown", raw };
        }
      }

      return { storageBackend: "unknown" };
    }
    default:
      throw new Error(`Unsupported runtime worker method: ${request.method}`);
  }
}

self.onmessage = async (event: MessageEvent<RuntimeRequest>) => {
  const request = event.data;
  const response: RuntimeResponse = {
    id: request.id,
    ok: true
  };

  try {
    response.result = await handleRequest(request);
  } catch (error) {
    response.ok = false;
    response.error = error instanceof Error ? error.message : String(error);
  }

  self.postMessage(response);
};
