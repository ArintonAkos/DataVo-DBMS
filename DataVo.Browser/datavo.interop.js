/**
 * datavo.interop.js
 *
 * Storage backend order:
 * 1) Worker + OPFS bridge (sync surface for JSImport via SharedArrayBuffer + Atomics)
 * 2) localStorage fallback for broad compatibility
 */

const STORAGE_PREFIX = "datavo:";
const SEQ_ROWID_KEY = "datavo:seq:rowid";
const CATALOG_KEY = "datavo:catalog";
const SELECTED_DATABASE_KEY = "datavo:selectedDatabase";
const WORKER_RESPONSE_BYTES = 8 * 1024 * 1024; // 8MB per sync call buffer
const STORAGE_FORCE_BACKEND_GLOBAL_KEY = "__datavoForceStorageBackend";
const STORAGE_BACKEND_SINGLETON_KEY = "__datavoStorageBackendSingleton";
const STORAGE_BACKEND_DIAGNOSTICS_KEY = "__datavoStorageBackendDiagnostics";

function bytesToBase64(bytes) {
    let binary = "";
    for (let i = 0; i < bytes.byteLength; i++) {
        binary += String.fromCharCode(bytes[i]);
    }

    return btoa(binary);
}

function base64ToBytes(base64) {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
    }

    return bytes;
}

function getStorageKey(databaseName, tableName, rowId) {
    return `datavo:data:${databaseName}:${tableName}:${rowId}`;
}

function canBlockCurrentThread() {
    if (typeof SharedArrayBuffer === "undefined" || typeof Atomics === "undefined" || typeof Atomics.wait !== "function") {
        return false;
    }

    // Atomics.wait throws on browser main thread.
    try {
        const control = new Int32Array(new SharedArrayBuffer(4));
        Atomics.wait(control, 0, 0, 0);
        return true;
    } catch {
        return false;
    }
}

function isWorkerThread() {
    return typeof WorkerGlobalScope !== "undefined" && self instanceof WorkerGlobalScope;
}

function readForcedBackend() {
    const globalOverride = globalThis[STORAGE_FORCE_BACKEND_GLOBAL_KEY];
    if (globalOverride === "worker-opfs" || globalOverride === "localStorage" || globalOverride === "worker-memory-fallback") {
        return globalOverride;
    }

    try {
        if (typeof location !== "undefined" && typeof location.search === "string") {
            const params = new URLSearchParams(location.search);
            const queryOverride = params.get("datavoStorageBackend");
            if (queryOverride === "worker-opfs" || queryOverride === "localStorage" || queryOverride === "worker-memory-fallback") {
                return queryOverride;
            }
        }
    } catch {
        // Ignore URL parsing failures.
    }

    return null;
}

function setBackendDiagnostics(selectedKind, requestedKind, reason) {
    globalThis[STORAGE_BACKEND_DIAGNOSTICS_KEY] = {
        selectedKind,
        requestedKind,
        reason,
        canBlockCurrentThread: canBlockCurrentThread(),
        isWorkerThread: isWorkerThread(),
        hasWorker: typeof Worker !== "undefined",
        hasSharedArrayBuffer: typeof SharedArrayBuffer !== "undefined",
        timestamp: Date.now()
    };
}

function createMemoryFallbackBackend() {
    const store = new Map();

    function getNextRowId() {
        let currentId = parseInt(store.get(SEQ_ROWID_KEY) || "0", 10);
        currentId++;
        store.set(SEQ_ROWID_KEY, currentId.toString());
        return currentId.toString();
    }

    return {
        kind: "worker-memory-fallback",
        insertRow(databaseName, tableName, rowBytes) {
            const rowId = getNextRowId();
            store.set(getStorageKey(databaseName, tableName, rowId), bytesToBase64(rowBytes));
            return rowId;
        },
        readRow(databaseName, tableName, rowId) {
            const payload = store.get(getStorageKey(databaseName, tableName, rowId));
            return payload ? base64ToBytes(payload) : null;
        },
        readAllRows(databaseName, tableName) {
            const prefix = `datavo:data:${databaseName}:${tableName}:`;
            const rows = [];

            for (const [key, value] of store.entries()) {
                if (key.startsWith(prefix)) {
                    rows.push([key.substring(prefix.length), value]);
                }
            }

            return JSON.stringify(rows);
        },
        deleteRow(databaseName, tableName, rowId) {
            store.delete(getStorageKey(databaseName, tableName, rowId));
        },
        dropTable(databaseName, tableName) {
            const prefix = `datavo:data:${databaseName}:${tableName}:`;
            for (const key of [...store.keys()]) {
                if (key.startsWith(prefix)) {
                    store.delete(key);
                }
            }
        },
        dropDatabase(databaseName) {
            const prefix = `datavo:data:${databaseName}:`;
            for (const key of [...store.keys()]) {
                if (key.startsWith(prefix)) {
                    store.delete(key);
                }
            }
        },
        readCatalog() {
            return store.get(CATALOG_KEY) || null;
        },
        writeCatalog(xml) {
            store.set(CATALOG_KEY, xml);
        },
        readSelectedDatabase() {
            return store.get(SELECTED_DATABASE_KEY) || null;
        },
        writeSelectedDatabase(databaseName) {
            if (!databaseName) {
                store.delete(SELECTED_DATABASE_KEY);
                return;
            }

            store.set(SELECTED_DATABASE_KEY, databaseName);
        },
        clearAllStorage() {
            for (const key of [...store.keys()]) {
                if (key.startsWith(STORAGE_PREFIX)) {
                    store.delete(key);
                }
            }
        },
        getCapabilities() {
            return {
                storageBackend: "worker-memory-fallback",
                mode: "fallback",
                hasWorker: typeof Worker !== "undefined",
                hasSharedArrayBuffer: typeof SharedArrayBuffer !== "undefined",
                canBlockCurrentThread: canBlockCurrentThread(),
                isWorkerThread: isWorkerThread(),
                opfsAvailable: false
            };
        }
    };
}

function createLocalStorageBackend() {
    function getNextRowId() {
        let currentId = parseInt(localStorage.getItem(SEQ_ROWID_KEY) || "0", 10);
        currentId++;
        localStorage.setItem(SEQ_ROWID_KEY, currentId.toString());
        return currentId.toString();
    }

    return {
        kind: "localStorage",
        insertRow(databaseName, tableName, rowBytes) {
            const rowId = getNextRowId();
            localStorage.setItem(getStorageKey(databaseName, tableName, rowId), bytesToBase64(rowBytes));
            return rowId;
        },
        readRow(databaseName, tableName, rowId) {
            const payload = localStorage.getItem(getStorageKey(databaseName, tableName, rowId));
            return payload ? base64ToBytes(payload) : null;
        },
        readAllRows(databaseName, tableName) {
            const prefix = `datavo:data:${databaseName}:${tableName}:`;
            const rows = [];

            for (let i = 0; i < localStorage.length; i++) {
                const key = localStorage.key(i);
                if (key && key.startsWith(prefix)) {
                    rows.push([key.substring(prefix.length), localStorage.getItem(key)]);
                }
            }

            return JSON.stringify(rows);
        },
        deleteRow(databaseName, tableName, rowId) {
            localStorage.removeItem(getStorageKey(databaseName, tableName, rowId));
        },
        dropTable(databaseName, tableName) {
            const prefix = `datavo:data:${databaseName}:${tableName}:`;
            const keys = [];

            for (let i = 0; i < localStorage.length; i++) {
                const key = localStorage.key(i);
                if (key && key.startsWith(prefix)) {
                    keys.push(key);
                }
            }

            keys.forEach((key) => localStorage.removeItem(key));
        },
        dropDatabase(databaseName) {
            const prefix = `datavo:data:${databaseName}:`;
            const keys = [];

            for (let i = 0; i < localStorage.length; i++) {
                const key = localStorage.key(i);
                if (key && key.startsWith(prefix)) {
                    keys.push(key);
                }
            }

            keys.forEach((key) => localStorage.removeItem(key));
        },
        readCatalog() {
            return localStorage.getItem(CATALOG_KEY);
        },
        writeCatalog(xml) {
            localStorage.setItem(CATALOG_KEY, xml);
        },
        readSelectedDatabase() {
            return localStorage.getItem(SELECTED_DATABASE_KEY);
        },
        writeSelectedDatabase(databaseName) {
            if (!databaseName) {
                localStorage.removeItem(SELECTED_DATABASE_KEY);
                return;
            }

            localStorage.setItem(SELECTED_DATABASE_KEY, databaseName);
        },
        clearAllStorage() {
            const keys = [];
            for (let i = 0; i < localStorage.length; i++) {
                const key = localStorage.key(i);
                if (key && key.startsWith(STORAGE_PREFIX)) {
                    keys.push(key);
                }
            }

            keys.forEach((key) => localStorage.removeItem(key));
        },
        getCapabilities() {
            return {
                storageBackend: "localStorage",
                mode: "fallback",
                hasWorker: typeof Worker !== "undefined",
                hasSharedArrayBuffer: typeof SharedArrayBuffer !== "undefined",
                canBlockCurrentThread: canBlockCurrentThread(),
                isWorkerThread: typeof WorkerGlobalScope !== "undefined" && self instanceof WorkerGlobalScope,
                opfsAvailable: false
            };
        }
    };
}

function createWorkerBackendOrNull() {
    if (typeof Worker === "undefined" || !canBlockCurrentThread()) {
        return null;
    }

    let worker;
    try {
        worker = new Worker(new URL("./datavo.storage.worker.js", import.meta.url), { type: "module" });
    } catch {
        return null;
    }

    const decoder = new TextDecoder();
    const encoder = new TextEncoder();

    function invokeSync(command, payload) {
        const controlBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * 3);
        const control = new Int32Array(controlBuffer);
        const responseBuffer = new SharedArrayBuffer(WORKER_RESPONSE_BYTES);

        worker.postMessage({ command, payload, controlBuffer, responseBuffer });

        // Wait until worker marks command as complete.
        Atomics.wait(control, 0, 0);

        const status = Atomics.load(control, 1);
        const length = Atomics.load(control, 2);

        if (length < 0 || length > WORKER_RESPONSE_BYTES) {
            throw new Error("Worker response payload length out of range.");
        }

        const bytes = new Uint8Array(responseBuffer, 0, length);
        const text = decoder.decode(bytes);

        if (status !== 0) {
            throw new Error(text || "Storage worker command failed.");
        }

        if (!text) {
            return null;
        }

        return JSON.parse(text);
    }

    // Probe backend once so we can gracefully fallback if worker initialization fails.
    try {
        invokeSync("getBackendKind", null);
    } catch {
        worker.terminate();
        return null;
    }

    return {
        kind: "worker-opfs",
        insertRow(databaseName, tableName, rowBytes) {
            return invokeSync("insertRow", {
                databaseName,
                tableName,
                rowBase64: bytesToBase64(rowBytes)
            });
        },
        readRow(databaseName, tableName, rowId) {
            const rowBase64 = invokeSync("readRow", { databaseName, tableName, rowId });
            return rowBase64 ? base64ToBytes(rowBase64) : null;
        },
        readAllRows(databaseName, tableName) {
            return invokeSync("readAllRows", { databaseName, tableName }) || "[]";
        },
        deleteRow(databaseName, tableName, rowId) {
            invokeSync("deleteRow", { databaseName, tableName, rowId });
        },
        dropTable(databaseName, tableName) {
            invokeSync("dropTable", { databaseName, tableName });
        },
        dropDatabase(databaseName) {
            invokeSync("dropDatabase", { databaseName });
        },
        readCatalog() {
            return invokeSync("readCatalog", null);
        },
        writeCatalog(xml) {
            invokeSync("writeCatalog", { xml });
        },
        readSelectedDatabase() {
            return invokeSync("readSelectedDatabase", null);
        },
        writeSelectedDatabase(databaseName) {
            invokeSync("writeSelectedDatabase", { databaseName });
        },
        clearAllStorage() {
            invokeSync("clearAllStorage", null);
        },
        backendKind() {
            return invokeSync("getBackendKind", null);
        },
        getCapabilities() {
            return invokeSync("getCapabilities", null);
        }
    };
}

function selectStorageBackend() {
    const existing = globalThis[STORAGE_BACKEND_SINGLETON_KEY];
    if (existing) {
        setBackendDiagnostics(existing.kind || "unknown", readForcedBackend(), "reused-singleton");
        return existing;
    }

    const forced = readForcedBackend();
    let backend = null;

    if (forced === "worker-opfs") {
        backend = createWorkerBackendOrNull();
        if (!backend) {
            backend = isWorkerThread() ? createMemoryFallbackBackend() : createLocalStorageBackend();
            setBackendDiagnostics(backend.kind, forced, "forced-worker-opfs-fallback");
        } else {
            setBackendDiagnostics(backend.kind, forced, "forced-worker-opfs");
        }
    } else if (forced === "localStorage") {
        backend = createLocalStorageBackend();
        setBackendDiagnostics(backend.kind, forced, "forced-localStorage");
    } else if (forced === "worker-memory-fallback") {
        backend = createMemoryFallbackBackend();
        setBackendDiagnostics(backend.kind, forced, "forced-memory-fallback");
    } else {
        // Default selection keeps worker contexts away from localStorage-only paths,
        // avoiding accidental context isolation between worker and window storage scopes.
        backend = createWorkerBackendOrNull();
        if (!backend) {
            backend = isWorkerThread() ? createMemoryFallbackBackend() : createLocalStorageBackend();
            setBackendDiagnostics(backend.kind, null, "auto-fallback");
        } else {
            setBackendDiagnostics(backend.kind, null, "auto-worker");
        }
    }

    globalThis[STORAGE_BACKEND_SINGLETON_KEY] = backend;
    return backend;
}

const StorageBackend = selectStorageBackend();

export function insertRow(databaseName, tableName, rowBytes) {
    return StorageBackend.insertRow(databaseName, tableName, rowBytes);
}

export function readRow(databaseName, tableName, rowId) {
    return StorageBackend.readRow(databaseName, tableName, rowId);
}

export function readAllRows(databaseName, tableName) {
    return StorageBackend.readAllRows(databaseName, tableName);
}

export function deleteRow(databaseName, tableName, rowId) {
    StorageBackend.deleteRow(databaseName, tableName, rowId);
}

export function dropTable(databaseName, tableName) {
    StorageBackend.dropTable(databaseName, tableName);
}

export function dropDatabase(databaseName) {
    StorageBackend.dropDatabase(databaseName);
}

export function readCatalog() {
    return StorageBackend.readCatalog();
}

export function writeCatalog(xml) {
    StorageBackend.writeCatalog(xml);
}

export function readSelectedDatabase() {
    return StorageBackend.readSelectedDatabase();
}

export function writeSelectedDatabase(databaseName) {
    StorageBackend.writeSelectedDatabase(databaseName);
}

export function clearAllStorage() {
    StorageBackend.clearAllStorage();
}

export function getStorageBackendKind() {
    return typeof StorageBackend.backendKind === "function"
        ? StorageBackend.backendKind()
        : StorageBackend.kind;
}

export function getStorageCapabilities() {
    const capabilities = typeof StorageBackend.getCapabilities === "function"
        ? StorageBackend.getCapabilities()
        : {
            storageBackend: getStorageBackendKind(),
            mode: "unknown"
        };

    capabilities.selectionDiagnostics = globalThis[STORAGE_BACKEND_DIAGNOSTICS_KEY] || null;

    return JSON.stringify(capabilities);
}

globalThis.DataVoStorage = {
    insertRow,
    readRow,
    readAllRows,
    deleteRow,
    dropTable,
    dropDatabase,
    readCatalog,
    writeCatalog,
    readSelectedDatabase,
    writeSelectedDatabase,
    clearAllStorage,
    getStorageBackendKind,
    getStorageCapabilities
};
