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

const StorageBackend = createWorkerBackendOrNull() || createLocalStorageBackend();

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
