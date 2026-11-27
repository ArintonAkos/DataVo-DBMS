const STORAGE_PREFIX = "datavo:";
const SEQ_ROWID_KEY = "datavo:seq:rowid";
const CATALOG_KEY = "datavo:catalog";
const SELECTED_DATABASE_KEY = "datavo:selectedDatabase";
const TXN_MARKER_PREFIX = "datavo:txn:pending:";
const TXN_TEMP_PREFIX = "datavo:txn:temp:";

const MEMORY_FALLBACK_WARNING =
    "DataVo storage worker has no OPFS access and is keeping data in memory only (worker-memory-fallback). " +
    "Data will be lost on reload.";

let rootDirectoryPromise = null;
let recoveryPromise = null;
let memoryFallbackWarned = false;
const memoryFallback = new Map();

// Shared with datavo.interop.js once (attach message) and reused for every response.
let control = null;
let responseBytes = null;
const encoder = new TextEncoder();

function toFileName(key) {
    return encodeURIComponent(key);
}

function fromFileName(name) {
    return decodeURIComponent(name);
}

async function getRootDirectory() {
    if (rootDirectoryPromise) {
        return rootDirectoryPromise;
    }

    rootDirectoryPromise = (async () => {
        if (typeof navigator === "undefined" || !navigator.storage || typeof navigator.storage.getDirectory !== "function") {
            if (!memoryFallbackWarned) {
                memoryFallbackWarned = true;
                console.warn(MEMORY_FALLBACK_WARNING);
            }

            return null;
        }

        const opfsRoot = await navigator.storage.getDirectory();
        return await opfsRoot.getDirectoryHandle("datavo", { create: true });
    })();

    // A transient failure (e.g. storage access denied mid-startup) must not be cached forever.
    rootDirectoryPromise.catch(() => {
        rootDirectoryPromise = null;
    });

    return rootDirectoryPromise;
}

async function writeRawValue(key, value) {
    const root = await getRootDirectory();
    if (!root) {
        memoryFallback.set(key, value);
        return;
    }

    const fileHandle = await root.getFileHandle(toFileName(key), { create: true });
    const writer = await fileHandle.createWritable();
    try {
        await writer.write(value);
    } finally {
        await writer.close();
    }
}

async function readRawValue(key) {
    const root = await getRootDirectory();
    if (!root) {
        return memoryFallback.has(key) ? memoryFallback.get(key) : null;
    }

    try {
        const fileHandle = await root.getFileHandle(toFileName(key));
        const file = await fileHandle.getFile();
        return await file.text();
    } catch {
        return null;
    }
}

async function deleteRawValue(key) {
    const root = await getRootDirectory();
    if (!root) {
        memoryFallback.delete(key);
        return;
    }

    try {
        await root.removeEntry(toFileName(key));
    } catch {
        // Ignore missing entries.
    }
}

async function listRawKeysByPrefix(prefix) {
    const root = await getRootDirectory();
    if (!root) {
        return [...memoryFallback.keys()].filter((key) => key.startsWith(prefix));
    }

    const keys = [];
    for await (const [name] of root.entries()) {
        const key = fromFileName(name);
        if (key.startsWith(prefix)) {
            keys.push(key);
        }
    }

    return keys;
}

function getMarkerKey(key) {
    return `${TXN_MARKER_PREFIX}${key}`;
}

function getTempKey(key) {
    return `${TXN_TEMP_PREFIX}${key}`;
}

async function recoverDurableWrites() {
    const markerKeys = await listRawKeysByPrefix(TXN_MARKER_PREFIX);
    for (const markerKey of markerKeys) {
        const targetKey = markerKey.substring(TXN_MARKER_PREFIX.length);
        const tempKey = getTempKey(targetKey);
        const pendingValue = await readRawValue(tempKey);

        if (pendingValue != null) {
            await writeRawValue(targetKey, pendingValue);
        }

        await deleteRawValue(tempKey);
        await deleteRawValue(markerKey);
    }
}

async function ensureRecoveryCompleted() {
    if (!recoveryPromise) {
        recoveryPromise = recoverDurableWrites();
    }

    await recoveryPromise;
}

async function durableWriteValue(key, value) {
    await ensureRecoveryCompleted();

    const markerKey = getMarkerKey(key);
    const tempKey = getTempKey(key);

    // Three-step durable protocol:
    // 1) marker indicates pending write
    // 2) temp contains target payload
    // 3) promote temp payload to final key and clear txn metadata
    await writeRawValue(markerKey, "1");
    await writeRawValue(tempKey, value);
    await writeRawValue(key, value);
    await deleteRawValue(tempKey);
    await deleteRawValue(markerKey);
}

async function readValue(key) {
    await ensureRecoveryCompleted();
    return await readRawValue(key);
}

async function deleteValue(key) {
    await ensureRecoveryCompleted();
    await deleteRawValue(key);
}

async function listKeysByPrefix(prefix) {
    await ensureRecoveryCompleted();
    return await listRawKeysByPrefix(prefix);
}

async function clearAllDataVoKeys() {
    await ensureRecoveryCompleted();

    const root = await getRootDirectory();
    if (!root) {
        for (const key of [...memoryFallback.keys()]) {
            if (key.startsWith(STORAGE_PREFIX)) {
                memoryFallback.delete(key);
            }
        }

        return;
    }

    const removals = [];
    for await (const [name] of root.entries()) {
        const key = fromFileName(name);
        // Transaction markers and temp payloads share the "datavo:" prefix.
        if (key.startsWith(STORAGE_PREFIX)) {
            removals.push(root.removeEntry(name));
        }
    }

    await Promise.all(removals);
}

function getStorageKey(databaseName, tableName, rowId) {
    return `datavo:data:${databaseName}:${tableName}:${rowId}`;
}

async function getNextRowId() {
    const current = await readValue(SEQ_ROWID_KEY);
    const next = (parseInt(current || "0", 10) + 1).toString();
    await durableWriteValue(SEQ_ROWID_KEY, next);
    return next;
}

async function getCapabilities() {
    const opfsRoot = await getRootDirectory();
    return {
        storageBackend: opfsRoot ? "worker-opfs" : "worker-memory-fallback",
        mode: "worker",
        persistent: !!opfsRoot,
        warning: opfsRoot ? null : MEMORY_FALLBACK_WARNING,
        isWorkerThread: true,
        hasWorker: true,
        hasSharedArrayBuffer: typeof SharedArrayBuffer !== "undefined",
        hasAtomics: typeof Atomics !== "undefined",
        opfsAvailable: !!opfsRoot,
        durableCommitProtocol: true
    };
}

async function handleCommand(command, payload) {
    switch (command) {
        case "getBackendKind": {
            return (await getRootDirectory()) ? "worker-opfs" : "worker-memory-fallback";
        }
        case "getCapabilities": {
            return await getCapabilities();
        }
        case "insertRow": {
            const rowId = await getNextRowId();
            await durableWriteValue(getStorageKey(payload.databaseName, payload.tableName, rowId), payload.rowBase64);
            return rowId;
        }
        case "readRow": {
            return await readValue(getStorageKey(payload.databaseName, payload.tableName, payload.rowId));
        }
        case "readAllRows": {
            const prefix = `datavo:data:${payload.databaseName}:${payload.tableName}:`;
            const keys = await listKeysByPrefix(prefix);
            const rows = [];

            for (const key of keys) {
                const rowValue = await readValue(key);
                if (rowValue != null) {
                    rows.push([key.substring(prefix.length), rowValue]);
                }
            }

            return JSON.stringify(rows);
        }
        case "deleteRow": {
            await deleteValue(getStorageKey(payload.databaseName, payload.tableName, payload.rowId));
            return null;
        }
        case "dropTable": {
            const prefix = `datavo:data:${payload.databaseName}:${payload.tableName}:`;
            const keys = await listKeysByPrefix(prefix);
            await Promise.all(keys.map(deleteValue));
            return null;
        }
        case "dropDatabase": {
            const prefix = `datavo:data:${payload.databaseName}:`;
            const keys = await listKeysByPrefix(prefix);
            await Promise.all(keys.map(deleteValue));
            return null;
        }
        case "readCatalog": {
            return await readValue(CATALOG_KEY);
        }
        case "writeCatalog": {
            await durableWriteValue(CATALOG_KEY, payload.xml || "");
            return null;
        }
        case "readSelectedDatabase": {
            return await readValue(SELECTED_DATABASE_KEY);
        }
        case "writeSelectedDatabase": {
            if (!payload.databaseName) {
                await deleteValue(SELECTED_DATABASE_KEY);
                return null;
            }

            await durableWriteValue(SELECTED_DATABASE_KEY, payload.databaseName);
            return null;
        }
        case "clearAllStorage": {
            await clearAllDataVoKeys();
            return null;
        }
        default:
            throw new Error(`Unsupported storage worker command: ${command}`);
    }
}

function publishResponse(status, text) {
    // encodeInto writes straight into the shared buffer: no per-call allocation or zero-fill.
    let { read, written } = encoder.encodeInto(text, responseBytes);
    if (read < text.length) {
        status = 1;
        ({ written } = encoder.encodeInto("Storage worker response exceeds shared buffer capacity.", responseBytes));
    }

    Atomics.store(control, 1, status);
    Atomics.store(control, 2, written);
    Atomics.store(control, 0, 1);
    Atomics.notify(control, 0, 1);
}

self.onmessage = async (event) => {
    const { attach, command, payload, controlBuffer, responseBuffer } = event.data;
    if (attach) {
        control = new Int32Array(controlBuffer);
        responseBytes = new Uint8Array(responseBuffer);
        return;
    }

    let status = 0;
    let response = "";

    try {
        const result = await handleCommand(command, payload || {});
        response = JSON.stringify(result);
    } catch (error) {
        status = 1;
        response = error && error.message ? error.message : String(error);
    }

    publishResponse(status, response);
};
