const STORAGE_PREFIX = "datavo:";
const SEQ_ROWID_KEY = "datavo:seq:rowid";
const CATALOG_KEY = "datavo:catalog";
const SELECTED_DATABASE_KEY = "datavo:selectedDatabase";

let rootDirectoryPromise = null;
const memoryFallback = new Map();

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
            return null;
        }

        const opfsRoot = await navigator.storage.getDirectory();
        return await opfsRoot.getDirectoryHandle("datavo", { create: true });
    })();

    return rootDirectoryPromise;
}

async function writeValue(key, value) {
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

async function readValue(key) {
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

async function deleteValue(key) {
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

async function listKeysByPrefix(prefix) {
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

async function clearAllDataVoKeys() {
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
    await writeValue(SEQ_ROWID_KEY, next);
    return next;
}

async function handleCommand(command, payload) {
    switch (command) {
        case "getBackendKind": {
            return (await getRootDirectory()) ? "worker-opfs" : "worker-memory-fallback";
        }
        case "insertRow": {
            const rowId = await getNextRowId();
            await writeValue(getStorageKey(payload.databaseName, payload.tableName, rowId), payload.rowBase64);
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
            await writeValue(CATALOG_KEY, payload.xml || "");
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

            await writeValue(SELECTED_DATABASE_KEY, payload.databaseName);
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

const encoder = new TextEncoder();

self.onmessage = async (event) => {
    const { command, payload, controlBuffer, responseBuffer } = event.data;
    const control = new Int32Array(controlBuffer);
    const responseBytes = new Uint8Array(responseBuffer);

    let status = 0;
    let response = "";

    try {
        const result = await handleCommand(command, payload || {});
        response = JSON.stringify(result);
    } catch (error) {
        status = 1;
        response = error && error.message ? error.message : String(error);
    }

    const encoded = encoder.encode(response);
    if (encoded.length > responseBytes.byteLength) {
        status = 1;
        const overflow = encoder.encode("Storage worker response exceeds shared buffer capacity.");
        responseBytes.fill(0);
        responseBytes.set(overflow.subarray(0, responseBytes.byteLength));
        Atomics.store(control, 1, status);
        Atomics.store(control, 2, Math.min(overflow.length, responseBytes.byteLength));
        Atomics.store(control, 0, 1);
        Atomics.notify(control, 0, 1);
        return;
    }

    responseBytes.fill(0);
    responseBytes.set(encoded);

    Atomics.store(control, 1, status);
    Atomics.store(control, 2, encoded.length);
    Atomics.store(control, 0, 1);
    Atomics.notify(control, 0, 1);
};
