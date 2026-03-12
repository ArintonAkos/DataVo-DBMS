/**
 * datavo.interop.js
 * 
 * Provides synchronous Javascript interop functions for the WebAssembly DataVo Engine.
 * Since WASM [JSImport] calls are synchronous on the main thread, we use localStorage 
 * (which is also synchronous) to physically persist byte arrays between browser sessions.
 * 
 * Note: localStorage has limits (typically 5-10MB). For larger datasets, developers
 * should run DataVo in a Web Worker and use SharedArrayBuffers/Atomics to block 
 * synchronously and use IndexedDB under the hood. For this package baseline, 
 * standard Window localStorage ensures maximum compatibility out of the box.
 */

// Helper: Convert Uint8Array to Base64
function bytesToBase64(bytes) {
    let binary = '';
    const len = bytes.byteLength;
    for (let i = 0; i < len; i++) {
        binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
}

// Helper: Convert Base64 to Uint8Array
function base64ToBytes(base64) {
    const binary = atob(base64);
    const len = binary.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
        bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
}

// Global row counter state synced with localStorage
function getNextRowId() {
    let currentId = parseInt(localStorage.getItem('datavo:seq:rowid') || '0', 10);
    currentId++;
    localStorage.setItem('datavo:seq:rowid', currentId.toString());
    return currentId.toString();
}

function getStorageKey(databaseName, tableName, rowId) {
    return `datavo:data:${databaseName}:${tableName}:${rowId}`;
}

function getCatalogKey() {
    return "datavo:catalog";
}

function getSelectedDatabaseKey() {
    return "datavo:selectedDatabase";
}

export function insertRow(databaseName, tableName, rowBytes) {
    const rowId = getNextRowId();
    const b64 = bytesToBase64(rowBytes);
    localStorage.setItem(getStorageKey(databaseName, tableName, rowId), b64);
    return rowId;
}

export function readRow(databaseName, tableName, rowId) {
    const b64 = localStorage.getItem(getStorageKey(databaseName, tableName, rowId));
    if (!b64) return null;
    return base64ToBytes(b64);
}

// Returns a stringified JSON array of [rowId, bytesBase64] tuples
export function readAllRows(databaseName, tableName) {
    const prefix = `datavo:data:${databaseName}:${tableName}:`;
    const results = [];
    
    for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key.startsWith(prefix)) {
            const rowId = key.substring(prefix.length);
            const b64 = localStorage.getItem(key);
            results.push([rowId, b64]);
        }
    }
    
    return JSON.stringify(results);
}

export function deleteRow(databaseName, tableName, rowId) {
    localStorage.removeItem(getStorageKey(databaseName, tableName, rowId));
}

export function dropTable(databaseName, tableName) {
    const prefix = `datavo:data:${databaseName}:${tableName}:`;
    const keysToRemove = [];
    
    for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key.startsWith(prefix)) {
            keysToRemove.push(key);
        }
    }
    
    keysToRemove.forEach(k => localStorage.removeItem(k));
}

export function dropDatabase(databaseName) {
    const prefix = `datavo:data:${databaseName}:`;
    const keysToRemove = [];
    
    for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key.startsWith(prefix)) {
            keysToRemove.push(key);
        }
    }
    
    keysToRemove.forEach(k => localStorage.removeItem(k));
}

export function readCatalog() {
    return localStorage.getItem(getCatalogKey());
}

export function writeCatalog(xml) {
    localStorage.setItem(getCatalogKey(), xml);
}

export function readSelectedDatabase() {
    return localStorage.getItem(getSelectedDatabaseKey());
}

export function writeSelectedDatabase(databaseName) {
    if (!databaseName) {
        localStorage.removeItem(getSelectedDatabaseKey());
        return;
    }

    localStorage.setItem(getSelectedDatabaseKey(), databaseName);
}

export function clearAllStorage() {
    const keysToRemove = [];

    for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key && key.startsWith("datavo:")) {
            keysToRemove.push(key);
        }
    }

    keysToRemove.forEach(key => localStorage.removeItem(key));
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
    clearAllStorage
};
