/* KaryaSetu — IndexedDB layer.
 * Stores the synced snapshot (tasks, messages, notifications, users, kv)
 * plus the offline outbox of pending actions. Every call is wrapped so a
 * blocked/unavailable IndexedDB (private mode, cleared site data) degrades
 * to in-memory state instead of crashing the app. */
(function () {
  'use strict';

  const DB_NAME = 'karyasetu';
  const DB_VERSION = 1;
  const STORES = ['kv', 'tasks', 'messages', 'notifications', 'users', 'outbox'];

  let dbPromise = null;

  function open() {
    if (dbPromise) return dbPromise;
    dbPromise = new Promise((resolve) => {
      let req;
      try {
        req = indexedDB.open(DB_NAME, DB_VERSION);
      } catch {
        resolve(null);
        return;
      }
      req.onupgradeneeded = () => {
        const db = req.result;
        for (const name of STORES) {
          if (!db.objectStoreNames.contains(name)) {
            db.createObjectStore(name, { keyPath: '_k' });
          }
        }
      };
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => resolve(null);
      req.onblocked = () => resolve(null);
    });
    return dbPromise;
  }

  function tx(db, store, mode) {
    return db.transaction(store, mode).objectStore(store);
  }

  async function getAll(store) {
    const db = await open();
    if (!db) return [];
    return new Promise((resolve) => {
      try {
        const req = tx(db, store, 'readonly').getAll();
        req.onsuccess = () => resolve(req.result || []);
        req.onerror = () => resolve([]);
      } catch { resolve([]); }
    });
  }

  async function put(store, key, value) {
    const db = await open();
    if (!db) return;
    return new Promise((resolve) => {
      try {
        const t = db.transaction(store, 'readwrite');
        t.objectStore(store).put(Object.assign({}, value, { _k: key }));
        t.oncomplete = () => resolve();
        t.onerror = () => resolve();
      } catch { resolve(); }
    });
  }

  async function remove(store, key) {
    const db = await open();
    if (!db) return;
    return new Promise((resolve) => {
      try {
        const t = db.transaction(store, 'readwrite');
        t.objectStore(store).delete(key);
        t.oncomplete = () => resolve();
        t.onerror = () => resolve();
      } catch { resolve(); }
    });
  }

  /** Replace a store's whole contents with `items`, keyed by keyFn. */
  async function replaceAll(store, items, keyFn) {
    const db = await open();
    if (!db) return;
    return new Promise((resolve) => {
      try {
        const t = db.transaction(store, 'readwrite');
        const os = t.objectStore(store);
        os.clear();
        for (const item of items) {
          os.put(Object.assign({}, item, { _k: String(keyFn(item)) }));
        }
        t.oncomplete = () => resolve();
        t.onerror = () => resolve();
      } catch { resolve(); }
    });
  }

  async function kvGet(key) {
    const db = await open();
    if (!db) return null;
    return new Promise((resolve) => {
      try {
        const req = tx(db, 'kv', 'readonly').get(key);
        req.onsuccess = () => resolve(req.result ? req.result.v : null);
        req.onerror = () => resolve(null);
      } catch { resolve(null); }
    });
  }

  async function kvSet(key, v) {
    return put('kv', key, { v });
  }

  async function clearAll() {
    const db = await open();
    if (!db) return;
    for (const s of STORES) {
      await new Promise((resolve) => {
        try {
          const t = db.transaction(s, 'readwrite');
          t.objectStore(s).clear();
          t.oncomplete = () => resolve();
          t.onerror = () => resolve();
        } catch { resolve(); }
      });
    }
  }

  window.KSDB = { getAll, put, remove, replaceAll, kvGet, kvSet, clearAll };
})();
