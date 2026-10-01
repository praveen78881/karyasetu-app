/* KaryaSetu service worker.
 * - Precache the app shell + all static assets (works fully offline).
 * - Navigations: network-first, falling back to the cached shell.
 * - /api/ and /socket.io: network-only (offline data lives in IndexedDB).
 * - Other static requests: cache-first with background revalidation.
 */
const VERSION = 'ks-v17';
const SHELL = [
  '/shell', '/login-shell',
  '/manifest.webmanifest',
  '/static/css/app.css',
  '/static/css/fonts.css',
  '/static/js/db.js',
  '/static/js/app.js',
  '/static/js/vendor/socket.io.min.js',
  '/static/icons/favicon.png',
  '/static/icons/icon-192.png',
  '/static/icons/icon-512.png',
  '/static/icons/icon-maskable-192.png',
  '/static/icons/icon-maskable-512.png',
  '/static/icons/apple-touch-icon.png',
  '/static/fonts/poppins-400.woff2',
  '/static/fonts/poppins-500.woff2',
  '/static/fonts/poppins-600.woff2',
  '/static/fonts/poppins-700.woff2',
  '/static/fonts/mukta-400.woff2',
  '/static/fonts/mukta-500.woff2',
  '/static/fonts/mukta-600.woff2',
  '/static/fonts/mukta-700.woff2',
  '/static/fonts/rozha-one-400.woff2',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(VERSION).then((cache) => cache.addAll(SHELL)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== VERSION).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);

  // Live data & realtime traffic never touch the cache.
  if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/socket.io')) {
    return;
  }

  // App navigations: try the network, fall back to the cached shell.
  if (req.mode === 'navigate') {
    event.respondWith(
      fetch(req).catch(() =>
        caches.match(url.pathname === '/login' ? '/login-shell' : '/shell')
          .then((res) => res || caches.match('/shell'))
      )
    );
    return;
  }

  // Static assets: cache-first, refresh in the background.
  event.respondWith(
    caches.match(req).then((cached) => {
      const refresh = fetch(req).then((res) => {
        if (res && res.ok && url.origin === location.origin) {
          const copy = res.clone();
          caches.open(VERSION).then((cache) => cache.put(req, copy));
        }
        return res;
      }).catch(() => cached);
      return cached || refresh;
    })
  );
});

/* ---- Web Push: show OS notifications even when the app is closed ---- */

self.addEventListener('push', (event) => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch (e) { data = {}; }
  const title = data.title || 'KaryaSetu';
  const body = data.body || '';
  const url = data.url || '/';
  event.waitUntil((async () => {
    // If an app window is already focused, let the in-app UI handle it.
    const wins = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    if (wins.some((c) => c.focused)) return;
    await self.registration.showNotification(title, {
      body,
      icon: '/static/icons/icon-192.png',
      badge: '/static/icons/icon-192.png',
      data: { url },
      tag: 'ks:' + url,
      renotify: true,
    });
  })());
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const url = (event.notification.data && event.notification.data.url) || '/';
  event.waitUntil((async () => {
    const wins = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const c of wins) {
      if ('focus' in c) {
        await c.focus();
        if ('navigate' in c) { try { await c.navigate(url); } catch (e) {} }
        return;
      }
    }
    if (self.clients.openWindow) await self.clients.openWindow(url);
  })());
});
