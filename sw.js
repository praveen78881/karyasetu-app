const VERSION = 'ks-gh-v1';
const SHELL = ['index.html','login.html','setup.html','ks-shim.js',
 'static/css/app.css','static/css/fonts.css','static/js/db.js','static/js/app.js',
 'static/js/vendor/socket.io.min.js',
 'static/icons/favicon.png','static/icons/icon-192.png','static/icons/icon-512.png',
 'static/icons/icon-maskable-192.png','static/icons/icon-maskable-512.png','static/icons/apple-touch-icon.png',
 'static/fonts/poppins-400.woff2','static/fonts/poppins-500.woff2','static/fonts/poppins-600.woff2','static/fonts/poppins-700.woff2',
 'static/fonts/mukta-400.woff2','static/fonts/mukta-500.woff2','static/fonts/mukta-600.woff2','static/fonts/mukta-700.woff2',
 'static/fonts/rozha-one-400.woff2'];
self.addEventListener('install', e => e.waitUntil(
  caches.open(VERSION).then(c => c.addAll(SHELL)).then(() => self.skipWaiting()).catch(() => self.skipWaiting())));
self.addEventListener('activate', e => e.waitUntil(
  caches.keys().then(ks => Promise.all(ks.filter(k => k !== VERSION).map(k => caches.delete(k)))).then(() => self.clients.claim())));
self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  if (url.origin !== location.origin) return;            // Supabase API calls pass straight through
  if (req.mode === 'navigate') { e.respondWith(fetch(req).catch(() => caches.match('index.html'))); return; }
  e.respondWith(caches.match(req).then(c => c || fetch(req).then(res => {
    if (res && res.ok) { const copy = res.clone(); caches.open(VERSION).then(cache => cache.put(req, copy)); }
    return res;
  }).catch(() => c)));
});
self.addEventListener('push', event => {
  let data = {}; try { data = event.data ? event.data.json() : {}; } catch (e) { data = {}; }
  const title = data.title || 'KaryaSetu', body = data.body || '', url = data.url || './';
  event.waitUntil((async () => {
    const wins = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    if (wins.some(c => c.focused)) return;
    await self.registration.showNotification(title, { body, icon: 'static/icons/icon-192.png', badge: 'static/icons/icon-192.png', data: { url }, tag: 'ks:' + url, renotify: true });
  })());
});
self.addEventListener('notificationclick', event => {
  event.notification.close();
  const url = (event.notification.data && event.notification.data.url) || './';
  event.waitUntil((async () => {
    const wins = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const c of wins) { if ('focus' in c) { await c.focus(); if ('navigate' in c) { try { await c.navigate(url); } catch (e) {} } return; } }
    if (self.clients.openWindow) await self.clients.openWindow(url);
  })());
});
