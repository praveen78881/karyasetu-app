// ks-shim.js — send the Flask-era /api/* calls to Supabase Edge Functions.
(function () {
  var FN = 'https://rmenvpvcbbfmuxxqurqi.supabase.co/functions/v1';
  var ANON = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJtZW52cHZjYmJmbXV4eHF1cnFpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3OTA3OTgyNTMsImV4cCI6MjEwNjM3NDI1M30.s5iUC_QaqwOh-GfdRAIg7PTUaovQQZWoMOS7zzl0YJA';
  function tok() { try { return localStorage.getItem('ks_token') || ''; } catch (e) { return ''; } }
  var _fetch = window.fetch.bind(window);
  window.fetch = function (input, init) {
    init = init || {};
    var url = (typeof input === 'string') ? input : (input && input.url) || '';
    var m = url.match(/^(?:https?:\/\/[^/]+)?(\/api\/.*)$/);
    if (!m) return _fetch(input, init);
    var rest = m[1].replace(/^\/api\//, '');
    if (rest === 'logout') {
      try { localStorage.removeItem('ks_token'); } catch (e) {}
      return Promise.resolve(new Response('{"ok":true}', { status: 200, headers: { 'Content-Type': 'application/json' } }));
    }
    var target;
    if (rest === 'login') target = FN + '/login';
    else if (rest === 'setup') target = FN + '/signup/setup';
    else if (rest === 'join') target = FN + '/signup/join';
    else target = FN + '/' + rest;
    // clean header set (drop the old same-site CSRF header; use the token)
    var headers = new Headers();
    var src = new Headers(init.headers || {});
    if (src.has('content-type')) headers.set('Content-Type', src.get('content-type'));
    headers.set('apikey', ANON);
    var t = tok(); if (t) headers.set('x-ks-token', t);
    var p = _fetch(target, { method: init.method || 'GET', body: init.body, headers: headers, credentials: 'omit' });
    if (rest === 'login' || rest === 'setup') {
      return p.then(function (res) {
        return res.clone().json().then(function (d) {
          if (res.ok && d && d.token) { try { localStorage.setItem('ks_token', d.token); } catch (e) {} }
          return res;
        }).catch(function () { return res; });
      });
    }
    return p;
  };
})();
