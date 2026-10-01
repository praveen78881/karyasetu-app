/* KaryaSetu — application logic.
 *
 * Architecture: the server snapshot (tasks/messages/notifications/users) is
 * the base truth, mirrored into IndexedDB. Every user action goes through a
 * single path — queueAction() — which writes it to the outbox, applies it
 * optimistically to local state, and attempts a sync. Online or offline is
 * the same code path; offline just means the sync attempt waits.
 */
(function () {
  'use strict';

  // ------------------------------------------------------------ utils

  const $ = (sel, root) => (root || document).querySelector(sel);

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function uuid() {
    if (crypto.randomUUID) return crypto.randomUUID();
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
      const r = Math.random() * 16 | 0;
      return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
    });
  }

  function lsGet(k) { try { return localStorage.getItem(k); } catch { return null; } }
  function lsSet(k, v) { try { localStorage.setItem(k, v); } catch {} }

  // Dates follow each viewer's own browser locale.
  const fmtD = iso => iso ? new Date(iso).toLocaleDateString(undefined,
    { day: 'numeric', month: 'short', year: 'numeric' }) : '—';
  const fmtDT = iso => iso ? new Date(iso).toLocaleString(undefined,
    { day: 'numeric', month: 'short', hour: 'numeric', minute: '2-digit' }) : '—';
  const fmtT = iso => iso ? new Date(iso).toLocaleTimeString(undefined,
    { hour: 'numeric', minute: '2-digit' }) : '';

  function rel(iso) {
    if (!iso) return '';
    const diff = Date.now() - new Date(iso).getTime();
    const abs = Math.abs(diff);
    const min = 60000, hr = 60 * min, day = 24 * hr;
    let txt;
    if (abs < min) txt = 'just now';
    else if (abs < hr) txt = Math.round(abs / min) + 'm';
    else if (abs < day) txt = Math.round(abs / hr) + 'h';
    else if (abs < 7 * day) txt = Math.round(abs / day) + 'd';
    else return fmtD(iso);
    if (txt === 'just now') return txt;
    return diff >= 0 ? txt + ' ago' : 'in ' + txt;
  }

  function icon(name, cls) {
    return '<svg class="icon ' + (cls || '') + '"><use href="#' + name + '"/></svg>';
  }

  // Validated categorical palette (identity job) for avatars.
  const AV_COLORS = ['#2a78d6', '#eb6834', '#1baf7a', '#eda100',
                     '#e87ba4', '#008300', '#4a3aa7', '#e34948'];

  function avColor(id) { return AV_COLORS[(id || 0) % AV_COLORS.length]; }

  function avInk(hex) {
    const n = parseInt(hex.slice(1), 16);
    const r = n >> 16 & 255, g = n >> 8 & 255, b = n & 255;
    return (0.299 * r + 0.587 * g + 0.114 * b) > 160 ? '#1a1a19' : '#ffffff';
  }

  function initials(name) {
    return (name || '?').split(/\s+/).slice(0, 2).map(w => w[0] || '').join('').toUpperCase();
  }

  function avatarHtml(user, cls) {
    if (!user) return '';
    const c = avColor(user.id);
    return '<span class="av ' + (cls || '') + '" style="background:' + c +
      ';color:' + avInk(c) + '">' + esc(initials(user.name)) + '</span>';
  }

  const STATUS_META = {
    pending:     { label: 'Pending',     icon: 'i-clock',        cls: 'st-pending' },
    in_progress: { label: 'In Progress', icon: 'i-play',         cls: 'st-progress' },
    completed:   { label: 'Completed',   icon: 'i-check-circle', cls: 'st-completed' },
    overdue:     { label: 'Overdue',     icon: 'i-alert-circle', cls: 'st-overdue' },
  };
  const PRIORITY_META = {
    low:    { label: 'Low',    cls: 'pr-low' },
    medium: { label: 'Medium', cls: 'pr-medium' },
    high:   { label: 'High',   cls: 'pr-high' },
    urgent: { label: 'Urgent', cls: 'pr-urgent' },
  };

  // ------------------------------------------------------------ state

  const S = {
    me: null, users: [], tasks: [], messages: [], notifications: [],
    outbox: [], syncing: false, offline: !navigator.onLine,
    lastSyncAt: null, syncQueued: false, socket: null,
    knownNotifIds: null, // Set once boot data is loaded; used to toast only new ones
    justSynced: false,
  };
  let deferredInstall = null;
  let pushSynced = false;

  // ---- Web Push subscription (OS notifications on laptop + mobile) ----
  function urlB64ToUint8Array(base64) {
    const pad = '='.repeat((4 - base64.length % 4) % 4);
    const b64 = (base64 + pad).replace(/-/g, '+').replace(/_/g, '/');
    const raw = atob(b64);
    return Uint8Array.from([...raw].map(c => c.charCodeAt(0)));
  }

  async function subscribePush() {
    try {
      if (!('serviceWorker' in navigator) || !('PushManager' in window)) return;
      const keyRes = await fetch('/api/push/key');
      if (!keyRes.ok) return;
      const { key, enabled } = await keyRes.json();
      if (!enabled || !key) return; // push not configured on this server
      const reg = await navigator.serviceWorker.ready;
      let sub = await reg.pushManager.getSubscription();
      if (!sub) {
        sub = await reg.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey: urlB64ToUint8Array(key),
        });
      }
      await fetch('/api/push/subscribe', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-KS': '1' },
        body: JSON.stringify(sub),
      });
    } catch (e) { /* best effort — never block the app */ }
  }

  async function unsubscribePush() {
    try {
      const reg = await navigator.serviceWorker.ready;
      const sub = await reg.pushManager.getSubscription();
      if (sub) {
        await fetch('/api/push/unsubscribe', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-KS': '1' },
          body: JSON.stringify({ endpoint: sub.endpoint }),
        });
      }
    } catch (e) {}
  }

  const userById = id => S.users.find(u => u.id === id) ||
    (S.me && S.me.id === id ? S.me : null);
  const userName = id => { const u = userById(id); return u ? u.name : 'Unknown'; };
  const firstName = name => (name || '').split(' ')[0];

  function effStatus(t) {
    if (t.status !== 'completed' && t.due_at && new Date(t.due_at) < new Date()) {
      return 'overdue';
    }
    return t.status;
  }

  function isDueSoon(t) {
    if (t.status === 'completed' || !t.due_at) return false;
    const d = new Date(t.due_at) - Date.now();
    return d >= 0 && d <= 24 * 3600 * 1000;
  }

  function findTaskByRef(ref) {
    const r = String(ref);
    if (r.startsWith('local-')) {
      const cid = r.slice(6);
      return S.tasks.find(t => t.client_id === cid) || null;
    }
    return S.tasks.find(t => String(t.id) === r) || null;
  }

  function taskRef(t) { return String(t.id); }

  function taskSort(a, b) {
    const ea = effStatus(a), eb = effStatus(b);
    const doneA = ea === 'completed', doneB = eb === 'completed';
    if (doneA !== doneB) return doneA ? 1 : -1;
    if (doneA) return new Date(b.completed_at || 0) - new Date(a.completed_at || 0);
    if ((ea === 'overdue') !== (eb === 'overdue')) return ea === 'overdue' ? -1 : 1;
    const da = a.due_at ? new Date(a.due_at).getTime() : Infinity;
    const dbb = b.due_at ? new Date(b.due_at).getTime() : Infinity;
    return da - dbb;
  }

  // ------------------------------------------------------------ local apply (optimistic)

  function applyActionLocally(a) {
    const p = a.payload || {};
    if (a.type === 'create_task') {
      if (S.tasks.some(t => t.client_id === a.client_id)) return;
      S.tasks.unshift({
        id: 'local-' + a.client_id, client_id: a.client_id,
        title: p.title, description: p.description || '', priority: p.priority || 'medium',
        status: 'pending', created_by: S.me.id, assigned_to: p.assigned_to,
        due_at: p.due_at || null, created_at: new Date().toISOString(),
        completed_at: null,
        history: [{ user_id: S.me.id, from: null, to: 'pending',
                    note: 'Task created', at: new Date().toISOString() }],
        _pending: true,
      });
    } else if (a.type === 'update_status') {
      const t = findTaskByRef(p.task_id);
      if (!t || t.status === p.status) return;
      t.history = t.history || [];
      t.history.push({ user_id: S.me.id, from: t.status, to: p.status,
                       note: p.note || '', at: new Date().toISOString() });
      t.status = p.status;
      t.completed_at = p.status === 'completed' ? new Date().toISOString() : null;
      t._pending = true;
    } else if (a.type === 'send_message') {
      if (S.messages.some(m => m.client_id === a.client_id)) return;
      S.messages.push({
        id: 'local-' + a.client_id, client_id: a.client_id,
        task_id: p.task_id != null ? p.task_id : null,
        sender_id: S.me.id, recipient_id: p.recipient_id || null,
        body: p.body, created_at: new Date().toISOString(), _pending: true,
      });
    } else if (a.type === 'mark_read') {
      for (const n of S.notifications) {
        if (!p.notification_ids || p.notification_ids.includes(n.id)) n.read = true;
      }
    }
  }

  async function queueAction(type, payload) {
    const a = { client_id: uuid(), type, payload, ts: Date.now() };
    S.outbox.push(a);
    await KSDB.put('outbox', a.client_id, a);
    applyActionLocally(a);
    renderAll();
    scheduleSync(80);
    return a;
  }

  // ------------------------------------------------------------ sync engine

  let syncTimer = null;
  function scheduleSync(delay) {
    clearTimeout(syncTimer);
    syncTimer = setTimeout(() => syncNow(), delay == null ? 250 : delay);
  }

  async function syncNow() {
    if (S.syncing) { S.syncQueued = true; return; }
    S.syncing = true;
    updateSyncPill();
    const hadOutbox = S.outbox.length > 0;
    try {
      const actions = S.outbox
        .slice().sort((a, b) => a.ts - b.ts)
        .map(a => ({ client_id: a.client_id, type: a.type, payload: a.payload }));
      const res = await fetch('/api/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-KS': '1' },
        body: JSON.stringify({ actions }),
      });
      if (res.status === 401) { location.href = 'login.html'; return; }
      if (!res.ok) throw new Error('sync_http_' + res.status);
      const snap = await res.json();
      await applySnapshot(snap);
      S.offline = false;
      S.lastSyncAt = Date.now();
      if (hadOutbox && S.outbox.length === 0) {
        toast('All changes synced', 'i-cloud-check');
      }
      S.justSynced = true;
      setTimeout(() => { S.justSynced = false; updateSyncPill(); }, 2200);
    } catch (err) {
      S.offline = true;
    } finally {
      S.syncing = false;
      updateSyncPill();
      renderAll();
      if (S.syncQueued) { S.syncQueued = false; scheduleSync(300); }
    }
  }

  async function applySnapshot(snap) {
    // Drop acknowledged outbox actions.
    const results = snap.results || [];
    for (const r of results) {
      if (r.status === 'error') continue; // server hiccup — retry next sync
      const idx = S.outbox.findIndex(a => a.client_id === r.client_id);
      if (idx >= 0) {
        await KSDB.remove('outbox', r.client_id);
        S.outbox.splice(idx, 1);
      }
      if (r.status === 'rejected') {
        toast('One change could not be applied (' + (r.detail || 'rejected') + ')',
              'i-alert-triangle');
      }
    }

    S.me = snap.me;
    S.teamCode = snap.team_code || null;
    S.companyName = snap.company_name || '';
    S.pendingMembers = snap.pending_members || [];
    S.users = snap.users || [];
    S.tasks = snap.tasks || [];
    S.messages = snap.messages || [];
    const prevKnown = S.knownNotifIds;
    S.notifications = snap.notifications || [];

    // Toast + browser-notify anything new since we last looked.
    if (prevKnown) {
      const fresh = S.notifications.filter(n => !n.read && !prevKnown.has(n.id));
      for (const n of fresh.slice(0, 3)) {
        toast(n.text, kindIcon(n.kind));
        try {
          if ('Notification' in window && Notification.permission === 'granted' &&
              document.hidden) {
            new Notification('KaryaSetu', { body: n.text, icon: '/static/icons/icon-192.png' });
          }
        } catch {}
      }
    }
    S.knownNotifIds = new Set(S.notifications.map(n => n.id));

    // Register this device for OS-level push once per session (if allowed).
    if (!pushSynced && 'Notification' in window && Notification.permission === 'granted') {
      pushSynced = true;
      subscribePush();
    }

    // Persist base truth, then overlay any still-pending offline actions.
    await KSDB.kvSet('me', S.me);
    await KSDB.replaceAll('users', S.users, u => u.id);
    await KSDB.replaceAll('tasks', S.tasks, t => t.id);
    await KSDB.replaceAll('messages', S.messages, m => m.id);
    await KSDB.replaceAll('notifications', S.notifications, n => n.id);
    for (const a of S.outbox.slice().sort((x, y) => x.ts - y.ts)) {
      applyActionLocally(a);
    }
  }

  // ------------------------------------------------------------ socket + connectivity

  function initSocket() {
    if (typeof io === 'undefined') return;
    try {
      S.socket = io({ transports: ['polling', 'websocket'] });
      S.socket.on('connect', () => { S.offline = false; scheduleSync(100); });
      S.socket.on('disconnect', () => updateSyncPill());
      S.socket.on('refresh', () => scheduleSync(350));
    } catch {}
  }

  window.addEventListener('online', () => { S.offline = false; scheduleSync(200); });
  window.addEventListener('offline', () => { S.offline = true; updateSyncPill(); renderAll(); });

  function updateSyncPill() {
    const pill = $('#sync-pill');
    if (!pill) return;
    let cls, ic, label;
    if (S.syncing) { cls = 'syncing'; ic = 'i-sync'; label = 'Syncing…'; }
    else if (S.offline && S.outbox.length) { cls = 'pending'; ic = 'i-clock'; label = 'Pending sync'; }
    else if (S.offline) { cls = 'offline'; ic = 'i-wifi-off'; label = 'Offline'; }
    else if (S.justSynced) { cls = 'synced'; ic = 'i-cloud-check'; label = 'Synced'; }
    else { cls = 'online'; ic = 'i-wifi'; label = 'Online'; }
    pill.className = 'sync-pill ' + cls;
    pill.innerHTML = icon(ic) + '<span>' + label + '</span>';
  }

  // ------------------------------------------------------------ toasts

  function toast(text, ic) {
    const wrap = $('#toasts');
    if (!wrap) return;
    const el = document.createElement('div');
    el.className = 'toast';
    el.innerHTML = icon(ic || 'i-bell') + '<span>' + esc(text) + '</span>';
    wrap.appendChild(el);
    requestAnimationFrame(() => el.classList.add('show'));
    setTimeout(() => {
      el.classList.remove('show');
      setTimeout(() => el.remove(), 350);
    }, 4200);
  }

  function kindIcon(kind) {
    return {
      task_assigned: 'i-inbox', message: 'i-chat', task_completed: 'i-check-circle',
      due_soon: 'i-clock', overdue: 'i-alert-circle', status_changed: 'i-history',
      team: 'i-users',
    }[kind] || 'i-bell';
  }

  // ------------------------------------------------------------ conversations

  function msgsForTask(t) {
    return S.messages.filter(m => m.task_id != null &&
      (String(m.task_id) === String(t.id) ||
       (t.client_id && String(m.task_id) === 'local-' + t.client_id)))
      .sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
  }

  function dmMsgs(otherId) {
    return S.messages.filter(m => m.task_id == null &&
      ((m.sender_id === S.me.id && m.recipient_id === otherId) ||
       (m.sender_id === otherId && m.recipient_id === S.me.id)))
      .sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
  }

  function lastReadMap() {
    try { return JSON.parse(lsGet('ks_lastread') || '{}'); } catch { return {}; }
  }

  function markConvRead(key) {
    const m = lastReadMap();
    m[key] = Date.now();
    lsSet('ks_lastread', JSON.stringify(m));
  }

  function unreadCount(key, msgs) {
    const last = lastReadMap()[key] || 0;
    return msgs.filter(m => m.sender_id !== S.me.id &&
      new Date(m.created_at).getTime() > last).length;
  }

  function conversations() {
    const convs = [];
    for (const u of S.users) {
      if (u.id === S.me.id) continue;
      const msgs = dmMsgs(u.id);
      convs.push({ key: 'dm:' + u.id, kind: 'dm', user: u, msgs,
                   last: msgs[msgs.length - 1] || null });
    }
    for (const t of S.tasks) {
      const msgs = msgsForTask(t);
      const involved = t.created_by === S.me.id || t.assigned_to === S.me.id;
      // My own task threads always listed; teammates' threads once they
      // have messages, so the whole team can follow the discussion.
      if (!involved && !msgs.length) continue;
      convs.push({ key: 'task:' + taskRef(t), kind: 'task', task: t, msgs,
                   last: msgs[msgs.length - 1] || null });
    }
    convs.sort((a, b) => {
      const ta = a.last ? new Date(a.last.created_at).getTime() : 0;
      const tb = b.last ? new Date(b.last.created_at).getTime() : 0;
      return tb - ta;
    });
    return convs;
  }

  function convByKey(key) {
    if (!key) return null;
    if (key.startsWith('dm:')) {
      const u = userById(parseInt(key.slice(3), 10));
      if (!u) return null;
      const msgs = dmMsgs(u.id);
      return { key, kind: 'dm', user: u, msgs };
    }
    if (key.startsWith('task:')) {
      const t = findTaskByRef(key.slice(5));
      if (!t) return null;
      return { key: 'task:' + taskRef(t), kind: 'task', task: t, msgs: msgsForTask(t) };
    }
    return null;
  }

  function totalUnread() {
    return conversations().reduce((n, c) => n + unreadCount(c.key, c.msgs), 0);
  }

  // ------------------------------------------------------------ chips & fragments

  function statusChip(t) {
    const st = STATUS_META[effStatus(t)];
    return '<span class="chip ' + st.cls + '">' + icon(st.icon) + st.label + '</span>';
  }

  function priorityChip(p) {
    const pm = PRIORITY_META[p] || PRIORITY_META.medium;
    return '<span class="chip ' + pm.cls + '">' + icon('i-flag') + pm.label + '</span>';
  }

  function dueChip(t) {
    if (!t.due_at) return '';
    const es = effStatus(t);
    const cls = es === 'overdue' ? ' due-over' : (isDueSoon(t) ? ' due-soon' : '');
    return '<span class="chip due' + cls + '">' + icon('i-calendar') +
      fmtDT(t.due_at) + '</span>';
  }

  function pendingBadge(item) {
    return item._pending
      ? '<span class="chip pending-sync">' + icon('i-clock') + 'Pending sync</span>'
      : '';
  }

  function personChip(id) {
    const u = userById(id);
    if (!u) return '';
    return '<span class="person">' + avatarHtml(u, 'av-xs') + esc(u.name) + '</span>';
  }

  // ------------------------------------------------------------ shell render

  function renderShell() {
    if (!S.me) return;
    const av = $('#avatar-btn');
    const c = avColor(S.me.id);
    av.textContent = initials(S.me.name);
    av.style.background = c;
    av.style.color = avInk(c);
    $('#menu-name').textContent = S.me.name;
    $('#menu-role').textContent = (S.me.title ? S.me.title + ' · ' : '') +
      (S.me.role === 'manager' ? 'Manager' : 'Team Member') +
      (S.companyName ? ' · ' + S.companyName : '');
    renderNav();
    renderBell();
  }

  function renderNav() {
    const nav = $('#nav');
    if (!nav || !S.me) return;
    const here = (location.hash || '#/dashboard').split('/')[1] || 'dashboard';
    const unread = totalUnread();
    const items = [
      { href: '#/dashboard', ic: 'i-grid', label: 'Dashboard', key: 'dashboard' },
      { href: '#/tasks', ic: 'i-tasks', label: 'Tasks', key: 'tasks' },
      { href: '#/messages', ic: 'i-chat', label: 'Messages', key: 'messages',
        badge: unread || null },
      { href: '#/notifications', ic: 'i-bell', label: 'Alerts', key: 'notifications',
        badge: S.notifications.filter(n => !n.read).length || null },
      { href: '#/team', ic: 'i-users', label: 'Team', key: 'team' },
    ];
    let html = items.map(it =>
      '<a class="nav-item' + (here === it.key ? ' active' : '') + '" href="' + it.href + '">' +
      icon(it.ic) + '<span>' + it.label + '</span>' +
      (it.badge ? '<span class="nav-badge">' + it.badge + '</span>' : '') +
      '</a>').join('');
    if (S.me.role === 'manager') {
      html += '<a class="nav-cta' + (here === 'new-task' ? ' active' : '') +
        '" href="#/new-task">' + icon('i-plus') + '<span>New Task</span></a>';
    }
    nav.innerHTML = html;
  }

  function renderBell() {
    const badge = $('#bell-badge');
    if (!badge) return;
    const n = S.notifications.filter(x => !x.read).length;
    badge.hidden = n === 0;
    badge.textContent = n > 99 ? '99+' : n;
  }

  // ------------------------------------------------------------ views

  function route() {
    const hash = location.hash || '#/dashboard';
    const parts = hash.slice(2).split('/');
    return { name: parts[0] || 'dashboard', param: parts.slice(1).join('/') || null };
  }

  function renderAll() {
    renderShell();
    updateSyncPill();
    renderView();
  }

  function preserveInputs(fn) {
    const view = $('#view');
    const saved = {};
    view.querySelectorAll('[data-preserve]').forEach(elm => {
      saved[elm.id] = { value: elm.value, focus: document.activeElement === elm,
                        selStart: elm.selectionStart };
    });
    fn();
    for (const [id, s] of Object.entries(saved)) {
      const elm = document.getElementById(id);
      if (!elm) continue;
      elm.value = s.value;
      if (s.focus) {
        elm.focus();
        try { elm.selectionStart = elm.selectionEnd = s.selStart; } catch {}
      }
    }
  }

  function renderView() {
    const view = $('#view');
    if (!view) return;
    if (!S.me) {
      view.innerHTML =
        '<div class="empty tall">' + icon('i-wifi-off', 'empty-ic') +
        '<h3>Can’t load your workspace</h3>' +
        '<p>You appear to be offline and no cached data was found on this device. ' +
        'Connect to the internet and try again.</p>' +
        '<button class="btn btn-primary" data-act="retry-boot">Retry</button></div>';
      return;
    }
    const r = route();
    preserveInputs(() => {
      if (r.name === 'dashboard') viewDashboard(view);
      else if (r.name === 'tasks' && r.param) viewTaskDetail(view, r.param);
      else if (r.name === 'tasks') viewTasks(view);
      else if (r.name === 'new-task') viewNewTask(view);
      else if (r.name === 'messages') viewMessages(view, r.param);
      else if (r.name === 'notifications') viewNotifications(view);
      else if (r.name === 'team') viewTeam(view);
      else viewDashboard(view);
    });
  }

  // ---------- dashboard

  function viewDashboard(view) {
    const mine = S.me.role === 'manager'
      ? S.tasks
      : S.tasks.filter(t => t.assigned_to === S.me.id);
    const by = st => mine.filter(t => effStatus(t) === st).length;
    const dueSoon = mine.filter(t => isDueSoon(t) && effStatus(t) !== 'overdue');
    const overdue = mine.filter(t => effStatus(t) === 'overdue');

    const today = new Date().toLocaleDateString(undefined, {
      weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' });

    const tiles = [
      { label: S.me.role === 'manager' ? 'Total tasks' : 'My tasks',
        val: mine.length, ic: 'i-tasks', cls: 'tl-total' },
      { label: 'Pending', val: by('pending'), ic: 'i-clock', cls: 'tl-pending' },
      { label: 'In progress', val: by('in_progress'), ic: 'i-play', cls: 'tl-progress' },
      { label: 'Completed', val: by('completed'), ic: 'i-check-circle', cls: 'tl-completed' },
      S.me.role === 'manager'
        ? { label: 'Overdue', val: overdue.length, ic: 'i-alert-circle', cls: 'tl-overdue' }
        : { label: 'Due soon', val: dueSoon.length, ic: 'i-calendar', cls: 'tl-duesoon' },
    ];

    let html =
      '<header class="view-head">' +
      '<div><h1 class="view-title">Welcome, ' + esc(firstName(S.me.name)) +
      '</h1><p class="page-sub">' + today + '</p></div>' +
      (S.me.role === 'manager'
        ? '<a class="btn btn-primary" href="#/new-task">' + icon('i-plus') + 'New Task</a>'
        : '') +
      '</header>';

    html += '<div class="tiles">' + tiles.map(t =>
      '<div class="tile ' + t.cls + '"><span class="tile-ic">' + icon(t.ic) + '</span>' +
      '<span class="tile-value">' + t.val + '</span>' +
      '<span class="tile-label">' + t.label + '</span></div>').join('') + '</div>';

    html += '<div class="grid-2">';

    // Left column — attention + tasks
    html += '<div class="col">';
    const attention = overdue.concat(dueSoon).slice(0, 6);
    html += '<section class="card"><h2 class="card-title">' +
      icon('i-alert-triangle') + 'Needs attention</h2>';
    if (!attention.length) {
      html += '<p class="empty-line">Nothing overdue or due soon. Everything is on track.</p>';
    } else {
      html += attention.map(t =>
        '<a class="list-row" href="#/tasks/' + taskRef(t) + '">' +
        '<div class="lr-main"><strong>' + esc(t.title) + '</strong>' +
        '<span class="lr-sub">' + (S.me.role === 'manager'
          ? esc(userName(t.assigned_to)) + ' · ' : '') +
        'due ' + fmtDT(t.due_at) + '</span></div>' + statusChip(t) + '</a>').join('');
    }
    html += '</section>';

    if (S.me.role !== 'manager') {
      const focus = mine.filter(t => t.status !== 'completed').sort(taskSort).slice(0, 5);
      html += '<section class="card"><h2 class="card-title">' + icon('i-tasks') +
        'My focus</h2>';
      html += focus.length ? focus.map(t =>
        '<a class="list-row" href="#/tasks/' + taskRef(t) + '">' +
        '<div class="lr-main"><strong>' + esc(t.title) + '</strong>' +
        '<span class="lr-sub">' + PRIORITY_META[t.priority].label + ' priority' +
        (t.due_at ? ' · due ' + fmtD(t.due_at) : '') + '</span></div>' +
        statusChip(t) + '</a>').join('')
        : '<p class="empty-line">No open tasks assigned to you.</p>';
      html += '</section>';
    }

    // Recent activity from status history
    const activity = [];
    for (const t of S.tasks) {
      for (const h of (t.history || [])) {
        activity.push({ t, h });
      }
    }
    activity.sort((a, b) => new Date(b.h.at) - new Date(a.h.at));
    html += '<section class="card"><h2 class="card-title">' + icon('i-history') +
      'Recent activity</h2>';
    html += activity.length ? activity.slice(0, 7).map(({ t, h }) => {
      const who = esc(userName(h.user_id));
      const what = h.from == null
        ? 'created'
        : 'moved to <b>' + STATUS_META[h.to].label + '</b>';
      return '<a class="list-row slim" href="#/tasks/' + taskRef(t) + '">' +
        '<div class="lr-main"><span>' + who + ' ' + what + ' · <b>' +
        esc(t.title) + '</b></span>' +
        '<span class="lr-sub">' + rel(h.at) + '</span></div></a>';
    }).join('') : '<p class="empty-line">No activity yet.</p>';
    html += '</section></div>';

    // Right column — team workload (visible to everyone for coordination)
    html += '<div class="col">';
    {
      const counts = S.users.map(u => ({
        u, n: S.tasks.filter(t => t.assigned_to === u.id && t.status !== 'completed').length,
      }));
      const max = Math.max(1, ...counts.map(c => c.n));
      const activeN = S.users.filter(u => u.online).length;
      html += '<section class="card"><h2 class="card-title">' + icon('i-users') +
        'Team workload <span class="card-note">' + activeN + ' active now</span></h2>';
      html += counts.map(({ u, n }) =>
        '<div class="bar-row" data-tip="' + esc(u.name) + ': ' + n +
        ' open task' + (n === 1 ? '' : 's') + '">' +
        '<span class="bar-name">' + avatarHtml(u, 'av-xs') + esc(firstName(u.name)) +
        (u.online ? '<span class="online-dot" title="Online"></span>' : '') + '</span>' +
        '<span class="bar-track"><span class="bar-fill" style="width:' +
        (n / max * 100) + '%"></span></span>' +
        '<span class="bar-val">' + n + '</span></div>').join('');
      html += '</section>';
    }

    const recentMsgs = S.messages.slice().sort((a, b) =>
      new Date(b.created_at) - new Date(a.created_at)).slice(0, 5);
    html += '<section class="card"><h2 class="card-title">' + icon('i-chat') +
      'Recent messages</h2>';
    html += recentMsgs.length ? recentMsgs.map(m => {
      const t = m.task_id != null ? findTaskByRef(m.task_id) : null;
      const key = t ? 'task:' + taskRef(t)
        : 'dm:' + (m.sender_id === S.me.id ? m.recipient_id : m.sender_id);
      return '<a class="list-row slim" href="#/messages/' + key + '">' +
        avatarHtml(userById(m.sender_id), 'av-sm') +
        '<div class="lr-main"><span><b>' + esc(firstName(userName(m.sender_id))) +
        '</b>' + (t ? ' · ' + esc(t.title) : '') + '</span>' +
        '<span class="lr-sub clamp">' + esc(m.body) + '</span></div>' +
        '<span class="lr-time">' + rel(m.created_at) + '</span></a>';
    }).join('') : '<p class="empty-line">No messages yet.</p>';
    html += '</section></div></div>';

    view.innerHTML = html;
  }

  // ---------- tasks list

  let taskFilter = 'all';
  let taskScope = 'all'; // 'all' = whole team (coordination), 'mine' = involving me
  let teamTab = 'employees'; // Team page: 'managers' | 'employees'

  function viewTasks(view) {
    const searchEl = document.getElementById('task-search');
    const q = (searchEl ? searchEl.value : '').trim().toLowerCase();
    const myCount = S.tasks.filter(t =>
      t.assigned_to === S.me.id || t.created_by === S.me.id).length;
    const scoped = taskScope === 'mine'
      ? S.tasks.filter(t => t.assigned_to === S.me.id || t.created_by === S.me.id)
      : S.tasks;
    const counts = { all: scoped.length };
    for (const st of ['pending', 'in_progress', 'completed', 'overdue']) {
      counts[st] = scoped.filter(t => effStatus(t) === st).length;
    }
    let list = scoped.filter(t => taskFilter === 'all' || effStatus(t) === taskFilter);
    if (q) {
      list = list.filter(t => (t.title + ' ' + t.description + ' ' +
        userName(t.assigned_to)).toLowerCase().includes(q));
    }
    list.sort(taskSort);

    const filters = [
      ['all', 'All'], ['pending', 'Pending'], ['in_progress', 'In Progress'],
      ['completed', 'Completed'], ['overdue', 'Overdue'],
    ];

    let html = '<header class="view-head"><div><h1 class="view-title">Tasks</h1>' +
      '<p class="page-sub">' + counts.all + ' task' + (counts.all === 1 ? '' : 's') +
      (taskScope === 'mine' ? ' involving you' : ' across the team') + '</p></div>' +
      (S.me.role === 'manager'
        ? '<a class="btn btn-primary" href="#/new-task">' + icon('i-plus') + 'New Task</a>'
        : '') + '</header>';

    html += '<div class="filter-row" style="margin-bottom:12px">' +
      '<button class="fchip' + (taskScope === 'all' ? ' active' : '') +
      '" data-act="scope" data-scope="all">' + icon('i-users') + ' Team <b>' +
      S.tasks.length + '</b></button>' +
      '<button class="fchip' + (taskScope === 'mine' ? ' active' : '') +
      '" data-act="scope" data-scope="mine">' + icon('i-user') + ' My tasks <b>' +
      myCount + '</b></button></div>';

    html += '<div class="toolbar"><div class="filter-row">' +
      filters.map(([k, lbl]) =>
        '<button class="fchip' + (taskFilter === k ? ' active' : '') +
        '" data-act="filter" data-filter="' + k + '">' + lbl +
        ' <b>' + counts[k] + '</b></button>').join('') +
      '</div><label class="searchbar">' + icon('i-search') +
      '<input id="task-search" data-preserve type="search" placeholder="Search tasks…"' +
      ' autocomplete="off"></label></div>';

    if (!list.length) {
      html += '<div class="empty">' + icon('i-tasks', 'empty-ic') +
        '<h3>No tasks here</h3><p>' +
        (q ? 'Nothing matches your search.' : 'Tasks in this view will appear here.') +
        '</p></div>';
    } else {
      html += '<div class="task-list">' + list.map(t => {
        return '<a class="task-card" href="#/tasks/' + taskRef(t) + '">' +
          '<div class="t-top"><h3 class="t-title">' + esc(t.title) + '</h3>' +
          statusChip(t) + '</div>' +
          (t.description ? '<p class="t-desc clamp">' + esc(t.description) + '</p>' : '') +
          '<div class="t-meta">' + personChip(t.assigned_to) +
          priorityChip(t.priority) + dueChip(t) + pendingBadge(t) +
          '</div></a>';
      }).join('') + '</div>';
    }
    view.innerHTML = html;
  }

  // ---------- task detail

  function viewTaskDetail(view, ref) {
    const t = findTaskByRef(ref);
    if (!t) {
      view.innerHTML = '<div class="empty tall">' + icon('i-tasks', 'empty-ic') +
        '<h3>Task not found</h3><p>It may not be synced to this device yet.</p>' +
        '<a class="btn btn-ghost" href="#/tasks">Back to tasks</a></div>';
      return;
    }
    const es = effStatus(t);
    const canAct = S.me.id === t.assigned_to || S.me.id === t.created_by ||
      S.me.role === 'manager';
    // The task thread is private to its creator and assignee only.
    const inThread = S.me.id === t.assigned_to || S.me.id === t.created_by;
    const msgs = inThread ? msgsForTask(t) : [];
    if (inThread) markConvRead('task:' + taskRef(t));

    let actions = '';
    if (canAct) {
      if (t.status === 'pending') {
        actions =
          btnStatus(t, 'in_progress', 'i-play', 'Start Task', 'btn-ghost') +
          btnStatus(t, 'completed', 'i-check', 'Mark as Completed', 'btn-primary');
      } else if (t.status === 'in_progress') {
        actions =
          btnStatus(t, 'pending', 'i-clock', 'Move to Pending', 'btn-ghost') +
          btnStatus(t, 'completed', 'i-check', 'Mark as Completed', 'btn-primary');
      } else {
        actions = btnStatus(t, 'in_progress', 'i-sync', 'Reopen Task', 'btn-ghost');
      }
    }

    let html = '<a class="backlink" href="#/tasks">' + icon('i-arrow-left') +
      'All tasks</a>';

    html += '<div class="detail-grid"><div class="col">';

    html += '<section class="card"><div class="t-top">' +
      '<h1 class="detail-title">' + esc(t.title) + '</h1>' + statusChip(t) + '</div>' +
      '<div class="t-meta">' + priorityChip(t.priority) + dueChip(t) +
      pendingBadge(t) + '</div>' +
      (t.description
        ? '<p class="detail-desc">' + esc(t.description).replace(/\n/g, '<br>') + '</p>'
        : '<p class="detail-desc muted">No description.</p>');

    if (t.status === 'completed') {
      html += '<div class="done-banner">' + icon('i-check-circle') +
        '<div><b>Completed</b><span>' + fmtDT(t.completed_at) + ' · by ' +
        esc(userName((t.history || []).filter(h => h.to === 'completed').slice(-1)[0]
          ? (t.history || []).filter(h => h.to === 'completed').slice(-1)[0].user_id
          : t.assigned_to)) + '</span></div></div>';
    } else if (es === 'overdue') {
      html += '<div class="over-banner">' + icon('i-alert-circle') +
        '<div><b>Overdue</b><span>was due ' + fmtDT(t.due_at) + '</span></div></div>';
    }
    if (actions) html += '<div class="action-row">' + actions + '</div>';
    html += '</section>';

    if (inThread) {
      html += '<section class="card"><h2 class="card-title">' + icon('i-chat') +
        'Conversation</h2><div class="chat-msgs inline" id="task-chat">' +
        (msgs.length ? msgs.map(msgBubble).join('')
          : '<p class="empty-line">No messages on this task yet. Start the conversation below.</p>') +
        '</div>' + composerHtml('task:' + taskRef(t)) + '</section>';
    } else {
      html += '<section class="card"><h2 class="card-title">' + icon('i-lock') +
        'Private conversation</h2><p class="empty-line">The discussion on this task ' +
        'is private to the person who created it and the person it’s assigned to.</p></section>';
    }

    html += '</div><div class="col">';

    html += '<section class="card"><h2 class="card-title">' + icon('i-user') +
      'Details</h2><dl class="info-grid">' +
      '<dt>Assigned to</dt><dd>' + personChip(t.assigned_to) + '</dd>' +
      '<dt>Created by</dt><dd>' + personChip(t.created_by) + '</dd>' +
      '<dt>Priority</dt><dd>' + PRIORITY_META[t.priority].label + '</dd>' +
      '<dt>Due</dt><dd>' + (t.due_at ? fmtDT(t.due_at) : 'No due date') + '</dd>' +
      '<dt>Created</dt><dd>' + fmtDT(t.created_at) + '</dd>' +
      (t.completed_at ? '<dt>Completed</dt><dd>' + fmtDT(t.completed_at) + '</dd>' : '') +
      '</dl></section>';

    const hist = (t.history || []).slice().reverse();
    html += '<section class="card"><h2 class="card-title">' + icon('i-history') +
      'Activity</h2><div class="timeline">' +
      (hist.length ? hist.map(h =>
        '<div class="tl-item"><span class="tl-dot ' +
        (STATUS_META[h.to] ? STATUS_META[h.to].cls : '') + '"></span>' +
        '<div><p>' + esc(userName(h.user_id)) + ' ' +
        (h.from == null ? 'created the task'
          : 'moved it to <b>' + STATUS_META[h.to].label + '</b>') +
        (h.note && h.from != null ? ' — “' + esc(h.note) + '”' : '') + '</p>' +
        '<span class="lr-sub">' + fmtDT(h.at) + '</span></div></div>').join('')
        : '<p class="empty-line">No activity yet.</p>') +
      '</div></section>';

    html += '</div></div>';
    view.innerHTML = html;
    const chat = $('#task-chat');
    if (chat) chat.scrollTop = chat.scrollHeight;
    bindComposer();
  }

  function btnStatus(t, status, ic, label, cls) {
    return '<button class="btn ' + cls + '" data-act="status" data-task="' +
      esc(taskRef(t)) + '" data-status="' + status + '">' + icon(ic) + label + '</button>';
  }

  // ---------- new task

  function viewNewTask(view) {
    if (S.me.role !== 'manager') { location.hash = '#/tasks'; return; }
    const options = S.users.filter(u => u.id !== S.me.id).map(u =>
      '<option value="' + u.id + '">' + esc(u.name) +
      (u.title ? ' — ' + esc(u.title) : '') + '</option>').join('');
    view.innerHTML =
      '<a class="backlink" href="#/tasks">' + icon('i-arrow-left') + 'All tasks</a>' +
      '<header class="view-head"><div><h1 class="view-title">Assign a new task</h1>' +
      '<p class="page-sub">The team member is notified the moment it syncs.</p></div></header>' +
      '<form id="new-task-form" class="card form">' +
      '<label class="field"><span>Task title *</span>' +
      '<input id="nt-title" data-preserve name="title" required maxlength="250" ' +
      'placeholder="e.g. Prepare monthly sales report"></label>' +
      '<label class="field"><span>Description</span>' +
      '<textarea id="nt-desc" data-preserve name="description" rows="4" ' +
      'placeholder="What exactly needs to be done?"></textarea></label>' +
      '<div class="form-row">' +
      '<label class="field"><span>Assign to *</span>' +
      '<select id="nt-assignee" data-preserve name="assigned_to" required>' +
      '<option value="">Select team member…</option>' + options + '</select></label>' +
      '<label class="field"><span>Priority</span>' +
      '<select id="nt-priority" data-preserve name="priority">' +
      '<option value="low">Low</option><option value="medium" selected>Medium</option>' +
      '<option value="high">High</option><option value="urgent">Urgent</option>' +
      '</select></label>' +
      '<label class="field"><span>Due date &amp; time</span>' +
      '<input id="nt-due" data-preserve type="datetime-local" name="due_at"></label>' +
      '</div>' +
      '<div class="action-row"><button type="submit" class="btn btn-primary">' +
      icon('i-plus') + 'Create &amp; Assign</button>' +
      '<a class="btn btn-ghost" href="#/tasks">Cancel</a></div></form>';

    $('#new-task-form').addEventListener('submit', async (e) => {
      e.preventDefault();
      const f = e.target;
      const assigned = parseInt(f.assigned_to.value, 10);
      if (!f.title.value.trim() || !assigned) return;
      const due = f.due_at.value ? new Date(f.due_at.value).toISOString() : null;
      await queueAction('create_task', {
        title: f.title.value.trim(),
        description: f.description.value.trim(),
        assigned_to: assigned,
        priority: f.priority.value,
        due_at: due,
      });
      toast(S.offline
        ? 'Task saved — it will be assigned when you’re back online'
        : 'Task assigned to ' + userName(assigned), 'i-check-circle');
      location.hash = '#/tasks';
    });
  }

  // ---------- messages

  function viewMessages(view, key) {
    const convs = conversations();
    const active = key ? convByKey(key) : null;
    if (active) markConvRead(active.key);

    const listHtml = convs.map(c => {
      const unread = unreadCount(c.key, c.msgs);
      const name = c.kind === 'dm' ? c.user.name : c.task.title;
      const av = c.kind === 'dm'
        ? avatarHtml(c.user, 'av-md')
        : '<span class="av av-md av-task">' + icon('i-tasks') + '</span>';
      const sub = c.last
        ? (c.last.sender_id === S.me.id ? 'You: ' : '') + c.last.body
        : (c.kind === 'task' ? 'Task conversation' : (c.user.title || 'Direct message'));
      return '<a class="conv-item' +
        (active && active.key === c.key ? ' active' : '') +
        '" href="#/messages/' + c.key + '">' + av +
        '<div class="lr-main"><span class="conv-name">' + esc(name) +
        (c.kind === 'task' ? '<i class="tag">Task</i>' : '') +
        (c.kind === 'dm' && c.user.online ? '<span class="online-dot"></span>' : '') +
        '</span><span class="lr-sub clamp">' + esc(sub) + '</span></div>' +
        '<div class="conv-side">' +
        (c.last ? '<span class="lr-time">' + rel(c.last.created_at) + '</span>' : '') +
        (unread ? '<span class="unread-dot">' + unread + '</span>' : '') +
        '</div></a>';
    }).join('');

    let chatHtml;
    if (!active) {
      chatHtml = '<div class="empty chat-empty">' + icon('i-chat', 'empty-ic') +
        '<h3>Select a conversation</h3>' +
        '<p>Message a teammate directly, or open a task thread.</p></div>';
    } else {
      const title = active.kind === 'dm' ? active.user.name : active.task.title;
      const sub = active.kind === 'dm'
        ? (active.user.online ? 'Online now' : (active.user.title || 'Offline'))
        : 'with ' + esc(userName(active.kind === 'task' &&
            S.me.id === active.task.assigned_to
            ? active.task.created_by : active.task.assigned_to));
      chatHtml =
        '<header class="chat-head">' +
        '<a class="iconbtn only-mobile" href="#/messages" aria-label="Back">' +
        icon('i-arrow-left') + '</a>' +
        (active.kind === 'dm' ? avatarHtml(active.user, 'av-md')
          : '<span class="av av-md av-task">' + icon('i-tasks') + '</span>') +
        '<div class="lr-main"><span class="conv-name">' + esc(title) + '</span>' +
        '<span class="lr-sub">' + sub + '</span></div>' +
        (active.kind === 'task'
          ? '<a class="btn btn-ghost btn-sm" href="#/tasks/' +
            taskRef(active.task) + '">Open task</a>'
          : '') +
        '</header>' +
        '<div class="chat-msgs" id="chat-scroll">' +
        (active.msgs.length ? active.msgs.map(msgBubble).join('')
          : '<p class="empty-line center">No messages yet. Start the conversation.</p>') +
        '</div>' + composerHtml(active.key);
    }

    view.innerHTML =
      '<div class="chat-layout' + (active ? ' has-active' : '') + '">' +
      '<aside class="conv-list"><header class="pane-head"><h1>Messages</h1></header>' +
      (convs.length ? listHtml
        : '<div class="empty">' + icon('i-chat', 'empty-ic') + '<p>No conversations yet.</p></div>') +
      '</aside>' +
      '<section class="chat-pane">' + chatHtml + '</section></div>';

    const scroll = $('#chat-scroll');
    if (scroll) scroll.scrollTop = scroll.scrollHeight;
    bindComposer();
  }

  function msgBubble(m) {
    const mine = m.sender_id === S.me.id;
    const status = m._pending
      ? icon('i-clock', 'msg-tick') : icon('i-check', 'msg-tick');
    return '<div class="msg' + (mine ? ' mine' : '') +
      (m._pending ? ' pending' : '') + '">' +
      (!mine ? avatarHtml(userById(m.sender_id), 'av-xs') : '') +
      '<div class="msg-bubble">' +
      (!mine ? '<span class="msg-who">' + esc(firstName(userName(m.sender_id))) +
        '</span>' : '') +
      '<p>' + esc(m.body).replace(/\n/g, '<br>') + '</p>' +
      '<span class="msg-meta">' + fmtT(m.created_at) +
      (mine ? status : '') + '</span></div></div>';
  }

  function composerHtml(convKey) {
    // Per-conversation input id so a draft never leaks into another thread.
    const cid = 'composer-' + convKey.replace(/[^a-z0-9]/gi, '-');
    return '<form class="composer" data-conv="' + esc(convKey) + '">' +
      '<input id="' + cid + '" data-preserve type="text" ' +
      'placeholder="Type a message…" autocomplete="off" maxlength="5000">' +
      '<button type="submit" class="btn btn-primary btn-send" aria-label="Send">' +
      icon('i-send') + '</button></form>';
  }

  function bindComposer() {
    document.querySelectorAll('.composer').forEach(form => {
      form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const input = form.querySelector('input');
        const body = input.value.trim();
        if (!body) return;
        const convKey = form.dataset.conv;
        input.value = '';
        if (convKey.startsWith('task:')) {
          await queueAction('send_message', { task_id: convKey.slice(5), body });
        } else {
          await queueAction('send_message',
            { recipient_id: parseInt(convKey.slice(3), 10), body });
        }
        markConvRead(convKey);
        const scroll = $('#chat-scroll') || $('#task-chat');
        if (scroll) scroll.scrollTop = scroll.scrollHeight;
      });
    });
  }

  // ---------- notifications

  function viewNotifications(view) {
    const list = S.notifications.slice().sort((a, b) =>
      new Date(b.created_at) - new Date(a.created_at));
    const unread = list.filter(n => !n.read).length;
    let html = '<header class="view-head"><div><h1 class="view-title">Notifications</h1>' +
      '<p class="page-sub">' + (unread ? unread + ' unread' : 'All caught up') +
      '</p></div><div class="action-row">';
    if ('Notification' in window && Notification.permission === 'default') {
      html += '<button class="btn btn-ghost" data-act="enable-notif">' +
        icon('i-bell') + 'Enable device alerts</button>';
    }
    if (unread) {
      html += '<button class="btn btn-ghost" data-act="read-all">' +
        icon('i-check') + 'Mark all read</button>';
    }
    html += '</div></header>';

    if (!list.length) {
      html += '<div class="empty tall">' + icon('i-bell', 'empty-ic') +
        '<h3>No notifications</h3><p>Task updates and messages will appear here.</p></div>';
    } else {
      html += '<div class="card notif-list">' + list.map(n =>
        '<button class="notif-item' + (n.read ? '' : ' unread') +
        '" data-act="notif" data-id="' + n.id + '" data-task="' +
        (n.task_id == null ? '' : n.task_id) + '">' +
        '<span class="n-ic ' + esc(n.kind) + '">' + icon(kindIcon(n.kind)) + '</span>' +
        '<div class="lr-main"><span>' + esc(n.text) + '</span>' +
        '<span class="lr-sub">' + rel(n.created_at) + '</span></div>' +
        (n.read ? '' : '<span class="unread-dot solo"></span>') +
        '</button>').join('') + '</div>';
    }
    view.innerHTML = html;
  }

  // ---------- team

  function memberCardHtml(u) {
    const open = S.tasks.filter(t => t.assigned_to === u.id &&
      t.status !== 'completed').length;
    const done = S.tasks.filter(t => t.assigned_to === u.id &&
      t.status === 'completed').length;
    return '<div class="card member-card">' +
      '<div class="member-top">' + avatarHtml(u, 'av-lg') +
      '<div class="lr-main"><span class="conv-name">' + esc(u.name) +
      (u.online ? '<span class="online-dot"></span>' : '') + '</span>' +
      '<span class="lr-sub">' + esc(u.title || (u.role === 'manager'
        ? 'Manager' : 'Team member')) + '</span></div>' +
      '<span class="chip ' + (u.role === 'manager' ? 'role-mgr' : 'role-member') +
      '">' + (u.role === 'manager' ? 'Manager' : 'Member') + '</span></div>' +
      '<div class="member-stats"><span><b>' + open + '</b> open</span>' +
      '<span><b>' + done + '</b> completed</span></div>' +
      (u.id !== S.me.id
        ? '<div class="action-row"><a class="btn btn-ghost btn-sm" ' +
          'href="#/messages/dm:' + u.id + '">' + icon('i-chat') + 'Message</a></div>'
        : '') + '</div>';
  }

  function pendingCardHtml(u) {
    const wants = u.requested_role === 'manager' ? 'manager' : 'member';
    const wantLabel = wants === 'manager' ? 'Manager / CEO' : 'Employee';
    return '<div class="card member-card">' +
      '<div class="member-top">' + avatarHtml(u, 'av-lg') +
      '<div class="lr-main"><span class="conv-name">' + esc(u.name) + '</span>' +
      '<span class="lr-sub">' + esc(u.email) +
      (u.title ? ' · ' + esc(u.title) : '') + '</span></div>' +
      '<span class="chip role-pending">Wants: ' + wantLabel + '</span></div>' +
      '<label class="field approve-role"><span>Approve as</span>' +
      '<select data-role-for="' + u.id + '">' +
      '<option value="member"' + (wants === 'member' ? ' selected' : '') +
      '>Employee</option>' +
      '<option value="manager"' + (wants === 'manager' ? ' selected' : '') +
      '>Manager / CEO</option></select></label>' +
      '<div class="action-row">' +
      '<button class="btn btn-primary btn-sm" data-act="approve-member" ' +
      'data-id="' + u.id + '">' + icon('i-check-circle') + 'Approve</button>' +
      '<button class="btn btn-ghost btn-sm" data-act="reject-member" ' +
      'data-id="' + u.id + '" data-name="' + esc(u.name) + '">' +
      icon('i-x') + 'Decline</button></div></div>';
  }

  function viewTeam(view) {
    const isMgr = S.me.role === 'manager';
    const pending = isMgr ? (S.pendingMembers || []) : [];
    const managers = S.users.filter(u => u.role === 'manager');
    const employees = S.users.filter(u => u.role !== 'manager');
    // Non-managers never see the Requests tab; keep them on a valid tab.
    if (teamTab === 'requests' && !isMgr) teamTab = 'employees';
    let html = '<header class="view-head"><div><h1 class="view-title">Team</h1>' +
      '<p class="page-sub">' + S.users.length + ' people · ' +
      S.users.filter(u => u.online).length + ' online</p></div></header>';

    // Managers, Employees, and (for managers) pending join Requests are tabs.
    html += '<div class="filter-row">' +
      '<button class="fchip' + (teamTab === 'managers' ? ' active' : '') +
      '" data-act="team-tab" data-tab="managers">' + icon('i-user') +
      ' Managers <b>' + managers.length + '</b></button>' +
      '<button class="fchip' + (teamTab === 'employees' ? ' active' : '') +
      '" data-act="team-tab" data-tab="employees">' + icon('i-users') +
      ' Employees <b>' + employees.length + '</b></button>' +
      (isMgr ? '<button class="fchip' + (teamTab === 'requests' ? ' active' : '') +
        (pending.length ? ' has-badge' : '') +
        '" data-act="team-tab" data-tab="requests">' + icon('i-inbox') +
        ' Requests <b>' + pending.length + '</b></button>' : '') +
      '</div>';

    if (teamTab === 'requests') {
      if (!pending.length) {
        html += '<div class="empty">' + icon('i-inbox', 'empty-ic') +
          '<p>No pending join requests.</p>' +
          '<p class="empty-line">When someone signs up with your team code, ' +
          'their request appears here for you to approve.</p></div>';
      } else {
        html += '<div class="member-grid">' +
          pending.map(pendingCardHtml).join('') + '</div>';
      }
      view.innerHTML = html;
      return;
    }

    const list = teamTab === 'managers' ? managers : employees;
    if (!list.length) {
      html += '<div class="empty"><p>No ' +
        (teamTab === 'managers' ? 'managers' : 'employees') + ' yet.</p></div>';
    } else {
      html += '<div class="member-grid">' + list.map(memberCardHtml).join('') + '</div>';
    }

    if (!isMgr) { view.innerHTML = html; return; }

    html += '<section class="card"><h2 class="card-title">' + icon('i-users') +
      'Team join code</h2><div class="code-row">' +
      '<span class="team-code">' + esc(S.teamCode || '· · · · ·') + '</span>' +
      '<button class="btn btn-ghost btn-sm" data-act="rotate-code">' +
      icon('i-sync') + 'Generate new code</button></div>' +
      '<p class="empty-line">Share this code with new members along with the app — ' +
      'they request an account from the sign-in page using it, and it stays ' +
      'locked until you approve them under <b>Requests</b> above. ' +
      'Generating a new code immediately disables the old one.</p></section>';

    html += '<form id="add-member-form" class="card form">' +
      '<h2 class="card-title">' + icon('i-plus') + 'Add a team member</h2>' +
      '<div class="form-row">' +
      '<label class="field"><span>Full name *</span>' +
      '<input id="am-name" data-preserve name="name" required maxlength="120"></label>' +
      '<label class="field"><span>Email *</span>' +
      '<input id="am-email" data-preserve name="email" type="email" required></label>' +
      '</div><div class="form-row">' +
      '<label class="field"><span>Job title</span>' +
      '<input id="am-title" data-preserve name="title" maxlength="120"></label>' +
      '<label class="field"><span>Temporary password *</span>' +
      '<input id="am-pass" data-preserve name="password" type="text" required ' +
      'minlength="6" placeholder="Share this with them"></label>' +
      '<label class="field"><span>Role</span>' +
      '<select id="am-role" data-preserve name="role">' +
      '<option value="member" selected>Team Member</option>' +
      '<option value="manager">Manager</option></select></label></div>' +
      '<div class="action-row"><button type="submit" class="btn btn-primary">' +
      icon('i-plus') + 'Add member</button></div></form>';

    view.innerHTML = html;

    $('#add-member-form').addEventListener('submit', async (e) => {
      e.preventDefault();
      if (S.offline) {
        toast('Adding members needs an internet connection', 'i-wifi-off');
        return;
      }
      const f = e.target;
      try {
        const res = await fetch('/api/members', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-KS': '1' },
          body: JSON.stringify({
            name: f.name.value, email: f.email.value,
            password: f.password.value, title: f.title.value,
            role: f.role.value,
          }),
        });
        const data = await res.json().catch(() => ({}));
        if (res.ok) {
          toast(f.name.value + ' added to the team', 'i-check-circle');
          f.reset();
          scheduleSync(50);
        } else {
          toast(data.detail || 'Could not add member', 'i-alert-triangle');
        }
      } catch {
        toast('Could not reach the server', 'i-wifi-off');
      }
    });
  }

  // ------------------------------------------------------------ delegated events

  document.addEventListener('click', async (e) => {
    const actEl = e.target.closest('[data-act]');
    if (!actEl) return;
    const act = actEl.dataset.act;

    if (act === 'filter') {
      taskFilter = actEl.dataset.filter;
      renderView();
    } else if (act === 'scope') {
      taskScope = actEl.dataset.scope;
      renderView();
    } else if (act === 'team-tab') {
      teamTab = actEl.dataset.tab;
      renderView();
    } else if (act === 'status') {
      const status = actEl.dataset.status;
      await queueAction('update_status', { task_id: actEl.dataset.task, status });
      const labels = { completed: 'Task marked as Completed',
        in_progress: 'Task started', pending: 'Task moved to Pending' };
      toast((labels[status] || 'Status updated') +
        (S.offline ? ' — will sync when online' : ''), 'i-check-circle');
    } else if (act === 'notif') {
      const id = parseInt(actEl.dataset.id, 10);
      const n = S.notifications.find(x => x.id === id);
      if (n && !n.read) await queueAction('mark_read', { notification_ids: [id] });
      if (actEl.dataset.task) location.hash = '#/tasks/' + actEl.dataset.task;
    } else if (act === 'read-all') {
      await queueAction('mark_read', {});
    } else if (act === 'rotate-code') {
      if (!confirm('Generate a new team code? The current code will stop working.')) return;
      try {
        const res = await fetch('/api/team-code/rotate', {
          method: 'POST', headers: { 'Content-Type': 'application/json', 'X-KS': '1' },
        });
        const data = await res.json().catch(() => ({}));
        if (res.ok) {
          S.teamCode = data.team_code;
          toast('New team code: ' + data.team_code, 'i-check-circle');
          renderView();
        } else {
          toast('Could not generate a new code', 'i-alert-triangle');
        }
      } catch {
        toast('Could not reach the server', 'i-wifi-off');
      }
    } else if (act === 'approve-member' || act === 'reject-member') {
      if (S.offline) {
        toast('Approving members needs an internet connection', 'i-wifi-off');
        return;
      }
      const id = actEl.dataset.id;
      const approve = act === 'approve-member';
      if (!approve &&
          !confirm('Decline ' + (actEl.dataset.name || 'this request') +
                   '? Their sign-up will be removed.')) return;
      actEl.disabled = true;
      let body = undefined;
      if (approve) {
        const sel = document.querySelector('[data-role-for="' + id + '"]');
        body = JSON.stringify({ role: sel ? sel.value : 'member' });
      }
      try {
        const res = await fetch('/api/members/' + id + '/' +
          (approve ? 'approve' : 'reject'), {
          method: 'POST', headers: { 'Content-Type': 'application/json', 'X-KS': '1' },
          body,
        });
        if (res.ok) {
          S.pendingMembers = (S.pendingMembers || []).filter(m => String(m.id) !== String(id));
          toast(approve ? 'Member approved' : 'Request declined', 'i-check-circle');
          renderView();
          scheduleSync(50);
        } else {
          toast('Could not update the request', 'i-alert-triangle');
          actEl.disabled = false;
        }
      } catch {
        toast('Could not reach the server', 'i-wifi-off');
        actEl.disabled = false;
      }
    } else if (act === 'enable-notif') {
      try {
        const perm = await Notification.requestPermission();
        if (perm === 'granted') { await subscribePush(); toast('Device alerts enabled', 'i-check-circle'); }
      } catch {}
      renderView();
    } else if (act === 'retry-boot') {
      scheduleSync(0);
    }
  });

  document.addEventListener('input', (e) => {
    if (e.target.id === 'task-search') renderView();
  });

  // topbar interactions
  $('#bell-btn').addEventListener('click', () => { location.hash = '#/notifications'; });
  $('#sync-pill').addEventListener('click', () => syncNow());

  const avatarBtn = $('#avatar-btn'), avatarMenu = $('#avatar-menu');
  avatarBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    avatarMenu.hidden = !avatarMenu.hidden;
  });
  document.addEventListener('click', (e) => {
    if (!avatarMenu.hidden && !e.target.closest('.avatar-wrap')) avatarMenu.hidden = true;
  });

  $('#logout-btn').addEventListener('click', async () => {
    if (S.outbox.length &&
        !confirm('You have unsynced changes that will be lost. Sign out anyway?')) {
      return;
    }
    await unsubscribePush();
    try {
      await fetch('/api/logout', { method: 'POST', headers: { 'X-KS': '1' } });
    } catch {}
    await KSDB.clearAll();
    try { localStorage.removeItem('ks_lastread'); } catch {}
    location.href = 'login.html';
  });

  window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    deferredInstall = e;
    $('#install-btn').hidden = false;
  });
  $('#install-btn').addEventListener('click', async () => {
    if (!deferredInstall) return;
    deferredInstall.prompt();
    await deferredInstall.userChoice.catch(() => {});
    deferredInstall = null;
    $('#install-btn').hidden = true;
    avatarMenu.hidden = true;
  });

  window.addEventListener('hashchange', () => {
    renderNav();
    renderView();
  });

  // ------------------------------------------------------------ boot

  async function boot() {
    // Rehydrate from IndexedDB so the app is instantly useful offline.
    S.me = await KSDB.kvGet('me');
    S.users = await KSDB.getAll('users');
    S.tasks = await KSDB.getAll('tasks');
    S.messages = await KSDB.getAll('messages');
    S.notifications = await KSDB.getAll('notifications');
    S.outbox = (await KSDB.getAll('outbox')).sort((a, b) => a.ts - b.ts);
    if (S.me) {
      S.knownNotifIds = new Set(S.notifications.map(n => n.id));
      for (const a of S.outbox) applyActionLocally(a);
    }
    renderAll();
    initSocket();
    syncNow();
    setInterval(() => {
      const r = route();
      updateSyncPill();
      if (['dashboard', 'tasks', 'notifications'].includes(r.name)) renderView();
    }, 60000);
    // Fallback freshness: on hosts without WebSocket support the socket
    // never connects, so poll for updates instead of waiting for hints.
    setInterval(() => {
      if ((!S.socket || !S.socket.connected) && !S.offline && !S.syncing) {
        scheduleSync(0);
      }
    }, 30000);

    if ('serviceWorker' in navigator) {
      0;
    }
  }

  boot();
})();
