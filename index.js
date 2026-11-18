/* 루미나 사가 멀티 서버 (Cloudflare Worker + Durable Object)
 * 게임 쪽에서는 예전 Firebase와 같은 방식(경로 트리 + 구독)으로 쓰도록 만든 작은 실시간 서버예요.
 * 메모리에 트리를 들고 있고, rank/ 만 저장소에 영구 저장해요.
 */
import { DurableObject } from 'cloudflare:workers';

const TOP = new Set(['p', 'ps', 'm', 'h', 'k', 'c', 'party', 'inv', 'pvp', 'pk', 'pqe', 'rank']);
const KEEP = { c: 40, k: 30, pk: 30, h: 200, inv: 20, pvp: 60 }; // 목록이 너무 길어지지 않게 (경로 깊이 2의 자식 수)
const MAX_MSG = 16384, RATE = 60; // 메시지 크기 · 초당 횟수 제한

const split = p => String(p || '').split('/').filter(Boolean);
const clone = v => (v === undefined ? null : JSON.parse(JSON.stringify(v)));
const isObj = v => v && typeof v === 'object' && !Array.isArray(v);

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/ws') {
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('websocket only', { status: 426 });
      return env.HUB.get(env.HUB.idFromName('main')).fetch(req);
    }
    return new Response('LUMINA SAGA server OK', { headers: { 'content-type': 'text/plain; charset=utf-8', 'access-control-allow-origin': '*' } });
  },
};

export class Hub extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.root = {};
    this.subs = new Map(); // ws -> Map(id -> {p, ev, lim})
    this.od = new Map(); // ws -> Set(path)
    this.rate = new Map();
    ctx.blockConcurrencyWhile(async () => {
      const r = await ctx.storage.get('rank');
      if (r) this.root.rank = r;
    });
  }

  async fetch(req) {
    const [client, server] = Object.values(new WebSocketPair());
    server.accept();
    this.subs.set(server, new Map());
    this.od.set(server, new Set());
    server.addEventListener('message', e => this.onMsg(server, e.data));
    const bye = () => this.onClose(server);
    server.addEventListener('close', bye);
    server.addEventListener('error', bye);
    server.send(JSON.stringify([{ t: 'hi', now: Date.now() }]));
    return new Response(null, { status: 101, webSocket: client });
  }

  /* ---------- 트리 ---------- */
  get(segs) { let n = this.root; for (const s of segs) { if (!isObj(n)) return null; n = n[s]; if (n === undefined) return null; } return n === undefined ? null : n; }
  put(segs, v) {
    if (!segs.length) return;
    let n = this.root; const path = [];
    for (let i = 0; i < segs.length - 1; i++) { if (!isObj(n[segs[i]])) { if (v === null) return; n[segs[i]] = {}; } path.push([n, segs[i]]); n = n[segs[i]]; }
    const last = segs[segs.length - 1];
    if (v === null || (isObj(v) && !Object.keys(v).length)) delete n[last]; else n[last] = v;
    for (let i = path.length - 1; i >= 0; i--) { const [par, k] = path[i]; if (isObj(par[k]) && !Object.keys(par[k]).length) delete par[k]; else break; }
  }
  stamp(v, now) {
    if (isObj(v)) { if (v['.sv'] === 'timestamp') return now; for (const k in v) { v[k] = this.stamp(v[k], now); if (v[k] === null) delete v[k]; } }
    return v;
  }

  /* ---------- 쓰기 + 알림 ---------- */
  write(segs, v) {
    const pstr = segs.join('/');
    const aff = [];
    for (const [ws, m] of this.subs) for (const [id, s] of m) {
      if (s.p === pstr || pstr.startsWith(s.p ? s.p + '/' : '') || s.p.startsWith(pstr + '/')) aff.push([ws, id, s, clone(this.get(split(s.p)))]);
    }
    this.put(segs, v);
    this.trim(segs);
    for (const [ws, id, s, before] of aff) this.notify(ws, id, s, before);
    if (segs[0] === 'rank') this.ctx.storage.put('rank', this.root.rank || {});
  }
  trim(segs) {
    const lim = KEEP[segs[0]];
    if (!lim || segs.length < 2) return;
    const listSegs = segs[0] === 'h' || segs[0] === 'c' || segs[0] === 'k' || segs[0] === 'inv' || segs[0] === 'pvp' ? segs.slice(0, 2) : segs.slice(0, 2);
    const list = this.get(listSegs);
    if (!isObj(list)) return;
    const keys = Object.keys(list).sort();
    for (let i = 0; i < keys.length - lim; i++) delete list[keys[i]];
  }
  notify(ws, id, s, before) {
    const after = this.get(split(s.p));
    const out = [];
    if (s.ev === 'value') {
      if (JSON.stringify(before) !== JSON.stringify(after)) out.push({ t: 'v', id, v: after });
    } else {
      const b = isObj(before) ? before : {}, a = isObj(after) ? after : {};
      for (const k in a) {
        if (!(k in b)) { if (s.ev === 'child_added') out.push({ t: 'c', id, k, v: a[k] }); }
        else if (s.ev === 'child_changed' && JSON.stringify(a[k]) !== JSON.stringify(b[k])) out.push({ t: 'c', id, k, v: a[k] });
      }
      if (s.ev === 'child_removed') for (const k in b) if (!(k in a)) out.push({ t: 'c', id, k, v: b[k] });
    }
    if (out.length) this.send(ws, out);
  }
  send(ws, arr) { try { ws.send(JSON.stringify(arr)); } catch (e) {} }

  /* ---------- 메시지 ---------- */
  onMsg(ws, data) {
    if (typeof data !== 'string' || data.length > MAX_MSG) return;
    const now = Date.now(), r = this.rate.get(ws) || { t: now, n: 0 };
    if (now - r.t > 1000) { r.t = now; r.n = 0; }
    if (++r.n > RATE) return;
    this.rate.set(ws, r);
    let ops; try { ops = JSON.parse(data); } catch (e) { return; }
    if (!Array.isArray(ops)) return;
    for (const o of ops.slice(0, 40)) this.op(ws, o, now);
  }
  op(ws, o, now) {
    if (!o || typeof o !== 'object') return;
    const segs = split(o.p);
    if (o.o !== 'unsub' && (!segs.length || !TOP.has(segs[0]) || segs.length > 6)) return;
    const subs = this.subs.get(ws);
    switch (o.o) {
      case 'set': this.write(segs, this.stamp(clone(o.v), now)); break;
      case 'upd': if (isObj(o.v)) for (const k in o.v) this.write(segs.concat(split(k)), this.stamp(clone(o.v[k]), now)); break;
      case 'rm': this.write(segs, null); break;
      case 'once': this.send(ws, [{ t: 'o', id: o.id, v: this.get(segs) }]); break;
      case 'sub': {
        if (!subs || subs.size > 80) return;
        const s = { p: segs.join('/'), ev: o.ev, lim: o.lim | 0 };
        subs.set(o.id, s);
        const cur = this.get(segs);
        if (s.ev === 'value') this.send(ws, [{ t: 'v', id: o.id, v: cur }]);
        else if (s.ev === 'child_added' && isObj(cur)) {
          let keys = Object.keys(cur).sort(); if (s.lim) keys = keys.slice(-s.lim);
          if (keys.length) this.send(ws, keys.map(k => ({ t: 'c', id: o.id, k, v: cur[k] })));
        }
        break;
      }
      case 'unsub': if (subs) subs.delete(o.id); break;
      case 'od': { const set = this.od.get(ws); if (!set) return; const p = segs.join('/'); if (o.a === 'cancel') set.delete(p); else set.add(p); break; }
    }
  }
  onClose(ws) {
    if (!this.subs.has(ws)) return;
    const set = this.od.get(ws) || new Set();
    this.subs.delete(ws); this.od.delete(ws); this.rate.delete(ws);
    for (const p of set) this.write(split(p), null);
  }
}
