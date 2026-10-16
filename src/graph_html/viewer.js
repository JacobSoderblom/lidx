// lidx graph viewer. Vanilla JS on a 2D canvas; no dependencies and no
// network access. Reads the document embedded in #lidx-graph-data (schema
// in src/graph_html/doc.rs).
(function () {
  'use strict';

  // ------------------------------------------------------------------ data
  var raw = JSON.parse(document.getElementById('lidx-graph-data').textContent);
  var S = raw.s;
  var META = raw.meta;
  var F = META.flags;
  function str(i) { return i >= 0 ? S[i] : null; }

  var N = raw.nodes.map(function (r, i) {
    return {
      i: i, name: S[r[0]], qual: S[r[1]], kind: S[r[2]], lang: str(r[3]), file: str(r[4]),
      mod: S[r[5]], line: r[6], flags: r[7], sig: str(r[8]), doc: str(r[9]), size: r[10],
      out: [], inn: []
    };
  });
  var E = raw.edges.map(function (r, j) {
    return {
      j: j, s: r[0], t: r[1], kind: S[r[2]], w: r[3], flags: r[4], evLine: r[5],
      ev: str(r[6]), evFile: str(r[7]), res: str(r[8])
    };
  });
  E.forEach(function (e) { N[e.s].out.push(e); N[e.t].inn.push(e); });
  var AGGREGATED = META.level !== 'symbol';

  function has(n, f) { return (n.flags & f) !== 0; }
  // Legend/filter key: the language, or what a language-less node is.
  function langKey(n) {
    if (n.lang) return n.lang;
    if (has(n, F.endpoint)) return '(bridge endpoint)';
    if (has(n, F.unresolved)) return '(unresolved)';
    return '(none)';
  }
  function family(kind) {
    if (kind.indexOf('CHANNEL_') === 0) return 'channel';
    if (kind.indexOf('RPC_') === 0) return 'rpc';
    if (kind.indexOf('HTTP_') === 0) return 'http';
    if (kind.indexOf('CONFIG_') === 0) return 'config';
    if (kind === 'CALLS' || kind === 'DISPATCH') return 'calls';
    if (kind === 'IMPORTS' || kind === 'IMPORTS_FILE' || kind === 'MODULE_EXPORT') return 'imports';
    if (kind === 'EXTENDS' || kind === 'IMPLEMENTS' || kind === 'INHERITS') return 'inherit';
    if (kind === 'USES' || kind === 'REFERENCES') return 'uses';
    if (kind === 'XREF') return 'xref';
    return 'other';
  }
  var BRIDGE_FAMILIES = { channel: 1, rpc: 1, http: 1, config: 1 };
  // Pseudo-groups: their members belong next to their users, not together.
  var LOOSE_GROUPS = { '(external)': 1, '(bridges)': 1, '(unresolved)': 1 };
  var FAMILY_ORDER = ['calls', 'imports', 'inherit', 'uses', 'xref', 'other', 'channel', 'rpc', 'http', 'config'];
  var RECEIVING_VERB = {
    CHANNEL_SUBSCRIBE: 'subscribes to', RPC_IMPL: 'implements', HTTP_ROUTE: 'serves', CONFIG_READ: 'reads'
  };

  var LANG_COLORS = {
    rust: '#d0773f', python: '#4c7fd0', typescript: '#2a9fbf', tsx: '#23a8a0',
    javascript: '#c9a400', go: '#16a37f', csharp: '#8f5bd6', sql: '#c2457a', postgres: '#c2457a',
    tsql: '#c2457a', proto: '#7f8f2a', yaml: '#d64545', bicep: '#5b6ee1', markdown: '#8c8c8c',
    lua: '#3b4fb8', external: '#9aa0a6'
  };
  var EXTRA_COLORS = ['#e67700', '#0ca678', '#ae3ec9', '#1971c2', '#f08c00', '#c92a2a', '#5c940d'];
  var extraLang = {};
  function langColor(lang) {
    if (!lang) return '#9aa0a6';
    if (LANG_COLORS[lang]) return LANG_COLORS[lang];
    if (!extraLang[lang]) {
      extraLang[lang] = EXTRA_COLORS[Object.keys(extraLang).length % EXTRA_COLORS.length];
    }
    return extraLang[lang];
  }

  // Seeded PRNG so layouts are identical across reloads.
  function hash(s) {
    var h = 2166136261;
    for (var i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); }
    return h >>> 0;
  }
  function rng(seed) {
    var a = seed >>> 0;
    return function () {
      a = (a + 0x6d2b79f5) >>> 0;
      var t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
  }
  var jiggle = rng(7);

  // ----------------------------------------------------------------- state
  var kindCounts = {};
  E.forEach(function (e) { kindCounts[e.kind] = (kindCounts[e.kind] || 0) + 1; });
  var langCounts = {};
  N.forEach(function (n) { var l = langKey(n); langCounts[l] = (langCounts[l] || 0) + 1; });
  var SEEDED = N.some(function (n) { return has(n, F.seed); });
  // A one-way walk from seeds reads best as layers with the seed first.
  var TRACE = !AGGREGATED && SEEDED && (META.direction === 'up' || META.direction === 'down');

  var st = {
    layout: TRACE ? 'layered' : 'force',
    group: 'tree',
    expanded: new Set(),
    depth: Infinity,
    kindOn: {},
    langOn: {},
    showExternal: true,
    showTests: true,
    showNonCode: false,
    showLabels: true,
    revealed: new Set(),
    selected: null,
    hops: 1,
    hoverNode: null,
    hoverEdge: null,
    view: { k: 1, x: 0, y: 0 },
    userMoved: false
  };
  Object.keys(kindCounts).forEach(function (k) { st.kindOn[k] = true; });
  Object.keys(langCounts).forEach(function (l) { st.langOn[l] = true; });

  // Folder hierarchy: every leaf's chain of folder prefixes (`syft/`,
  // `syft/pkg/`, ...) and, for a symbol, its file. A leaf is drawn inside the
  // first prefix of its chain that is not expanded.
  function chainOf(n) {
    if (n.mod.charAt(0) === '(') return [n.mod];
    var chain = [], acc = '';
    var moduleLeaf = AGGREGATED && n.kind === 'module';
    var dirs = moduleLeaf ? n.qual.split('/').filter(Boolean)
      : n.file ? n.file.split('/').slice(0, -1) : n.mod.split('/').filter(Boolean);
    dirs.forEach(function (d) { if (d === '.') return; acc += d + '/'; chain.push(acc); });
    if (moduleLeaf) chain.pop();
    if (!chain.length && !moduleLeaf) chain.push('./');
    if (!AGGREGATED && n.file) chain.push(n.file);
    return chain;
  }
  N.forEach(function (n) { n.chain = chainOf(n); });
  var MAX_DEPTH = N.reduce(function (m, n) { return Math.max(m, n.chain.length); }, 1);
  var PREFIX_DEPTH = new Map();
  N.forEach(function (n) {
    n.chain.forEach(function (p, i) {
      if (!PREFIX_DEPTH.has(p) || PREFIX_DEPTH.get(p) > i + 1) PREFIX_DEPTH.set(p, i + 1);
    });
  });
  var ALL_PREFIXES = Array.from(PREFIX_DEPTH.keys()).sort();
  function prefixDepth(p) { return PREFIX_DEPTH.get(p) || 1; }

  // The cluster a leaf is drawn in, or null when it is drawn itself.
  function groupOf(n) {
    if (st.group === 'none') return null;
    for (var i = 0; i < n.chain.length; i++) if (!st.expanded.has(n.chain[i])) return n.chain[i];
    return null;
  }
  // The folder whose outline a drawn leaf sits in.
  function hullOf(n) {
    if (st.group === 'none' || !n.chain.length) return null;
    var c = n.chain;
    var last = c[c.length - 1];
    if (last.charAt(last.length - 1) !== '/' && last.charAt(0) !== '(' && c.length > 1) return c[c.length - 2];
    return last;
  }
  function parentOf(p) {
    if (p.charAt(0) === '(' || p === './') return null;
    var isDir = p.charAt(p.length - 1) === '/';
    var t = isDir ? p.slice(0, -1) : p;
    var i = t.lastIndexOf('/');
    return i < 0 ? null : t.slice(0, i + 1);
  }

  // A folder with a single sub-folder and nothing else opens straight
  // through to it, so drilling down never shows a one-box level.
  function compress() {
    for (var guard = 0; guard < 64; guard++) {
      var kids = new Map();
      N.forEach(function (n) {
        if (!leafVisible(n)) return;
        var c = n.chain;
        for (var i = 0; i < c.length; i++) {
          if (!st.expanded.has(c[i])) {
            var s = kids.get(c[i]);
            if (!s) { s = new Set(); kids.set(c[i], s); }
            s.add(i + 1 < c.length ? c[i + 1] : '#' + n.i);
            return;
          }
        }
      });
      var changed = false;
      kids.forEach(function (s, p) {
        var only = s.size === 1 ? s.values().next().value : null;
        if (only !== null && only.charAt(0) !== '#') { st.expanded.add(p); changed = true; }
      });
      if (!changed) return;
    }
  }
  function setDepth(d) {
    st.depth = d;
    st.expanded = new Set(ALL_PREFIXES.filter(function (p) {
      return p.charAt(0) !== '(' && prefixDepth(p) < d;
    }));
    if (d === Infinity) ALL_PREFIXES.forEach(function (p) { st.expanded.add(p); });
    compress();
  }
  function expandPrefix(p) { st.expanded.add(p); compress(); }
  function collapsePrefix(p) {
    st.expanded.delete(p);
    Array.from(st.expanded).forEach(function (q) { if (q !== p && q.indexOf(p) === 0) st.expanded.delete(q); });
    st.depth = NaN;
  }

  // ------------------------------------------------------------------ view
  // The view graph is what is drawn: visible leaves, plus one node per
  // collapsed cluster, with edges aggregated between them.
  var V = { nodes: [], edges: [], links: [], byId: new Map(), leafTo: new Int32Array(N.length) };
  var pos = new Map(); // node id -> {x, y}, survives rebuilds

  function leafVisible(n) {
    if (has(n, F.scope_out) && !st.showNonCode && !st.revealed.has(n.i)) return false;
    if (has(n, F.halo) && !st.revealed.has(n.i) && !(has(n, F.scope_out) && st.showNonCode)) return false;
    if (has(n, F.external | F.unresolved) && !st.showExternal) return false;
    if (has(n, F.test) && !st.showTests) return false;
    if (st.langOn[langKey(n)] === false) return false;
    return true;
  }

  function nodeRadius(v) {
    if (v.cluster !== null) return Math.min(34, 9 + Math.sqrt(v.members.length) * 2.2);
    var n = v.leaf;
    if (AGGREGATED) return Math.min(22, 4 + Math.sqrt(Math.max(1, n.size)) * 0.9);
    return Math.min(11, 4 + Math.sqrt(v.deg) * 0.9);
  }

  function shortGroup(g) {
    if (g.charAt(g.length - 1) !== '/') return g.split('/').pop();
    var t = g.replace(/\/$/, '');
    var parts = t.split('/');
    return parts.length > 2 ? '…/' + parts.slice(-2).join('/') + '/' : g;
  }

  // Code kinds between one pair of nodes merge into one weighted line; each
  // bridge family keeps its own, so cross-process flow stays visible.
  function edgeBucket(kind) {
    var f = family(kind);
    return BRIDGE_FAMILIES[f] ? f : 'code';
  }
  function kindsText(e) {
    var ks = Object.keys(e.kinds).sort(function (a, b) { return e.kinds[b] - e.kinds[a]; });
    return ks.map(function (k) { return k + (e.kinds[k] > 1 || ks.length > 1 ? ' ×' + e.kinds[k] : ''); }).join(', ');
  }

  function rebuild() {
    var nodes = [];
    var byId = new Map();
    var leafTo = new Int32Array(N.length).fill(-1);
    var visible = [];
    var groupCount = new Map();
    N.forEach(function (n) {
      if (!leafVisible(n)) return;
      var g = groupOf(n);
      visible.push([n, g]);
      if (g !== null) groupCount.set(g, (groupCount.get(g) || 0) + 1);
    });
    var clusters = new Map();
    visible.forEach(function (it) {
      var n = it[0], g = it[1];
      if (g !== null && groupCount.get(g) > 1) {
        var c = clusters.get(g);
        if (!c) {
          c = { id: 'c:' + g, idx: nodes.length, leaf: null, cluster: g, members: [], deg: 0,
                label: shortGroup(g), group: parentOf(g), x: 0, y: 0, vx: 0, vy: 0, r: 8, q: 1 };
          clusters.set(g, c);
          nodes.push(c);
          byId.set(c.id, c);
        }
        c.members.push(n);
        leafTo[n.i] = c.idx;
      } else {
        var v = { id: 'n' + n.i, idx: nodes.length, leaf: n, cluster: null, members: null, deg: 0,
                  label: n.name, group: g !== null ? parentOf(g) : hullOf(n), x: 0, y: 0, vx: 0, vy: 0, r: 5, q: 1 };
        nodes.push(v);
        byId.set(v.id, v);
        leafTo[n.i] = v.idx;
      }
    });
    var edges = [];
    var agg = new Map();
    E.forEach(function (e) {
      if (!st.kindOn[e.kind]) return;
      var a = leafTo[e.s], b = leafTo[e.t];
      if (a < 0 || b < 0 || a === b) return;
      var bucket = edgeBucket(e.kind);
      var key = a + '|' + b + '|' + bucket;
      var ve = agg.get(key);
      if (!ve) {
        ve = { a: a, b: b, kind: e.kind, fam: family(e.kind), kinds: {}, w: 0, n: 0, refs: [], halo: true };
        agg.set(key, ve);
        edges.push(ve);
      }
      ve.kinds[e.kind] = (ve.kinds[e.kind] || 0) + e.w;
      ve.w += e.w;
      ve.n += 1;
      if (!(e.flags & F.edge_halo)) ve.halo = false;
      if (ve.refs.length < 6) ve.refs.push(e);
    });
    edges.forEach(function (ve) {
      // Colour by the heaviest kind.
      var best = ve.kind;
      Object.keys(ve.kinds).forEach(function (k) { if (ve.kinds[k] > ve.kinds[best]) best = k; });
      ve.kind = best;
      ve.fam = family(best);
    });
    var pairSeen = new Set();
    edges.forEach(function (e) {
      var key = e.a < e.b ? e.a + '|' + e.b : e.b + '|' + e.a;
      if (pairSeen.has(key)) return;
      pairSeen.add(key);
      nodes[e.a].deg++; nodes[e.b].deg++;
    });
    nodes.forEach(function (v) {
      v.r = nodeRadius(v);
      v.q = 45 + v.r * 6;
      v.seed = v.leaf ? has(v.leaf, F.seed) : v.members.some(function (m) { return has(m, F.seed); });
    });
    // One spring per connected pair, however many kinds join it.
    var links = new Map();
    edges.forEach(function (e) {
      var key = e.a < e.b ? e.a + '|' + e.b : e.b + '|' + e.a;
      var l = links.get(key);
      if (!l) { l = { a: e.a, b: e.b, w: 0, bridge: true }; links.set(key, l); }
      l.w += e.w;
      if (!BRIDGE_FAMILIES[e.fam]) l.bridge = false;
    });
    V.links = Array.from(links.values());
    V.nodes = nodes; V.edges = edges; V.byId = byId; V.leafTo = leafTo;
    placeNodes();
    buildAdjacency();
    labelWidths.clear();
    updateEmpty();
    updateDepthLabel();
  }

  function buildAdjacency() {
    V.nodes.forEach(function (v) { v.outE = []; v.inE = []; });
    V.edges.forEach(function (e) { V.nodes[e.a].outE.push(e); V.nodes[e.b].inE.push(e); });
  }

  // Connected components (by any drawn edge), largest first.
  function components() {
    var n = V.nodes.length, parent = new Int32Array(n);
    for (var i = 0; i < n; i++) parent[i] = i;
    function find(x) { while (parent[x] !== x) { parent[x] = parent[parent[x]]; x = parent[x]; } return x; }
    V.links.forEach(function (l) { var a = find(l.a), b = find(l.b); if (a !== b) parent[a] = b; });
    var groups = new Map();
    for (i = 0; i < n; i++) {
      var r = find(i);
      var g = groups.get(r);
      if (!g) { g = []; groups.set(r, g); }
      g.push(i);
    }
    return Array.from(groups.values()).sort(function (a, b) {
      var sa = a.some(function (i) { return V.nodes[i].seed; }) ? 1 : 0;
      var sb = b.some(function (i) { return V.nodes[i].seed; }) ? 1 : 0;
      return sb - sa || b.length - a.length || a[0] - b[0];
    });
  }

  // Positions: remembered ones first, then a cluster's member centroid, an
  // enclosing cluster's position, a neighbour's, or a seeded spiral spot.
  var firstPlacement = true;
  function ancestorPos(n) {
    for (var i = n.chain.length - 1; i >= 0; i--) {
      var p = pos.get('c:' + n.chain[i]);
      if (p) return p;
    }
    return null;
  }
  function placeNodes() {
    var groups = [];
    var groupIndex = new Map();
    V.nodes.forEach(function (v) {
      var g = v.group === null || v.group === undefined ? '' : v.group;
      if (!groupIndex.has(g)) { groupIndex.set(g, groups.length); groups.push(g); }
    });
    var sortedGroups = groups.slice().sort();
    var centre = new Map();
    var spread = Math.max(60, Math.sqrt(V.nodes.length) * 26);
    sortedGroups.forEach(function (g, i) {
      var a = i * 2.399963, r = spread * Math.sqrt(i / Math.max(1, sortedGroups.length)) * 1.6;
      centre.set(g, { x: Math.cos(a) * r, y: Math.sin(a) * r });
    });
    var sizes = new Map();
    V.nodes.forEach(function (v) { var g = v.group || ''; sizes.set(g, (sizes.get(g) || 0) + 1); });
    V.nodes.forEach(function (v) {
      var p = pos.get(v.id);
      if (p) { v.x = p.x; v.y = p.y; return; }
      var r = rng(hash(v.id));
      var member = v.cluster !== null ? v.members[0] : v.leaf;
      if (v.cluster !== null) {
        var sx = 0, sy = 0, c = 0;
        v.members.forEach(function (m) {
          var mp = pos.get('n' + m.i);
          if (mp) { sx += mp.x; sy += mp.y; c++; }
        });
        if (c) { v.x = sx / c; v.y = sy / c; return; }
      }
      var cp = ancestorPos(member);
      if (cp) {
        v.x = cp.x + (r() - 0.5) * 70; v.y = cp.y + (r() - 0.5) * 70;
        return;
      }
      if (v.cluster === null) {
        var anchor = neighbourWithPos(v.leaf);
        if (anchor && !firstPlacement) {
          var ang = r() * Math.PI * 2;
          v.x = anchor.x + Math.cos(ang) * 45; v.y = anchor.y + Math.sin(ang) * 45;
          return;
        }
      }
      var g = centre.get(v.group || '') || { x: 0, y: 0 };
      var rad = Math.sqrt(sizes.get(v.group || '') || 1) * 14;
      var a2 = r() * Math.PI * 2, d = Math.sqrt(r()) * rad;
      v.x = g.x + Math.cos(a2) * d; v.y = g.y + Math.sin(a2) * d;
    });
    firstPlacement = false;
    V.nodes.forEach(function (v) { pos.set(v.id, { x: v.x, y: v.y }); });
  }
  function neighbourWithPos(n) {
    var list = n.inn.concat(n.out);
    for (var k = 0; k < list.length; k++) {
      var e = list[k];
      var o = e.s === n.i ? e.t : e.s;
      var p = pos.get('n' + o);
      if (p) return p;
    }
    return null;
  }
  function savePositions() { V.nodes.forEach(function (v) { pos.set(v.id, { x: v.x, y: v.y }); }); }

  // ---------------------------------------------------------- force layout
  // d3-force style: Barnes-Hut charge, spring links, cluster cohesion and a
  // weak centre pull, with deterministic alpha decay.
  var sim = { alpha: 0, running: false, pinned: null };
  var THETA2 = 0.81;

  function quadNew(x0, y0, s) { return { x0: x0, y0: y0, s: s, m: 0, cx: 0, cy: 0, b: null, k: null }; }
  function quadInsert(q, v, depth) {
    if (q.k === null && q.b === null) {
      q.b = v; q.m = v.q; q.cx = v.x; q.cy = v.y;
      return;
    }
    var m = q.m + v.q;
    q.cx = (q.cx * q.m + v.x * v.q) / m;
    q.cy = (q.cy * q.m + v.y * v.q) / m;
    q.m = m;
    if (depth > 48) return; // coincident points: mass counted, body merged
    if (q.k === null) {
      q.k = [null, null, null, null];
      var old = q.b;
      q.b = null;
      quadPut(q, old, depth);
    }
    quadPut(q, v, depth);
  }
  function quadPut(q, v, depth) {
    var h = q.s / 2;
    var ix = v.x >= q.x0 + h ? 1 : 0, iy = v.y >= q.y0 + h ? 1 : 0;
    var i = ix + 2 * iy;
    var c = q.k[i];
    if (!c) c = q.k[i] = quadNew(q.x0 + ix * h, q.y0 + iy * h, h);
    quadInsert(c, v, depth + 1);
  }
  function repel(q, v, alpha) {
    if (q.m === 0 || q.b === v) return;
    var dx = q.cx - v.x, dy = q.cy - v.y;
    var d2 = dx * dx + dy * dy;
    if (q.k === null || q.s * q.s / THETA2 < d2) {
      if (d2 < 1e-6) { dx = (jiggle() - 0.5) * 1e-3; dy = (jiggle() - 0.5) * 1e-3; d2 = dx * dx + dy * dy; }
      if (d2 < 1) d2 = Math.sqrt(d2);
      var f = q.m * alpha / d2;
      v.vx -= dx * f; v.vy -= dy * f;
      return;
    }
    for (var i = 0; i < 4; i++) if (q.k[i]) repel(q.k[i], v, alpha);
  }

  function tick() {
    var ns = V.nodes, n = ns.length, alpha = sim.alpha;
    if (!n) return;
    var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity, i, v;
    for (i = 0; i < n; i++) {
      v = ns[i];
      if (v.x < x0) x0 = v.x; if (v.y < y0) y0 = v.y;
      if (v.x > x1) x1 = v.x; if (v.y > y1) y1 = v.y;
    }
    var size = Math.max(x1 - x0, y1 - y0) + 1;
    var root = quadNew(x0, y0, size);
    for (i = 0; i < n; i++) quadInsert(root, ns[i], 0);
    for (i = 0; i < n; i++) repel(root, ns[i], alpha);

    var es = V.links;
    for (i = 0; i < es.length; i++) {
      var e = es[i], s = ns[e.a], t = ns[e.b];
      var dx = t.x + t.vx - s.x - s.vx, dy = t.y + t.vy - s.y - s.vy;
      var l = Math.sqrt(dx * dx + dy * dy) || 1e-3;
      var dist = 30 + (s.r + t.r) * 1.6;
      var strength = 1 / Math.max(1, Math.min(s.deg, t.deg));
      if (e.bridge) strength *= 0.6;
      var f = (l - dist) / l * alpha * strength;
      dx *= f; dy *= f;
      var bias = s.deg / (s.deg + t.deg || 1);
      t.vx -= dx * bias; t.vy -= dy * bias;
      s.vx += dx * (1 - bias); s.vy += dy * (1 - bias);
    }

    if (st.group !== 'none') {
      var cent = new Map();
      for (i = 0; i < n; i++) {
        v = ns[i];
        if (v.group === null || v.group === undefined || LOOSE_GROUPS[v.group]) continue;
        var c = cent.get(v.group);
        if (!c) { c = { x: 0, y: 0, n: 0 }; cent.set(v.group, c); }
        c.x += v.x; c.y += v.y; c.n++;
      }
      for (i = 0; i < n; i++) {
        v = ns[i];
        if (v.group === null || v.group === undefined || LOOSE_GROUPS[v.group]) continue;
        var g = cent.get(v.group);
        if (g.n < 2) continue;
        v.vx += (g.x / g.n - v.x) * 0.12 * alpha;
        v.vy += (g.y / g.n - v.y) * 0.12 * alpha;
      }
    }
    // Seeds are pulled hard to the centre so a trace grows around them;
    // components that end up apart are packed beside the main one at the end.
    var gravity = 0.03 * alpha, seedGravity = 0.25 * alpha;
    for (i = 0; i < n; i++) {
      v = ns[i];
      var gr = v.seed ? seedGravity : gravity;
      v.vx -= v.x * gr; v.vy -= v.y * gr;
      if (v === sim.pinned) { v.vx = 0; v.vy = 0; continue; }
      v.vx *= 0.6; v.vy *= 0.6;
      v.x += v.vx; v.y += v.vy;
    }
    collide(ns);
    sim.alpha += (0 - sim.alpha) * 0.0228;
  }

  // Push overlapping nodes apart (uniform grid, so linear time).
  function collide(ns) {
    var maxR = 0, i;
    for (i = 0; i < ns.length; i++) if (ns[i].r > maxR) maxR = ns[i].r;
    var cell = 2 * maxR + 6;
    var grid = new Map();
    for (i = 0; i < ns.length; i++) {
      var v = ns[i];
      var key = Math.floor(v.x / cell) + ',' + Math.floor(v.y / cell);
      var l = grid.get(key);
      if (!l) { l = []; grid.set(key, l); }
      l.push(v);
    }
    for (i = 0; i < ns.length; i++) {
      var a = ns[i];
      var gx = Math.floor(a.x / cell), gy = Math.floor(a.y / cell);
      for (var dx = -1; dx <= 1; dx++) {
        for (var dy = -1; dy <= 1; dy++) {
          var l2 = grid.get((gx + dx) + ',' + (gy + dy));
          if (!l2) continue;
          for (var j = 0; j < l2.length; j++) {
            var b = l2[j];
            if (b.idx <= a.idx) continue;
            var x = b.x - a.x, y = b.y - a.y;
            var min = a.r + b.r + 4;
            var d2 = x * x + y * y;
            if (d2 >= min * min) continue;
            var d = Math.sqrt(d2) || 0.01;
            var push = (min - d) / d * 0.5;
            if (d2 === 0) { x = jiggle() - 0.5; y = jiggle() - 0.5; }
            var ra = b.r * b.r / (a.r * a.r + b.r * b.r);
            if (a !== sim.pinned) { a.x -= x * push * ra; a.y -= y * push * ra; }
            if (b !== sim.pinned) { b.x += x * push * (1 - ra); b.y += y * push * (1 - ra); }
          }
        }
      }
    }
  }

  function startForce(alpha) {
    if (st.layout !== 'force') return;
    sim.alpha = Math.max(sim.alpha, alpha);
    if (!sim.running) { sim.running = true; requestAnimationFrame(simFrame); }
  }
  function simFrame() {
    if (st.layout !== 'force') { sim.running = false; return; }
    var t0 = performance.now();
    do { tick(); } while (sim.alpha > 0.002 && performance.now() - t0 < 14);
    savePositions();
    if (!st.userMoved) fit(false);
    if (sim.alpha <= 0.002) {
      sim.running = false;
      if (window.lidxGraph && !window.lidxGraph.settledAt) window.lidxGraph.settledAt = Math.round(performance.now());
      if (!packComponents() && !st.userMoved) fit(false);
      setStatus();
    }
    draw();
    if (sim.running) requestAnimationFrame(simFrame);
  }

  // -------------------------------------------------------- layered layout
  // Left-to-right Sugiyama-style layout, one connected component at a time
  // (largest first, stacked; lone nodes in a grid below). Layers are hop
  // distance from the seeds in a one-way trace (the seed in the first
  // column, also for an upward walk, which is mirrored), and longest-path
  // layers otherwise; barycentre sweeps order each layer.
  function layered() {
    var ns = V.nodes, n = ns.length;
    var out = ns.map(function () { return []; });
    var inn = ns.map(function () { return []; });
    var seenPair = new Set();
    V.edges.forEach(function (e) {
      var key = e.a + '>' + e.b;
      if (seenPair.has(key)) return;
      seenPair.add(key);
      out[e.a].push(e.b);
      inn[e.b].push(e.a);
    });
    var colGap = AGGREGATED ? 240 : 270;
    var targets = new Array(n);
    var comps = components();
    var lone = comps.filter(function (c) { return c.length === 1; });
    var y = 0;
    comps.forEach(function (comp) {
      if (comp.length === 1) return;
      var t = layoutComponent(comp, out, inn, colGap);
      comp.forEach(function (i) { targets[i] = { x: t.pos[i].x, y: t.pos[i].y + y - t.y0 }; });
      y += t.y1 - t.y0 + 70;
    });
    // Lone nodes: a grid under everything else.
    var cols = Math.max(1, Math.ceil(Math.sqrt(lone.length * 3)));
    lone.forEach(function (c, k) {
      targets[c[0]] = { x: (k % cols) * colGap * 0.8, y: y + Math.floor(k / cols) * 34 };
    });
    animateTo(targets);
  }

  function layoutComponent(comp, out, inn, colGap) {
    var ns = V.nodes;
    var inComp = new Set(comp);
    var layer = new Map();
    var seeds = comp.filter(function (i) { return ns[i].seed; });
    var up = META.direction === 'up';
    if (TRACE && seeds.length) {
      // Hop distance from the seeds along the walk's direction.
      var next = up ? inn : out;
      var queue = seeds.slice();
      seeds.forEach(function (s) { layer.set(s, 0); });
      while (queue.length) {
        var u = queue.shift();
        next[u].forEach(function (w) {
          if (inComp.has(w) && !layer.has(w)) { layer.set(w, layer.get(u) + 1); queue.push(w); }
        });
      }
      // Whatever the walk did not reach (revealed halo, reverse edges) sits
      // one column past a placed neighbour.
      var changed = true;
      while (changed) {
        changed = false;
        comp.forEach(function (i) {
          if (layer.has(i)) return;
          var nb = out[i].concat(inn[i]).filter(function (w) { return layer.has(w); });
          if (nb.length) {
            layer.set(i, Math.min.apply(null, nb.map(function (w) { return layer.get(w); })) + 1);
            changed = true;
          }
        });
      }
      comp.forEach(function (i) { if (!layer.has(i)) layer.set(i, 0); });
    } else {
      longestPathLayers(comp, out, inn, inComp, layer);
    }
    var layers = [];
    comp.forEach(function (i) { var l = layer.get(i); (layers[l] = layers[l] || []).push(i); });
    for (var li = 0; li < layers.length; li++) if (!layers[li]) layers[li] = [];
    var at = new Map();
    layers.forEach(function (l) {
      l.sort(function (a, b) {
        var ga = ns[a].group || '', gb = ns[b].group || '';
        return (ns[b].seed ? 1 : 0) - (ns[a].seed ? 1 : 0) ||
          (ga < gb ? -1 : ga > gb ? 1 : (ns[a].label < ns[b].label ? -1 : 1));
      });
      l.forEach(function (v, k) { at.set(v, k); });
    });
    function sweep(l, nbrs) {
      var bc = new Map();
      l.forEach(function (v) {
        var s = 0, c = 0;
        nbrs[v].forEach(function (w) { if (inComp.has(w) && layer.get(w) !== layer.get(v)) { s += at.get(w); c++; } });
        bc.set(v, c ? s / c : at.get(v));
      });
      l.sort(function (a, b) { return bc.get(a) - bc.get(b) || at.get(a) - at.get(b); });
      l.forEach(function (v, k) { at.set(v, k); });
    }
    for (var it = 0; it < 6; it++) {
      for (li = 1; li < layers.length; li++) sweep(layers[li], up ? out : inn);
      for (li = layers.length - 2; li >= 0; li--) sweep(layers[li], up ? inn : out);
    }
    // A tall layer (a hub's callees) wraps into side-by-side sub-columns so
    // the trace stays readable at a zoom where labels show.
    var perCol = Math.max(14, Math.round(Math.sqrt(comp.length) * 1.6));
    var res = { pos: {}, y0: Infinity, y1: -Infinity };
    var x = 0;
    layers.forEach(function (l) {
      var cols = Math.max(1, Math.ceil(l.length / perCol));
      var rows = Math.ceil(l.length / cols);
      for (var cI = 0; cI < cols; cI++) {
        var col = l.slice(cI * rows, (cI + 1) * rows);
        var yy = 0;
        var ys = col.map(function (v) { var r = ns[v].r; yy += r + 9; var here = yy; yy += r + 9; return here; });
        var mid = yy / 2;
        col.forEach(function (v, j) {
          var p = { x: x + cI * colGap * 0.85, y: ys[j] - mid };
          res.pos[v] = p;
          res.y0 = Math.min(res.y0, p.y - ns[v].r);
          res.y1 = Math.max(res.y1, p.y + ns[v].r);
        });
      }
      x += (cols - 1) * colGap * 0.85 + colGap;
    });
    return res;
  }

  // Longest-path layers on the graph with DFS back edges dropped; sources
  // pulled next to their first target.
  function longestPathLayers(comp, out, inn, inComp, layer) {
    var ns = V.nodes;
    var order = comp.slice().sort(function (a, b) {
      var ra = ns[a].seed ? 0 : inn[a].length === 0 ? 1 : 2;
      var rb = ns[b].seed ? 0 : inn[b].length === 0 ? 1 : 2;
      return ra - rb || a - b;
    });
    var state = new Map();
    var dagOut = new Map();
    comp.forEach(function (i) { dagOut.set(i, []); });
    order.forEach(function (start) {
      if (state.get(start)) return;
      var stack = [[start, 0]];
      state.set(start, 1);
      while (stack.length) {
        var top = stack[stack.length - 1];
        var u = top[0];
        if (top[1] < out[u].length) {
          var w = out[u][top[1]++];
          if (!inComp.has(w) || state.get(w) === 1) continue;
          dagOut.get(u).push(w);
          if (!state.get(w)) { state.set(w, 1); stack.push([w, 0]); }
        } else {
          state.set(u, 2);
          stack.pop();
        }
      }
    });
    var indeg = new Map();
    comp.forEach(function (i) { indeg.set(i, 0); layer.set(i, 0); });
    dagOut.forEach(function (l) { l.forEach(function (w) { indeg.set(w, indeg.get(w) + 1); }); });
    var queue = comp.filter(function (i) { return !indeg.get(i); });
    var topo = [];
    while (queue.length) {
      var u = queue.shift();
      topo.push(u);
      dagOut.get(u).forEach(function (w) {
        if (layer.get(u) + 1 > layer.get(w)) layer.set(w, layer.get(u) + 1);
        indeg.set(w, indeg.get(w) - 1);
        if (!indeg.get(w)) queue.push(w);
      });
    }
    for (var t = topo.length - 1; t >= 0; t--) {
      var x = topo[t];
      if (!ns[x].seed && inn[x].length === 0 && dagOut.get(x).length) {
        var m = Infinity;
        dagOut.get(x).forEach(function (w) { if (layer.get(w) < m) m = layer.get(w); });
        layer.set(x, Math.max(0, m - 1));
      }
    }
  }

  // After a force layout settles: keep the main component in place and shelf-
  // pack every other component (and lone node) in a column beside it.
  function packComponents() {
    var comps = components();
    if (comps.length < 2) return false;
    var ns = V.nodes;
    function box(c) {
      var b = { x0: Infinity, y0: Infinity, x1: -Infinity, y1: -Infinity };
      c.forEach(function (i) {
        var v = ns[i];
        b.x0 = Math.min(b.x0, v.x - v.r); b.y0 = Math.min(b.y0, v.y - v.r);
        b.x1 = Math.max(b.x1, v.x + v.r + 60); b.y1 = Math.max(b.y1, v.y + v.r);
      });
      return b;
    }
    var main = box(comps[0]);
    var rest = comps.slice(1).map(function (c) { return { c: c, b: box(c) }; });
    var area = rest.reduce(function (s, r) { return s + (r.b.x1 - r.b.x0 + 30) * (r.b.y1 - r.b.y0 + 30); }, 0);
    var shelf = Math.max(260, Math.sqrt(area) * 1.15, (main.y1 - main.y0) ? area / (main.y1 - main.y0) : 0);
    var gap = 30;
    var x0 = main.x1 + 80, x = x0, y = main.y0, rowH = 0;
    var targets = ns.map(function (v) { return { x: v.x, y: v.y }; });
    rest.forEach(function (r) {
      var w = r.b.x1 - r.b.x0, h = r.b.y1 - r.b.y0;
      if (x > x0 && x + w > x0 + shelf) { x = x0; y += rowH + gap; rowH = 0; }
      var dx = x - r.b.x0, dy = y - r.b.y0;
      r.c.forEach(function (i) { targets[i] = { x: ns[i].x + dx, y: ns[i].y + dy }; });
      x += w + gap;
      rowH = Math.max(rowH, h);
    });
    animateTo(targets);
    return true;
  }

  var anim = null;
  function animateTo(targets) {
    var from = V.nodes.map(function (v) { return { x: v.x, y: v.y }; });
    var t0 = performance.now();
    var reduce = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    var dur = reduce ? 0 : 450;
    if (anim) cancelAnimationFrame(anim);
    function step(now) {
      var p = dur ? Math.min(1, (now - t0) / dur) : 1;
      var e = p < 0.5 ? 2 * p * p : 1 - Math.pow(-2 * p + 2, 2) / 2;
      V.nodes.forEach(function (v, i) {
        v.x = from[i].x + (targets[i].x - from[i].x) * e;
        v.y = from[i].y + (targets[i].y - from[i].y) * e;
      });
      if (!st.userMoved) fit(false);
      draw();
      if (p < 1) anim = requestAnimationFrame(step);
      else { anim = null; savePositions(); }
    }
    anim = requestAnimationFrame(step);
  }

  function relayout(alpha) {
    if (st.layout === 'force') startForce(alpha);
    else layered();
  }

  // ------------------------------------------------------------- rendering
  var canvas = document.getElementById('canvas');
  var ctx = canvas.getContext('2d');
  var stage = document.getElementById('stage');
  var W = 0, H = 0, DPR = 1;
  var theme = {};
  function readTheme() {
    var cs = getComputedStyle(document.documentElement);
    ['canvas', 'hull', 'hull-line', 'hull-label', 'label', 'label-halo', 'node-stroke', 'seed',
     'select', 'text-dim'].forEach(function (k) { theme[k] = cs.getPropertyValue('--' + k).trim(); });
    FAMILY_ORDER.forEach(function (f) { theme['k-' + f] = cs.getPropertyValue('--k-' + f).trim(); });
  }
  function famColor(f) { return theme['k-' + f] || theme['k-other']; }

  function resize() {
    var r = stage.getBoundingClientRect();
    DPR = window.devicePixelRatio || 1;
    W = Math.max(1, r.width); H = Math.max(1, r.height);
    canvas.width = Math.round(W * DPR);
    canvas.height = Math.round(H * DPR);
    draw();
  }

  function sx(x) { return x * st.view.k + st.view.x; }
  function sy(y) { return y * st.view.k + st.view.y; }
  function screenR(v) { return Math.max(2, v.r * st.view.k); }

  var hl = null; // Set of highlighted node idx, or null
  var hlEdges = null;
  function computeHighlight() {
    hl = null; hlEdges = null;
    var sel = selectedV();
    if (!sel) return;
    hl = new Map();
    hl.set(sel.idx, 0);
    var frontier = [sel.idx];
    for (var h = 1; h <= st.hops && frontier.length; h++) {
      var next = [];
      frontier.forEach(function (i) {
        var v = V.nodes[i];
        v.outE.forEach(function (e) { if (!hl.has(e.b)) { hl.set(e.b, h); next.push(e.b); } });
        v.inE.forEach(function (e) { if (!hl.has(e.a)) { hl.set(e.a, h); next.push(e.a); } });
      });
      frontier = next;
    }
    hlEdges = new Set();
    V.edges.forEach(function (e, k) { if (hl.has(e.a) && hl.has(e.b)) hlEdges.add(k); });
  }

  var drawQueued = false;
  function requestDraw() {
    if (drawQueued) return;
    drawQueued = true;
    requestAnimationFrame(function () { drawQueued = false; draw(); });
  }

  function edgeGeom(e) {
    var a = V.nodes[e.a], b = V.nodes[e.b];
    return { x1: sx(a.x), y1: sy(a.y), x2: sx(b.x), y2: sy(b.y), rb: screenR(b), ra: screenR(a) };
  }

  function draw(target) {
    var c = target || ctx;
    var w = target ? target.canvas.width / DPR : W, h = target ? target.canvas.height / DPR : H;
    c.setTransform(DPR, 0, 0, DPR, 0, 0);
    c.fillStyle = theme.canvas;
    c.fillRect(0, 0, w, h);
    if (!V.nodes.length) return; // the #empty overlay explains it
    var k = st.view.k;
    var pad = 40;
    function onScreen(x, y, r) { return x > -r - pad && y > -r - pad && x < w + r + pad && y < h + r + pad; }

    // Cluster hulls
    var hullLabels = st.layout === 'force' && st.group !== 'none' ? drawHulls(c, w, h) : [];

    // Edges, batched by family and emphasis
    var curved = st.layout === 'layered';
    var batches = new Map();
    var arrows = new Map();
    var showArrows = k > 0.35 && (V.edges.length < 6000 || hlEdges);
    var manyEdges = V.edges.length > 2500;
    V.edges.forEach(function (e, idx) {
      var g = edgeGeom(e);
      var minx = Math.min(g.x1, g.x2), maxx = Math.max(g.x1, g.x2);
      var miny = Math.min(g.y1, g.y2), maxy = Math.max(g.y1, g.y2);
      if (maxx < -pad || minx > w + pad || maxy < -pad || miny > h + pad) return;
      var emph = hl ? (hlEdges.has(idx) ? 2 : 0) : 1;
      if (st.hoverEdge === e) emph = 3;
      var key = e.fam + '|' + emph;
      var list = batches.get(key);
      if (!list) { list = []; batches.set(key, list); }
      list.push([e, g]);
      if ((showArrows && emph > 0) || emph >= 2) {
        var al = arrows.get(key);
        if (!al) { al = []; arrows.set(key, al); }
        al.push([e, g]);
      }
    });
    var keys = Array.from(batches.keys()).sort(function (a, b) {
      var ea = +a.split('|')[1], eb = +b.split('|')[1];
      return ea - eb || (BRIDGE_FAMILIES[a.split('|')[0]] ? 1 : 0) - (BRIDGE_FAMILIES[b.split('|')[0]] ? 1 : 0);
    });
    keys.forEach(function (key) {
      var parts = key.split('|'), fam = parts[0], emph = +parts[1];
      var bridge = !!BRIDGE_FAMILIES[fam];
      var list = batches.get(key);
      var alpha = emph === 0 ? 0.07 : emph === 1 ? (V.edges.length > 8000 ? 0.16 : manyEdges ? 0.28 : 0.5) : 0.95;
      if (bridge && emph === 1) alpha = 0.85;
      c.strokeStyle = famColor(fam);
      if (bridge && emph > 0) {
        // Faint wide underlay, then a dashed line: bridges read at any zoom.
        c.globalAlpha = alpha * 0.22;
        c.lineWidth = 7;
        c.setLineDash([]);
        strokeEdges(c, list, curved);
      }
      c.globalAlpha = alpha;
      c.setLineDash(bridge ? [7, 4] : []);
      c.lineWidth = bridge ? 2.2 : emph >= 2 ? 1.6 : 1;
      if (list.length < 4000) {
        list.forEach(function (it) {
          c.lineWidth = edgeWidth(it[0], emph);
          c.beginPath();
          pathEdge(c, it[1], curved);
          c.stroke();
        });
      } else {
        strokeEdges(c, list, curved);
      }
      c.setLineDash([]);
    });
    c.globalAlpha = 1;
    arrows.forEach(function (list, key) {
      var parts = key.split('|'), fam = parts[0], emph = +parts[1];
      c.fillStyle = famColor(fam);
      c.globalAlpha = emph === 0 ? 0.1 : emph === 1 ? 0.7 : 1;
      c.beginPath();
      list.forEach(function (it) { pathArrow(c, it[1], curved); });
      c.fill();
    });
    c.globalAlpha = 1;

    // Edge kind labels for the hovered edge and, zoomed in, highlighted ones
    if (hlEdges && k > 1.4 && hlEdges.size <= 30) {
      c.font = '10.5px ' + MONO;
      c.textAlign = 'center';
      c.textBaseline = 'middle';
      hlEdges.forEach(function (idx) { edgeLabel(c, V.edges[idx]); });
    }

    // Nodes
    var labelled = [];
    V.nodes.forEach(function (v) {
      var x = sx(v.x), y = sy(v.y), r = screenR(v);
      if (!onScreen(x, y, r)) return;
      var dim = hl && !hl.has(v.idx);
      c.globalAlpha = dim ? 0.16 : 1;
      drawNode(c, v, x, y, r);
      if (!dim) labelled.push(v);
    });
    c.globalAlpha = 1;
    drawLabels(c, labelled, w, h, hullLabels);
  }

  // Weight drives thickness: an aggregated edge between two folders reads
  // as heavy as the calls it stands for.
  function edgeWidth(e, emph) {
    var base = BRIDGE_FAMILIES[e.fam] ? 2.2 : emph >= 2 ? 1.5 : 1;
    return Math.min(10, base * (1 + Math.log2(Math.max(1, e.w)) * 0.75));
  }

  var MONO = 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace';
  var SANS = 'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif';

  function pathEdge(c, g, curved) {
    c.moveTo(g.x1, g.y1);
    if (curved) {
      var dx = (g.x2 - g.x1) * 0.5;
      if (Math.abs(dx) < 30) dx = dx < 0 ? -30 : 30;
      c.bezierCurveTo(g.x1 + dx, g.y1, g.x2 - dx, g.y2, g.x2, g.y2);
    } else {
      c.lineTo(g.x2, g.y2);
    }
  }
  function strokeEdges(c, list, curved) {
    c.beginPath();
    list.forEach(function (it) { pathEdge(c, it[1], curved); });
    c.stroke();
  }
  function endTangent(g, curved) {
    var dx, dy;
    if (curved) {
      // A bezier's end tangent is P3 - P2, here (dx, 0).
      dx = g.x2 - g.x1 < 0 ? -1 : 1; dy = 0;
    } else {
      dx = g.x2 - g.x1; dy = g.y2 - g.y1;
    }
    var l = Math.sqrt(dx * dx + dy * dy) || 1;
    return [dx / l, dy / l];
  }
  function pathArrow(c, g, curved) {
    var t = endTangent(g, curved);
    var size = 7;
    var bx = g.x2 - t[0] * (g.rb + 1.5), by = g.y2 - t[1] * (g.rb + 1.5);
    var px = -t[1], py = t[0];
    c.moveTo(bx, by);
    c.lineTo(bx - t[0] * size + px * size * 0.45, by - t[1] * size + py * size * 0.45);
    c.lineTo(bx - t[0] * size - px * size * 0.45, by - t[1] * size - py * size * 0.45);
    c.closePath();
  }
  // Also the bezier's t = 0.5 point: its control points mirror each other.
  function edgeMid(g) { return [(g.x1 + g.x2) / 2, (g.y1 + g.y2) / 2]; }
  function edgeLabel(c, e) {
    var g = edgeGeom(e);
    var m = edgeMid(g);
    var extra = Object.keys(e.kinds).length - 1;
    var text = e.kind + (e.kinds[e.kind] > 1 ? ' ×' + e.kinds[e.kind] : '') + (extra > 0 ? ' +' + extra : '');
    c.lineWidth = 3;
    c.strokeStyle = theme['label-halo'];
    c.strokeText(text, m[0], m[1]);
    c.fillStyle = famColor(e.fam);
    c.fillText(text, m[0], m[1]);
  }

  function drawNode(c, v, x, y, r) {
    var n = v.leaf;
    var fill = v.cluster !== null ? clusterColor(v) : langColor(n.lang);
    c.lineWidth = 1.5;
    c.strokeStyle = theme['node-stroke'];
    c.beginPath();
    if (v.cluster !== null) {
      roundRect(c, x - r, y - r * 0.75, r * 2, r * 1.5, Math.min(8, r * 0.4));
      c.fillStyle = fill;
      c.globalAlpha *= 0.9;
      c.fill();
      c.globalAlpha /= 0.9;
      c.stroke();
    } else if (has(n, F.endpoint)) {
      c.moveTo(x, y - r * 1.25); c.lineTo(x + r * 1.25, y); c.lineTo(x, y + r * 1.25); c.lineTo(x - r * 1.25, y);
      c.closePath();
      c.fillStyle = theme['k-' + endpointFamily(n)] || theme['k-other'];
      c.fill();
      c.stroke();
    } else if (has(n, F.external | F.unresolved)) {
      c.arc(x, y, r, 0, Math.PI * 2);
      c.fillStyle = theme.canvas;
      c.fill();
      c.setLineDash(has(n, F.unresolved) ? [2, 2] : [3, 2]);
      c.strokeStyle = theme['text-dim'];
      c.stroke();
      c.setLineDash([]);
    } else if (AGGREGATED && (n.kind === 'file' || n.kind === 'module')) {
      roundRect(c, x - r, y - r, r * 2, r * 2, Math.min(5, r * 0.35));
      c.fillStyle = fill;
      c.fill();
      c.stroke();
    } else {
      c.arc(x, y, r, 0, Math.PI * 2);
      c.fillStyle = fill;
      c.fill();
      c.stroke();
      if (has(n, F.test) && r > 3) {
        c.beginPath();
        c.arc(x, y, r * 0.42, 0, Math.PI * 2);
        c.fillStyle = theme.canvas;
        c.fill();
      }
    }
    if (n && has(n, F.halo)) {
      c.beginPath();
      c.arc(x, y, r + 2.5, 0, Math.PI * 2);
      c.setLineDash([2, 3]);
      c.strokeStyle = theme['text-dim'];
      c.lineWidth = 1;
      c.stroke();
      c.setLineDash([]);
    }
    if (v.seed) {
      c.beginPath();
      c.arc(x, y, r + 3.5, 0, Math.PI * 2);
      c.strokeStyle = theme.seed;
      c.lineWidth = 2.2;
      c.stroke();
    }
    if (st.selected === v.id || st.hoverNode === v) {
      c.beginPath();
      c.arc(x, y, r + (st.selected === v.id ? 7 : 5), 0, Math.PI * 2);
      c.strokeStyle = theme.select;
      c.lineWidth = st.selected === v.id ? 2.5 : 1.5;
      c.stroke();
    }
    if (v.cluster !== null && r > 9) {
      c.fillStyle = '#fff';
      c.font = '600 ' + Math.min(13, r * 0.6).toFixed(1) + 'px ' + SANS;
      c.textAlign = 'center';
      c.textBaseline = 'middle';
      c.fillText(String(v.members.length), x, y + 0.5);
    }
  }
  function endpointFamily(n) {
    if (n.kind === 'message_bus') return 'channel';
    if (n.kind === 'grpc') return 'rpc';
    if (n.kind === 'http') return 'http';
    if (n.kind === 'config') return 'config';
    return 'other';
  }
  function clusterColor(v) {
    var counts = {};
    var best = null, bestN = 0;
    v.members.forEach(function (m) {
      var l = m.lang || '';
      counts[l] = (counts[l] || 0) + 1;
      if (counts[l] > bestN) { bestN = counts[l]; best = m.lang; }
    });
    return langColor(best);
  }
  function roundRect(c, x, y, w, h, r) {
    c.moveTo(x + r, y);
    c.lineTo(x + w - r, y); c.quadraticCurveTo(x + w, y, x + w, y + r);
    c.lineTo(x + w, y + h - r); c.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
    c.lineTo(x + r, y + h); c.quadraticCurveTo(x, y + h, x, y + h - r);
    c.lineTo(x, y + r); c.quadraticCurveTo(x, y, x + r, y);
    c.closePath();
  }

  function convexHull(pts) {
    pts.sort(function (a, b) { return a[0] - b[0] || a[1] - b[1]; });
    function cross(o, a, b) { return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]); }
    var lower = [], upper = [], i;
    for (i = 0; i < pts.length; i++) {
      while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], pts[i]) <= 0) lower.pop();
      lower.push(pts[i]);
    }
    for (i = pts.length - 1; i >= 0; i--) {
      while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], pts[i]) <= 0) upper.pop();
      upper.push(pts[i]);
    }
    upper.pop(); lower.pop();
    return lower.concat(upper);
  }
  function drawHulls(c, w, h) {
    var groups = new Map();
    V.nodes.forEach(function (v) {
      if (v.group === null || v.group === undefined || LOOSE_GROUPS[v.group]) return;
      var l = groups.get(v.group);
      if (!l) { l = []; groups.set(v.group, l); }
      l.push(v);
    });
    var k = st.view.k;
    var labels = [];
    c.lineJoin = 'round';
    groups.forEach(function (members, g) {
      if (members.length < 2) return;
      var pts = members.map(function (v) { return [sx(v.x), sy(v.y)]; });
      var minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
      pts.forEach(function (p) {
        if (p[0] < minx) minx = p[0]; if (p[1] < miny) miny = p[1];
        if (p[0] > maxx) maxx = p[0]; if (p[1] > maxy) maxy = p[1];
      });
      if (maxx < -50 || minx > w + 50 || maxy < -50 || miny > h + 50) return;
      // A group the layout could not keep together would only paint over
      // everything else.
      var spread = Math.hypot(maxx - minx, maxy - miny) / k;
      if (spread > 120 * Math.sqrt(members.length) + 120) return;
      var hull = convexHull(pts);
      var padPx = Math.max(10, 16 * k);
      var dim = hl && !members.some(function (v) { return hl.has(v.idx); });
      c.globalAlpha = dim ? 0.4 : 1;
      c.beginPath();
      hull.forEach(function (p, i) { if (i) c.lineTo(p[0], p[1]); else c.moveTo(p[0], p[1]); });
      c.closePath();
      c.lineWidth = padPx * 2;
      c.strokeStyle = theme.hull;
      c.stroke();
      c.fillStyle = theme.hull;
      c.fill();
      if (!dim) {
        labels.push({ id: 'h:' + g, text: shortGroup(g), x: minx - padPx + 4, y: miny - padPx - 4,
                      prio: members.length * 3, hull: true });
      }
    });
    c.globalAlpha = 1;
    c.lineJoin = 'miter';
    return labels;
  }

  var labelWidths = new Map();
  function labelText(v) {
    if (v.cluster !== null) return v.label;
    return v.label.length > 48 ? v.label.slice(0, 47) + '…' : v.label;
  }
  function drawLabels(c, list, w, h, hullLabels) {
    if (!st.showLabels) return;
    var k = st.view.k;
    // Always labelled; the selection's neighbours only get first pick of
    // the free space.
    var important = function (v) {
      return st.selected === v.id || st.hoverNode === v || (v.leaf && has(v.leaf, F.seed));
    };
    var near = function (v) { return hl && hl.has(v.idx) && hl.get(v.idx) <= 1; };
    var lodOk = function (v) {
      if (v.cluster !== null) return true;
      // Layered rows are ~26 units apart: labels fit once a row is ~13px.
      if (st.layout === 'layered') return k >= 0.5;
      return screenR(v) >= 5.5 && k >= 0.55;
    };
    // One collision pass over node, cluster and folder labels: a label is
    // skipped when it would overlap one already drawn, best-connected first.
    var cand = list.filter(function (v) { return important(v) || near(v) || lodOk(v); }).map(function (v) {
      var prio = important(v) ? 1e9 : near(v) ? 1e7 + v.deg : v.cluster !== null ? v.members.length * 4 + v.deg : v.deg * 2 + v.r;
      return { v: v, prio: prio };
    });
    (hullLabels || []).forEach(function (hl2) { cand.push({ hull: hl2, prio: hl2.prio }); });
    cand.sort(function (a, b) { return b.prio - a.prio; });
    if (cand.length > 1500) cand.length = 1500;
    c.font = '12px ' + SANS;
    c.textBaseline = 'middle';
    c.textAlign = 'left';
    var cell = 16;
    var grid = new Set();
    function free(x0, y0, x1, y1) {
      for (var gx = Math.floor(x0 / cell); gx <= Math.floor(x1 / cell); gx++) {
        for (var gy = Math.floor(y0 / cell); gy <= Math.floor(y1 / cell); gy++) {
          if (grid.has(gx + ',' + gy)) return false;
        }
      }
      return true;
    }
    function take(x0, y0, x1, y1) {
      for (var gx = Math.floor(x0 / cell); gx <= Math.floor(x1 / cell); gx++) {
        for (var gy = Math.floor(y0 / cell); gy <= Math.floor(y1 / cell); gy++) grid.add(gx + ',' + gy);
      }
    }
    c.lineWidth = 3.5;
    c.lineJoin = 'round';
    cand.forEach(function (item) {
      var v = item.v, hullItem = item.hull;
      var text, id, x, y;
      if (hullItem) { text = hullItem.text; id = hullItem.id; x = hullItem.x; y = hullItem.y; }
      else { text = labelText(v); id = v.id; }
      c.font = hullItem ? '600 11px ' + SANS : '12px ' + SANS;
      var tw = labelWidths.get(id);
      if (tw === undefined) { tw = c.measureText(text).width; labelWidths.set(id, tw); }
      if (!hullItem) {
        x = sx(v.x) + screenR(v) + 5; y = sy(v.y);
        if (v.cluster !== null) { x = sx(v.x) - tw / 2; y = sy(v.y) + screenR(v) * 0.75 + 10; }
      }
      if (x > w || x + tw < 0 || y < -10 || y > h + 10) return;
      var imp = !hullItem && important(v);
      if (!imp && !free(x, y - 7, x + tw, y + 7)) return;
      take(x, y - 7, x + tw, y + 7);
      c.strokeStyle = theme['label-halo'];
      c.strokeText(text, x, y);
      c.fillStyle = hullItem ? theme['hull-label'] : theme.label;
      c.fillText(text, x, y);
    });
    c.lineJoin = 'miter';
  }

  // ------------------------------------------------------------- hit tests
  function nodeAt(px, py) {
    var best = null, bestD = Infinity;
    for (var i = V.nodes.length - 1; i >= 0; i--) {
      var v = V.nodes[i];
      var dx = sx(v.x) - px, dy = sy(v.y) - py;
      var r = screenR(v) + 4;
      var d = dx * dx + dy * dy;
      if (d <= r * r && d < bestD) { best = v; bestD = d; }
    }
    return best;
  }
  function segDist2(px, py, x1, y1, x2, y2) {
    var dx = x2 - x1, dy = y2 - y1;
    var l = dx * dx + dy * dy;
    var t = l ? ((px - x1) * dx + (py - y1) * dy) / l : 0;
    t = Math.max(0, Math.min(1, t));
    var x = x1 + t * dx - px, y = y1 + t * dy - py;
    return x * x + y * y;
  }
  function edgeAt(px, py) {
    var best = null, bestD = 36;
    var curved = st.layout === 'layered';
    for (var i = 0; i < V.edges.length; i++) {
      var e = V.edges[i];
      if (hlEdges && !hlEdges.has(i)) continue;
      var g = edgeGeom(e);
      if (px < Math.min(g.x1, g.x2) - 6 || px > Math.max(g.x1, g.x2) + 6) continue;
      if (!curved && (py < Math.min(g.y1, g.y2) - 6 || py > Math.max(g.y1, g.y2) + 6)) continue;
      var d;
      if (curved) {
        d = Infinity;
        var dx = (g.x2 - g.x1) * 0.5;
        if (Math.abs(dx) < 30) dx = dx < 0 ? -30 : 30;
        var lx = g.x1, ly = g.y1;
        for (var s = 1; s <= 12; s++) {
          var t = s / 12, u = 1 - t;
          var x = u * u * u * g.x1 + 3 * u * u * t * (g.x1 + dx) + 3 * u * t * t * (g.x2 - dx) + t * t * t * g.x2;
          var y = u * u * u * g.y1 + 3 * u * u * t * g.y1 + 3 * u * t * t * g.y2 + t * t * t * g.y2;
          d = Math.min(d, segDist2(px, py, lx, ly, x, y));
          lx = x; ly = y;
        }
      } else {
        d = segDist2(px, py, g.x1, g.y1, g.x2, g.y2);
      }
      if (d < bestD) { bestD = d; best = e; }
    }
    return best;
  }

  // ------------------------------------------------------------ navigation
  function clampK(k) { return Math.max(0.02, Math.min(8, k)); }
  function zoomAt(px, py, factor) {
    var v = st.view;
    var k = clampK(v.k * factor);
    var f = k / v.k;
    v.x = px - (px - v.x) * f;
    v.y = py - (py - v.y) * f;
    v.k = k;
    st.userMoved = true;
    requestDraw();
    setStatus();
  }
  function fit(markUser) {
    if (!V.nodes.length) return;
    var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    V.nodes.forEach(function (v) {
      x0 = Math.min(x0, v.x - v.r); y0 = Math.min(y0, v.y - v.r);
      // Leave room for the label to the right of the node.
      x1 = Math.max(x1, v.x + v.r + (v.cluster === null ? Math.min(160, v.label.length * 6.5) : 0));
      y1 = Math.max(y1, v.y + v.r);
    });
    var bw = x1 - x0 + 80, bh = y1 - y0 + 60;
    var k = clampK(Math.min(W / bw, H / bh, 2.2));
    st.view.k = k;
    st.view.x = W / 2 - (x0 + x1) / 2 * k;
    st.view.y = H / 2 - (y0 + y1) / 2 * k;
    if (markUser) st.userMoved = false;
    requestDraw();
    setStatus();
  }
  function centreOn(v, minK) {
    var k = Math.max(st.view.k, minK || 0.9);
    st.view.k = clampK(k);
    // On a phone the details sheet covers the lower half: centre above it.
    var sheet = !panel.hidden && window.matchMedia && window.matchMedia('(max-width: 760px)').matches;
    st.view.x = W / 2 - v.x * st.view.k;
    st.view.y = (sheet ? H * 0.25 : H / 2) - v.y * st.view.k;
    st.userMoved = true;
    requestDraw();
    setStatus();
  }

  // Pointer: pan, drag a node, click, double-click, pinch.
  var pointers = new Map();
  var drag = null;
  var pinch = null;
  function localXY(ev) {
    var r = canvas.getBoundingClientRect();
    return [ev.clientX - r.left, ev.clientY - r.top];
  }
  canvas.addEventListener('pointerdown', function (ev) {
    canvas.setPointerCapture(ev.pointerId);
    var p = localXY(ev);
    pointers.set(ev.pointerId, p);
    if (pointers.size === 2) {
      var ps = Array.from(pointers.values());
      pinch = { d: Math.hypot(ps[0][0] - ps[1][0], ps[0][1] - ps[1][1]), k: st.view.k };
      drag = null;
      return;
    }
    var v = nodeAt(p[0], p[1]);
    drag = { x: p[0], y: p[1], node: v, moved: false, vx: st.view.x, vy: st.view.y };
    canvas.classList.add('dragging');
  });
  canvas.addEventListener('pointermove', function (ev) {
    var p = localXY(ev);
    if (pointers.has(ev.pointerId)) pointers.set(ev.pointerId, p);
    if (pinch && pointers.size === 2) {
      var ps = Array.from(pointers.values());
      var d = Math.hypot(ps[0][0] - ps[1][0], ps[0][1] - ps[1][1]);
      var mx = (ps[0][0] + ps[1][0]) / 2, my = (ps[0][1] + ps[1][1]) / 2;
      zoomAt(mx, my, (pinch.k * d / pinch.d) / st.view.k);
      return;
    }
    if (drag) {
      var dx = p[0] - drag.x, dy = p[1] - drag.y;
      if (!drag.moved && dx * dx + dy * dy < 16) return;
      drag.moved = true;
      hideTooltip();
      if (drag.node) {
        drag.node.x = (p[0] - st.view.x) / st.view.k;
        drag.node.y = (p[1] - st.view.y) / st.view.k;
        pos.set(drag.node.id, { x: drag.node.x, y: drag.node.y });
        sim.pinned = drag.node;
        if (st.layout === 'force') startForce(0.12);
        requestDraw();
      } else {
        st.view.x = drag.vx + dx;
        st.view.y = drag.vy + dy;
        st.userMoved = true;
        requestDraw();
      }
      return;
    }
    hover(p[0], p[1], ev);
  });
  function endPointer(ev) {
    pointers.delete(ev.pointerId);
    if (pointers.size < 2) pinch = null;
    canvas.classList.remove('dragging');
    if (!drag) return;
    var d = drag;
    drag = null;
    sim.pinned = null;
    if (!d.moved && ev.type === 'pointerup') {
      if (d.node) select(d.node.id, false);
      else clearSelection();
    }
  }
  canvas.addEventListener('pointerup', endPointer);
  canvas.addEventListener('pointercancel', endPointer);
  canvas.addEventListener('pointerleave', function () {
    if (!drag) { st.hoverNode = null; st.hoverEdge = null; hideTooltip(); requestDraw(); }
  });
  canvas.addEventListener('dblclick', function (ev) {
    var p = localXY(ev);
    var v = nodeAt(p[0], p[1]);
    if (v) expand(v);
  });
  canvas.addEventListener('wheel', function (ev) {
    ev.preventDefault();
    var p = localXY(ev);
    var dy = ev.deltaMode === 1 ? ev.deltaY * 16 : ev.deltaY;
    zoomAt(p[0], p[1], Math.exp(-dy * (ev.ctrlKey ? 0.01 : 0.0018)));
  }, { passive: false });

  var hoverQueued = null;
  function hover(px, py, ev) {
    hoverQueued = [px, py, ev.clientX, ev.clientY];
    requestAnimationFrame(function () {
      if (!hoverQueued) return;
      var q = hoverQueued;
      hoverQueued = null;
      var v = nodeAt(q[0], q[1]);
      var e = v ? null : edgeAt(q[0], q[1]);
      var changed = v !== st.hoverNode || e !== st.hoverEdge;
      st.hoverNode = v; st.hoverEdge = e;
      canvas.classList.toggle('pointing', !!(v || e));
      if (v) showTooltip(nodeTooltip(v), q[0], q[1]);
      else if (e) showTooltip(edgeTooltip(e), q[0], q[1]);
      else hideTooltip();
      if (changed) requestDraw();
    });
  }

  // ---------------------------------------------------------------- tooltip
  var tooltip = document.getElementById('tooltip');
  function esc(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function loc(n) {
    if (!n.file) return '';
    return n.line ? n.file + ':' + n.line : n.file;
  }
  function nodeTooltip(v) {
    if (v.cluster !== null) {
      return '<div class="t-kind">' + esc(v.cluster) + '</div><div class="t-dim">cluster · ' +
        v.members.length + ' nodes · double-click to expand</div>';
    }
    var n = v.leaf;
    return '<div class="t-kind">' + esc(n.name) + ' <span class="t-dim">' + esc(n.kind) +
      (n.lang ? ' · ' + esc(n.lang) : '') + '</span></div>' +
      (n.qual !== n.name ? '<div class="t-qual">' + esc(n.qual) + '</div>' : '') +
      (loc(n) ? '<div class="t-dim">' + esc(loc(n)) + '</div>' : '') +
      '<div class="t-dim">' + v.outE.length + ' out · ' + v.inE.length + ' in' +
      (hiddenNeighbours(v) ? ' · ' + hiddenNeighbours(v) + ' hidden' : '') + '</div>';
  }
  function vName(i) {
    var v = V.nodes[i];
    return v.cluster !== null ? v.cluster : v.leaf.name;
  }
  function edgeTooltip(e) {
    var reversed = e.refs[0] && (e.refs[0].flags & F.edge_reversed);
    var html = '<div class="t-kind" style="color:' + esc(famColor(e.fam)) + '">' + esc(kindsText(e)) + (BRIDGE_FAMILIES[e.fam] ? ' <span class="t-dim">bridge</span>' : '') + '</div>' +
      '<div class="t-qual">' + esc(vName(e.a)) + ' → ' + esc(vName(e.b)) + '</div>';
    if (reversed) {
      html += '<div class="t-dim">' + esc(vName(e.b)) + ' ' + (RECEIVING_VERB[e.kind] || 'receives') +
        ' ' + esc(vName(e.a)) + ' (drawn in flow direction)</div>';
    }
    e.refs.slice(0, 3).forEach(function (r) {
      var where = r.evFile ? r.evFile + (r.evLine ? ':' + r.evLine : '') : '';
      if (where) html += '<div class="t-dim">' + esc(where) + (r.res ? ' · ' + esc(r.res) : '') + '</div>';
      if (r.ev) html += '<span class="t-ev">' + esc(r.ev) + '</span>';
    });
    if (e.n > 3) html += '<div class="t-dim">+' + (e.n - 3) + ' more</div>';
    return html;
  }
  function showTooltip(html, x, y) {
    tooltip.innerHTML = html;
    tooltip.hidden = false;
    var tw = tooltip.offsetWidth, th = tooltip.offsetHeight;
    var left = x + 14, top = y + 14;
    if (left + tw > W - 8) left = Math.max(8, x - tw - 14);
    if (top + th > H - 8) top = Math.max(8, y - th - 14);
    tooltip.style.left = left + 'px';
    tooltip.style.top = top + 'px';
  }
  function hideTooltip() { tooltip.hidden = true; }

  // -------------------------------------------------------------- selection
  var panel = document.getElementById('panel');
  var live = document.getElementById('live');
  function selectedV() { return st.selected ? V.byId.get(st.selected) || null : null; }

  function select(id, centre) {
    st.selected = id;
    var v = selectedV();
    computeHighlight();
    renderPanel();
    if (v && centre) centreOn(v);
    if (v) {
      live.textContent = 'Selected ' + (v.cluster !== null ? 'cluster ' + v.cluster :
        v.leaf.name + ', ' + v.leaf.kind) + '. ' + v.outE.length + ' outgoing, ' + v.inE.length + ' incoming.';
    }
    requestDraw();
  }
  function clearSelection() {
    st.selected = null;
    computeHighlight();
    panel.hidden = true;
    requestDraw();
  }

  // Make leaf `i` visible (reveal halo, show its language/external, expand
  // its cluster), then select and centre it.
  function focusLeaf(i) {
    var n = N[i];
    var changed = false;
    if (has(n, F.halo) && !st.revealed.has(i)) { st.revealed.add(i); changed = true; }
    if (has(n, F.external | F.unresolved) && !st.showExternal) { st.showExternal = true; changed = true; }
    if (has(n, F.test) && !st.showTests) { st.showTests = true; changed = true; }
    if (st.langOn[langKey(n)] === false) { st.langOn[langKey(n)] = true; changed = true; }
    if (st.group !== 'none') {
      n.chain.forEach(function (p) { if (!st.expanded.has(p)) { st.expanded.add(p); changed = true; } });
    }
    if (changed) { savePositions(); rebuild(); renderLegend(); relayout(0.3); }
    select('n' + i, true);
  }

  function hiddenNeighbours(v) {
    if (v.cluster !== null || !v.leaf) return 0;
    var n = v.leaf, c = 0;
    n.out.forEach(function (e) { if (V.leafTo[e.t] < 0) c++; });
    n.inn.forEach(function (e) { if (V.leafTo[e.s] < 0) c++; });
    return c;
  }
  function expand(v) {
    if (v.cluster !== null) {
      savePositions();
      expandPrefix(v.cluster);
      st.depth = NaN;
      rebuild();
      st.selected = null;
      computeHighlight();
      renderPanel();
      relayout(0.35);
      return;
    }
    var n = v.leaf, added = 0;
    n.out.concat(n.inn).forEach(function (e) {
      var o = e.s === n.i ? e.t : e.s;
      var on = N[o];
      if ((has(on, F.halo) || (has(on, F.scope_out) && !st.showNonCode)) && !st.revealed.has(o)) { st.revealed.add(o); added++; }
      if (has(on, F.external | F.unresolved) && !st.showExternal) { st.showExternal = true; added++; }
      if (has(on, F.test) && !st.showTests) { st.showTests = true; added++; }
      if (st.langOn[langKey(on)] === false) { st.langOn[langKey(on)] = true; added++; }
    });
    if (added) {
      savePositions();
      rebuild();
      renderLegend();
      relayout(0.3);
      toast('Revealed ' + added + ' neighbour' + (added === 1 ? '' : 's'));
    } else {
      toast(n.flags & F.halo ? 'Its further neighbours are not embedded' : 'All embedded neighbours are shown');
    }
    select(v.id, false);
  }
  // Collapse the folder (or file) a node is drawn in.
  function collapseGroupOf(v) {
    if (!v || st.group === 'none') return;
    var g = null;
    if (v.cluster !== null) g = parentOf(v.cluster);
    else {
      var c = v.leaf.chain;
      for (var i = c.length - 1; i >= 0; i--) if (st.expanded.has(c[i])) { g = c[i]; break; }
    }
    if (!g) return;
    savePositions();
    collapsePrefix(g);
    rebuild();
    relayout(0.25);
    if (V.byId.has('c:' + g)) select('c:' + g, false);
    else clearSelection();
  }

  function badge(text, cls) { return '<span class="badge ' + (cls || '') + '">' + esc(text) + '</span>'; }

  function renderPanel() {
    var v = selectedV();
    if (!v) { panel.hidden = true; return; }
    panel.hidden = false;
    var html = '';
    if (v.cluster !== null) {
      html += '<div class="panel-head"><h2>' + esc(v.cluster) + '</h2><button type="button" data-act="close" aria-label="Close details">×</button></div>';
      html += '<p class="facts">Cluster of ' + v.members.length + ' nodes</p>';
      html += '<div class="actions"><button type="button" data-act="expand">Expand cluster</button><button type="button" data-act="centre">Centre</button></div>';
      html += hopsControl();
      var members = v.members.slice().sort(function (a, b) {
        return (b.out.length + b.inn.length) - (a.out.length + a.inn.length) || (a.qual < b.qual ? -1 : 1);
      });
      html += '<h3>Members</h3><div class="edges"><ul>' + members.slice(0, 300).map(function (m) {
        return '<li><button type="button" data-node="' + m.i + '"><span class="nm">' + esc(m.name) + '</span>' +
          '<span class="loc">' + esc(m.kind + (loc(m) ? ' · ' + loc(m) : '')) + '</span></button></li>';
      }).join('') + '</ul>' + (members.length > 300 ? '<div class="more">and ' + (members.length - 300) + ' more</div>' : '') + '</div>';
      html += edgeSections(v);
    } else {
      var n = v.leaf;
      html += '<div class="panel-head"><h2>' + esc(n.name) + '</h2><button type="button" data-act="close" aria-label="Close details">×</button></div>';
      html += '<div class="qual"><code>' + esc(n.qual) + '</code><button type="button" data-act="copy" title="Copy qualname">Copy</button></div>';
      var badges = '';
      if (has(n, F.seed)) badges += badge(META.scope.indexOf('diff') === 0 ? 'changed' : 'seed', 'seed');
      if (has(n, F.halo)) badges += badge('halo');
      if (has(n, F.external)) badges += badge('external');
      if (has(n, F.unresolved)) badges += badge('unresolved');
      if (has(n, F.endpoint)) badges += badge('bridge endpoint', 'bridge');
      if (has(n, F.test)) badges += badge('test');
      if (badges) html += '<div class="badges">' + badges + '</div>';
      html += '<dl class="facts"><dt>Kind</dt><dd>' + esc(n.kind) + '</dd>';
      if (n.lang) html += '<dt>Language</dt><dd>' + esc(n.lang) + '</dd>';
      if (n.file) html += '<dt>Location</dt><dd><code>' + esc(loc(n)) + '</code></dd>';
      if (AGGREGATED && n.size) html += '<dt>Symbols</dt><dd>' + n.size + '</dd>';
      if (n.mod) html += '<dt>Module</dt><dd><code>' + esc(n.mod) + '</code></dd>';
      html += '</dl>';
      if (n.sig) html += '<pre class="sig">' + esc(dedent(n.sig)) + '</pre>';
      if (n.doc) html += '<div class="doc">' + esc(n.doc) + '</div>';
      var hidden = hiddenNeighbours(v);
      html += '<div class="actions">';
      html += '<button type="button" data-act="expand"' + (hidden ? '' : ' disabled') + '>' +
        (hidden ? 'Reveal ' + hidden + ' hidden neighbour' + (hidden === 1 ? '' : 's') : 'No hidden neighbours') + '</button>';
      html += '<button type="button" data-act="centre">Centre</button>';
      if (v.group !== null && v.group !== undefined) html += '<button type="button" data-act="collapse">Collapse cluster</button>';
      html += '</div>';
      html += hopsControl();
      if (has(n, F.halo) && !AGGREGATED) {
        html += '<p class="more">Outside the scoped walk; to explore from here:</p><p class="cmd">lidx graph --seed ' + esc(shellQuote(n.qual)) + '</p>';
      }
      html += edgeSections(v);
    }
    panel.innerHTML = html;
  }
  // Strip the indentation continuation lines share (a multi-line signature
  // keeps its source indentation).
  function dedent(text) {
    var lines = text.split('\n');
    var indent = Infinity;
    lines.slice(1).forEach(function (l) {
      if (l.trim()) indent = Math.min(indent, l.match(/^\s*/)[0].length);
    });
    if (!isFinite(indent) || !indent) return text;
    return [lines[0]].concat(lines.slice(1).map(function (l) { return l.slice(Math.min(indent, l.length - l.trimStart().length)); })).join('\n');
  }
  function shellQuote(s) { return /^[\w:.\/-]+$/.test(s) ? s : "'" + s.replace(/'/g, "'\\''") + "'"; }
  function hopsControl() {
    var html = '<div class="hops" role="group" aria-label="Highlight distance"><span>Highlight</span>';
    [1, 2, 3, 4].forEach(function (h) {
      html += '<button type="button" data-hops="' + h + '" aria-pressed="' + (st.hops === h) + '">' + h + ' hop' + (h > 1 ? 's' : '') + '</button>';
    });
    return html + '</div>';
  }

  // Incoming/outgoing edges grouped by kind, from the full embedded data
  // (so hidden neighbours are listed too).
  function edgeSections(v) {
    var leaves = v.cluster !== null ? v.members : [v.leaf];
    var inside = new Set(leaves.map(function (n) { return n.i; }));
    var out = {}, inn = {};
    leaves.forEach(function (n) {
      n.out.forEach(function (e) { if (!inside.has(e.t)) (out[e.kind] = out[e.kind] || []).push([e, e.t]); });
      n.inn.forEach(function (e) { if (!inside.has(e.s)) (inn[e.kind] = inn[e.kind] || []).push([e, e.s]); });
    });
    return section('Outgoing', out) + section('Incoming', inn);
  }
  function section(title, byKind) {
    var kinds = Object.keys(byKind).sort(function (a, b) {
      return FAMILY_ORDER.indexOf(family(a)) - FAMILY_ORDER.indexOf(family(b)) || (a < b ? -1 : 1);
    });
    var total = kinds.reduce(function (s, k) { return s + byKind[k].length; }, 0);
    var html = '<h3>' + title + ' (' + total + ')</h3>';
    if (!total) return html + '<p class="more">None</p>';
    html += '<div class="edges">';
    kinds.forEach(function (k) {
      var list = byKind[k];
      list.sort(function (a, b) { return b[0].w - a[0].w || (N[a[1]].qual < N[b[1]].qual ? -1 : 1); });
      var bridge = BRIDGE_FAMILIES[family(k)];
      html += '<details' + (list.length <= 40 ? ' open' : '') + '><summary><span class="swatch' + (bridge ? ' bridge' : '') +
        '" style="border-color:' + esc(famColor(family(k))) + '"></span>' + esc(k) + ' (' + list.length + ')</summary><ul>';
      list.slice(0, 200).forEach(function (it) {
        var e = it[0], o = N[it[1]];
        var shown = V.leafTo[o.i] >= 0;
        html += '<li><button type="button" data-node="' + o.i + '"><span class="nm">' + esc(o.name) + '</span>' +
          (e.w > 1 ? ' <span class="w">×' + e.w + '</span>' : '') +
          (shown ? '' : ' <span class="hidden-tag">hidden</span>') +
          '<span class="loc">' + esc(o.qual !== o.name ? o.qual : o.kind) + '</span>' +
          (e.ev ? '<span class="ev">' + esc((e.evLine ? 'L' + e.evLine + ': ' : '') + e.ev) + '</span>' : '') +
          '</button></li>';
      });
      if (list.length > 200) html += '<li class="more">and ' + (list.length - 200) + ' more</li>';
      html += '</ul></details>';
    });
    return html + '</div>';
  }

  panel.addEventListener('click', function (ev) {
    var b = ev.target.closest('button');
    if (!b) return;
    if (b.dataset.node !== undefined) { focusLeaf(+b.dataset.node); return; }
    if (b.dataset.hops) { st.hops = +b.dataset.hops; computeHighlight(); renderPanel(); requestDraw(); return; }
    var v = selectedV();
    switch (b.dataset.act) {
      case 'close': clearSelection(); canvas.focus(); break;
      case 'copy': copyText(v.leaf.qual); break;
      case 'expand': expand(v); break;
      case 'centre': centreOn(v); break;
      case 'collapse': collapseGroupOf(v); break;
    }
  });

  function copyText(text) {
    function fallback() {
      var ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed'; ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      var ok = false;
      try { ok = document.execCommand('copy'); } catch (e) { ok = false; }
      document.body.removeChild(ta);
      toast(ok ? 'Copied qualname' : 'Copy failed');
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(function () { toast('Copied qualname'); }, fallback);
    } else {
      fallback();
    }
  }
  var toastEl = document.getElementById('toast');
  var toastTimer = null;
  function toast(msg) {
    toastEl.textContent = msg;
    toastEl.classList.add('show');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () { toastEl.classList.remove('show'); }, 1800);
  }

  // ----------------------------------------------------------------- search
  var search = document.getElementById('search');
  var list = document.getElementById('search-list');
  var results = [];
  var active = -1;
  var groupNames = ALL_PREFIXES.filter(function (p) { return p.charAt(p.length - 1) === '/' || p.charAt(0) === '('; });
  function runSearch() {
    var q = search.value.trim().toLowerCase();
    results = [];
    active = -1;
    if (!q) { closeList(); return; }
    var scored = [];
    N.forEach(function (n) {
      var nm = n.name.toLowerCase(), qn = n.qual.toLowerCase();
      var s;
      if (nm === q) s = 0;
      else if (nm.indexOf(q) === 0) s = 1;
      else if (qn.slice(-q.length) === q) s = 2;
      else if (nm.indexOf(q) >= 0) s = 3;
      else if (qn.indexOf(q) >= 0) s = 4;
      else return;
      if (has(n, F.halo)) s += 0.5;
      scored.push([s, n.qual.length, n]);
    });
    groupNames.forEach(function (g) {
      if (g.toLowerCase().indexOf(q) >= 0) scored.push([5, g.length, { group: g }]);
    });
    scored.sort(function (a, b) { return a[0] - b[0] || a[1] - b[1]; });
    results = scored.slice(0, 40).map(function (s) { return s[2]; });
    renderList();
  }
  function renderList() {
    if (!results.length) {
      list.innerHTML = '<li class="empty" role="option" aria-disabled="true">No matches</li>';
    } else {
      list.innerHTML = results.map(function (r, i) {
        if (r.group) {
          return '<li role="option" id="sr-' + i + '" data-i="' + i + '" aria-selected="' + (i === active) + '">' +
            '<span class="nm">' + esc(r.group) + '</span><span class="kd">folder</span></li>';
        }
        var hiddenTag = V.leafTo[r.i] < 0 ? ' · hidden' : '';
        return '<li role="option" id="sr-' + i + '" data-i="' + i + '" aria-selected="' + (i === active) + '">' +
          '<span class="nm">' + esc(r.name) + '</span><span class="kd">' + esc(r.kind + hiddenTag) + '</span>' +
          '<span class="qn">' + esc(r.qual) + '</span></li>';
      }).join('');
    }
    list.hidden = false;
    search.setAttribute('aria-expanded', 'true');
    if (active >= 0) {
      search.setAttribute('aria-activedescendant', 'sr-' + active);
      var el = document.getElementById('sr-' + active);
      if (el) el.scrollIntoView({ block: 'nearest' });
    } else {
      search.removeAttribute('aria-activedescendant');
    }
  }
  function closeList() {
    list.hidden = true;
    search.setAttribute('aria-expanded', 'false');
    search.removeAttribute('aria-activedescendant');
  }
  function choose(i) {
    var r = results[i];
    if (!r) return;
    closeList();
    if (r.group) {
      // Open the folders above it so it is on screen, as a box or open.
      var opened = false;
      for (var p = parentOf(r.group); p; p = parentOf(p)) {
        if (!st.expanded.has(p)) { st.expanded.add(p); opened = true; }
      }
      if (opened) { savePositions(); st.depth = NaN; rebuild(); relayout(0.3); }
      var c = V.byId.get('c:' + r.group);
      if (c) { select(c.id, true); return; }
      var m = V.nodes.find(function (v) { return v.group === r.group; });
      if (m) select(m.id, true);
      return;
    }
    focusLeaf(r.i);
    canvas.focus();
  }
  search.addEventListener('input', runSearch);
  search.addEventListener('focus', function () { if (search.value.trim()) runSearch(); });
  search.addEventListener('keydown', function (ev) {
    if (ev.key === 'ArrowDown') { ev.preventDefault(); if (results.length) { active = (active + 1) % results.length; renderList(); } }
    else if (ev.key === 'ArrowUp') { ev.preventDefault(); if (results.length) { active = active <= 0 ? results.length - 1 : active - 1; renderList(); } }
    else if (ev.key === 'Enter') { ev.preventDefault(); choose(active >= 0 ? active : 0); }
    else if (ev.key === 'Escape') { if (!list.hidden) { closeList(); } else { search.value = ''; canvas.focus(); } }
  });
  search.addEventListener('blur', function () { setTimeout(closeList, 150); });
  list.addEventListener('mousedown', function (ev) {
    var li = ev.target.closest('li[data-i]');
    if (li) { ev.preventDefault(); choose(+li.dataset.i); }
  });

  // ----------------------------------------------------------------- legend
  var legend = document.getElementById('legend');
  function renderLegend() {
    var html = '<h3>Edges</h3>';
    var kinds = Object.keys(kindCounts).sort(function (a, b) {
      return FAMILY_ORDER.indexOf(family(a)) - FAMILY_ORDER.indexOf(family(b)) || (a < b ? -1 : 1);
    });
    var code = kinds.filter(function (k) { return !BRIDGE_FAMILIES[family(k)]; });
    var bridges = kinds.filter(function (k) { return BRIDGE_FAMILIES[family(k)]; });
    function row(k) {
      var fam = family(k);
      return '<label><input type="checkbox" data-kind="' + esc(k) + '"' + (st.kindOn[k] ? ' checked' : '') + '>' +
        '<span class="swatch' + (BRIDGE_FAMILIES[fam] ? ' bridge' : '') + '" style="border-color:' + esc(famColor(fam)) + '"></span>' +
        '<span>' + esc(k) + '</span><span class="count">' + kindCounts[k] + '</span></label>';
    }
    html += code.map(row).join('') || '<p class="notes">None</p>';
    if (bridges.length) {
      html += '<h3>Bridges (cross-process)</h3>' + bridges.map(row).join('');
    }
    html += '<div class="row-actions"><button type="button" data-legend="all">All</button>' +
      '<button type="button" data-legend="calls">Calls only</button>' +
      (bridges.length ? '<button type="button" data-legend="bridges">Bridges + calls</button>' : '') + '</div>';
    html += '<h3>Languages</h3>';
    Object.keys(langCounts).sort(function (a, b) { return langCounts[b] - langCounts[a] || (a < b ? -1 : 1); }).forEach(function (l) {
      html += '<label><input type="checkbox" data-lang="' + esc(l) + '"' + (st.langOn[l] ? ' checked' : '') + '>' +
        '<span class="dot" style="background:' + esc(langColor(l.charAt(0) === '(' ? null : l)) + '"></span>' +
        '<span>' + esc(l) + '</span><span class="count">' + langCounts[l] + '</span></label>';
    });
    var ext = N.filter(function (n) { return has(n, F.external | F.unresolved); }).length;
    var tests = N.filter(function (n) { return has(n, F.test); }).length;
    var halo = N.filter(function (n) { return has(n, F.halo); }).length;
    var nonCode = N.filter(function (n) { return has(n, F.scope_out); }).length;
    html += '<h3>Show</h3>';
    if (nonCode) {
      html += '<label class="plain"><input type="checkbox" data-toggle="showNonCode"' + (st.showNonCode ? ' checked' : '') +
        '><span>Outside ' + esc(META.file_scope || 'code') + ' scope (tests, docs, fixtures, tooling)</span><span class="count">' + nonCode + '</span></label>';
    }
    html += '<label class="plain"><input type="checkbox" data-toggle="showExternal"' + (st.showExternal ? ' checked' : '') + '><span>External &amp; unresolved</span><span class="count">' + ext + '</span></label>';
    if (tests) html += '<label class="plain"><input type="checkbox" data-toggle="showTests"' + (st.showTests ? ' checked' : '') + '><span>Tests</span><span class="count">' + tests + '</span></label>';
    html += '<label class="plain"><input type="checkbox" data-toggle="showLabels"' + (st.showLabels ? ' checked' : '') + '><span>Labels</span><span></span></label>';
    if (halo) {
      var hiddenHalo = N.filter(function (n) { return has(n, F.halo) && !st.revealed.has(n.i); }).length;
      html += '<p class="notes">' + halo + ' one-hop neighbours outside the scope are embedded; ' + hiddenHalo +
        ' hidden. Double-click a node (or Enter) to reveal its own.</p>' +
        '<div class="row-actions"><button type="button" data-legend="halo-all">Reveal all</button>' +
        '<button type="button" data-legend="halo-none">Hide all</button></div>';
    }
    html += '<h3>Nodes</h3><div class="shape-key">' +
      keySvg('<circle cx="9" cy="9" r="5" fill="' + langColor('rust') + '"/><circle cx="9" cy="9" r="8" fill="none" stroke="' + esc(theme.seed) + '" stroke-width="2"/>') + '<span>' + (META.scope.indexOf('diff') === 0 ? 'changed symbol' : 'seed') + '</span>' +
      keySvg('<circle cx="9" cy="9" r="6" fill="' + langColor('python') + '"/>') + '<span>symbol (colour = language)</span>' +
      keySvg('<rect x="3" y="3" width="12" height="12" rx="3" fill="' + langColor('go') + '"/>') + '<span>file / module</span>' +
      keySvg('<rect x="1" y="4" width="16" height="10" rx="3" fill="' + langColor('csharp') + '"/>') + '<span>collapsed cluster</span>' +
      keySvg('<path d="M9 2 L16 9 L9 16 L2 9Z" fill="' + esc(famColor('channel')) + '"/>') + '<span>bridge endpoint (channel, route, config key)</span>' +
      keySvg('<circle cx="9" cy="9" r="6" fill="none" stroke="' + esc(theme['text-dim']) + '" stroke-dasharray="3 2" stroke-width="1.5"/>') + '<span>external / unresolved</span>' +
      keySvg('<circle cx="9" cy="9" r="5" fill="' + langColor('rust') + '"/><circle cx="9" cy="9" r="8" fill="none" stroke="' + esc(theme['text-dim']) + '" stroke-dasharray="2 3"/>') + '<span>halo (outside scope)</span>' +
      '</div>';
    if (META.notes && META.notes.length) {
      html += '<h3>Notes</h3><ul class="notes">' + META.notes.map(function (n) { return '<li>' + esc(n) + '</li>'; }).join('') + '</ul>';
    }
    legend.innerHTML = html;
  }
  function keySvg(inner) { return '<svg viewBox="0 0 18 18" aria-hidden="true">' + inner + '</svg>'; }
  legend.addEventListener('change', function (ev) {
    var t = ev.target;
    if (t.dataset.kind) st.kindOn[t.dataset.kind] = t.checked;
    else if (t.dataset.lang) st.langOn[t.dataset.lang] = t.checked;
    else if (t.dataset.toggle) st[t.dataset.toggle] = t.checked;
    else return;
    if (t.dataset.toggle === 'showLabels') { requestDraw(); return; }
    applyFilters();
  });
  legend.addEventListener('click', function (ev) {
    var b = ev.target.closest('button[data-legend]');
    if (!b) return;
    var what = b.dataset.legend;
    if (what === 'halo-all') N.forEach(function (n) { if (has(n, F.halo)) st.revealed.add(n.i); });
    else if (what === 'halo-none') st.revealed.clear();
    else {
      Object.keys(st.kindOn).forEach(function (k) {
        var fam = family(k);
        st.kindOn[k] = what === 'all' || fam === 'calls' || (what === 'bridges' && !!BRIDGE_FAMILIES[fam]);
      });
    }
    applyFilters();
    renderLegend();
  });
  function applyFilters() {
    savePositions();
    rebuild();
    if (st.selected && !V.byId.has(st.selected)) st.selected = null;
    computeHighlight();
    renderPanel();
    relayout(0.25);
    setStatus();
    requestDraw();
  }

  // ---------------------------------------------------------------- toolbar
  var layForce = document.getElementById('lay-force');
  var layLayer = document.getElementById('lay-layer');
  function setLayout(name) {
    st.layout = name;
    layForce.setAttribute('aria-pressed', String(name === 'force'));
    layLayer.setAttribute('aria-pressed', String(name === 'layered'));
    st.userMoved = false;
    if (name === 'force') startForce(0.6);
    else { sim.running = false; layered(); }
    setStatus();
  }
  layForce.addEventListener('click', function () { setLayout('force'); });
  layLayer.addEventListener('click', function () { setLayout('layered'); });
  var groupSel = document.getElementById('group');
  groupSel.value = st.group;
  groupSel.addEventListener('change', function () {
    savePositions();
    st.group = groupSel.value;
    rebuild();
    if (st.selected && !V.byId.has(st.selected)) st.selected = null;
    computeHighlight();
    renderPanel();
    relayout(0.5);
  });
  // Folder depth: clusters at depth d (1 = top-level folders); All = none.
  function applyDepth(d) {
    if (st.group === 'none') { st.group = 'tree'; groupSel.value = 'tree'; }
    savePositions();
    setDepth(d);
    rebuild();
    if (st.selected && !V.byId.has(st.selected)) st.selected = null;
    computeHighlight();
    renderPanel();
    st.userMoved = false;
    relayout(0.5);
  }
  function currentDepth() {
    if (st.depth === Infinity) return MAX_DEPTH;
    if (st.depth > 0) return st.depth;
    // After a manual expand/collapse: the shallowest cluster shown.
    var d = Infinity;
    V.nodes.forEach(function (v) { if (v.cluster !== null && v.cluster.charAt(0) !== '(') d = Math.min(d, prefixDepth(v.cluster)); });
    return isFinite(d) ? d : MAX_DEPTH;
  }
  var depthLabel = document.getElementById('depth-label');
  function updateDepthLabel() {
    if (!depthLabel) return;
    depthLabel.textContent = st.group === 'none' ? 'flat' : st.depth === Infinity ? 'all' :
      st.depth > 0 ? 'depth ' + st.depth : 'custom';
  }
  document.getElementById('collapse-all').addEventListener('click', function () { applyDepth(1); });
  document.getElementById('expand-all').addEventListener('click', function () { applyDepth(Infinity); });
  document.getElementById('depth-less').addEventListener('click', function () { applyDepth(Math.max(1, currentDepth() - 1)); });
  document.getElementById('depth-more').addEventListener('click', function () {
    var d = currentDepth() + 1;
    applyDepth(d >= MAX_DEPTH ? Infinity : d);
  });

  // Empty state: every node filtered out.
  var emptyEl = document.getElementById('empty');
  function updateEmpty() {
    if (!emptyEl) return;
    emptyEl.hidden = V.nodes.length > 0;
  }
  document.getElementById('empty-reset').addEventListener('click', function () {
    Object.keys(st.kindOn).forEach(function (k) { st.kindOn[k] = true; });
    Object.keys(st.langOn).forEach(function (l) { st.langOn[l] = true; });
    st.showExternal = true; st.showTests = true;
    if (!N.some(function (n) { return !has(n, F.scope_out); })) st.showNonCode = true;
    applyFilters();
    renderLegend();
  });
  document.getElementById('fit').addEventListener('click', function () { fit(true); });
  var legendBtn = document.getElementById('legend-toggle');
  function setLegend(open) {
    legend.hidden = !open;
    legendBtn.setAttribute('aria-expanded', String(open));
    requestAnimationFrame(resize);
  }
  legendBtn.addEventListener('click', function () { setLegend(legend.hidden); });

  function storageGet(k) { try { return window.localStorage.getItem(k); } catch (e) { return null; } }
  function storageSet(k, v) { try { window.localStorage.setItem(k, v); } catch (e) { /* unavailable */ } }
  var themeBtn = document.getElementById('theme');
  function effectiveDark() {
    var t = document.documentElement.getAttribute('data-theme');
    if (t) return t === 'dark';
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  }
  function applyTheme(t) {
    if (t) document.documentElement.setAttribute('data-theme', t);
    readTheme();
    themeBtn.textContent = effectiveDark() ? 'Light' : 'Dark';
    themeBtn.setAttribute('aria-label', 'Switch to ' + (effectiveDark() ? 'light' : 'dark') + ' theme');
    renderLegend();
    renderPanel();
    requestDraw();
  }
  themeBtn.addEventListener('click', function () {
    var next = effectiveDark() ? 'light' : 'dark';
    storageSet('lidx-graph-theme', next);
    applyTheme(next);
  });
  if (window.matchMedia) {
    var mq = window.matchMedia('(prefers-color-scheme: dark)');
    var onScheme = function () { applyTheme(null); };
    if (mq.addEventListener) mq.addEventListener('change', onScheme);
  }

  var helpDialog = document.getElementById('help-dialog');
  function openHelp() {
    if (helpDialog.showModal) helpDialog.showModal(); else helpDialog.setAttribute('open', '');
  }
  document.getElementById('help').addEventListener('click', openHelp);

  // Export
  function download(name, blob) {
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = name;
    document.body.appendChild(a);
    a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
  }
  var baseName = 'lidx-graph-' + (META.repo || 'repo').replace(/[^\w.-]+/g, '_');
  document.getElementById('export-png').addEventListener('click', function () {
    draw();
    canvas.toBlob(function (b) { if (b) download(baseName + '.png', b); }, 'image/png');
  });
  document.getElementById('export-svg').addEventListener('click', function () {
    download(baseName + '.svg', new Blob([toSvg()], { type: 'image/svg+xml' }));
  });
  function toSvg() {
    var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    V.nodes.forEach(function (v) {
      x0 = Math.min(x0, v.x - v.r - 4); y0 = Math.min(y0, v.y - v.r - 4);
      x1 = Math.max(x1, v.x + v.r + 160); y1 = Math.max(y1, v.y + v.r + 4);
    });
    if (!isFinite(x0)) { x0 = 0; y0 = 0; x1 = 100; y1 = 100; }
    var curved = st.layout === 'layered';
    var out = ['<svg xmlns="http://www.w3.org/2000/svg" viewBox="' + [x0, y0, x1 - x0, y1 - y0].map(function (n) { return n.toFixed(1); }).join(' ') + '" font-family="system-ui, sans-serif" font-size="11">'];
    out.push('<rect x="' + x0 + '" y="' + y0 + '" width="' + (x1 - x0) + '" height="' + (y1 - y0) + '" fill="' + esc(theme.canvas) + '"/>');
    out.push('<defs>' + FAMILY_ORDER.map(function (f) {
      return '<marker id="a-' + f + '" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="' + esc(famColor(f)) + '"/></marker>';
    }).join('') + '</defs>');
    V.edges.forEach(function (e) {
      var a = V.nodes[e.a], b = V.nodes[e.b];
      var dx = b.x - a.x, dy = b.y - a.y, l = Math.sqrt(dx * dx + dy * dy) || 1;
      var ex = b.x - dx / l * (b.r + 2), ey = b.y - dy / l * (b.r + 2);
      var d;
      if (curved) {
        var cx = (b.x - a.x) * 0.5;
        if (Math.abs(cx) < 30) cx = cx < 0 ? -30 : 30;
        ex = b.x - (cx < 0 ? -1 : 1) * (b.r + 2); ey = b.y;
        d = 'M' + a.x.toFixed(1) + ' ' + a.y.toFixed(1) + 'C' + (a.x + cx).toFixed(1) + ' ' + a.y.toFixed(1) + ' ' + (b.x - cx).toFixed(1) + ' ' + b.y.toFixed(1) + ' ' + ex.toFixed(1) + ' ' + ey.toFixed(1);
      } else {
        d = 'M' + a.x.toFixed(1) + ' ' + a.y.toFixed(1) + 'L' + ex.toFixed(1) + ' ' + ey.toFixed(1);
      }
      var bridge = BRIDGE_FAMILIES[e.fam];
      out.push('<path d="' + d + '" fill="none" stroke="' + esc(famColor(e.fam)) + '" stroke-opacity="' + (bridge ? 0.9 : 0.55) +
        '" stroke-width="' + (bridge ? 2 : (1 + Math.log2(e.w) * 0.45).toFixed(2)) + '"' + (bridge ? ' stroke-dasharray="7 4"' : '') +
        ' marker-end="url(#a-' + e.fam + ')"><title>' + esc(kindsText(e) + ': ' + vName(e.a) + ' → ' + vName(e.b)) + '</title></path>');
    });
    V.nodes.forEach(function (v) {
      var n = v.leaf;
      var fill = v.cluster !== null ? clusterColor(v) : n && has(n, F.endpoint) ? famColor(endpointFamily(n)) : langColor(n.lang);
      var shape;
      if (v.cluster !== null) shape = '<rect x="' + (v.x - v.r).toFixed(1) + '" y="' + (v.y - v.r * 0.75).toFixed(1) + '" width="' + (v.r * 2).toFixed(1) + '" height="' + (v.r * 1.5).toFixed(1) + '" rx="6"';
      else if (has(n, F.endpoint)) shape = '<path d="M' + v.x.toFixed(1) + ' ' + (v.y - v.r * 1.25).toFixed(1) + 'l' + (v.r * 1.25).toFixed(1) + ' ' + (v.r * 1.25).toFixed(1) + 'l' + (-v.r * 1.25).toFixed(1) + ' ' + (v.r * 1.25).toFixed(1) + 'l' + (-v.r * 1.25).toFixed(1) + ' ' + (-v.r * 1.25).toFixed(1) + 'z"';
      else shape = '<circle cx="' + v.x.toFixed(1) + '" cy="' + v.y.toFixed(1) + '" r="' + v.r.toFixed(1) + '"';
      var hollow = n && has(n, F.external | F.unresolved);
      out.push(shape + ' fill="' + esc(hollow ? theme.canvas : fill) + '" stroke="' + esc(hollow ? theme['text-dim'] : theme['node-stroke']) + '"' +
        (hollow ? ' stroke-dasharray="3 2"' : '') + (n && has(n, F.seed) ? ' stroke-width="3" style="stroke:' + esc(theme.seed) + '"' : '') +
        '><title>' + esc(n ? n.qual : v.cluster) + '</title></' + (shape.indexOf('<rect') === 0 ? 'rect' : shape.indexOf('<path') === 0 ? 'path' : 'circle') + '>');
      out.push('<text x="' + (v.x + v.r + 4).toFixed(1) + '" y="' + (v.y + 4).toFixed(1) + '" fill="' + esc(theme.label) + '">' + esc(labelText(v)) + '</text>');
    });
    out.push('</svg>');
    return out.join('\n');
  }

  // ----------------------------------------------------------- keyboard
  function neighbourCycle(step) {
    var v = selectedV();
    if (!v) return;
    var ids = [];
    v.outE.forEach(function (e) { ids.push(e.b); });
    v.inE.forEach(function (e) { ids.push(e.a); });
    ids = Array.from(new Set(ids));
    if (!ids.length) return;
    st.cycle = st.cycle && st.cycle.from === v.id ? st.cycle : { from: v.id, i: -1, ids: ids };
    st.cycle.i = (st.cycle.i + step + st.cycle.ids.length) % st.cycle.ids.length;
    var target = V.nodes[st.cycle.ids[st.cycle.i]];
    var keep = st.cycle;
    select(target.id, true);
    keep.from = target.id;
    keep.ids = Array.from(new Set(target.outE.map(function (e) { return e.b; }).concat(target.inE.map(function (e) { return e.a; }))));
    keep.i = keep.ids.indexOf(v.idx);
    st.cycle = keep;
  }
  document.addEventListener('keydown', function (ev) {
    var tag = (ev.target.tagName || '').toLowerCase();
    var typing = tag === 'input' || tag === 'select' || tag === 'textarea';
    if (ev.key === '/' && !typing) { ev.preventDefault(); search.focus(); search.select(); return; }
    if (typing || ev.metaKey || ev.ctrlKey || ev.altKey) return;
    var onCanvas = ev.target === canvas || ev.target === document.body;
    if (ev.key === '?') { openHelp(); return; }
    if (ev.key === 'Escape') { clearSelection(); return; }
    if (!onCanvas) return;
    var step = 60;
    switch (ev.key) {
      case 'ArrowLeft': st.view.x += step; break;
      case 'ArrowRight': st.view.x -= step; break;
      case 'ArrowUp': st.view.y += step; break;
      case 'ArrowDown': st.view.y -= step; break;
      case '+': case '=': zoomAt(W / 2, H / 2, 1.25); return;
      case '-': case '_': zoomAt(W / 2, H / 2, 0.8); return;
      case 'f': case 'F': case '0': fit(true); return;
      case 'l': case 'L': setLayout(st.layout === 'force' ? 'layered' : 'force'); return;
      case ']': neighbourCycle(1); return;
      case '[': neighbourCycle(-1); return;
      case 'Enter': if (selectedV()) expand(selectedV()); return;
      case 'c': case 'C': collapseGroupOf(selectedV()); return;
      case '1': case '2': case '3': case '4':
        st.hops = +ev.key; computeHighlight(); renderPanel(); requestDraw(); return;
      default: return;
    }
    ev.preventDefault();
    st.userMoved = true;
    requestDraw();
  });

  // ----------------------------------------------------------------- header
  function fmt(n) { return Number(n).toLocaleString('en-US'); }
  function setHeader() {
    document.getElementById('repo-name').textContent = META.repo;
    var c = META.counts;
    var parts = [];
    if (META.commit) parts.push('commit <code>' + esc(META.commit.slice(0, 10)) + '</code>');
    parts.push('graph v' + esc(META.graph_version));
    parts.push(esc(META.scope));
    parts.push(fmt(c.nodes) + ' nodes · ' + fmt(c.edges) + ' edges' +
      (c.halo_nodes ? ' · ' + fmt(c.halo_nodes) + ' halo' : '') +
      (c.endpoints ? ' · ' + fmt(c.endpoints) + ' bridge endpoints' : ''));
    if (META.truncated) parts.push('<span class="warn">truncated</span>');
    var when = META.generated_at ? new Date(META.generated_at * 1000).toISOString().slice(0, 16).replace('T', ' ') + ' UTC' : '';
    parts.push(esc(META.generated_by + (when ? ', ' + when : '')));
    document.getElementById('meta').innerHTML = parts.join(' · ');
  }
  var statusEl = document.getElementById('status');
  function setStatus() {
    var running = sim.running ? 'laying out… · ' : '';
    statusEl.textContent = running + fmt(V.nodes.length) + ' shown · ' + fmt(V.edges.length) + ' edges · zoom ' +
      Math.round(st.view.k * 100) + '%';
  }

  // ------------------------------------------------------------------- boot
  var savedTheme = storageGet('lidx-graph-theme');
  if (savedTheme === 'dark' || savedTheme === 'light') document.documentElement.setAttribute('data-theme', savedTheme);
  readTheme();
  setHeader();
  if (window.matchMedia && window.matchMedia('(max-width: 760px)').matches) setLegend(false);
  // A whole-repo overview starts at the top-level folders; a seeded view
  // starts open. When nothing in scope exists, show everything instead.
  if (!N.some(function (n) { return !has(n, F.scope_out); })) st.showNonCode = true;
  setDepth(META.collapse_modules ? 1 : Infinity);
  rebuild();
  applyTheme(null);
  if (typeof ResizeObserver !== 'undefined') new ResizeObserver(resize).observe(stage);
  else window.addEventListener('resize', resize);
  resize();
  fit(true);
  layForce.setAttribute('aria-pressed', String(st.layout === 'force'));
  layLayer.setAttribute('aria-pressed', String(st.layout === 'layered'));
  if (st.layout === 'layered') layered(); else startForce(1);
  setStatus();
  // Exposed for automated checks (headless screenshots, tests).
  window.lidxGraph = {
    nodes: N.length, edges: E.length,
    visible: function () { return V.nodes.length; },
    settled: function () { return !sim.running && !anim; },
    svg: function () { return toSvg(); },
    // Client coordinates of a drawn node ('c:<folder>' or 'n<index>').
    clientXY: function (id) {
      var v = V.byId.get(id);
      if (!v) return null;
      var r = canvas.getBoundingClientRect();
      return [r.left + sx(v.x), r.top + sy(v.y)];
    },
    clusters: function () { return V.nodes.filter(function (v) { return v.cluster !== null; }).map(function (v) { return v.id; }); },
    layoutKey: function () {
      return V.nodes.map(function (v) { return v.id + '@' + Math.round(v.x) + ',' + Math.round(v.y); }).join(';');
    },
    drawMs: function () { var t = performance.now(); draw(); return performance.now() - t; },
    tickMs: function () { var a = sim.alpha, t = performance.now(); sim.alpha = 0.1; tick(); sim.alpha = a; return performance.now() - t; },
    select: function (q) {
      var n = N.find(function (x) { return x.qual === q || x.name === q; });
      if (n) focusLeaf(n.i);
      return !!n;
    },
    setLayout: setLayout
  };
})();
