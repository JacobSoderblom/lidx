// lidx graph viewer. Vanilla JS on two stacked 2D canvases (a static layer
// for edges, nodes and labels, an overlay for hover). No dependencies and no
// network access. Reads the document embedded in #lidx-graph-data (schema in
// src/graph_html/doc.rs).
(function () {
  'use strict';

  var BOOT_T0 = performance.now();
  // ------------------------------------------------------------------ data
  var raw;
  try {
    raw = JSON.parse(document.getElementById('lidx-graph-data').textContent);
  } catch (err) {
    var msg = document.getElementById('empty');
    document.getElementById('empty-text').textContent =
      'This page could not read its embedded graph data (' + String(err && err.message || err) +
      '). Regenerate it with lidx graph.';
    document.getElementById('empty-reset').hidden = true;
    msg.hidden = false;
    return;
  }
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
      ev: str(r[6]), evFile: str(r[7]), res: str(r[8]),
      keys: r[9] ? r[9].map(function (k) { return S[k]; }) : null
    };
  });
  E.forEach(function (e) { N[e.s].out.push(e); N[e.t].inn.push(e); });
  var AGGREGATED = META.level !== 'symbol';
  var REDUCE = !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);
  // Applications and libraries (src/graph_html/apps.rs): the units, the
  // links between them, shared package chips and each node's unit.
  var APPS = raw.apps || null;
  var AU = APPS ? APPS.units : [];
  var AE = APPS ? APPS.edges : [];
  var CHIPS = APPS ? APPS.chips : [];
  var NODE_UNIT = APPS ? APPS.node_unit : [];
  var HAS_APPS = !!(APPS && APPS.links && AU.length);

  function has(n, f) { return (n.flags & f) !== 0; }
  function derived(e) { return (e.flags & (F.edge_derived || 0)) !== 0; }
  function langKey(n) {
    if (n.lang) return n.lang;
    if (has(n, F.endpoint)) return '(bridge endpoint)';
    if (has(n, F.unresolved)) return '(unresolved)';
    return '(none)';
  }

  // Edge families: the unit the legend toggles, the colour and dash a line
  // gets, and how kinds merge into one line per node pair.
  function family(kind) {
    if (kind === 'VIA_BUS' || kind.indexOf('CHANNEL_') === 0) return 'bus';
    if (kind === 'VIA_RPC' || kind.indexOf('RPC_') === 0) return 'rpc';
    if (kind === 'VIA_HTTP' || kind.indexOf('HTTP_') === 0) return 'http';
    if (kind === 'VIA_CONFIG' || kind.indexOf('CONFIG_') === 0) return 'config';
    if (kind === 'VIA_BRIDGE') return 'bus';
    if (kind === 'CALLS' || kind === 'DISPATCH') return 'calls';
    if (kind === 'IMPORTS' || kind === 'IMPORTS_FILE' || kind === 'MODULE_EXPORT') return 'imports';
    if (kind === 'EXTENDS' || kind === 'IMPLEMENTS' || kind === 'INHERITS') return 'inherit';
    if (kind === 'USES' || kind === 'REFERENCES') return 'uses';
    if (kind === 'XREF') return 'xref';
    if (kind === 'DECLARES_RPC') return 'declares';
    return 'other';
  }
  var BRIDGE = { bus: 1, rpc: 1, http: 1, config: 1 };
  var FAMILIES = ['calls', 'imports', 'inherit', 'uses', 'xref', 'other', 'bus', 'rpc', 'http', 'config', 'declares'];
  // Shown with the bridges, but not a call: a proto declaring the route its
  // implementers serve.
  var CROSS = { bus: 1, rpc: 1, http: 1, config: 1, declares: 1 };
  // Colour is never the only cue (WCAG 1.4.1): every family has its own dash.
  var STYLE = {
    calls: { tok: 'edge', dash: [], w: 1, label: 'calls', solid: true },
    imports: { tok: 'edge-2', dash: [1.5, 3], w: 0.85, label: 'imports' },
    inherit: { tok: 'edge-3', dash: [], w: 1.1, label: 'inherits / implements', hollow: true, solid: true },
    uses: { tok: 'edge-2', dash: [7, 3], w: 0.9, label: 'uses / references' },
    xref: { tok: 'edge-4', dash: [7, 3, 1.5, 3], w: 0.9, label: 'cross-reference' },
    other: { tok: 'edge-2', dash: [2, 2], w: 0.85, label: 'other' },
    declares: { tok: 'edge-4', dash: [2, 3], w: 0.9, label: 'proto declares (to implementers)', tag: 'declares' },
    bus: { tok: 'bridge', dash: [11, 4], w: 1.6, label: 'message bus', tag: 'bus' },
    rpc: { tok: 'bridge', dash: [3, 3], w: 1.6, label: 'RPC', tag: 'rpc' },
    http: { tok: 'bridge', dash: [11, 3, 2, 3], w: 1.6, label: 'HTTP', tag: 'http' },
    config: { tok: 'bridge', dash: [0.5, 5], w: 2, label: 'config key', tag: 'config', round: true }
  };
  var RECEIVING_VERB = {
    CHANNEL_SUBSCRIBE: 'subscribes to', RPC_IMPL: 'implements', HTTP_ROUTE: 'serves', CONFIG_READ: 'reads'
  };
  var PSEUDO = { '(external)': 1, '(bridges)': 1, '(unresolved)': 1 };

  // Okabe-Ito hues for the five most common languages; the rest fold to grey.
  var langRank = (function () {
    var counts = {};
    N.forEach(function (n) {
      if (!n.lang || n.lang === 'external' || has(n, F.endpoint)) return;
      counts[n.lang] = (counts[n.lang] || 0) + Math.max(1, n.size || 1);
    });
    var order = Object.keys(counts).sort(function (a, b) { return counts[b] - counts[a] || (a < b ? -1 : 1); });
    var rank = {};
    order.forEach(function (l, i) { rank[l] = i < 5 ? 'l' + (i + 1) : 'l-other'; });
    return rank;
  })();
  function langTok(lang) { return lang && langRank[lang] ? langRank[lang] : 'l-other'; }

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
  function esc(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function fmt(n) { return Number(n).toLocaleString('en-US'); }
  function plural(n, one, many) { return fmt(n) + ' ' + (n === 1 ? one : many); }

  // ----------------------------------------------------------------- state
  var SEEDED = N.some(function (n) { return has(n, F.seed); });
  var TRACE = !AGGREGATED && SEEDED && (META.direction === 'up' || META.direction === 'down');
  var BUDGET = 900; // nodes drawn at once; lower-interest folders fold past it

  var st = {
    // 'app' (the architecture sheet) or 'graph' (folders, files, symbols).
    level: HAS_APPS && META.level === 'app' ? 'app' : 'graph',
    colorBy: SEEDED && APPS && NODE_UNIT.some(function (u) { return u >= 0; }) ? 'app' : 'lang',
    layout: TRACE ? 'layered' : 'force',
    view: 'graph',
    group: 'tree',
    expanded: new Set(),
    folded: new Set(),
    depth: Infinity,
    famOn: {},
    langOn: {},
    showExternal: true,
    showTests: true,
    showNonCode: false,
    showEndpoints: !AGGREGATED,
    showLabels: true,
    revealed: new Set(),
    isolate: null,
    pins: new Set(),
    selected: null,
    selectedEdge: null,
    mode: 'hops',
    hops: 1,
    pathFrom: null,
    pathTo: null,
    picking: false,
    hoverNode: null,
    hoverEdge: null,
    cam: { k: 1, x: 0, y: 0 },
    userMoved: false
  };
  FAMILIES.forEach(function (f) { st.famOn[f] = true; });
  N.forEach(function (n) { st.langOn[langKey(n)] = true; });

  // Folder hierarchy: each leaf's chain of folder prefixes (`syft/`,
  // `syft/pkg/`, ...) and, for a symbol, its file. A leaf is drawn inside the
  // first prefix of its chain that is not open.
  function chainOf(n) {
    if (PSEUDO[n.mod]) return [n.mod];
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
  var PREFIX_DEPTH = new Map();
  N.forEach(function (n) {
    n.chain.forEach(function (p, i) {
      if (!PREFIX_DEPTH.has(p) || PREFIX_DEPTH.get(p) > i + 1) PREFIX_DEPTH.set(p, i + 1);
    });
  });
  var ALL_PREFIXES = Array.from(PREFIX_DEPTH.keys()).sort();
  var MAX_DEPTH = N.reduce(function (m, n) { return Math.max(m, n.chain.length); }, 1);
  function prefixDepth(p) { return PREFIX_DEPTH.get(p) || 1; }
  function isOpen(p) { return st.expanded.has(p) && !st.folded.has(p); }
  function groupOf(n) {
    if (st.group === 'none') return null;
    for (var i = 0; i < n.chain.length; i++) if (!isOpen(n.chain[i])) return n.chain[i];
    return null;
  }
  function parentOf(p) {
    if (PSEUDO[p] || p === './') return null;
    var t = p.charAt(p.length - 1) === '/' ? p.slice(0, -1) : p;
    var i = t.lastIndexOf('/');
    return i < 0 ? null : t.slice(0, i + 1);
  }
  // The folder whose outline a drawn leaf sits in.
  function hullOf(n) {
    if (st.group === 'none' || !n.chain.length) return null;
    var c = n.chain, last = c[c.length - 1];
    if (last.charAt(last.length - 1) !== '/' && !PSEUDO[last] && c.length > 1) return c[c.length - 2];
    return last;
  }
  // A closed folder's label: its path below the nearest open ancestor, so a
  // folder that opened straight through (`dotnet/` -> `dotnet/src/`) and two
  // folders with the same last segment (`syft/`, `cmd/syft/`) stay distinct.
  function relativeName(p) {
    if (PSEUDO[p]) return p;
    if (p === './') return '(root)';
    for (var q = parentOf(p); q; q = parentOf(q)) if (isOpen(q) && (drawnIn.get(q) || 0) > 1) return p.slice(q.length);
    return p;
  }
  var drawnIn = new Map();
  function folderName(p) {
    if (PSEUDO[p]) return p;
    if (p === './') return '(root)';
    if (p.charAt(p.length - 1) !== '/') return p.split('/').pop();
    var parts = p.replace(/\/$/, '').split('/');
    return parts[parts.length - 1] + '/';
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
    st.expanded = new Set(ALL_PREFIXES.filter(function (p) { return !PSEUDO[p] && prefixDepth(p) < d; }));
    if (d === Infinity) ALL_PREFIXES.forEach(function (p) { st.expanded.add(p); });
    compress();
  }
  function expandPrefix(p) { st.expanded.add(p); compress(); st.depth = NaN; }
  function collapsePrefix(p) {
    st.expanded.delete(p);
    Array.from(st.expanded).forEach(function (q) { if (q !== p && q.indexOf(p) === 0) st.expanded.delete(q); });
    st.depth = NaN;
  }

  // Shown under the scope, halo and isolate rules, before legend filters.
  function inView(n) {
    if (st.isolate && !st.isolate.has(n.i)) return false;
    if (has(n, F.scope_out) && !st.showNonCode && !st.revealed.has(n.i)) return false;
    if (has(n, F.halo) && !st.revealed.has(n.i) && !(has(n, F.scope_out) && st.showNonCode)) return false;
    return true;
  }
  function leafVisible(n) {
    if (!inView(n)) return false;
    if (has(n, F.endpoint) && !st.showEndpoints) return false;
    if (has(n, F.external | F.unresolved) && !st.showExternal) return false;
    if (has(n, F.test) && !st.showTests) return false;
    if (st.langOn[langKey(n)] === false) return false;
    return true;
  }
  function edgeVisible(e) {
    if (!st.famOn[family(e.kind)]) return false;
    // A folder-to-folder bridge stands in for its endpoint node: draw one.
    if (derived(e) && st.showEndpoints) return false;
    return true;
  }

  // ------------------------------------------------------------------ view
  // What is drawn: visible leaves, one node per closed folder, and edges
  // merged per node pair (code kinds into one line, each bridge family apart).
  var V = { nodes: [], edges: [], links: [], byId: new Map(), leafTo: new Int32Array(N.length), routes: null, cols: null };
  var pos = new Map(); // node id -> {x, y}, survives rebuilds

  function nodeRadius(v) {
    // A folder box is wide enough for its name at 100% (the label sits
    // inside every box, never below some).
    if (v.cluster !== null) return Math.max(Math.min(40, 12 + Math.sqrt(v.members.length) * 2.4), (v.label.length * 7.4 + 18) / 2.5);
    var n = v.leaf;
    if (has(n, F.external) && AGGREGATED) return 6;
    if (AGGREGATED) return Math.min(18, 4.5 + Math.sqrt(Math.max(1, n.size)) * 0.8);
    return Math.min(10, 4 + Math.log2(1 + v.deg) * 1.3);
  }

  // Degree of interest of a leaf: seeds and the selection first, then degree.
  function interest(n) {
    var id = 'n' + n.i;
    var kept = st.selected === id || st.pathTo === id || st.pathFrom === id || st.focusId === id;
    return (has(n, F.seed) ? 1e6 : 0) + (kept ? 2e6 : 0) + n.out.length + n.inn.length;
  }

  function rebuild() {
    st.folded = new Set();
    var visible = [];
    N.forEach(function (n) { if (leafVisible(n)) visible.push(n); });
    // Visible budget: fold the open folders of least interest until the view
    // fits, deepest first; a folded folder is a box marked "+N".
    var drawn = countDrawn(visible);
    if (drawn > BUDGET && st.group !== 'none') {
      var score = new Map(), size = new Map();
      visible.forEach(function (n) {
        var w = interest(n);
        n.chain.forEach(function (p) {
          if (!st.expanded.has(p)) return;
          score.set(p, Math.max(score.get(p) || 0, w));
          size.set(p, (size.get(p) || 0) + 1);
        });
      });
      var cands = Array.from(score.keys()).filter(function (p) { return !PSEUDO[p] && (size.get(p) || 0) > 1; });
      cands.sort(function (a, b) {
        return (score.get(a) - score.get(b)) || (prefixDepth(b) - prefixDepth(a)) || (a < b ? -1 : 1);
      });
      for (var ci = 0; ci < cands.length && drawn > BUDGET; ci++) {
        if (score.get(cands[ci]) >= 1e6) continue;
        st.folded.add(cands[ci]);
        drawn = countDrawn(visible);
      }
    }
    var nodes = [], byId = new Map();
    var leafTo = new Int32Array(N.length).fill(-1);
    var groupCount = new Map();
    var groups = visible.map(function (n) {
      var g = groupOf(n);
      if (g !== null) groupCount.set(g, (groupCount.get(g) || 0) + 1);
      return g;
    });
    var clusters = new Map();
    visible.forEach(function (n, k) {
      var g = groups[k];
      if (g !== null && groupCount.get(g) > 1) {
        var c = clusters.get(g);
        if (!c) {
          c = { id: 'c:' + g, idx: nodes.length, leaf: null, cluster: g, members: [], deg: 0,
                label: g, group: parentOf(g), folded: st.folded.has(g),
                x: 0, y: 0, vx: 0, vy: 0, r: 8, q: 1 };
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
    // Label each closed folder below the nearest open folder that shows more
    // than this one thing (a folder opened straight through is not a level).
    drawnIn = new Map();
    nodes.forEach(function (v) {
      var chain = v.leaf ? v.leaf.chain : v.members[0].chain;
      for (var i = 0; i < chain.length; i++) {
        if (v.cluster !== null && chain[i] === v.cluster) break;
        if (!isOpen(chain[i])) break;
        drawnIn.set(chain[i], (drawnIn.get(chain[i]) || 0) + 1);
      }
    });
    nodes.forEach(function (v) { if (v.cluster !== null) v.label = relativeName(v.cluster); });
    var edges = [], agg = new Map();
    E.forEach(function (e) {
      if (!edgeVisible(e)) return;
      var a = leafTo[e.s], b = leafTo[e.t];
      if (a < 0 || b < 0 || a === b) return;
      var fam = family(e.kind);
      var bucket = BRIDGE[fam] ? fam : 'code';
      var key = a + '|' + b + '|' + bucket;
      var ve = agg.get(key);
      if (!ve) {
        ve = { a: a, b: b, kind: e.kind, fam: fam, kinds: {}, w: 0, n: 0, refs: [], idx: edges.length };
        agg.set(key, ve);
        edges.push(ve);
      }
      ve.kinds[e.kind] = (ve.kinds[e.kind] || 0) + e.w;
      ve.w += e.w;
      ve.n += 1;
      ve.refs.push(e);
      if (e.keys) {
        if (!ve.keys) ve.keys = {};
        e.keys.forEach(function (k) { (ve.keys[e.kind] = ve.keys[e.kind] || new Set()).add(k); });
      }
    });
    // A derived bridge merged into folders weighs its distinct keys: two
    // files of one folder sharing a key with another folder count it once.
    edges.forEach(function (ve) {
      if (!ve.keys) return;
      var w = 0;
      Object.keys(ve.keys).forEach(function (k) {
        var raw = ve.refs.filter(function (r) { return r.kind === k && !r.keys; })
          .reduce(function (s0, r) { return s0 + r.w; }, 0);
        ve.kinds[k] = ve.keys[k].size + raw;
      });
      Object.keys(ve.kinds).forEach(function (k) { w += ve.kinds[k]; });
      ve.w = w;
    });
    edges.forEach(function (ve) {
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
      v.chip = !!(v.leaf && has(v.leaf, F.external));
      v.anc = ancestorsOf(v);
      v.pinned = st.pins.has(v.id);
    });
    var links = new Map();
    edges.forEach(function (e) {
      var key = e.a < e.b ? e.a + '|' + e.b : e.b + '|' + e.a;
      var l = links.get(key);
      if (!l) { l = { a: e.a, b: e.b, w: 0, bridge: true }; links.set(key, l); }
      l.w += e.w;
      if (!BRIDGE[e.fam]) l.bridge = false;
    });
    V.links = Array.from(links.values());
    V.nodes = nodes; V.edges = edges; V.byId = byId; V.leafTo = leafTo; V.routes = null; V.cols = null;
    // While panning only the heaviest edges are drawn: find the weight cut.
    var ws = edges.map(function (e) { return e.w; }).sort(function (a, b) { return b - a; });
    V.wCut = ws.length > 1500 ? ws[1499] : 0;
    nodes.forEach(function (v) { v.hidden = hiddenNeighbours(v); });
    placeNodes();
    buildAdjacency();
    labelWidths.clear();
    moved();
    updateEmpty();
    updateDepthLabel();
  }
  function countDrawn(visible) {
    var seen = new Set(), loose = 0;
    visible.forEach(function (n) {
      var g = groupOf(n);
      if (g === null) loose++;
      else seen.add(g);
    });
    return loose + seen.size;
  }
  // Open folders a drawn node sits inside, outermost first (bundling path).
  function ancestorsOf(v) {
    var chain = v.leaf ? v.leaf.chain : v.members[0].chain;
    var out = [];
    for (var i = 0; i < chain.length; i++) {
      if (v.cluster !== null && chain[i] === v.cluster) break;
      if (!isOpen(chain[i])) break;
      if (chain[i].charAt(chain[i].length - 1) === '/' && !PSEUDO[chain[i]]) out.push(chain[i]);
    }
    return out;
  }
  function buildAdjacency() {
    V.nodes.forEach(function (v) { v.outE = []; v.inE = []; });
    V.edges.forEach(function (e) { V.nodes[e.a].outE.push(e); V.nodes[e.b].inE.push(e); });
  }
  function hiddenNeighbours(v) {
    if (v.cluster !== null || !v.leaf) return 0;
    var n = v.leaf, c = 0;
    n.out.forEach(function (e) { if (V.leafTo[e.t] < 0 && edgeVisible(e)) c++; });
    n.inn.forEach(function (e) { if (V.leafTo[e.s] < 0 && edgeVisible(e)) c++; });
    return c;
  }

  // Connected components (by any drawn edge), seeded or largest first.
  function components() {
    var n = V.nodes.length, parent = new Int32Array(n), i;
    for (i = 0; i < n; i++) parent[i] = i;
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
      var sa = a.some(function (k) { return V.nodes[k].seed; }) ? 1 : 0;
      var sb = b.some(function (k) { return V.nodes[k].seed; }) ? 1 : 0;
      return sb - sa || b.length - a.length || a[0] - b[0];
    });
  }

  // Positions: remembered ones, then a cluster's member centroid, an
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
    var groups = [], seen = new Set();
    V.nodes.forEach(function (v) { var g = v.group || ''; if (!seen.has(g)) { seen.add(g); groups.push(g); } });
    groups.sort();
    var centre = new Map();
    var spread = Math.max(60, Math.sqrt(V.nodes.length) * 26);
    groups.forEach(function (g, i) {
      var a = i * 2.399963, r = spread * Math.sqrt(i / Math.max(1, groups.length)) * 1.6;
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
        var sx0 = 0, sy0 = 0, c = 0;
        v.members.forEach(function (m) {
          var mp = pos.get('n' + m.i);
          if (mp) { sx0 += mp.x; sy0 += mp.y; c++; }
        });
        if (c) { v.x = sx0 / c; v.y = sy0 / c; return; }
      }
      var cp = ancestorPos(member);
      if (cp) { v.x = cp.x + (r() - 0.5) * 70; v.y = cp.y + (r() - 0.5) * 70; return; }
      if (v.cluster === null && !firstPlacement) {
        var anchor = neighbourWithPos(v.leaf);
        if (anchor) {
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
    savePositions();
  }
  function neighbourWithPos(n) {
    var list = n.inn.concat(n.out);
    for (var k = 0; k < list.length; k++) {
      var e = list[k];
      var p = pos.get('n' + (e.s === n.i ? e.t : e.s));
      if (p) return p;
    }
    return null;
  }
  function savePositions() { V.nodes.forEach(function (v) { pos.set(v.id, { x: v.x, y: v.y }); }); }

  // ---------------------------------------------------------- force layout
  // ForceAtlas-style: Barnes-Hut charge, springs, folder cohesion, collision
  // and a centre pull; deterministic alpha decay. External chips feel no
  // centre pull, so they stay with their folder instead of forming a hub.
  var sim = { alpha: 0, running: false, pinned: null };
  var THETA2 = 0.81;
  function quadNew(x0, y0, s) { return { x0: x0, y0: y0, s: s, m: 0, cx: 0, cy: 0, b: null, k: null }; }
  function quadInsert(q, v, depth) {
    if (q.k === null && q.b === null) { q.b = v; q.m = v.q; q.cx = v.x; q.cy = v.y; return; }
    var m = q.m + v.q;
    q.cx = (q.cx * q.m + v.x * v.q) / m;
    q.cy = (q.cy * q.m + v.y * v.q) / m;
    q.m = m;
    if (depth > 48) return;
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
    var ns = V.nodes, n = ns.length, alpha = sim.alpha, i, v;
    if (!n) return;
    var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (i = 0; i < n; i++) {
      v = ns[i];
      if (v.x < x0) x0 = v.x; if (v.y < y0) y0 = v.y;
      if (v.x > x1) x1 = v.x; if (v.y > y1) y1 = v.y;
    }
    var root = quadNew(x0, y0, Math.max(x1 - x0, y1 - y0) + 1);
    for (i = 0; i < n; i++) quadInsert(root, ns[i], 0);
    for (i = 0; i < n; i++) repel(root, ns[i], alpha);
    var es = V.links;
    for (i = 0; i < es.length; i++) {
      var e = es[i], s = ns[e.a], t = ns[e.b];
      var dx = t.x + t.vx - s.x - s.vx, dy = t.y + t.vy - s.y - s.vy;
      var l = Math.sqrt(dx * dx + dy * dy) || 1e-3;
      var dist = 30 + (s.r + t.r) * (s.cluster !== null || t.cluster !== null ? 2.2 : 1.6);
      var strength = 1 / Math.max(1, Math.min(s.deg, t.deg));
      if (e.bridge) strength *= 0.6;
      if (s.chip || t.chip) { strength *= 1.5; dist = 18 + (s.r + t.r); }
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
        if (v.group === null || v.group === undefined || PSEUDO[v.group]) continue;
        var c = cent.get(v.group);
        if (!c) { c = { x: 0, y: 0, n: 0 }; cent.set(v.group, c); }
        c.x += v.x; c.y += v.y; c.n++;
      }
      for (i = 0; i < n; i++) {
        v = ns[i];
        if (v.group === null || v.group === undefined || PSEUDO[v.group]) continue;
        var g = cent.get(v.group);
        if (g.n < 2) continue;
        var pull = v.chip ? 0.25 : 0.12;
        v.vx += (g.x / g.n - v.x) * pull * alpha;
        v.vy += (g.y / g.n - v.y) * pull * alpha;
      }
    }
    var gravity = 0.03 * alpha, seedGravity = 0.25 * alpha;
    for (i = 0; i < n; i++) {
      v = ns[i];
      var gr = v.seed ? seedGravity : v.chip ? 0 : gravity;
      v.vx -= v.x * gr; v.vy -= v.y * gr;
      if (v === sim.pinned || v.pinned) { v.vx = 0; v.vy = 0; continue; }
      v.vx *= 0.6; v.vy *= 0.6;
      v.x += v.vx; v.y += v.vy;
    }
    collide(ns);
    sim.alpha += (0 - sim.alpha) * 0.0228;
  }
  // Push overlapping nodes apart (uniform grid, linear time).
  function collide(ns) {
    var maxR = 0, i;
    for (i = 0; i < ns.length; i++) { ns[i].cr = ns[i].cluster !== null ? ns[i].r * 1.4 : ns[i].r; if (ns[i].cr > maxR) maxR = ns[i].cr; }
    var cell = 2 * maxR + 6, grid = new Map();
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
            var min = a.cr + b.cr + 6, d2 = x * x + y * y;
            if (d2 >= min * min) continue;
            var d = Math.sqrt(d2) || 0.01;
            var push = (min - d) / d * 0.5;
            if (d2 === 0) { x = jiggle() - 0.5; y = jiggle() - 0.5; }
            var ra = b.r * b.r / (a.r * a.r + b.r * b.r);
            if (a !== sim.pinned && !a.pinned) { a.x -= x * push * ra; a.y -= y * push * ra; }
            if (b !== sim.pinned && !b.pinned) { b.x += x * push * (1 - ra); b.y += y * push * (1 - ra); }
          }
        }
      }
    }
  }
  function startForce(alpha) {
    if (st.layout !== 'force') return;
    sim.alpha = Math.max(sim.alpha, alpha);
    if (REDUCE) { settleNow(4000); return; }
    if (!sim.running) { sim.running = true; requestAnimationFrame(simFrame); }
  }
  // Run the layout to rest without animation (reduced motion, first load).
  function settleNow(budgetMs) {
    var t0 = performance.now();
    while (sim.alpha > 0.002 && performance.now() - t0 < budgetMs) tick();
    savePositions();
    moved();
    if (sim.alpha <= 0.002) {
      sim.alpha = 0;
      if (window.lidxGraph && !window.lidxGraph.settledAt) window.lidxGraph.settledAt = Math.round(performance.now());
      packComponents(true);
    }
    if (!st.userMoved) fit(false);
    draw();
    return sim.alpha <= 0.002;
  }
  function simFrame() {
    if (st.layout !== 'force') { sim.running = false; return; }
    var t0 = performance.now();
    do { tick(); } while (sim.alpha > 0.002 && performance.now() - t0 < 14);
    savePositions();
    moved();
    if (!st.userMoved) fit(false);
    if (sim.alpha <= 0.002) {
      sim.running = false;
      if (window.lidxGraph && !window.lidxGraph.settledAt) window.lidxGraph.settledAt = Math.round(performance.now());
      if (!packComponents(false) && !st.userMoved) fit(false);
      setStatus();
    }
    draw();
    if (sim.running) requestAnimationFrame(simFrame);
  }

  // -------------------------------------------------------- layered layout
  // Sugiyama-style, left to right, one connected component at a time:
  // layers (hop distance from the seed in a one-way trace, longest path
  // otherwise), a dummy node in every layer a long edge crosses (so it is
  // drawn as short straight-ish segments), barycentre sweeps over real and
  // dummy nodes with folders kept together, and layers taller than a screen
  // wrapped into side-by-side sub-columns within their band.
  var COL_GAP = 270, SUB_GAP = 210;
  function layered(instant) {
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
    var targets = new Array(n);
    var routes = new Map();
    var comps = components();
    var lone = comps.filter(function (c) { return c.length === 1; });
    var y = 0;
    V.cols = null;
    comps.forEach(function (comp) {
      if (comp.length === 1) return;
      var t = layoutComponent(comp, out, inn);
      var dy = y - t.y0;
      comp.forEach(function (i) { targets[i] = { x: t.pos[i].x, y: t.pos[i].y + dy }; });
      t.routes.forEach(function (pts, key) {
        routes.set(key, pts.map(function (p) { return { x: p.x, y: p.y + dy }; }));
      });
      if (!V.cols) V.cols = t.cols.map(function (cl) { return { x0: cl.x0, x1: cl.x1, top: cl.top + dy, bottom: cl.bottom + dy, n: cl.n }; });
      y += t.y1 - t.y0 + 80;
    });
    var cols = Math.max(1, Math.ceil(Math.sqrt(lone.length * 3)));
    lone.forEach(function (c, k) {
      targets[c[0]] = { x: (k % cols) * SUB_GAP, y: y + Math.floor(k / cols) * 34 };
    });
    V.routes = routes;
    animateTo(targets, instant);
  }

  function layoutComponent(comp, out, inn) {
    var ns = V.nodes;
    var inComp = new Set(comp);
    var layer = new Map();
    var seeds = comp.filter(function (i) { return ns[i].seed; });
    var up = META.direction === 'up';
    if (TRACE && seeds.length) {
      var next = up ? inn : out;
      var queue = seeds.slice();
      seeds.forEach(function (s) { layer.set(s, 0); });
      while (queue.length) {
        var u = queue.shift();
        next[u].forEach(function (w) {
          if (inComp.has(w) && !layer.has(w)) { layer.set(w, layer.get(u) + 1); queue.push(w); }
        });
      }
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
    // The way edges run in this layout: rightwards, or leftwards in an
    // upward trace (callers sit to the right of what they call).
    var dirSign = TRACE && up ? -1 : 1;
    // Dummy nodes for edges that skip layers. Virtual ids start at n.
    var n0 = ns.length, nextId = n0;
    var vOut = new Map(), vIn = new Map();
    function addAdj(m, a, b) { var l = m.get(a); if (!l) { l = []; m.set(a, l); } l.push(b); }
    var chains = new Map(); // pair key -> [from, d1, ..., to]
    var layerOf = new Map(layer);
    comp.forEach(function (a) {
      out[a].forEach(function (b) {
        if (!inComp.has(b)) return;
        var la = layer.get(a), lb = layer.get(b);
        var span = (lb - la) * dirSign;
        if (span === 1) { addAdj(vOut, a, b); addAdj(vIn, b, a); chains.set(a + '>' + b, [a, b]); return; }
        if (span < 1) return; // same column or backwards: routed apart
        var chain = [a], prev = a;
        for (var s = 1; s < span; s++) {
          var d = nextId++;
          layerOf.set(d, la + s * dirSign);
          addAdj(vOut, prev, d); addAdj(vIn, d, prev);
          chain.push(d);
          prev = d;
        }
        addAdj(vOut, prev, b); addAdj(vIn, b, prev);
        chain.push(b);
        chains.set(a + '>' + b, chain);
      });
    });
    var layers = [];
    layerOf.forEach(function (l, id) { (layers[l] = layers[l] || []).push(id); });
    for (var li = 0; li < layers.length; li++) if (!layers[li]) layers[li] = [];
    var isDummy = function (id) { return id >= n0; };
    var groupKey = function (id) { return isDummy(id) ? '￿' : (ns[id].group || ''); };
    var at = new Map();
    layers.forEach(function (l) {
      l.sort(function (a, b) {
        if (!isDummy(a) && !isDummy(b) && ns[a].seed !== ns[b].seed) return ns[a].seed ? -1 : 1;
        var ga = groupKey(a), gb = groupKey(b);
        if (ga !== gb) return ga < gb ? -1 : 1;
        if (isDummy(a) || isDummy(b)) return a - b;
        return ns[a].label < ns[b].label ? -1 : ns[a].label > ns[b].label ? 1 : a - b;
      });
      l.forEach(function (v, k) { at.set(v, k); });
    });
    function sweep(l, nbrs) {
      var bc = new Map();
      l.forEach(function (v) {
        var s = 0, c = 0;
        (nbrs.get(v) || []).forEach(function (w) { s += at.get(w); c++; });
        bc.set(v, c ? s / c : at.get(v));
      });
      // Barycentre, then folder, so a folder's nodes stay adjacent on ties.
      l.sort(function (a, b) {
        return bc.get(a) - bc.get(b) || (groupKey(a) < groupKey(b) ? -1 : groupKey(a) > groupKey(b) ? 1 : 0) || at.get(a) - at.get(b);
      });
      l.forEach(function (v, k) { at.set(v, k); });
    }
    var prevAdj = dirSign > 0 ? vIn : vOut, nextAdj = dirSign > 0 ? vOut : vIn;
    for (var it = 0; it < 8; it++) {
      for (li = 1; li < layers.length; li++) sweep(layers[li], prevAdj);
      for (li = layers.length - 2; li >= 0; li--) sweep(layers[li], nextAdj);
    }
    // Coordinates. A layer taller than ~a screen wraps into sub-columns.
    var real = function (l) { return l.filter(function (id) { return !isDummy(id); }).length; };
    var perCol = Math.max(18, Math.round(Math.sqrt(comp.length) * 2.2));
    var res = { pos: {}, y0: Infinity, y1: -Infinity, cols: [], routes: new Map() };
    var vpos = new Map();
    var x = 0;
    layers.forEach(function (l) {
      var count = real(l);
      var cols = Math.max(1, Math.ceil(count / perCol));
      var rows = Math.ceil(l.length / cols);
      var top = Infinity, bottom = -Infinity;
      for (var cI = 0; cI < cols; cI++) {
        var col = l.slice(cI * rows, (cI + 1) * rows);
        var yy = 0;
        var ys = col.map(function (id) {
          var half = isDummy(id) ? 6 : Math.max(ns[id].r + 3, count <= 20 || cols > 1 ? 14 : 10);
          yy += half; var here = yy; yy += half;
          return here;
        });
        var mid = yy / 2;
        col.forEach(function (id, j) {
          var p = { x: x + cI * SUB_GAP, y: ys[j] - mid };
          vpos.set(id, p);
          if (!isDummy(id)) {
            res.pos[id] = p;
            ns[id].hop = layers.indexOf(l);
            top = Math.min(top, p.y - ns[id].r);
            bottom = Math.max(bottom, p.y + ns[id].r);
          }
          res.y0 = Math.min(res.y0, p.y - 10);
          res.y1 = Math.max(res.y1, p.y + 10);
        });
      }
      res.cols.push({ x0: x - 30, x1: x + (cols - 1) * SUB_GAP + 190, top: isFinite(top) ? top : 0, bottom: isFinite(bottom) ? bottom : 0, n: count });
      x += (cols - 1) * SUB_GAP + COL_GAP;
    });
    res.y0 -= 34; // room for the column headers
    chains.forEach(function (chain, key) {
      res.routes.set(key, chain.map(function (id) { return vpos.get(id); }));
    });
    return res;
  }

  function longestPathLayers(comp, out, inn, inComp, layer) {
    var ns = V.nodes;
    var order = comp.slice().sort(function (a, b) {
      var ra = ns[a].seed ? 0 : inn[a].length === 0 ? 1 : 2;
      var rb = ns[b].seed ? 0 : inn[b].length === 0 ? 1 : 2;
      return ra - rb || a - b;
    });
    var state = new Map(), dagOut = new Map();
    comp.forEach(function (i) { dagOut.set(i, []); });
    order.forEach(function (start) {
      if (state.get(start)) return;
      var stack = [[start, 0]];
      state.set(start, 1);
      while (stack.length) {
        var top = stack[stack.length - 1], u = top[0];
        if (top[1] < out[u].length) {
          var w = out[u][top[1]++];
          if (!inComp.has(w) || state.get(w) === 1) continue;
          dagOut.get(u).push(w);
          if (!state.get(w)) { state.set(w, 1); stack.push([w, 0]); }
        } else { state.set(u, 2); stack.pop(); }
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

  // After a force layout settles: the main component stays, every other
  // component (and lone node) is shelf-packed in a column beside it.
  function packComponents(instant) {
    var comps = components();
    if (comps.length < 2) return false;
    var ns = V.nodes;
    function box(c) {
      var b = { x0: Infinity, y0: Infinity, x1: -Infinity, y1: -Infinity };
      c.forEach(function (i) {
        var v = ns[i];
        b.x0 = Math.min(b.x0, v.x - v.r); b.y0 = Math.min(b.y0, v.y - v.r);
        b.x1 = Math.max(b.x1, v.x + v.r + 70); b.y1 = Math.max(b.y1, v.y + v.r);
      });
      return b;
    }
    var main = box(comps[0]);
    var rest = comps.slice(1).map(function (c) { return { c: c, b: box(c) }; });
    var area = rest.reduce(function (s, r) { return s + (r.b.x1 - r.b.x0 + 30) * (r.b.y1 - r.b.y0 + 30); }, 0);
    var shelf = Math.max(260, Math.sqrt(area) * 1.15, (main.y1 - main.y0) ? area / (main.y1 - main.y0) : 0);
    var gap = 30, x0 = main.x1 + 80, x = x0, y = main.y0, rowH = 0;
    var targets = ns.map(function (v) { return { x: v.x, y: v.y }; });
    rest.forEach(function (r) {
      var w = r.b.x1 - r.b.x0, h = r.b.y1 - r.b.y0;
      if (x > x0 && x + w > x0 + shelf) { x = x0; y += rowH + gap; rowH = 0; }
      var dx = x - r.b.x0, dy = y - r.b.y0;
      r.c.forEach(function (i) { if (!ns[i].pinned) targets[i] = { x: ns[i].x + dx, y: ns[i].y + dy }; });
      x += w + gap;
      rowH = Math.max(rowH, h);
    });
    animateTo(targets, instant);
    return true;
  }

  var anim = null;
  function animateTo(targets, instant) {
    var from = V.nodes.map(function (v) { return { x: v.x, y: v.y }; });
    var t0 = performance.now();
    var dur = REDUCE ? 0 : 420;
    if (anim) cancelAnimationFrame(anim);
    if (instant || !dur) {
      V.nodes.forEach(function (v, i) { if (targets[i]) { v.x = targets[i].x; v.y = targets[i].y; } });
      anim = null;
      savePositions();
      moved();
      if (!st.userMoved) fit(false);
      draw();
      return;
    }
    function step(now) {
      var p = Math.min(1, (now - t0) / dur);
      var e = 1 - Math.pow(1 - p, 4); // ease-out-quart
      V.nodes.forEach(function (v, i) {
        if (!targets[i]) return;
        v.x = from[i].x + (targets[i].x - from[i].x) * e;
        v.y = from[i].y + (targets[i].y - from[i].y) * e;
      });
      moved();
      if (!st.userMoved) fit(false);
      draw();
      if (p < 1) anim = requestAnimationFrame(step);
      else { anim = null; savePositions(); draw(); }
    }
    anim = requestAnimationFrame(step);
  }
  function relayout(alpha) {
    if (st.layout === 'force') { V.routes = null; startForce(alpha); }
    else layered(REDUCE);
  }

  // ------------------------------------------------------------- rendering
  var canvas = document.getElementById('canvas');
  var overlay = document.getElementById('overlay');
  var mini = document.getElementById('minimap');
  var ctx = canvas.getContext && (canvas.getContext('2d', { alpha: false }) || canvas.getContext('2d'));
  if (!ctx) {
    document.getElementById('empty-text').textContent = 'This browser cannot draw on a canvas, which the graph needs. Open the file in a current Chrome, Firefox, Safari or Edge.';
    document.getElementById('empty-reset').hidden = true;
    document.getElementById('empty').hidden = false;
    return;
  }
  var octx = overlay.getContext('2d');
  var mctx = mini.getContext('2d');
  var stage = document.getElementById('stage');
  var W = 0, H = 0, DPR = 1;
  var theme = {};
  var MONO = 'ui-monospace, "SF Mono", Menlo, Consolas, monospace';
  var SANS = '"Avenir Next", "Segoe UI Variable Text", "Segoe UI", "Helvetica Neue", system-ui, sans-serif';
  function readTheme() {
    var cs = getComputedStyle(document.documentElement);
    ['canvas', 'paper', 'paper-2', 'paper-3', 'rule', 'ink', 'ink-2', 'ink-3', 'focus', 'focus-soft', 'bridge',
     'edge', 'edge-2', 'edge-3', 'edge-4', 'hull', 'hull-line', 'label-halo', 'node-line',
     'l1', 'l2', 'l3', 'l4', 'l5', 'l-other'].forEach(function (k) {
      theme[k] = cs.getPropertyValue('--' + k).trim();
    });
  }
  function famColor(f) { return theme[(STYLE[f] || STYLE.other).tok]; }
  function langColor(lang) { return lang === 'external' ? theme['ink-3'] : theme[langTok(lang)]; }

  function resize() {
    var r = stage.getBoundingClientRect();
    DPR = window.devicePixelRatio || 1;
    W = Math.max(1, r.width); H = Math.max(1, r.height);
    [canvas, overlay].forEach(function (c) { c.width = Math.round(W * DPR); c.height = Math.round(H * DPR); });
    mini.width = Math.round(180 * DPR); mini.height = Math.round(120 * DPR);
    draw();
  }
  function sx(x) { return x * st.cam.k + st.cam.x; }
  function sy(y) { return y * st.cam.k + st.cam.y; }
  function screenR(v) { return Math.max(2, v.r * st.cam.k); }

  var hl = null, hlEdges = null;
  function selectedV() { return st.selected ? V.byId.get(st.selected) || null : null; }
  function computeHighlight() {
    hl = null; hlEdges = null;
    var sel = selectedV();
    if (!sel) return;
    hl = new Map();
    if (st.mode === 'path' && st.pathTo && V.byId.get(st.pathTo)) {
      var p = pathBetween(sel, V.byId.get(st.pathTo));
      hl = p.nodes; hlEdges = p.edges;
      return;
    }
    hl.set(sel.idx, 0);
    var limit = st.mode === 'hops' ? st.hops : Infinity;
    var frontier = [sel.idx];
    for (var h = 1; h <= limit && frontier.length; h++) {
      var next = [];
      frontier.forEach(function (i) {
        var v = V.nodes[i];
        if (st.mode !== 'up') v.outE.forEach(function (e) { if (!hl.has(e.b)) { hl.set(e.b, h); next.push(e.b); } });
        if (st.mode !== 'down') v.inE.forEach(function (e) { if (!hl.has(e.a)) { hl.set(e.a, h); next.push(e.a); } });
      });
      frontier = next;
    }
    hlEdges = new Set();
    V.edges.forEach(function (e, k) {
      if (!hl.has(e.a) || !hl.has(e.b)) return;
      if (st.mode === 'up' && !(hl.get(e.a) > hl.get(e.b))) return;
      if (st.mode === 'down' && !(hl.get(e.b) > hl.get(e.a))) return;
      hlEdges.add(k);
    });
  }
  // Nodes and edges on shortest paths from a to b (directed either way,
  // else undirected).
  function pathBetween(a, b) {
    function bfs(start, dirOut, undirected) {
      var d = new Map([[start.idx, 0]]), q = [start.idx];
      while (q.length) {
        var u = q.shift(), v = V.nodes[u];
        var nb = [];
        if (dirOut || undirected) v.outE.forEach(function (e) { nb.push(e.b); });
        if (!dirOut || undirected) v.inE.forEach(function (e) { nb.push(e.a); });
        nb.forEach(function (w) { if (!d.has(w)) { d.set(w, d.get(u) + 1); q.push(w); } });
      }
      return d;
    }
    var tries = [[a, b, false], [b, a, false], [a, b, true]];
    for (var t = 0; t < tries.length; t++) {
      var s = tries[t][0], g = tries[t][1], und = tries[t][2];
      var ds = bfs(s, true, und), dg = bfs(g, false, und);
      if (!ds.has(g.idx)) continue;
      var dmin = ds.get(g.idx), nodes = new Map(), edges = new Set();
      ds.forEach(function (d, i) { if (dg.has(i) && d + dg.get(i) === dmin) nodes.set(i, d); });
      V.edges.forEach(function (e, k) {
        var ok = function (x, y) { return ds.has(x) && dg.has(y) && ds.get(x) + 1 + dg.get(y) === dmin; };
        if (ok(e.a, e.b) || (und && ok(e.b, e.a))) edges.add(k);
      });
      return { nodes: nodes, edges: edges, found: true, steps: dmin };
    }
    return { nodes: new Map([[a.idx, 0], [b.idx, 0]]), edges: new Set(), found: false, steps: 0 };
  }

  var drawQueued = false;
  function requestDraw() {
    if (drawQueued) return;
    drawQueued = true;
    requestAnimationFrame(function () { drawQueued = false; draw(); });
  }
  var interacting = false, idleTimer = null;
  function markInteracting() {
    interacting = true;
    clearTimeout(idleTimer);
    idleTimer = setTimeout(function () { interacting = false; draw(); }, 160);
  }

  // Folder centroids for edge bundling, keyed by folder prefix.
  var centCache = { ver: -1, nodes: null, map: null };
  function folderCentroids() {
    if (centCache.ver === posVer && centCache.nodes === V.nodes) return centCache.map;
    var c = new Map();
    V.nodes.forEach(function (v) {
      v.anc.forEach(function (p) {
        var e = c.get(p);
        if (!e) { e = { x: 0, y: 0, n: 0 }; c.set(p, e); }
        e.x += v.x; e.y += v.y; e.n++;
      });
    });
    c.forEach(function (e) { e.x /= e.n; e.y /= e.n; });
    centCache = { ver: posVer, nodes: V.nodes, map: c };
    return c;
  }
  // Holten's hierarchical edge bundling: control points through the open
  // folders from one end up to their common ancestor and down to the other,
  // straightened by beta.
  var BETA = 0.82;
  function bundlePoints(e, cents) {
    if (e.bwVer !== posVer) { e.bw = bundleWorld(e, cents); e.bwVer = posVer; }
    if (!e.bw) return null;
    return e.bw.map(function (p) { return { x: sx(p.x), y: sy(p.y) }; });
  }
  function bundleWorld(e, cents) {
    var a = V.nodes[e.a], b = V.nodes[e.b];
    var pa = a.anc, pb = b.anc, k = 0;
    while (k < pa.length && k < pb.length && pa[k] === pb[k]) k++;
    if (pa.length - k + pb.length - k === 0) return null;
    var pts = [{ x: a.x, y: a.y }];
    for (var i = pa.length - 1; i >= k; i--) pts.push(cents.get(pa[i]));
    if (k > 0) pts.push(cents.get(pa[k - 1]));
    for (i = k; i < pb.length; i++) pts.push(cents.get(pb[i]));
    pts.push({ x: b.x, y: b.y });
    var n = pts.length - 1;
    return pts.map(function (p, j) {
      var lx = pts[0].x + (pts[n].x - pts[0].x) * j / n, ly = pts[0].y + (pts[n].y - pts[0].y) * j / n;
      return { x: BETA * p.x + (1 - BETA) * lx, y: BETA * p.y + (1 - BETA) * ly };
    });
  }

  // Screen geometry of a drawn edge: straight, bundled, or a layered route.
  function edgeGeom(e, cents) {
    var a = V.nodes[e.a], b = V.nodes[e.b];
    var g = { x1: sx(a.x), y1: sy(a.y), x2: sx(b.x), y2: sy(b.y), ra: screenR(a), rb: screenR(b), pts: null, type: 'line' };
    if (st.layout === 'layered') {
      var key = e.a + '>' + e.b;
      var r = V.routes && !anim ? V.routes.get(key) : null;
      if (r && r.length) {
        g.pts = r.map(function (p, i) {
          if (i === 0) return { x: g.x1, y: g.y1 };
          if (i === r.length - 1) return { x: g.x2, y: g.y2 };
          return { x: sx(p.x), y: sy(p.y) };
        });
        g.type = 'route';
      } else {
        g.type = 'arc';
        g.c = arcControls(g, a, b);
      }
    } else if (cents && st.group !== 'none') {
      var p = bundlePoints(e, cents);
      if (p) { g.pts = p; g.type = 'bundle'; }
    }
    return g;
  }
  // Same-column edges bow out to the side; back edges arch over both ends.
  function arcControls(g, a, b) {
    var k = st.cam.k, dx = g.x2 - g.x1;
    if (Math.abs(dx) < 40 * k) {
      var bow = (50 + Math.min(140, Math.abs(g.y2 - g.y1) * 0.3)) * k;
      return [g.x1 + bow, g.y1, g.x2 + bow, g.y2];
    }
    var top = Math.min(g.y1, g.y2) - (40 + Math.min(220, Math.abs(dx) * 0.2)) * k;
    var s = dx < 0 ? 1 : -1;
    return [g.x1 + 70 * k * s, top, g.x2 - 70 * k * s, top];
  }
  function pathEdge(c, g) {
    c.moveTo(g.x1, g.y1);
    if (g.type === 'route') {
      for (var i = 1; i < g.pts.length; i++) {
        var p0 = g.pts[i - 1], p1 = g.pts[i];
        var h = (p1.x - p0.x) * 0.5;
        c.bezierCurveTo(p0.x + h, p0.y, p1.x - h, p1.y, p1.x, p1.y);
      }
    } else if (g.type === 'bundle') {
      var pts = g.pts;
      for (var j = 1; j < pts.length - 1; j++) {
        var mx = (pts[j].x + pts[j + 1].x) / 2, my = (pts[j].y + pts[j + 1].y) / 2;
        c.quadraticCurveTo(pts[j].x, pts[j].y, j === pts.length - 2 ? pts[j + 1].x : mx, j === pts.length - 2 ? pts[j + 1].y : my);
      }
    } else if (g.type === 'arc') {
      c.bezierCurveTo(g.c[0], g.c[1], g.c[2], g.c[3], g.x2, g.y2);
    } else {
      c.lineTo(g.x2, g.y2);
    }
  }
  // Direction without arrowheads: a straight solid edge is a wedge, wide at
  // its source and narrow at its target.
  function pathTaper(c, g, w) {
    var dx = g.x2 - g.x1, dy = g.y2 - g.y1, l = Math.sqrt(dx * dx + dy * dy) || 1;
    var px = -dy / l, py = dx / l, w1 = w * 0.9, w2 = Math.max(0.35, w * 0.2);
    c.moveTo(g.x1 + px * w1, g.y1 + py * w1);
    c.lineTo(g.x2 + px * w2, g.y2 + py * w2);
    c.lineTo(g.x2 - px * w2, g.y2 - py * w2);
    c.lineTo(g.x1 - px * w1, g.y1 - py * w1);
    c.closePath();
  }
  function endTangent(g) {
    var dx, dy;
    if (g.type === 'route') { var p = g.pts[g.pts.length - 2]; dx = g.x2 - p.x; dy = 0; if (!dx) dx = 1; }
    else if (g.type === 'bundle') { var q = g.pts[g.pts.length - 2]; dx = g.x2 - q.x; dy = g.y2 - q.y; }
    else if (g.type === 'arc') { dx = g.x2 - g.c[2]; dy = g.y2 - g.c[3]; }
    else { dx = g.x2 - g.x1; dy = g.y2 - g.y1; }
    var l = Math.sqrt(dx * dx + dy * dy) || 1;
    return [dx / l, dy / l];
  }
  function pathArrow(c, g, size) {
    var t = endTangent(g);
    var bx = g.x2 - t[0] * (g.rb + 2), by = g.y2 - t[1] * (g.rb + 2);
    var px = -t[1], py = t[0];
    c.moveTo(bx, by);
    c.lineTo(bx - t[0] * size + px * size * 0.48, by - t[1] * size + py * size * 0.48);
    c.lineTo(bx - t[0] * size - px * size * 0.48, by - t[1] * size - py * size * 0.48);
    c.closePath();
  }
  function edgeMid(g) {
    if (g.pts && g.pts.length > 2) return g.pts[Math.floor(g.pts.length / 2)];
    if (g.type === 'arc') return { x: (g.x1 + 3 * g.c[0] + 3 * g.c[2] + g.x2) / 8, y: (g.y1 + 3 * g.c[1] + 3 * g.c[3] + g.y2) / 8 };
    return { x: (g.x1 + g.x2) / 2, y: (g.y1 + g.y2) / 2 };
  }
  function edgeWidth(e, emph) {
    var s = STYLE[e.fam] || STYLE.other;
    var w = Math.min(6, Math.max(1, 1 + Math.log2(Math.max(1, e.w)) * 0.8)) * s.w;
    if (BRIDGE[e.fam]) w = Math.min(4, w);
    return emph >= 2 ? w * 1.25 : w;
  }
  function edgeAlpha(e, emph) {
    if (emph === 0) return 0.07;
    if (emph >= 2) return 0.95;
    if (BRIDGE[e.fam]) return 0.85;
    var many = V.edges.length > 8000 ? 0.45 : V.edges.length > 2500 ? 0.7 : 1;
    return many * (0.3 + 0.4 * Math.min(1, Math.log2(e.w + 1) / 5));
  }

  var lastGeom = null; // edge index -> geometry of the last frame, for hit tests
  function draw(target) {
    var c = target || ctx;
    var w = target ? target.canvas.width / DPR : W, h = target ? target.canvas.height / DPR : H;
    c.setTransform(DPR, 0, 0, DPR, 0, 0);
    c.fillStyle = theme.canvas;
    c.fillRect(0, 0, w, h);
    lastGeom = new Array(V.edges.length);
    if (!V.nodes.length) { if (!target) { drawOverlay(); drawMinimap(); } return; }
    var pad = 40;
    function onScreen(x, y, r) { return x > -r - pad && y > -r - pad && x < w + r + pad && y < h + r + pad; }

    var hullLabels = [];
    if (st.layout === 'force' && st.group !== 'none') hullLabels = drawHulls(c, w, h);
    if (st.layout === 'layered' && V.cols && !anim) drawBands(c, h);

    var bundling = st.layout === 'force' && st.group !== 'none' && V.edges.length < 3000 && !interacting;
    var cents = bundling ? folderCentroids() : null;
    var batches = new Map(), arrows = [], tags = [];
    var lod = interacting && V.edges.length > 1500;
    var dashCheap = V.edges.length > 1200 && st.cam.k < 0.9;
    V.edges.forEach(function (e, idx) {
      if (lod && e.w < V.wCut && !(hlEdges && hlEdges.has(idx))) return;
      var a = V.nodes[e.a], b = V.nodes[e.b];
      var x1 = sx(a.x), y1 = sy(a.y), x2 = sx(b.x), y2 = sy(b.y);
      var routed = st.layout === 'layered' || bundling;
      if (!routed && (Math.max(x1, x2) < -pad || Math.min(x1, x2) > w + pad || Math.max(y1, y2) < -pad || Math.min(y1, y2) > h + pad)) return;
      var g = edgeGeom(e, cents);
      lastGeom[idx] = g;
      var emph = hl ? (hlEdges.has(idx) ? 2 : 0) : 1;
      if (st.selectedEdge === e) emph = 3;
      var s = STYLE[e.fam] || STYLE.other;
      var taper = s.solid && g.type === 'line' && emph < 2;
      var back = g.type === 'arc';
      var wpx = Math.round(edgeWidth(e, emph) * 2) / 2;
      var alpha = edgeAlpha(e, emph) * (back && emph < 2 ? 0.45 : 1);
      var key = e.fam + '|' + emph + '|' + (taper ? 't' : back ? 'b' : 's') + '|' + wpx + '|' + Math.max(1, Math.round(alpha * 10));
      var list = batches.get(key);
      if (!list) { list = []; batches.set(key, list); }
      list.push(g);
      if (emph >= 2 || (!hl && st.cam.k > 1.6 && !taper && V.edges.length < 600)) arrows.push([e, g, emph]);
      if (CROSS[e.fam] && emph > 0 && derived(e.refs[0])) tags.push([e, g]);
    });
    var keys = Array.from(batches.keys()).sort(function (a, b) {
      var pa = a.split('|'), pb = b.split('|');
      return (+pa[1]) - (+pb[1]) || (BRIDGE[pa[0]] ? 1 : 0) - (BRIDGE[pb[0]] ? 1 : 0);
    });
    keys.forEach(function (key) {
      var parts = key.split('|'), fam = parts[0], emph = +parts[1], mode = parts[2], wpx = +parts[3];
      var alpha = +parts[4] / 10;
      var s = STYLE[fam] || STYLE.other;
      var list = batches.get(key);
      c.globalAlpha = alpha;
      c.strokeStyle = famColor(fam);
      c.fillStyle = famColor(fam);
      c.beginPath();
      if (mode === 't') {
        // One fill per wedge keeps the rasteriser off a huge self-crossing
        // path (filling thousands of overlapping polygons as one path costs
        // ~20 ms); small batches are cheaper still.
        list.forEach(function (g, gi) {
          pathTaper(c, g, wpx);
          if (gi % 64 === 63) { c.fill(); c.beginPath(); }
        });
        c.fill();
      } else {
        c.lineWidth = wpx;
        c.lineCap = s.round ? 'round' : 'butt';
        // Dashes are what costs on canvas; on a dense, far-out view code
        // edges are drawn solid (their dash is unreadable there anyway).
        // Bridges keep theirs at every zoom.
        var plain = !BRIDGE[fam] && emph < 2 && dashCheap;
        c.setLineDash(plain ? [] : mode === 'b' && !s.dash.length ? [3, 4] : s.dash);
        list.forEach(function (g) { pathEdge(c, g); });
        c.stroke();
        c.setLineDash([]);
      }
    });
    c.globalAlpha = 1;
    // Arrowheads only where the eye is: the highlighted neighbourhood.
    arrows.forEach(function (it) {
      var e = it[0], g = it[1], s = STYLE[e.fam] || STYLE.other;
      c.beginPath();
      pathArrow(c, g, it[2] >= 2 ? 8 : 6);
      if (s.hollow) {
        c.fillStyle = theme.canvas; c.fill();
        c.strokeStyle = famColor(e.fam); c.lineWidth = 1.3; c.stroke();
      } else {
        c.fillStyle = famColor(e.fam); c.fill();
      }
    });
    // Transport tags on folder-to-folder bridges.
    if (tags.length && tags.length < 400) {
      c.font = '600 10.5px ' + SANS;
      c.textAlign = 'center';
      c.textBaseline = 'middle';
      tags.forEach(function (it) {
        var e = it[0], m = edgeMid(it[1]);
        var text = (STYLE[e.fam].tag || e.fam) + (e.w > 1 ? ' ×' + e.w : '');
        var tw = c.measureText(text).width + 10;
        c.fillStyle = theme.canvas;
        c.globalAlpha = 0.92;
        c.fillRect(Math.round(m.x - tw / 2), Math.round(m.y - 8), Math.round(tw), 16);
        c.globalAlpha = 1;
        c.strokeStyle = famColor(e.fam);
        c.lineWidth = 1;
        c.strokeRect(Math.round(m.x - tw / 2) + 0.5, Math.round(m.y - 8) + 0.5, Math.round(tw) - 1, 15);
        c.fillStyle = famColor(e.fam);
        c.fillText(text, Math.round(m.x), Math.round(m.y));
      });
    }
    if (hlEdges && st.cam.k > 1.4 && hlEdges.size <= 30) {
      c.font = '10.5px ' + MONO;
      c.textAlign = 'center';
      c.textBaseline = 'middle';
      hlEdges.forEach(function (idx) {
        var g = lastGeom[idx];
        if (!g) return;
        var e = V.edges[idx], m = edgeMid(g);
        var text = e.kind + (e.kinds[e.kind] > 1 ? ' ×' + e.kinds[e.kind] : '') + (Object.keys(e.kinds).length > 1 ? ' +' + (Object.keys(e.kinds).length - 1) : '');
        c.lineWidth = 3; c.strokeStyle = theme['label-halo']; c.strokeText(text, m.x, m.y);
        c.fillStyle = famColor(e.fam); c.fillText(text, m.x, m.y);
      });
    }
    var labelled = [];
    V.nodes.forEach(function (v) {
      var x = Math.round(sx(v.x)), y = Math.round(sy(v.y)), r = screenR(v);
      if (!onScreen(x, y, r * 1.6)) return;
      var dim = hl && !hl.has(v.idx);
      c.globalAlpha = dim ? 0.18 : 1;
      drawNode(c, v, x, y, r);
      if (!dim) labelled.push(v);
    });
    c.globalAlpha = 1;
    drawLabels(c, labelled, w, h, hullLabels);
    if (!target) { drawOverlay(); drawMinimap(); }
  }

  function roundRect(c, x, y, w, h, r) {
    c.moveTo(x + r, y);
    c.lineTo(x + w - r, y); c.quadraticCurveTo(x + w, y, x + w, y + r);
    c.lineTo(x + w, y + h - r); c.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
    c.lineTo(x + r, y + h); c.quadraticCurveTo(x, y + h, x, y + h - r);
    c.lineTo(x, y + r); c.quadraticCurveTo(x, y, x + r, y);
    c.closePath();
  }
  function hexagon(c, x, y, r) {
    for (var i = 0; i < 6; i++) {
      var a = Math.PI / 6 + i * Math.PI / 3;
      if (i) c.lineTo(x + Math.cos(a) * r, y + Math.sin(a) * r);
      else c.moveTo(x + Math.cos(a) * r, y + Math.sin(a) * r);
    }
    c.closePath();
  }
  function clusterBox(v, x, y, r) { return { x: x - r * 1.25, y: y - r * 0.78, w: r * 2.5, h: r * 1.56 }; }
  // Folder language mix, largest first: [[lang, share], ...].
  // A closed folder's direct sub-folders (or files) with their sizes.
  function childSummary(v) {
    var counts = new Map();
    v.members.forEach(function (m) {
      var i = m.chain.indexOf(v.cluster);
      var next = i >= 0 && i + 1 < m.chain.length ? m.chain[i + 1] : null;
      var key = next ? next.slice(v.cluster.length) : m.name;
      counts.set(key, (counts.get(key) || 0) + 1);
    });
    return Array.from(counts.entries()).sort(function (a, b) { return b[1] - a[1] || (a[0] < b[0] ? -1 : 1); });
  }
  function langMix(members) {
    var counts = {}, total = 0;
    members.forEach(function (m) {
      if (has(m, F.external | F.endpoint)) return;
      var k = langTok(m.lang);
      counts[k] = (counts[k] || 0) + Math.max(1, m.size || 1);
      total += Math.max(1, m.size || 1);
    });
    return Object.keys(counts).sort(function (a, b) { return counts[b] - counts[a]; })
      .map(function (k) { return [k, counts[k] / (total || 1)]; });
  }

  function drawNode(c, v, x, y, r) {
    var n = v.leaf;
    var k = st.cam.k;
    c.lineWidth = 1;
    if (v.cluster !== null) {
      // A closed folder: an inked box, its language mix as a strip along the
      // bottom, its name inside when there is room.
      var b = clusterBox(v, x, y, r);
      c.beginPath();
      roundRect(c, b.x, b.y, b.w, b.h, Math.min(7, r * 0.3));
      c.fillStyle = theme.paper;
      c.fill();
      c.lineWidth = v.folded ? 1 : 1.6;
      c.setLineDash(v.folded ? [4, 3] : []);
      c.strokeStyle = theme.ink;
      c.stroke();
      c.setLineDash([]);
      var mix = v.mix || (v.mix = langMix(v.members));
      var sh = Math.max(2, Math.min(5, b.h * 0.12)), sx0 = b.x + 1.5, sw = b.w - 3;
      c.save();
      c.beginPath();
      roundRect(c, b.x, b.y, b.w, b.h, Math.min(7, r * 0.3));
      c.clip();
      mix.forEach(function (m) {
        c.fillStyle = theme[m[0]];
        c.fillRect(sx0, b.y + b.h - sh - 1, sw * m[1], sh);
        sx0 += sw * m[1];
      });
      c.restore();
      if (b.h > 64 && b.w > 90) {
        var kids = v.kids || (v.kids = childSummary(v));
        var lines = Math.min(kids.length, Math.floor((b.h - 48) / 14)); // clear of the count below
        c.font = '11px ' + SANS;
        c.textAlign = 'left';
        c.textBaseline = 'top';
        c.fillStyle = theme['ink-3'];
        // Name on the left, cut with an ellipsis to fit; count right-aligned,
        // both inside the box.
        var left = b.x + 8, right = b.x + b.w - 8;
        for (var li2 = 0; li2 < lines; li2++) {
          var ly = b.y + 24 + li2 * 14;
          if (li2 === lines - 1 && kids.length > lines && lines > 1) {
            c.textAlign = 'left';
            c.fillText('+' + (kids.length - lines + 1) + ' more', left, ly);
            continue;
          }
          var cnt = fmt(kids[li2][1]);
          c.textAlign = 'right';
          c.fillText(cnt, right, ly);
          var room = right - left - c.measureText(cnt).width - 8;
          var nm = kids[li2][0];
          if (c.measureText(nm).width > room) {
            while (nm.length > 1 && c.measureText(nm + '…').width > room) nm = nm.slice(0, -1);
            nm += '…';
          }
          c.textAlign = 'left';
          if (room > 12) c.fillText(nm, left, ly);
        }
      }
      if (b.h > 22) {
        c.fillStyle = theme['ink-3'];
        c.font = '600 ' + Math.min(12, Math.max(9, b.h * 0.26)).toFixed(1) + 'px ' + SANS;
        c.textAlign = 'right';
        c.textBaseline = 'bottom';
        c.fillText((v.folded ? '+' : '') + fmt(v.members.length), b.x + b.w - 6, b.y + b.h - sh - 3);
      }
    } else if (has(n, F.endpoint)) {
      c.beginPath();
      hexagon(c, x, y, r * 1.2);
      c.fillStyle = theme.bridge;
      c.fill();
      c.strokeStyle = theme['node-line'];
      c.stroke();
    } else if (has(n, F.external | F.unresolved)) {
      c.beginPath();
      if (has(n, F.unresolved)) c.arc(x, y, r, 0, Math.PI * 2); else hexagon(c, x, y, r * 1.1);
      c.fillStyle = theme.canvas;
      c.fill();
      c.setLineDash(has(n, F.unresolved) ? [2, 2] : []);
      c.strokeStyle = theme['ink-3'];
      c.lineWidth = 1.2;
      c.stroke();
      c.setLineDash([]);
    } else if (AGGREGATED && (n.kind === 'file' || n.kind === 'module')) {
      c.beginPath();
      roundRect(c, x - r, y - r * 0.8, r * 2, r * 1.6, Math.min(4, r * 0.3));
      c.fillStyle = nodeColor(n);
      c.fill();
      c.strokeStyle = theme['node-line'];
      c.stroke();
    } else {
      c.beginPath();
      c.arc(x, y, r, 0, Math.PI * 2);
      c.fillStyle = nodeColor(n);
      c.fill();
      c.strokeStyle = theme['node-line'];
      c.stroke();
      if (has(n, F.test) && r > 3) {
        c.beginPath();
        c.arc(x, y, r * 0.45, 0, Math.PI * 2);
        c.fillStyle = theme.canvas;
        c.fill();
      }
    }
    if (n && has(n, F.halo)) {
      c.beginPath();
      c.arc(x, y, r + 3, 0, Math.PI * 2);
      c.setLineDash([2, 3]);
      c.strokeStyle = theme['ink-3'];
      c.lineWidth = 1;
      c.stroke();
      c.setLineDash([]);
    }
    if (v.seed) {
      c.beginPath();
      if (v.cluster !== null) { var bb = clusterBox(v, x, y, r); roundRect(c, bb.x - 4, bb.y - 4, bb.w + 8, bb.h + 8, 9); }
      else c.arc(x, y, r + 4, 0, Math.PI * 2);
      c.strokeStyle = theme.focus;
      c.lineWidth = 2.2;
      c.stroke();
    }
    if (st.selected === v.id) {
      c.beginPath();
      if (v.cluster !== null) { var sb = clusterBox(v, x, y, r); roundRect(c, sb.x - 6, sb.y - 6, sb.w + 12, sb.h + 12, 10); }
      else c.arc(x, y, r + 7, 0, Math.PI * 2);
      c.fillStyle = theme['focus-soft'];
      c.fill();
      c.strokeStyle = theme.focus;
      c.lineWidth = 2.5;
      c.stroke();
    }
    if (v.pinned) {
      c.beginPath();
      c.arc(x - r - 2, y - r - 2, 2.6, 0, Math.PI * 2);
      c.fillStyle = theme.ink;
      c.fill();
      c.beginPath();
      c.moveTo(x - r - 2, y - r + 0.5); c.lineTo(x - r + 0.5, y - r + 3);
      c.strokeStyle = theme.ink; c.lineWidth = 1.2; c.stroke();
    }
    // "+N": neighbours embedded but not shown (halo, scope, filters).
    // Shown where the eye is (selection, its neighbourhood, hover) or zoomed in.
    var badgeHere = st.selected === v.id || st.hoverNode === v || (hl && hl.has(v.idx) && hl.get(v.idx) <= 1) || k > 1.8;
    if (v.hidden && badgeHere && r > 3.5) {
      var t = '+' + v.hidden;
      c.font = '600 9.5px ' + SANS;
      c.textAlign = 'left';
      c.textBaseline = 'middle';
      var tw = c.measureText(t).width + 6;
      var bx = x - r * 0.7 - tw, by = y - r - 5;
      c.beginPath();
      roundRect(c, bx, by - 6, tw, 12, 6);
      c.fillStyle = theme.paper;
      c.fill();
      c.strokeStyle = theme['ink-3'];
      c.lineWidth = 0.8;
      c.stroke();
      c.fillStyle = theme['ink-2'];
      c.fillText(t, bx + 3, by);
    }
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
  var hullCache = { ver: -1, nodes: null, list: null };
  // Folder outlines in world space, recomputed only when nodes move.
  function hullsWorld() {
    if (hullCache.ver === posVer && hullCache.nodes === V.nodes) return hullCache.list;
    var groups = new Map();
    V.nodes.forEach(function (v) {
      if (v.group === null || v.group === undefined || PSEUDO[v.group]) return;
      var l = groups.get(v.group);
      if (!l) { l = []; groups.set(v.group, l); }
      l.push(v);
    });
    var list = [];
    groups.forEach(function (members, g) {
      if (members.length < 2) return;
      var pts = [];
      members.forEach(function (v) {
        var r = v.cluster !== null ? v.r * 1.3 : v.r;
        pts.push([v.x - r, v.y], [v.x + r, v.y], [v.x, v.y - r], [v.x, v.y + r]);
      });
      var minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
      pts.forEach(function (p) {
        if (p[0] < minx) minx = p[0]; if (p[1] < miny) miny = p[1];
        if (p[0] > maxx) maxx = p[0]; if (p[1] > maxy) maxy = p[1];
      });
      if (Math.hypot(maxx - minx, maxy - miny) > 130 * Math.sqrt(members.length) + 160) return;
      list.push({ g: g, members: members, hull: convexHull(pts), box: [minx, miny, maxx, maxy] });
    });
    hullCache = { ver: posVer, nodes: V.nodes, list: list };
    return list;
  }
  function drawHulls(c, w, h) {
    var k = st.cam.k, labels = [];
    c.lineJoin = 'round';
    hullsWorld().forEach(function (it) {
      var members = it.members, g = it.g;
      var minx = sx(it.box[0]), miny = sy(it.box[1]), maxx = sx(it.box[2]), maxy = sy(it.box[3]);
      if (maxx < -50 || minx > w + 50 || maxy < -50 || miny > h + 50) return;
      var hull = it.hull.map(function (p) { return [sx(p[0]), sy(p[1])]; });
      var padPx = Math.max(8, 12 * k);
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
      if (!dim) labels.push({ id: 'h:' + g, text: folderName(g), x: minx - padPx + 4, y: miny - padPx - 3, prio: members.length * 3, hull: true });
    });
    c.globalAlpha = 1;
    c.lineJoin = 'miter';
    return labels;
  }
  // Layered hop bands: a faint tint behind every other column, with headers.
  function drawBands(c, h) {
    c.font = '600 11px ' + SANS;
    c.textAlign = 'left';
    c.textBaseline = 'bottom';
    V.cols.forEach(function (cl, i) {
      if (!cl.n) return;
      var x0 = sx(cl.x0), x1 = sx(cl.x1);
      if (i % 2 === 1) {
        c.fillStyle = theme.hull;
        c.fillRect(Math.round(x0), 0, Math.round(x1 - x0), h);
      }
      var text = TRACE ? (i === 0 ? 'seed' : i + (i === 1 ? ' hop' : ' hops')) : 'layer ' + (i + 1);
      c.fillStyle = theme['ink-3'];
      c.fillText(text + ' · ' + fmt(cl.n), Math.round(x0 + 6), Math.round(sy(cl.top) - 10));
    });
  }

  var labelWidths = new Map();
  function labelText(v) {
    if (v.cluster !== null) return v.label;
    var t = v.label;
    // Zoomed in, a symbol also names its owner (file or type).
    if (st.cam.k > 2.2 && v.leaf && !AGGREGATED && v.leaf.qual !== v.leaf.name) {
      var q = v.leaf.qual, i = Math.max(q.lastIndexOf('.', q.length - v.leaf.name.length - 2), q.lastIndexOf(':', q.length - v.leaf.name.length - 3));
      if (i > 0) t = q.slice(i + 1).replace(/^:+/, '');
    }
    return t.length > 52 ? t.slice(0, 51) + '…' : t;
  }
  // Semantic zoom plus a greedy no-overlap pass: highest priority first, a
  // label is skipped when it would overlap one already placed.
  function drawLabels(c, list, w, h, hullLabels) {
    if (!st.showLabels) return;
    var k = st.cam.k;
    var important = function (v) {
      return st.selected === v.id || st.hoverNode === v || v.seed || (st.pathTo === v.id);
    };
    var near = function (v) { return hl && hl.has(v.idx) && hl.get(v.idx) <= 1; };
    var tier = function (v) {
      if (v.cluster !== null) return true;
      if (st.layout === 'layered') return k >= 0.45;
      return screenR(v) >= 5 && k >= 0.5;
    };
    var cand = list.filter(function (v) { return important(v) || near(v) || tier(v); }).map(function (v) {
      var prio = important(v) ? 1e9 : near(v) ? 1e7 + v.deg : v.cluster !== null ? 1e5 + v.members.length * 4 + v.deg : v.deg * 2 + v.r;
      if (st.layout === 'layered' && v.hop !== undefined && prio < 1e7) prio += (30 - Math.min(30, v.hop)) * 1e3;
      return { v: v, prio: prio };
    });
    (hullLabels || []).forEach(function (x) { cand.push({ hull: x, prio: x.prio }); });
    cand.sort(function (a, b) { return b.prio - a.prio; });
    if (cand.length > 1200) cand.length = 1200;
    var cell = 16, grid = new Set();
    function free(x0, y0, x1, y1) {
      for (var gx = Math.floor(x0 / cell); gx <= Math.floor(x1 / cell); gx++) {
        for (var gy = Math.floor(y0 / cell); gy <= Math.floor(y1 / cell); gy++) if (grid.has(gx + ',' + gy)) return false;
      }
      return true;
    }
    function take(x0, y0, x1, y1) {
      for (var gx = Math.floor(x0 / cell); gx <= Math.floor(x1 / cell); gx++) {
        for (var gy = Math.floor(y0 / cell); gy <= Math.floor(y1 / cell); gy++) grid.add(gx + ',' + gy);
      }
    }
    c.textBaseline = 'middle';
    c.textAlign = 'left';
    c.lineJoin = 'round';
    cand.forEach(function (item) {
      var v = item.v, hullItem = item.hull, text, id, x, y, font;
      if (hullItem) { text = hullItem.text; id = hullItem.id; x = hullItem.x; y = hullItem.y; font = '700 11.5px ' + SANS; }
      else {
        text = labelText(v); id = v.id + (k > 2.2 ? '+' : '');
        font = v.cluster !== null ? '700 12.5px ' + SANS : (v.seed ? '700 12px ' : '12px ') + SANS;
      }
      c.font = font;
      var tw = labelWidths.get(id);
      if (tw === undefined) { tw = c.measureText(text).width; labelWidths.set(id, tw); }
      if (!hullItem) {
        var r = screenR(v);
        if (v.cluster !== null) {
          var b = clusterBox(v, sx(v.x), sy(v.y), r);
          if (b.h > 22) {
            // Inside the box; a name too long for it at this zoom is cut.
            x = b.x + 7; y = b.y + 12;
            if (tw > b.w - 14) {
              while (text.length > 2 && c.measureText(text + '…').width > b.w - 14) text = text.slice(0, -1);
              text += '…';
              tw = c.measureText(text).width;
            }
          } else { x = sx(v.x) - tw / 2; y = b.y + b.h + 10; }
        } else { x = sx(v.x) + r + 5; y = sy(v.y); }
      }
      x = Math.round(x); y = Math.round(y);
      if (x > w || x + tw < 0 || y < -10 || y > h + 10) return;
      var imp = !hullItem && important(v);
      if (!imp && !free(x, y - 7, x + tw, y + 7)) return;
      take(x, y - 7, x + tw, y + 7);
      c.lineWidth = 3.5;
      c.strokeStyle = theme['label-halo'];
      c.strokeText(text, x, y);
      c.fillStyle = hullItem ? theme['ink-3'] : theme.ink;
      c.fillText(text, x, y);
    });
    c.lineJoin = 'miter';
  }

  // Overlay: hover feedback only, so pointer moves never repaint the graph.
  function drawOverlay() {
    octx.setTransform(DPR, 0, 0, DPR, 0, 0);
    octx.clearRect(0, 0, W, H);
    var v = st.hoverNode;
    if (v && V.byId.get(v.id) === v) {
      var x = sx(v.x), y = sy(v.y), r = screenR(v);
      octx.beginPath();
      if (v.cluster !== null) { var b = clusterBox(v, x, y, r); roundRect(octx, b.x - 4, b.y - 4, b.w + 8, b.h + 8, 9); }
      else octx.arc(x, y, r + 5, 0, Math.PI * 2);
      octx.strokeStyle = theme.ink;
      octx.lineWidth = 1.5;
      octx.stroke();
    }
    var e = st.hoverEdge;
    if (e && lastGeom && lastGeom[e.idx]) {
      var g = lastGeom[e.idx];
      octx.beginPath();
      pathEdge(octx, g);
      octx.strokeStyle = famColor(e.fam);
      octx.globalAlpha = 0.35;
      octx.lineWidth = edgeWidth(e, 2) + 5;
      octx.stroke();
      octx.globalAlpha = 1;
      octx.beginPath();
      pathArrow(octx, g, 8);
      octx.fillStyle = famColor(e.fam);
      octx.fill();
    }
    if (st.picking) {
      octx.font = '600 12px ' + SANS;
      octx.fillStyle = theme.focus;
      octx.textAlign = 'center';
      octx.fillText('Pick the other end of the path: click a node or search', W / 2, 24);
    }
  }

  // Minimap: every drawn node as a dot and the viewport as a frame.
  var miniView = null;
  function drawMinimap() {
    if (mini.offsetParent === null) return;
    var mw = 180, mh = 120;
    mctx.setTransform(DPR, 0, 0, DPR, 0, 0);
    mctx.fillStyle = theme.paper;
    mctx.fillRect(0, 0, mw, mh);
    if (!V.nodes.length) return;
    var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    V.nodes.forEach(function (v) {
      x0 = Math.min(x0, v.x - v.r); y0 = Math.min(y0, v.y - v.r);
      x1 = Math.max(x1, v.x + v.r); y1 = Math.max(y1, v.y + v.r);
    });
    var s = Math.min((mw - 12) / Math.max(1, x1 - x0), (mh - 12) / Math.max(1, y1 - y0));
    var ox = (mw - (x1 - x0) * s) / 2 - x0 * s, oy = (mh - (y1 - y0) * s) / 2 - y0 * s;
    miniView = { s: s, ox: ox, oy: oy };
    V.nodes.forEach(function (v) {
      mctx.fillStyle = v.seed || st.selected === v.id ? theme.focus : v.cluster !== null ? theme['ink-2'] : theme['ink-3'];
      var r = Math.max(1, v.r * s * (v.cluster !== null ? 1.2 : 1));
      mctx.fillRect(Math.round(v.x * s + ox - r / 2), Math.round(v.y * s + oy - r / 2), Math.ceil(r), Math.ceil(r));
    });
    var vx0 = (-st.cam.x / st.cam.k) * s + ox, vy0 = (-st.cam.y / st.cam.k) * s + oy;
    mctx.strokeStyle = theme.focus;
    mctx.lineWidth = 1.5;
    mctx.strokeRect(vx0, vy0, W / st.cam.k * s, H / st.cam.k * s);
  }
  function miniMove(ev) {
    if (!miniView) return;
    var r = mini.getBoundingClientRect();
    var wx = (ev.clientX - r.left - miniView.ox) / miniView.s, wy = (ev.clientY - r.top - miniView.oy) / miniView.s;
    st.cam.x = W / 2 - wx * st.cam.k;
    st.cam.y = H / 2 - wy * st.cam.k;
    st.userMoved = true;
    markInteracting();
    requestDraw();
  }
  var miniDrag = false;
  mini.addEventListener('pointerdown', function (ev) { miniDrag = true; mini.setPointerCapture(ev.pointerId); miniMove(ev); });
  mini.addEventListener('pointermove', function (ev) { if (miniDrag) miniMove(ev); });
  mini.addEventListener('pointerup', function () { miniDrag = false; });

  // ------------------------------------------------------------- hit tests
  // World-space grid over drawn nodes, rebuilt only when positions changed.
  var grid = null, gridDirty = true, GRID_CELL = 60;
  // Bumped whenever a node moves: world-space caches (bundles, hulls,
  // centroids) are rebuilt only then, so a pan or zoom just re-projects.
  var posVer = 0;
  function moved() { gridDirty = true; posVer++; }
  function buildGrid() {
    grid = new Map();
    V.nodes.forEach(function (v) {
      var r = v.cluster !== null ? v.r * 1.3 : v.r;
      for (var gx = Math.floor((v.x - r) / GRID_CELL); gx <= Math.floor((v.x + r) / GRID_CELL); gx++) {
        for (var gy = Math.floor((v.y - r) / GRID_CELL); gy <= Math.floor((v.y + r) / GRID_CELL); gy++) {
          var key = gx + ',' + gy, l = grid.get(key);
          if (!l) { l = []; grid.set(key, l); }
          l.push(v);
        }
      }
    });
    gridDirty = false;
  }
  function nodeAt(px, py) {
    if (gridDirty || !grid) buildGrid();
    var wx = (px - st.cam.x) / st.cam.k, wy = (py - st.cam.y) / st.cam.k;
    var reach = Math.max(1, Math.ceil((8 / st.cam.k) / GRID_CELL));
    var best = null, bestD = Infinity;
    var gx0 = Math.floor(wx / GRID_CELL), gy0 = Math.floor(wy / GRID_CELL);
    for (var gx = gx0 - reach; gx <= gx0 + reach; gx++) {
      for (var gy = gy0 - reach; gy <= gy0 + reach; gy++) {
        (grid.get(gx + ',' + gy) || []).forEach(function (v) {
          var x = sx(v.x), y = sy(v.y), r = screenR(v);
          var d;
          if (v.cluster !== null) {
            var b = clusterBox(v, x, y, r);
            if (px < b.x - 3 || px > b.x + b.w + 3 || py < b.y - 3 || py > b.y + b.h + 3) return;
            d = Math.hypot(px - x, py - y) * 0.5;
          } else {
            d = Math.hypot(px - x, py - y);
            if (d > r + 4) return;
          }
          if (d < bestD) { best = v; bestD = d; }
        });
      }
    }
    return best;
  }
  function segDist2(px, py, x1, y1, x2, y2) {
    var dx = x2 - x1, dy = y2 - y1, l = dx * dx + dy * dy;
    var t = l ? ((px - x1) * dx + (py - y1) * dy) / l : 0;
    t = Math.max(0, Math.min(1, t));
    var x = x1 + t * dx - px, y = y1 + t * dy - py;
    return x * x + y * y;
  }
  function geomPolyline(g) {
    if (g.type === 'line') return [{ x: g.x1, y: g.y1 }, { x: g.x2, y: g.y2 }];
    if (g.type === 'arc') {
      var out = [];
      for (var s = 0; s <= 12; s++) {
        var t = s / 12, u = 1 - t;
        out.push({ x: u * u * u * g.x1 + 3 * u * u * t * g.c[0] + 3 * u * t * t * g.c[2] + t * t * t * g.x2,
                   y: u * u * u * g.y1 + 3 * u * u * t * g.c[1] + 3 * u * t * t * g.c[3] + t * t * t * g.y2 });
      }
      return out;
    }
    return g.pts;
  }
  function edgeAt(px, py) {
    if (!lastGeom) return null;
    var best = null, bestD = 36;
    for (var i = 0; i < lastGeom.length; i++) {
      var g = lastGeom[i];
      if (!g || (hlEdges && !hlEdges.has(i))) continue;
      var pts = geomPolyline(g);
      for (var j = 1; j < pts.length; j++) {
        var a = pts[j - 1], b = pts[j];
        if (px < Math.min(a.x, b.x) - 6 || px > Math.max(a.x, b.x) + 6 || py < Math.min(a.y, b.y) - 6 || py > Math.max(a.y, b.y) + 6) continue;
        var d = segDist2(px, py, a.x, a.y, b.x, b.y);
        if (d < bestD) { bestD = d; best = V.edges[i]; }
      }
    }
    return best;
  }

  // ------------------------------------------------------------ navigation
  function clampK(k) { return Math.max(0.02, Math.min(8, k)); }
  function zoomAt(px, py, factor) {
    var v = st.cam, k = clampK(v.k * factor), f = k / v.k;
    v.x = px - (px - v.x) * f;
    v.y = py - (py - v.y) * f;
    v.k = k;
    st.userMoved = true;
    markInteracting();
    requestDraw();
    setStatus();
  }
  // Fit the view to every drawn node, or to `only` (node indices).
  // Labels are drawn at a fixed screen size, so the fit reserves their
  // width in pixels (not world units) beyond the rightmost nodes.
  function labelPx(v) { return v.cluster !== null ? 0 : Math.min(400, labelText(v).length * 7) + 8; }
  function fit(markUser, only) {
    if (!V.nodes.length) return;
    var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    var list = only ? only.map(function (i) { return V.nodes[i]; }) : V.nodes;
    list.forEach(function (v) {
      var r = v.cluster !== null ? v.r * 1.3 : v.r;
      x0 = Math.min(x0, v.x - r); y0 = Math.min(y0, v.y - r);
      x1 = Math.max(x1, v.x + r);
      y1 = Math.max(y1, v.y + r + (v.cluster !== null ? 18 : 0));
    });
    var layeredView = st.layout === 'layered' && !only;
    if (layeredView) y0 -= 40;
    var avail = W, ww = Math.max(1, x1 - x0), wh = Math.max(1, y1 - y0);
    // Largest label overhang among nodes in the rightmost tenth.
    var over = 0;
    list.forEach(function (v) { if (v.x >= x1 - ww * 0.1 - 1) over = Math.max(over, labelPx(v)); });
    var k = clampK(Math.min((avail - 80 - over) / ww, (H - 60) / wh, 2.2));
    // A label beyond the right edge at this zoom tightens it further.
    list.forEach(function (v) {
      var room = avail - 40 - labelPx(v) - (layeredView ? 0 : 0);
      if (v.x > x0 && (v.x - x0) * k > room && room > 40) k = Math.min(k, room / (v.x - x0));
    });
    k = clampK(k);
    var bw = ww * k + over;
    st.cam.k = k;
    st.cam.x = layeredView ? 40 - x0 * k : (avail - bw) / 2 - x0 * k;
    st.cam.y = H / 2 - (y0 + y1) / 2 * k;
    if (markUser) st.userMoved = false;
    requestDraw();
    setStatus();
  }
  var panel = document.getElementById('panel');
  function centreOn(v, minK) {
    st.cam.k = clampK(Math.max(st.cam.k, minK || 0.9));
    var sheet = !panel.hidden && window.matchMedia && window.matchMedia('(max-width: 760px)').matches;
    st.cam.x = W / 2 - v.x * st.cam.k;
    st.cam.y = (sheet ? H * 0.25 : H / 2) - v.y * st.cam.k;
    st.userMoved = true;
    requestDraw();
    setStatus();
  }
  function onScreenV(v) { var x = sx(v.x), y = sy(v.y); return x > 30 && y > 30 && x < W - 30 && y < H - 30; }

  var pointers = new Map(), drag = null, pinch = null;
  function localXY(ev) { var r = canvas.getBoundingClientRect(); return [ev.clientX - r.left, ev.clientY - r.top]; }
  canvas.addEventListener('pointerdown', function (ev) {
    canvas.setPointerCapture(ev.pointerId);
    var p = localXY(ev);
    pointers.set(ev.pointerId, p);
    if (pointers.size === 2) {
      var ps = Array.from(pointers.values());
      pinch = { d: Math.hypot(ps[0][0] - ps[1][0], ps[0][1] - ps[1][1]), k: st.cam.k };
      drag = null;
      return;
    }
    drag = { x: p[0], y: p[1], node: nodeAt(p[0], p[1]), moved: false, cx: st.cam.x, cy: st.cam.y };
    canvas.classList.add('dragging');
  });
  canvas.addEventListener('pointermove', function (ev) {
    var p = localXY(ev);
    if (pointers.has(ev.pointerId)) pointers.set(ev.pointerId, p);
    if (pinch && pointers.size === 2) {
      var ps = Array.from(pointers.values());
      var d = Math.hypot(ps[0][0] - ps[1][0], ps[0][1] - ps[1][1]);
      zoomAt((ps[0][0] + ps[1][0]) / 2, (ps[0][1] + ps[1][1]) / 2, (pinch.k * d / pinch.d) / st.cam.k);
      return;
    }
    if (drag) {
      var dx = p[0] - drag.x, dy = p[1] - drag.y;
      if (!drag.moved && dx * dx + dy * dy < 16) return;
      drag.moved = true;
      hideTooltip();
      if (drag.node) {
        drag.node.x = (p[0] - st.cam.x) / st.cam.k;
        drag.node.y = (p[1] - st.cam.y) / st.cam.k;
        pos.set(drag.node.id, { x: drag.node.x, y: drag.node.y });
        // Dragging pins: the layout stops moving what you placed.
        drag.node.pinned = true;
        st.pins.add(drag.node.id);
        sim.pinned = drag.node;
        moved();
        if (st.layout === 'force') startForce(0.1);
        requestDraw();
      } else {
        st.cam.x = drag.cx + dx;
        st.cam.y = drag.cy + dy;
        st.userMoved = true;
        markInteracting();
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
      if (d.node) {
        if (st.picking) { finishPath(d.node); return; }
        select(d.node.id, false);
      } else {
        var p = localXY(ev);
        var e = edgeAt(p[0], p[1]);
        if (e) selectEdge(e);
        else clearSelection();
      }
    }
  }
  canvas.addEventListener('pointerup', endPointer);
  canvas.addEventListener('pointercancel', endPointer);
  canvas.addEventListener('pointerleave', function () {
    if (!drag) { st.hoverNode = null; st.hoverEdge = null; hideTooltip(); drawOverlay(); }
  });
  canvas.addEventListener('dblclick', function (ev) {
    var p = localXY(ev), v = nodeAt(p[0], p[1]);
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
    var first = !hoverQueued;
    hoverQueued = [px, py];
    if (!first) return;
    requestAnimationFrame(function () {
      var q = hoverQueued;
      hoverQueued = null;
      if (!q) return;
      var v = nodeAt(q[0], q[1]);
      var e = v ? null : edgeAt(q[0], q[1]);
      st.hoverNode = v; st.hoverEdge = e;
      canvas.classList.toggle('pointing', !!(v || e));
      if (v) showTooltip(nodeTooltip(v), q[0], q[1]);
      else if (e) showTooltip(edgeTooltip(e), q[0], q[1]);
      else hideTooltip();
      drawOverlay();
    });
  }

  // ---------------------------------------------------------------- tooltip
  var tooltip = document.getElementById('tooltip');
  function loc(n) { return n.file ? (n.line ? n.file + ':' + n.line : n.file) : ''; }
  function vName(i) { var v = V.nodes[i]; return v.cluster !== null ? v.cluster : v.leaf.name; }
  function nodeTooltip(v) {
    if (v.cluster !== null) {
      return '<div class="t-kind">' + esc(v.cluster) + '</div><div class="t-dim">folder · ' +
        esc(plural(v.members.length, 'node', 'nodes')) + (v.folded ? ' · folded to fit the view' : '') +
        ' · double-click to open</div>';
    }
    var n = v.leaf;
    return '<div class="t-kind">' + esc(n.name) + ' <span class="t-dim">' + esc(n.kind) + (n.lang ? ' · ' + esc(n.lang) : '') + '</span></div>' +
      (n.qual !== n.name ? '<div class="t-qual">' + esc(n.qual) + '</div>' : '') +
      (loc(n) ? '<div class="t-dim">' + esc(loc(n)) + '</div>' : '') +
      (n.sig && has(n, F.external) ? '<div class="t-dim">' + esc(n.sig) + '</div>' : '') +
      '<div class="t-dim">' + v.outE.length + ' out · ' + v.inE.length + ' in' +
      (v.hidden ? ' · ' + v.hidden + ' hidden' : '') + '</div>';
  }
  function kindsText(e) {
    var ks = Object.keys(e.kinds).sort(function (a, b) { return e.kinds[b] - e.kinds[a]; });
    return ks.map(function (k) { return k + ' ×' + e.kinds[k]; }).join(', ');
  }
  function edgeTooltip(e) {
    var r0 = e.refs[0];
    var html = '<div class="t-kind">' + esc(vName(e.a)) + ' → ' + esc(vName(e.b)) + '</div>' +
      '<div class="t-dim">' + esc(kindsText(e)) + ' · weight ' + fmt(e.w) + '</div>';
    if (r0 && (r0.flags & F.edge_reversed)) {
      html += '<div class="t-dim">' + esc(vName(e.b)) + ' ' + (RECEIVING_VERB[r0.kind] || 'receives') + ' ' +
        esc(vName(e.a)) + ' (drawn in flow direction)</div>';
    }
    if (r0 && derived(r0)) html += '<div class="t-dim">over ' + esc(STYLE[e.fam].label) + ': ' + esc(r0.ev || '') + '</div>';
    else {
      e.refs.slice(0, 3).forEach(function (r) {
        var where = r.evFile ? r.evFile + (r.evLine ? ':' + r.evLine : '') : '';
        if (where) html += '<div class="t-dim">' + esc(where) + (r.res ? ' · ' + esc(r.res) : '') + '</div>';
        if (r.ev) html += '<span class="t-ev">' + esc(r.ev) + '</span>';
      });
    }
    if (e.n > 3) html += '<div class="t-dim">+' + (e.n - 3) + ' more · click for all</div>';
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

  // ---------------------------------------------------------------- history
  // Breadcrumbs and back/forward over view states (Shneiderman's "history").
  var past = [], future = [];
  function snapshot() {
    return {
      expanded: Array.from(st.expanded), depth: st.depth, group: st.group, layout: st.layout, view: st.view,
      selected: st.selected, mode: st.mode, hops: st.hops, pathTo: st.pathTo,
      isolate: st.isolate ? Array.from(st.isolate) : null, revealed: Array.from(st.revealed),
      showNonCode: st.showNonCode, showExternal: st.showExternal, showEndpoints: st.showEndpoints, showTests: st.showTests,
      level: st.level, aSel: ast.selected, aOpen: Array.from(ast.open),
      zpath: JSON.parse(JSON.stringify(ast.zpath)), zsel: ast.zsel, zfocus: ast.zfocus
    };
  }
  function pushHistory() {
    past.push(snapshot());
    if (past.length > 80) past.shift();
    future = [];
    updateHistoryButtons();
  }
  function restore(s) {
    if (s.level === 'app') {
      ast.selected = s.aSel; ast.selConn = null; ast.open = new Set(s.aOpen || []);
      ast.zpath = s.zpath || {}; ast.zsel = s.zsel || null; ast.zfocus = s.zfocus === undefined ? null : s.zfocus; ast.zlink = null;
      ast.zitems = {};
      if (st.level !== 'app') setLevel('app', true); else { renderApps(true); renderPanel(); }
      writeHash();
      return;
    }
    if (st.level === 'app') setLevel('folder', true);
    ast.open = new Set(s.aOpen || []);
    savePositions();
    st.expanded = new Set(s.expanded); st.depth = s.depth; st.group = s.group;
    st.selected = s.selected; st.mode = s.mode; st.hops = s.hops; st.pathTo = s.pathTo;
    st.isolate = s.isolate ? new Set(s.isolate) : null; st.revealed = new Set(s.revealed);
    st.showNonCode = s.showNonCode; st.showExternal = s.showExternal; st.showEndpoints = s.showEndpoints; st.showTests = s.showTests;
    groupSel.value = st.group;
    rebuild();
    if (st.selected && !V.byId.has(st.selected)) st.selected = null;
    computeHighlight();
    renderPanel();
    renderLegend();
    updateBanner();
    if (s.layout !== st.layout) setLayout(s.layout, true); else relayout(0.25);
    setView(s.view, true);
  }
  function goBack() {
    if (!past.length) return;
    future.push(snapshot());
    restore(past.pop());
    updateHistoryButtons();
  }
  function goForward() {
    if (!future.length) return;
    past.push(snapshot());
    restore(future.pop());
    updateHistoryButtons();
  }
  var histBack = document.getElementById('hist-back'), histFwd = document.getElementById('hist-fwd');
  function updateHistoryButtons() { histBack.disabled = !past.length; histFwd.disabled = !future.length; }
  histBack.addEventListener('click', goBack);
  histFwd.addEventListener('click', goForward);

  var crumbs = document.getElementById('crumbs');
  function renderCrumbs() {
    var v = selectedV(), path = [];
    if (v) {
      var chain = v.leaf ? v.leaf.chain : v.members[0].chain;
      for (var i = 0; i < chain.length; i++) {
        if (PSEUDO[chain[i]]) break;
        if (chain[i].charAt(chain[i].length - 1) !== '/') break;
        path.push(chain[i]);
        if (v.cluster === chain[i]) break;
      }
    }
    if (!path.length) { crumbs.innerHTML = ''; return; }
    var html = '<button type="button" data-crumb="" title="Top-level folders">top</button>';
    path.forEach(function (p, i) {
      html += '<span class="sep" aria-hidden="true">/</span><button type="button" data-crumb="' + esc(p) + '"' +
        (i === path.length - 1 ? ' aria-current="location"' : '') + '>' + esc(folderName(p).replace(/\/$/, '')) + '</button>';
    });
    crumbs.innerHTML = html;
  }
  crumbs.addEventListener('click', function (ev) {
    var b = ev.target.closest('button[data-crumb]');
    if (!b) return;
    drillTo(b.dataset.crumb);
  });
  // Drill into a folder: open its path, close everything else.
  function drillTo(p) {
    pushHistory();
    savePositions();
    if (!p) { setDepth(1); }
    else {
      st.expanded = new Set();
      for (var q = p; q; q = parentOf(q)) st.expanded.add(q);
      compress();
      st.depth = NaN;
    }
    st.group = 'tree'; groupSel.value = 'tree';
    rebuild();
    if (st.selected && !V.byId.has(st.selected)) st.selected = null;
    computeHighlight();
    renderPanel();
    st.userMoved = false;
    relayout(0.5);
  }

  // -------------------------------------------------------------- selection
  var live = document.getElementById('live');
  function select(id, centre) {
    st.selected = id;
    st.selectedEdge = null;
    if (st.mode === 'path') { st.mode = 'hops'; st.pathTo = null; }
    var v = selectedV();
    computeHighlight();
    renderPanel();
    renderCrumbs();
    if (v && (centre || !onScreenV(v))) centreOn(v);
    if (v) {
      live.textContent = 'Selected ' + (v.cluster !== null ? 'folder ' + v.cluster : v.leaf.name + ', ' + v.leaf.kind) +
        '. ' + v.outE.length + ' outgoing, ' + v.inE.length + ' incoming.';
    }
    draw();
  }
  function selectEdge(e) {
    st.selected = null;
    st.selectedEdge = e;
    hl = new Map([[e.a, 0], [e.b, 0]]);
    hlEdges = new Set([e.idx]);
    renderPanel();
    draw();
  }
  function clearSelection() {
    st.selected = null;
    st.selectedEdge = null;
    st.picking = false;
    if (st.mode === 'path') { st.mode = 'hops'; st.pathTo = null; }
    computeHighlight();
    panel.hidden = true;
    renderCrumbs();
    draw();
  }
  function startPath() {
    if (!selectedV()) return;
    st.picking = true;
    st.pathFrom = st.selected;
    toast('Pick the other end: click a node or search');
    drawOverlay();
  }
  function finishPath(v) {
    st.picking = false;
    var a = selectedV();
    if (!a || v === a) { drawOverlay(); return; }
    st.mode = 'path';
    st.pathTo = v.id;
    var p = pathBetween(a, v);
    hl = p.nodes; hlEdges = p.edges;
    toast(p.found ? 'Shortest paths: ' + plural(p.steps, 'step', 'steps') : 'No path between them in this view');
    fit(false, Array.from(hl.keys()));
    st.userMoved = true;
    renderPanel();
    draw();
  }

  // Make leaf i visible (reveal halo, filters, open its folders), then
  // select and centre it.
  function focusLeaf(i, keepHistory) {
    var n = N[i], changed = false;
    // Mark it first: the rebuild below must not fold it into its folder.
    st.focusId = 'n' + i;
    if (V.leafTo[i] < 0 || V.nodes[V.leafTo[i]].id !== st.focusId) changed = true;
    if (st.isolate && !st.isolate.has(i)) { st.isolate = null; changed = true; updateBanner(); }
    if ((has(n, F.halo) || has(n, F.scope_out)) && !st.revealed.has(i)) { st.revealed.add(i); changed = true; }
    if (has(n, F.external | F.unresolved) && !st.showExternal) { st.showExternal = true; changed = true; }
    if (has(n, F.endpoint) && !st.showEndpoints) { st.showEndpoints = true; changed = true; }
    if (has(n, F.test) && !st.showTests) { st.showTests = true; changed = true; }
    if (st.langOn[langKey(n)] === false) { st.langOn[langKey(n)] = true; changed = true; }
    if (st.group !== 'none') n.chain.forEach(function (p) { if (!st.expanded.has(p)) { st.expanded.add(p); changed = true; } });
    if (changed) {
      if (!keepHistory) pushHistory();
      savePositions(); rebuild(); renderLegend(); relayout(0.3);
    }
    var id = V.leafTo[i] >= 0 ? V.nodes[V.leafTo[i]].id : 'n' + i;
    if (st.picking && V.byId.get(id)) { finishPath(V.byId.get(id)); st.focusId = null; return; }
    select(id, true);
    st.focusId = null;
  }
  function expand(v) {
    pushHistory();
    if (v.cluster !== null) {
      savePositions();
      if (v.folded) BUDGET += v.members.length;
      expandPrefix(v.cluster);
      rebuild();
      st.selected = null;
      computeHighlight();
      renderPanel();
      relayout(0.35);
      return;
    }
    var n = v.leaf, added = 0;
    n.out.concat(n.inn).forEach(function (e) {
      var o = e.s === n.i ? e.t : e.s, on = N[o];
      if ((has(on, F.halo) || (has(on, F.scope_out) && !st.showNonCode)) && !st.revealed.has(o)) { st.revealed.add(o); added++; }
      if (has(on, F.external | F.unresolved) && !st.showExternal) { st.showExternal = true; added++; }
      if (has(on, F.test) && !st.showTests) { st.showTests = true; added++; }
      if (st.langOn[langKey(on)] === false) { st.langOn[langKey(on)] = true; added++; }
    });
    if (added) {
      savePositions(); rebuild(); renderLegend(); relayout(0.3);
      toast('Revealed ' + plural(added, 'neighbour', 'neighbours'));
    } else {
      past.pop(); updateHistoryButtons();
      toast(has(n, F.halo) ? 'Its further neighbours are not embedded' : 'All embedded neighbours are shown');
    }
    select(v.id, false);
  }
  function collapseGroupOf(v) {
    if (!v || st.group === 'none') return;
    var g = null;
    if (v.cluster !== null) g = parentOf(v.cluster);
    else {
      var c = v.leaf.chain;
      for (var i = c.length - 1; i >= 0; i--) if (st.expanded.has(c[i])) { g = c[i]; break; }
    }
    if (!g) return;
    pushHistory();
    savePositions();
    collapsePrefix(g);
    rebuild();
    relayout(0.25);
    if (V.byId.has('c:' + g)) select('c:' + g, false); else clearSelection();
  }
  // Isolate: show only the highlighted neighbourhood (reversible).
  function isolate() {
    var v = selectedV();
    if (!v || !hl) return;
    pushHistory();
    var keep = new Set();
    hl.forEach(function (_, idx) {
      var w = V.nodes[idx];
      if (w.leaf) keep.add(w.leaf.i); else w.members.forEach(function (m) { keep.add(m.i); });
    });
    st.isolate = keep;
    savePositions();
    rebuild();
    computeHighlight();
    renderPanel();
    updateBanner();
    st.userMoved = false;
    relayout(0.4);
  }
  function clearIsolate() {
    if (!st.isolate) return;
    pushHistory();
    st.isolate = null;
    savePositions();
    rebuild();
    computeHighlight();
    renderPanel();
    updateBanner();
    relayout(0.3);
  }
  var banner = document.getElementById('banner');
  function updateBanner() {
    if (!st.isolate) { banner.hidden = true; return; }
    banner.hidden = false;
    banner.innerHTML = '<span>Isolated: ' + esc(plural(st.isolate.size, 'node', 'nodes')) + '</span>' +
      '<button type="button" data-act="unisolate">Show all</button>';
  }
  banner.addEventListener('click', function (ev) { if (ev.target.closest('button')) clearIsolate(); });
  function togglePin(v) {
    if (!v) return;
    v.pinned = !v.pinned;
    if (v.pinned) st.pins.add(v.id); else st.pins.delete(v.id);
    if (!v.pinned) startForce(0.15);
    renderPanel();
    draw();
  }

  function badge(text, cls) { return '<span class="badge ' + (cls || '') + '">' + esc(text) + '</span>'; }
  function swatch(fam) {
    var s = STYLE[fam] || STYLE.other;
    return '<svg viewBox="0 0 22 10" aria-hidden="true"><line x1="1" y1="5" x2="21" y2="5" stroke="' + esc(famColor(fam)) +
      '" stroke-width="' + (BRIDGE[fam] ? 2.2 : 1.8) + '"' + (s.dash.length ? ' stroke-dasharray="' + s.dash.join(' ') + '"' : '') +
      (s.round ? ' stroke-linecap="round"' : '') + '/></svg>';
  }
  function shellQuote(s) { return /^[\w:.\/-]+$/.test(s) ? s : "'" + s.replace(/'/g, "'\\''") + "'"; }
  function dedent(text) {
    var lines = text.split('\n'), indent = Infinity;
    lines.slice(1).forEach(function (l) { if (l.trim()) indent = Math.min(indent, l.match(/^\s*/)[0].length); });
    if (!isFinite(indent) || !indent) return text;
    return [lines[0]].concat(lines.slice(1).map(function (l) { return l.slice(Math.min(indent, l.length - l.trimStart().length)); })).join('\n');
  }

  function renderPanel() {
    if (st.level === 'app') { renderAppPanel(); return; }
    var v = selectedV(), e = st.selectedEdge;
    if (!v && !e) { panel.hidden = true; return; }
    panel.hidden = false;
    var html = '';
    if (e) { panel.innerHTML = edgePanel(e); return; }
    var leaves = v.cluster !== null ? v.members : [v.leaf];
    if (v.cluster !== null) {
      html += '<div class="panel-head"><h2>' + esc(v.cluster) + ' <span class="kind">folder</span></h2>' +
        '<button type="button" data-act="close" aria-label="Close details">×</button></div>';
      html += '<p class="facts">' + esc(plural(v.members.length, 'node', 'nodes')) + (v.folded ? ' · folded to keep the view under ' + fmt(BUDGET) + ' nodes' : '') + '</p>';
      html += statsHtml(leaves);
      html += '<div class="actions"><button type="button" data-act="expand">Open folder</button>' +
        '<button type="button" data-act="drill">Drill in</button><button type="button" data-act="isolate">Isolate</button>' +
        '<button type="button" data-act="centre">Centre</button></div>';
      html += modesHtml();
      var members = v.members.slice().sort(function (a, b) {
        return (b.out.length + b.inn.length) - (a.out.length + a.inn.length) || (a.qual < b.qual ? -1 : 1);
      });
      html += '<h3>Most connected inside</h3><div class="edges"><ul>' + members.slice(0, 12).map(function (m) {
        return '<li><button type="button" data-node="' + m.i + '"><span class="nm">' + esc(m.name) + '</span>' +
          '<span class="loc">' + esc(m.kind + (loc(m) ? ' · ' + loc(m) : '')) + '</span></button></li>';
      }).join('') + '</ul>' + (members.length > 12 ? '<div class="more">and ' + fmt(members.length - 12) + ' more</div>' : '') + '</div>';
      html += topLists(leaves) + edgeSections(leaves);
    } else {
      var n = v.leaf;
      html += '<div class="panel-head"><h2>' + esc(n.name) + ' <span class="kind">' + esc(n.kind) + '</span></h2>' +
        '<button type="button" data-act="close" aria-label="Close details">×</button></div>';
      html += '<div class="qual"><code>' + esc(n.qual) + '</code><button type="button" data-act="copy" title="Copy qualname">Copy</button></div>';
      var badges = '';
      if (has(n, F.seed)) badges += badge(META.scope.indexOf('diff') === 0 ? 'changed' : 'seed', 'focus');
      if (has(n, F.halo)) badges += badge('outside the walk');
      if (has(n, F.scope_out)) badges += badge('outside ' + (META.file_scope || 'code') + ' scope');
      if (has(n, F.external)) badges += badge('external');
      if (has(n, F.unresolved)) badges += badge('unresolved');
      if (has(n, F.endpoint)) badges += badge('bridge endpoint', 'bridge');
      if (has(n, F.test)) badges += badge('test');
      if (v.pinned) badges += badge('pinned');
      if (badges) html += '<div class="badges">' + badges + '</div>';
      html += '<dl class="facts">';
      if (n.lang) html += '<dt>Language</dt><dd>' + esc(n.lang) + '</dd>';
      if (n.file) html += '<dt>Location</dt><dd><code>' + esc(loc(n)) + '</code></dd>';
      if (AGGREGATED && n.size) html += '<dt>Symbols</dt><dd>' + fmt(n.size) + '</dd>';
      if (n.mod && !PSEUDO[n.mod]) html += '<dt>Folder</dt><dd><code>' + esc(n.mod) + '</code></dd>';
      html += '</dl>';
      if (n.sig) html += has(n, F.external) ? '<div class="doc">' + esc(n.sig) + '</div>' : '<pre class="sig">' + esc(dedent(n.sig)) + '</pre>';
      if (n.doc) html += '<div class="doc">' + esc(n.doc) + '</div>';
      html += statsHtml(leaves);
      html += '<div class="actions">';
      if (v.hidden) html += '<button type="button" data-act="expand">Reveal ' + esc(plural(v.hidden, 'hidden neighbour', 'hidden neighbours')) + '</button>';
      html += '<button type="button" data-act="isolate">Isolate</button><button type="button" data-act="path">Paths to…</button>' +
        '<button type="button" data-act="pin">' + (v.pinned ? 'Unpin' : 'Pin') + '</button><button type="button" data-act="centre">Centre</button>';
      if (v.group !== null && v.group !== undefined) html += '<button type="button" data-act="collapse">Close folder</button>';
      html += '</div>';
      html += modesHtml();
      if (has(n, F.halo) && !AGGREGATED) {
        html += '<p class="more">Outside the walk. To explore from here:</p><p class="cmd">lidx graph --seed ' + esc(shellQuote(n.qual)) + '</p>';
      }
      html += topLists(leaves) + edgeSections(leaves);
    }
    panel.innerHTML = html;
  }
  function modesHtml() {
    var html = '<div class="modes" role="group" aria-label="Highlight"><span>Highlight</span>';
    [1, 2, 3, 4].forEach(function (h) {
      html += '<button type="button" data-hops="' + h + '" aria-pressed="' + (st.mode === 'hops' && st.hops === h) + '">' + h + '</button>';
    });
    html += '<button type="button" data-mode="up" aria-pressed="' + (st.mode === 'up') + '" title="Everything that reaches this (U)">Upstream</button>';
    html += '<button type="button" data-mode="down" aria-pressed="' + (st.mode === 'down') + '" title="Everything this reaches (D)">Downstream</button>';
    if (st.mode === 'path') html += '<button type="button" aria-pressed="true" data-mode="hops">Path ×</button>';
    return html + '</div>';
  }
  // Counts by edge family, in and out.
  function statsHtml(leaves) {
    var inside = new Set(leaves.map(function (n) { return n.i; }));
    var cin = {}, cout = {};
    leaves.forEach(function (n) {
      n.out.forEach(function (e) { if (!inside.has(e.t)) { var f = family(e.kind); cout[f] = (cout[f] || 0) + e.w; } });
      n.inn.forEach(function (e) { if (!inside.has(e.s)) { var f = family(e.kind); cin[f] = (cin[f] || 0) + e.w; } });
    });
    var fams = FAMILIES.filter(function (f) { return cin[f] || cout[f]; });
    if (!fams.length) return '';
    return '<div class="stats">' + fams.map(function (f) {
      return '<span class="stat" title="' + esc(STYLE[f].label) + ': ' + fmt(cin[f] || 0) + ' in, ' + fmt(cout[f] || 0) + ' out">' +
        swatch(f) + esc(STYLE[f].tag || STYLE[f].label.split(' ')[0]) + ' · <b>' + fmt(cin[f] || 0) + '</b> in · <b>' + fmt(cout[f] || 0) + '</b> out</span>';
    }).join('') + '</div>';
  }
  var expandedLists = new Set();
  // Top callers and callees by weight, as links.
  function topLists(leaves) {
    var inside = new Set(leaves.map(function (n) { return n.i; }));
    function collect(dirOut) {
      var m = new Map();
      leaves.forEach(function (n) {
        (dirOut ? n.out : n.inn).forEach(function (e) {
          var o = dirOut ? e.t : e.s;
          if (inside.has(o) || family(e.kind) === 'imports') return;
          m.set(o, (m.get(o) || 0) + e.w);
        });
      });
      return Array.from(m.entries()).sort(function (a, b) { return b[1] - a[1] || (N[a[0]].qual < N[b[0]].qual ? -1 : 1); });
    }
    function list(title, key, items) {
      if (!items.length) return '';
      var all = expandedLists.has(key), shown = all ? items : items.slice(0, 8);
      return '<h3>' + title + ' (' + fmt(items.length) + ')</h3><div class="edges"><ul>' + shown.map(function (it) {
        var o = N[it[0]], vis = V.leafTo[o.i] >= 0;
        return '<li><button type="button" data-node="' + o.i + '"><span class="nm">' + esc(o.name) + '</span>' +
          (it[1] > 1 ? ' <span class="w">×' + fmt(it[1]) + '</span>' : '') + (vis ? '' : ' <span class="hidden-tag">hidden</span>') +
          '<span class="loc">' + esc(o.qual !== o.name ? o.qual : o.kind) + '</span></button></li>';
      }).join('') + '</ul>' + (items.length > shown.length ? '<div class="more"><button type="button" data-list="' + key + '">Show all ' + fmt(items.length) + '</button></div>' : '') + '</div>';
    }
    return list('Callers and users', 'in', collect(false)) + list('Callees and uses', 'out', collect(true));
  }
  // Every edge, grouped by kind, collapsed: the full record on demand.
  function edgeSections(leaves) {
    var inside = new Set(leaves.map(function (n) { return n.i; }));
    var out = {}, inn = {};
    leaves.forEach(function (n) {
      n.out.forEach(function (e) { if (!inside.has(e.t)) (out[e.kind] = out[e.kind] || []).push([e, e.t]); });
      n.inn.forEach(function (e) { if (!inside.has(e.s)) (inn[e.kind] = inn[e.kind] || []).push([e, e.s]); });
    });
    var total = Object.keys(out).length + Object.keys(inn).length;
    if (!total) return '';
    return '<h3>All edges by kind</h3>' + section('Out', out) + section('In', inn);
  }
  function section(title, byKind) {
    var kinds = Object.keys(byKind).sort(function (a, b) {
      return FAMILIES.indexOf(family(a)) - FAMILIES.indexOf(family(b)) || (a < b ? -1 : 1);
    });
    if (!kinds.length) return '';
    var html = '<div class="edges">';
    kinds.forEach(function (k) {
      var list = byKind[k];
      list.sort(function (a, b) { return b[0].w - a[0].w || (N[a[1]].qual < N[b[1]].qual ? -1 : 1); });
      html += '<details><summary>' + swatch(family(k)) + esc(title + ' · ' + k) + ' <span class="n">' + fmt(list.length) + '</span></summary><ul>';
      list.slice(0, 200).forEach(function (it) {
        var e = it[0], o = N[it[1]], shown = V.leafTo[o.i] >= 0;
        html += '<li><button type="button" data-node="' + o.i + '"><span class="nm">' + esc(o.name) + '</span>' +
          (e.w > 1 ? ' <span class="w">×' + fmt(e.w) + '</span>' : '') + (shown ? '' : ' <span class="hidden-tag">hidden</span>') +
          '<span class="loc">' + esc(o.qual !== o.name ? o.qual : o.kind) + '</span>' +
          (e.ev ? '<span class="ev">' + esc((e.evLine ? 'L' + e.evLine + ': ' : '') + e.ev) + '</span>' : '') + '</button></li>';
      });
      if (list.length > 200) html += '<li class="more">and ' + fmt(list.length - 200) + ' more</li>';
      html += '</ul></details>';
    });
    return html + '</div>';
  }
  // An edge's panel: the underlying edges with their evidence.
  function edgePanel(e) {
    var html = '<div class="panel-head"><h2>' + esc(vName(e.a)) + ' → ' + esc(vName(e.b)) + '</h2>' +
      '<button type="button" data-act="close" aria-label="Close details">×</button></div>';
    html += '<div class="stats">' + Object.keys(e.kinds).map(function (k) {
      return '<span class="stat">' + swatch(family(k)) + esc(k) + ' <b>' + fmt(e.kinds[k]) + '</b></span>';
    }).join('') + '</div>';
    html += '<div class="actions"><button type="button" data-vnode="' + e.a + '">Select source</button>' +
      '<button type="button" data-vnode="' + e.b + '">Select target</button></div>';
    html += '<h3>Underlying edges (' + fmt(e.n) + ')</h3><div class="edges"><ul>';
    e.refs.slice(0, 120).forEach(function (r) {
      var s = N[r.s], t = N[r.t];
      html += '<li><button type="button" data-node="' + t.i + '"><span class="nm">' + esc(s.name) + ' → ' + esc(t.name) + '</span>' +
        ' <span class="w">' + esc(r.kind) + (r.w > 1 ? ' ×' + fmt(r.w) : '') + '</span>' +
        (r.evFile ? '<span class="loc">' + esc(r.evFile + (r.evLine ? ':' + r.evLine : '')) + '</span>' : '') +
        (r.ev ? '<span class="ev">' + esc(r.ev) + '</span>' : '') + '</button></li>';
    });
    if (e.refs.length > 120) html += '<li class="more">and ' + fmt(e.refs.length - 120) + ' more</li>';
    return html + '</ul></div>';
  }

  panel.addEventListener('click', function (ev) {
    var b = ev.target.closest('button');
    if (!b) return;
    if (st.level === 'app' || b.dataset.unit !== undefined) { appPanelClick(b); return; }
    if (b.dataset.node !== undefined) { focusLeaf(+b.dataset.node); return; }
    if (b.dataset.vnode !== undefined) { select(V.nodes[+b.dataset.vnode].id, true); return; }
    if (b.dataset.list) { expandedLists.add(b.dataset.list); renderPanel(); return; }
    if (b.dataset.hops) { st.mode = 'hops'; st.hops = +b.dataset.hops; st.pathTo = null; computeHighlight(); renderPanel(); draw(); return; }
    if (b.dataset.mode) { st.mode = b.dataset.mode; st.pathTo = null; computeHighlight(); renderPanel(); draw(); return; }
    var v = selectedV();
    switch (b.dataset.act) {
      case 'close': clearSelection(); canvas.focus(); break;
      case 'copy': copyText(v.leaf.qual); break;
      case 'expand': expand(v); break;
      case 'drill': drillTo(v.cluster); break;
      case 'centre': centreOn(v); break;
      case 'collapse': collapseGroupOf(v); break;
      case 'isolate': isolate(); break;
      case 'path': startPath(); break;
      case 'pin': togglePin(v); break;
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
    if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(text).then(function () { toast('Copied qualname'); }, fallback);
    else fallback();
  }
  var toastEl = document.getElementById('toast'), toastTimer = null;
  function toast(msg) {
    toastEl.textContent = msg;
    toastEl.classList.add('show');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(function () { toastEl.classList.remove('show'); }, 2000);
  }

  // ----------------------------------------------------------------- search
  var search = document.getElementById('search'), list = document.getElementById('search-list');
  var results = [], active = -1;
  var groupNames = ALL_PREFIXES.filter(function (p) { return p.charAt(p.length - 1) === '/' || PSEUDO[p]; });
  function runSearch() {
    var q = search.value.trim().toLowerCase();
    results = []; active = -1;
    if (!q) { closeList(); return; }
    var scored = [];
    N.forEach(function (n) {
      var nm = n.name.toLowerCase(), qn = n.qual.toLowerCase(), s;
      if (nm === q) s = 0;
      else if (nm.indexOf(q) === 0) s = 1;
      else if (qn.slice(-q.length) === q) s = 2;
      else if (nm.indexOf(q) >= 0) s = 3;
      else if (qn.indexOf(q) >= 0) s = 4;
      else return;
      if (has(n, F.halo) || has(n, F.scope_out)) s += 0.5;
      scored.push([s, n.qual.length, n]);
    });
    groupNames.forEach(function (g) { if (g.toLowerCase().indexOf(q) >= 0) scored.push([5, g.length, { group: g }]); });
    if (HAS_APPS) {
      AU.forEach(function (u) {
        var nm = u.name.toLowerCase(), s = nm === q ? 0 : nm.indexOf(q) === 0 ? 1 : nm.indexOf(q) >= 0 ? 3 : u.root.toLowerCase().indexOf(q) >= 0 ? 4 : -1;
        if (s < 0) return;
        // On the sheet, units come first.
        scored.push([s - (st.level === 'app' ? 1 : 0.25) + (u.hidden ? 0.5 : 0), u.name.length, { unit: u.id }]);
      });
    }
    scored.sort(function (a, b) { return a[0] - b[0] || a[1] - b[1]; });
    results = scored.slice(0, 40).map(function (s) { return s[2]; });
    renderList();
  }
  function renderList() {
    if (!results.length) list.innerHTML = '<li class="empty" role="option" aria-disabled="true">No matches. Try part of a name or a folder path.</li>';
    else {
      list.innerHTML = results.map(function (r, i) {
        if (r.group) {
          return '<li role="option" id="sr-' + i + '" data-i="' + i + '" aria-selected="' + (i === active) + '">' +
            '<span class="nm">' + esc(r.group) + '</span><span class="kd">folder</span></li>';
        }
        if (r.unit !== undefined) {
          var u = AU[r.unit];
          return '<li role="option" id="sr-' + i + '" data-i="' + i + '" aria-selected="' + (i === active) + '">' +
            '<span class="nm">' + esc(u.name) + '</span><span class="kd">' + esc(unitKind(u)) + (u.hidden ? ' · hidden' : '') + '</span>' +
            '<span class="qn">' + esc(u.root) + '</span></li>';
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
    } else search.removeAttribute('aria-activedescendant');
  }
  function closeList() { list.hidden = true; search.setAttribute('aria-expanded', 'false'); search.removeAttribute('aria-activedescendant'); }
  function choose(i) {
    var r = results[i];
    if (!r) return;
    closeList();
    if (r.unit !== undefined) {
      if (st.level !== 'app') setLevel('app');
      revealUnit(r.unit);
      var el = cardEl('u' + r.unit);
      if (el) el.focus();
      selectUnit('u' + r.unit, true);
      return;
    }
    if (st.level === 'app') setLevel('folder');
    if (r.group) {
      var opened = false;
      for (var p = parentOf(r.group); p; p = parentOf(p)) if (!st.expanded.has(p)) { if (!opened) pushHistory(); st.expanded.add(p); opened = true; }
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
    else if (ev.key === 'Escape') { if (!list.hidden) closeList(); else { search.value = ''; canvas.focus(); } }
  });
  search.addEventListener('blur', function () { setTimeout(closeList, 150); });
  list.addEventListener('mousedown', function (ev) {
    var li = ev.target.closest('li[data-i]');
    if (li) { ev.preventDefault(); choose(+li.dataset.i); }
  });

  // ----------------------------------------------------------------- legend
  var legend = document.getElementById('legend');
  var legendOpen = { edges: true, langs: true, show: true, shapes: false, notes: true };
  function renderLegend() {
    if (st.level === 'app') { renderAppLegend(); return; }
    // Counts cover what the scope and halo rules show.
    var fc = {}, lc = {};
    N.forEach(function (n) {
      if (!inView(n) || (has(n, F.endpoint) && !st.showEndpoints)) return;
      lc[langKey(n)] = (lc[langKey(n)] || 0) + 1;
    });
    E.forEach(function (e) {
      if (!inView(N[e.s]) || !inView(N[e.t])) return;
      if (derived(e) && st.showEndpoints) return;
      if (!st.showEndpoints && (has(N[e.s], F.endpoint) || has(N[e.t], F.endpoint))) return;
      var f = family(e.kind);
      fc[f] = (fc[f] || 0) + 1;
    });
    function fam(f) {
      if (!fc[f] && st.famOn[f]) return '';
      return '<label><input type="checkbox" data-fam="' + f + '"' + (st.famOn[f] ? ' checked' : '') + '>' + swatch(f) +
        '<span' + (st.famOn[f] ? '' : ' class="off"') + '>' + esc(STYLE[f].label) + '</span><span class="count">' + fmt(fc[f] || 0) + '</span></label>';
    }
    var code = FAMILIES.filter(function (f) { return !CROSS[f]; }).map(fam).join('');
    var bridges = FAMILIES.filter(function (f) { return CROSS[f]; }).map(fam).join('');
    var html = '<details data-sec="edges"' + (legendOpen.edges ? ' open' : '') + '><summary>Edges</summary>' + (code || '<p class="hint">None in view</p>');
    if (bridges) html += '<p class="hint">Across processes, between the folders that own each side:</p>' + bridges;
    html += '<div class="row-actions"><button type="button" data-legend="all">All</button><button type="button" data-legend="calls">Calls only</button>' +
      (bridges ? '<button type="button" data-legend="bridges">Bridges + calls</button>' : '') + '</div></details>';
    if (APPS && NODE_UNIT.some(function (u) { return u >= 0; })) {
      html += '<details data-sec="colour" open><summary>Colour</summary><div class="row-actions" role="radiogroup" aria-label="Colour symbols by">' +
        ['app', 'lang'].map(function (c) {
          return '<label class="plain"><input type="radio" name="colorby" data-colorby="1" value="' + c + '"' + (st.colorBy === c ? ' checked' : '') + '><span>' +
            (c === 'app' ? 'by app' : 'by language') + '</span><span></span></label>';
        }).join('') + '</div>';
      if (st.colorBy === 'app') {
        APP_HUES.order.slice(0, 14).forEach(function (u) {
          html += '<label class="plain" style="grid-template-columns:16px 1fr auto"><span class="dot" style="background:' + esc(appColor(u)) + '"></span><span>' +
            esc(AU[u].name) + ' <span class="count">' + esc(unitKind(AU[u])) + '</span></span><span class="count">' + fmt(APP_HUES.counts[u]) + '</span></label>';
        });
      }
      html += '</details>';
    }
    html += '<details data-sec="langs"' + (legendOpen.langs ? ' open' : '') + '><summary>Languages</summary>';
    Object.keys(lc).concat(Object.keys(st.langOn).filter(function (l) { return st.langOn[l] === false && !lc[l]; }))
      .sort(function (a, b) { return (lc[b] || 0) - (lc[a] || 0) || (a < b ? -1 : 1); }).forEach(function (l) {
        var colour = l.charAt(0) === '(' ? theme['l-other'] : langColor(l);
        html += '<label><input type="checkbox" data-lang="' + esc(l) + '"' + (st.langOn[l] ? ' checked' : '') + '>' +
          '<span class="dot" style="background:' + esc(colour) + '"></span><span' + (st.langOn[l] ? '' : ' class="off"') + '>' + esc(l) +
          '</span><span class="count">' + fmt(lc[l] || 0) + '</span></label>';
      });
    html += '</details>';
    var nonCode = N.filter(function (n) { return has(n, F.scope_out) && (!st.isolate || st.isolate.has(n.i)); }).length;
    var ext = N.filter(function (n) { return inView(n) && has(n, F.external | F.unresolved); }).length;
    var eps = N.filter(function (n) { return inView(n) && has(n, F.endpoint); }).length;
    var tests = N.filter(function (n) { return inView(n) && has(n, F.test); }).length;
    var halo = N.filter(function (n) { return has(n, F.halo); }).length;
    html += '<details data-sec="show"' + (legendOpen.show ? ' open' : '') + '><summary>Show</summary>';
    if (nonCode) html += toggle('showNonCode', 'Outside ' + (META.file_scope || 'code') + ' scope', nonCode);
    if (ext) html += toggle('showExternal', AGGREGATED ? 'External chips (per folder)' : 'External and unresolved', ext);
    if (eps) html += toggle('showEndpoints', 'Channel, route and key nodes', eps);
    if (tests) html += toggle('showTests', 'Tests', tests);
    html += toggle('showLabels', 'Labels', '');
    if (halo) {
      var hiddenHalo = N.filter(function (n) { return has(n, F.halo) && !st.revealed.has(n.i); }).length;
      html += '<p class="hint">Neighbours outside the walk: ' + fmt(hiddenHalo) + ' of ' + fmt(halo) +
        ' hidden. A +N badge counts a node\'s; double-click reveals them.</p><div class="row-actions">' +
        '<button type="button" data-legend="halo-all">Reveal all</button><button type="button" data-legend="halo-none">Hide all</button></div>';
    }
    html += '</details>';
    html += '<details data-sec="shapes"' + (legendOpen.shapes ? ' open' : '') + '><summary>Shapes</summary><div class="shape-key">' +
      keySvg('<rect x="1.5" y="3" width="19" height="12" rx="3" fill="' + esc(theme.paper) + '" stroke="' + esc(theme.ink) + '" stroke-width="1.5"/><rect x="3" y="12" width="9" height="2" fill="' + esc(theme.l1) + '"/><rect x="12" y="12" width="7" height="2" fill="' + esc(theme.l2) + '"/>') + '<span>closed folder (strip: languages)</span>' +
      keySvg('<rect x="1.5" y="3" width="19" height="12" rx="3" fill="' + esc(theme.paper) + '" stroke="' + esc(theme.ink) + '" stroke-dasharray="4 3"/>') + '<span>folded to fit the view (+N)</span>' +
      keySvg('<circle cx="11" cy="9" r="5.5" fill="' + esc(theme.l2) + '" stroke="' + esc(theme['node-line']) + '"/>') + '<span>symbol (colour: language)</span>' +
      keySvg('<path d="M11 2.5 L16.6 5.75 L16.6 12.25 L11 15.5 L5.4 12.25 L5.4 5.75Z" fill="none" stroke="' + esc(theme['ink-3']) + '" stroke-width="1.3"/>') + '<span>external</span>' +
      keySvg('<path d="M11 2.5 L16.6 5.75 L16.6 12.25 L11 15.5 L5.4 12.25 L5.4 5.75Z" fill="' + esc(theme.bridge) + '"/>') + '<span>channel, route or key</span>' +
      keySvg('<circle cx="11" cy="9" r="4.5" fill="' + esc(theme.l1) + '"/><circle cx="11" cy="9" r="7.5" fill="none" stroke="' + esc(theme.focus) + '" stroke-width="2"/>') + '<span>' + (META.scope.indexOf('diff') === 0 ? 'changed symbol' : 'seed') + '</span>' +
      keySvg('<path d="M2 9 L20 7 L20 11Z" fill="' + esc(theme.edge) + '"/>') + '<span>direction: wide at the source</span>' +
      '</div></details>';
    if (META.notes && META.notes.length) {
      html += '<details data-sec="notes"' + (legendOpen.notes ? ' open' : '') + '><summary>Notes</summary><ul class="notes">' +
        META.notes.map(function (n) { return '<li>' + esc(n) + '</li>'; }).join('') + '</ul></details>';
    }
    legend.innerHTML = html;
  }
  function toggle(key, label, count) {
    return '<label class="plain"><input type="checkbox" data-toggle="' + key + '"' + (st[key] ? ' checked' : '') + '><span>' + esc(label) +
      '</span><span class="count">' + (count === '' ? '' : fmt(count)) + '</span></label>';
  }
  function keySvg(inner) { return '<svg viewBox="0 0 22 18" aria-hidden="true">' + inner + '</svg>'; }
  legend.addEventListener('toggle', function (ev) {
    var d = ev.target;
    if (d.dataset && d.dataset.sec) legendOpen[d.dataset.sec] = d.open;
  }, true);
  legend.addEventListener('change', function (ev) {
    var t = ev.target;
    if (st.level === 'app') { appLegendChange(t); return; }
    if (t.dataset.colorby) { st.colorBy = t.value; V.nodes.forEach(function (v) { v.mix = null; }); renderLegend(); draw(); return; }
    if (t.dataset.fam) st.famOn[t.dataset.fam] = t.checked;
    else if (t.dataset.lang) st.langOn[t.dataset.lang] = t.checked;
    else if (t.dataset.toggle) st[t.dataset.toggle] = t.checked;
    else return;
    if (t.dataset.toggle === 'showLabels') { draw(); return; }
    applyFilters();
    renderLegend();
  });
  legend.addEventListener('click', function (ev) {
    var b = ev.target.closest('button[data-legend]');
    if (!b) return;
    var what = b.dataset.legend;
    if (what === 'halo-all') N.forEach(function (n) { if (has(n, F.halo)) st.revealed.add(n.i); });
    else if (what === 'halo-none') st.revealed.clear();
    else FAMILIES.forEach(function (f) { st.famOn[f] = what === 'all' || f === 'calls' || (what === 'bridges' && !!BRIDGE[f]); });
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
    if (st.view === 'matrix') renderMatrix();
    setStatus();
    draw();
  }

  // ----------------------------------------------------------------- matrix
  // Folder-by-folder dependency matrix of the current view, rows ordered so
  // most weight sits above the diagonal; a cell in both directions is a
  // cycle, outlined in vermillion.
  var matrixEl = document.getElementById('matrix');
  var MATRIX_MAX = 60;
  function renderMatrix() {
    var ns = V.nodes.slice();
    if (ns.length < 2) {
      matrixEl.innerHTML = '<p class="cap">The matrix needs at least two nodes in view. Open a folder, clear a filter, or go back to the graph.</p>';
      return;
    }
    var trimmed = 0;
    if (ns.length > MATRIX_MAX) {
      ns.sort(function (a, b) { return b.deg - a.deg || a.idx - b.idx; });
      trimmed = ns.length - MATRIX_MAX;
      ns = ns.slice(0, MATRIX_MAX);
    }
    var idx = new Map(ns.map(function (v, i) { return [v.idx, i]; }));
    var m = ns.map(function () { return ns.map(function () { return null; }); });
    V.edges.forEach(function (e) {
      if (!idx.has(e.a) || !idx.has(e.b)) return;
      var cell = m[idx.get(e.a)][idx.get(e.b)];
      if (!cell) cell = m[idx.get(e.a)][idx.get(e.b)] = { w: 0, kinds: {}, bridge: true, edges: [] };
      cell.w += e.w;
      cell.edges.push(e);
      if (!BRIDGE[e.fam]) cell.bridge = false;
      Object.keys(e.kinds).forEach(function (k) { cell.kinds[k] = (cell.kinds[k] || 0) + e.kinds[k]; });
    });
    // Triangularise: repeatedly take the node with least remaining inflow.
    var left = new Set(ns.map(function (_, i) { return i; })), order = [];
    while (left.size) {
      var best = -1, bestScore = Infinity;
      left.forEach(function (i) {
        var inflow = 0, outflow = 0;
        left.forEach(function (j) { if (m[j][i] && j !== i) inflow += m[j][i].w; if (m[i][j] && j !== i) outflow += m[i][j].w; });
        var s = inflow - outflow * 0.001;
        if (s < bestScore || (s === bestScore && i < best)) { bestScore = s; best = i; }
      });
      order.push(best);
      left.delete(best);
    }
    var max = 1;
    m.forEach(function (row) { row.forEach(function (c) { if (c) max = Math.max(max, c.w); }); });
    var name = function (v) { return v.cluster !== null ? v.cluster : v.leaf.qual; };
    var html = '<p class="cap">Rows depend on columns. Darker cells carry more edges; a <b>vermillion outline</b> marks a pair that depends both ways (a cycle). ' +
      'Purple cells are bridges only. Rows are ordered so most weight sits above the diagonal. Click a cell for its edges.' +
      (trimmed ? ' Showing the ' + MATRIX_MAX + ' most connected of ' + fmt(ns.length + trimmed) + ' nodes.' : '') + '</p>';
    html += '<table><thead><tr><th></th>' + order.map(function (i) {
      return '<th scope="col" title="' + esc(name(ns[i])) + '"><span>' + esc(ns[i].label) + '</span></th>';
    }).join('') + '</tr></thead><tbody>';
    order.forEach(function (i) {
      html += '<tr><th scope="row" title="' + esc(name(ns[i])) + '">' + esc(ns[i].label) + '</th>';
      order.forEach(function (j) {
        if (i === j) { html += '<td class="self"></td>'; return; }
        var c = m[i][j];
        if (!c) { html += '<td></td>'; return; }
        var v = Math.log(1 + c.w) / Math.log(1 + max);
        var cls = (m[j][i] ? 'cycle ' : '') + (c.bridge ? 'bridge' : '');
        var title = name(ns[i]) + ' → ' + name(ns[j]) + ': ' + Object.keys(c.kinds).map(function (k) { return k + ' ×' + c.kinds[k]; }).join(', ');
        html += '<td class="' + cls + '"><button type="button" data-cell="' + ns[i].idx + ',' + ns[j].idx + '" style="--v:' + v.toFixed(3) + '"' +
          (v > 0.6 ? ' class="hot"' : '') + ' title="' + esc(title) + '" aria-label="' + esc(title) + '">' + (c.w > 999 ? '1k+' : c.w) + '</button></td>';
      });
      html += '</tr>';
    });
    matrixEl.innerHTML = html + '</tbody></table>';
  }
  matrixEl.addEventListener('click', function (ev) {
    var b = ev.target.closest('button[data-cell]');
    if (!b) return;
    var p = b.dataset.cell.split(',').map(Number);
    var es = V.edges.filter(function (e) { return e.a === p[0] && e.b === p[1]; });
    if (!es.length) return;
    // Merge the pair's lines into one record for the panel.
    var merged = { a: p[0], b: p[1], kinds: {}, w: 0, n: 0, refs: [], fam: es[0].fam, idx: es[0].idx, kind: es[0].kind };
    es.forEach(function (e) {
      merged.w += e.w; merged.n += e.n; merged.refs = merged.refs.concat(e.refs);
      Object.keys(e.kinds).forEach(function (k) { merged.kinds[k] = (merged.kinds[k] || 0) + e.kinds[k]; });
    });
    st.selected = null;
    st.selectedEdge = merged;
    renderPanel();
  });
  var viewGraph = document.getElementById('view-graph'), viewMatrix = document.getElementById('view-matrix');
  function setView(v, fromHistory) {
    if (!fromHistory && v !== st.view) pushHistory();
    st.view = v;
    viewGraph.setAttribute('aria-pressed', String(v === 'graph'));
    viewMatrix.setAttribute('aria-pressed', String(v === 'matrix'));
    matrixEl.hidden = v !== 'matrix';
    mini.style.visibility = v === 'matrix' ? 'hidden' : '';
    if (v === 'matrix') renderMatrix(); else draw();
  }
  viewGraph.addEventListener('click', function () { setView('graph'); });
  viewMatrix.addEventListener('click', function () { setView('matrix'); });

  // ---------------------------------------------------------------- toolbar
  var layForce = document.getElementById('lay-force'), layLayer = document.getElementById('lay-layer');
  function setLayout(name, fromHistory) {
    if (!fromHistory && name !== st.layout) pushHistory();
    st.layout = name;
    layForce.setAttribute('aria-pressed', String(name === 'force'));
    layLayer.setAttribute('aria-pressed', String(name === 'layered'));
    st.userMoved = false;
    if (name === 'force') { V.routes = null; V.cols = null; startForce(0.6); }
    else { sim.running = false; layered(REDUCE); }
    setStatus();
  }
  layForce.addEventListener('click', function () { setLayout('force'); });
  layLayer.addEventListener('click', function () { setLayout('layered'); });
  var groupSel = { value: 'tree' };
  function applyDepth(d) {
    pushHistory();
    depthTo(d);
  }
  function depthTo(d) {
    st.group = 'tree';
    savePositions();
    BUDGET = 900;
    setDepth(d);
    rebuild();
    if (st.selected && !V.byId.has(st.selected)) st.selected = null;
    computeHighlight();
    renderPanel();
    st.userMoved = false;
    relayout(0.5);
    if (st.view === 'matrix') renderMatrix();
  }
  function currentDepth() {
    if (st.depth === Infinity) return MAX_DEPTH;
    if (st.depth > 0) return st.depth;
    var d = Infinity;
    V.nodes.forEach(function (v) { if (v.cluster !== null && !PSEUDO[v.cluster]) d = Math.min(d, prefixDepth(v.cluster)); });
    return isFinite(d) ? d : MAX_DEPTH;
  }
  var depthLabel = document.getElementById('depth-label');
  function updateDepthLabel() {
    if (!depthLabel) return;
    depthLabel.textContent = st.depth === Infinity ? 'all folders' : st.depth > 0 ? 'depth ' + st.depth : 'custom';
    updateLevelButtons();
  }
  document.getElementById('collapse-all').addEventListener('click', function () { closeMenu(); applyDepth(1); });
  document.getElementById('expand-all').addEventListener('click', function () { closeMenu(); applyDepth(Infinity); });
  document.getElementById('depth-less').addEventListener('click', function () { applyDepth(Math.max(1, currentDepth() - 1)); });
  document.getElementById('depth-more').addEventListener('click', function () {
    var d = currentDepth() + 1;
    applyDepth(d >= MAX_DEPTH ? Infinity : d);
  });
  document.getElementById('fit').addEventListener('click', function () { fit(true); });

  var moreBtn = document.getElementById('more'), moreMenu = document.getElementById('more-menu');
  function closeMenu() { moreMenu.hidden = true; moreBtn.setAttribute('aria-expanded', 'false'); }
  moreBtn.addEventListener('click', function () {
    var open = moreMenu.hidden;
    moreMenu.hidden = !open;
    moreBtn.setAttribute('aria-expanded', String(open));
    if (open) { var first = moreMenu.querySelector('button'); if (first) first.focus(); }
  });
  moreMenu.addEventListener('keydown', function (ev) {
    var items = Array.from(moreMenu.querySelectorAll('button'));
    var i = items.indexOf(document.activeElement);
    if (ev.key === 'ArrowDown') { ev.preventDefault(); items[(i + 1) % items.length].focus(); }
    else if (ev.key === 'ArrowUp') { ev.preventDefault(); items[(i - 1 + items.length) % items.length].focus(); }
    else if (ev.key === 'Escape') { closeMenu(); moreBtn.focus(); }
  });
  document.addEventListener('click', function (ev) { if (!ev.target.closest('.menu-wrap')) closeMenu(); });

  var empty = document.getElementById('empty'), emptyText = document.getElementById('empty-text');
  function updateEmpty() {
    if (!N.length) {
      emptyText.innerHTML = '<strong>This graph is empty.</strong> The index had no nodes for this scope; try <code>--scope all</code> or another <code>--seed</code>.';
      document.getElementById('empty-reset').hidden = true;
      empty.hidden = false;
      return;
    }
    empty.hidden = V.nodes.length > 0;
  }
  document.getElementById('empty-reset').addEventListener('click', function () {
    FAMILIES.forEach(function (f) { st.famOn[f] = true; });
    Object.keys(st.langOn).forEach(function (l) { st.langOn[l] = true; });
    st.showExternal = true; st.showTests = true; st.isolate = null;
    if (!N.some(function (n) { return !has(n, F.scope_out); })) st.showNonCode = true;
    updateBanner();
    applyFilters();
    renderLegend();
  });

  var legendBtn = document.getElementById('legend-toggle');
  function setLegend(open) {
    legend.hidden = !open;
    legendBtn.setAttribute('aria-expanded', String(open));
    legendBtn.textContent = open ? 'Hide legend and filters' : 'Show legend and filters';
    requestAnimationFrame(resize);
  }
  legendBtn.addEventListener('click', function () { closeMenu(); setLegend(legend.hidden); });

  function storageGet(k) { try { return window.localStorage.getItem(k); } catch (e) { return null; } }
  function storageSet(k, v) { try { window.localStorage.setItem(k, v); } catch (e) { /* unavailable */ } }
  var themeBtn = document.getElementById('theme');
  function effectiveDark() {
    var t = document.documentElement.getAttribute('data-theme');
    if (t) return t === 'dark';
    return !!(window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches);
  }
  function applyTheme(t) {
    if (t) document.documentElement.setAttribute('data-theme', t);
    readTheme();
    themeBtn.textContent = effectiveDark() ? 'Light theme' : 'Dark theme';
    V.nodes.forEach(function (v) { v.mix = null; });
    renderLegend();
    renderPanel();
    draw();
  }
  themeBtn.addEventListener('click', function () {
    closeMenu();
    var next = effectiveDark() ? 'light' : 'dark';
    storageSet('lidx-graph-theme', next);
    applyTheme(next);
  });
  if (window.matchMedia) {
    var mq = window.matchMedia('(prefers-color-scheme: dark)');
    if (mq.addEventListener) mq.addEventListener('change', function () { applyTheme(null); });
  }
  var helpDialog = document.getElementById('help-dialog');
  function openHelp() { closeMenu(); if (helpDialog.showModal) helpDialog.showModal(); else helpDialog.setAttribute('open', ''); }
  document.getElementById('help').addEventListener('click', openHelp);

  // ----------------------------------------------------------------- export
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
    closeMenu(); draw();
    canvas.toBlob(function (b) { if (b) download(baseName + '.png', b); }, 'image/png');
  });
  document.getElementById('export-svg').addEventListener('click', function () {
    closeMenu(); download(baseName + '.svg', new Blob([toSvg()], { type: 'image/svg+xml' }));
  });
  document.getElementById('export-json').addEventListener('click', function () {
    closeMenu();
    var data = st.level === 'app' ? { repo: META.repo, commit: META.commit, units: AU.filter(unitVisible), links: AE.filter(linkVisible), chips: CHIPS } : toJson();
    download(baseName + (st.level === 'app' ? '-apps.json' : '-view.json'), new Blob([JSON.stringify(data, null, 1)], { type: 'application/json' }));
  });
  // The visible subgraph: what is drawn, with the edges behind each line.
  function toJson() {
    return {
      repo: META.repo, commit: META.commit, graph_version: META.graph_version, scope: META.scope,
      nodes: V.nodes.map(function (v) {
        return v.cluster !== null
          ? { id: v.id, folder: v.cluster, members: v.members.length }
          : { id: v.id, qualname: v.leaf.qual, kind: v.leaf.kind, language: v.leaf.lang, file: v.leaf.file, line: v.leaf.line || null };
      }),
      edges: V.edges.map(function (e) {
        return { from: V.nodes[e.a].id, to: V.nodes[e.b].id, kinds: e.kinds, weight: e.w };
      })
    };
  }
  function toSvg() {
    var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    V.nodes.forEach(function (v) {
      x0 = Math.min(x0, v.x - v.r * 1.3 - 4); y0 = Math.min(y0, v.y - v.r - 24);
      x1 = Math.max(x1, v.x + v.r * 1.3 + 160); y1 = Math.max(y1, v.y + v.r + 24);
    });
    if (!isFinite(x0)) { x0 = 0; y0 = 0; x1 = 100; y1 = 100; }
    var f = function (n) { return n.toFixed(1); };
    var out = ['<svg xmlns="http://www.w3.org/2000/svg" viewBox="' + [x0, y0, x1 - x0, y1 - y0].map(f).join(' ') + '" font-family="system-ui, sans-serif" font-size="11">'];
    out.push('<rect x="' + f(x0) + '" y="' + f(y0) + '" width="' + f(x1 - x0) + '" height="' + f(y1 - y0) + '" fill="' + esc(theme.canvas) + '"/>');
    out.push('<defs>' + FAMILIES.map(function (fm) {
      return '<marker id="a-' + fm + '" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="' + esc(famColor(fm)) + '"/></marker>';
    }).join('') + '</defs>');
    V.edges.forEach(function (e) {
      var a = V.nodes[e.a], b = V.nodes[e.b], s = STYLE[e.fam] || STYLE.other;
      var r = st.layout === 'layered' && V.routes ? V.routes.get(e.a + '>' + e.b) : null;
      var d = 'M' + f(a.x) + ' ' + f(a.y);
      if (r && r.length > 1) {
        for (var i = 1; i < r.length; i++) {
          var p0 = i === 1 ? a : r[i - 1], p1 = i === r.length - 1 ? b : r[i], h = (p1.x - p0.x) * 0.5;
          d += 'C' + f(p0.x + h) + ' ' + f(p0.y) + ' ' + f(p1.x - h) + ' ' + f(p1.y) + ' ' + f(p1.x) + ' ' + f(p1.y);
        }
      } else d += 'L' + f(b.x) + ' ' + f(b.y);
      out.push('<path d="' + d + '" fill="none" stroke="' + esc(famColor(e.fam)) + '" stroke-opacity="' + (BRIDGE[e.fam] ? 0.9 : 0.55) +
        '" stroke-width="' + edgeWidth(e, 1).toFixed(2) + '"' + (s.dash.length ? ' stroke-dasharray="' + s.dash.join(' ') + '"' : '') +
        ' marker-end="url(#a-' + e.fam + ')"><title>' + esc(kindsText(e) + ': ' + vName(e.a) + ' → ' + vName(e.b)) + '</title></path>');
    });
    V.nodes.forEach(function (v) {
      var n = v.leaf;
      if (v.cluster !== null) {
        out.push('<rect x="' + f(v.x - v.r * 1.25) + '" y="' + f(v.y - v.r * 0.78) + '" width="' + f(v.r * 2.5) + '" height="' + f(v.r * 1.56) +
          '" rx="6" fill="' + esc(theme.paper) + '" stroke="' + esc(theme.ink) + '" stroke-width="1.5"><title>' + esc(v.cluster) + '</title></rect>');
        out.push('<text x="' + f(v.x) + '" y="' + f(v.y + v.r * 0.78 + 14) + '" text-anchor="middle" font-weight="700" fill="' + esc(theme.ink) + '">' + esc(v.label) + '</text>');
        return;
      }
      var fill = has(n, F.endpoint) ? theme.bridge : has(n, F.external | F.unresolved) ? theme.canvas : nodeColor(n);
      out.push('<circle cx="' + f(v.x) + '" cy="' + f(v.y) + '" r="' + f(v.r) + '" fill="' + esc(fill) + '" stroke="' + esc(theme['node-line']) + '"' +
        (v.seed ? ' stroke-width="3" style="stroke:' + esc(theme.focus) + '"' : '') + '><title>' + esc(n.qual) + '</title></circle>');
      out.push('<text x="' + f(v.x + v.r + 4) + '" y="' + f(v.y + 4) + '" fill="' + esc(theme.ink) + '">' + esc(labelText(v)) + '</text>');
    });
    out.push('</svg>');
    return out.join('\n');
  }

  // ----------------------------------------------------------- keyboard
  // Arrow keys move to the neighbour in that direction; [ and ] step through
  // callers and callees.
  // Stepping keeps its origin: repeated presses walk the origin's callers
  // (or callees) in turn, not the neighbours of each stop.
  var cycle = null;
  function neighbourStep(dirOut, step) {
    var v = selectedV();
    if (!v) return;
    if (!cycle || cycle.dirOut !== dirOut || cycle.ids.indexOf(v.idx) < 0 || !V.nodes[cycle.ids[0]]) {
      var ids = Array.from(new Set(dirOut ? v.outE.map(function (e) { return e.b; }) : v.inE.map(function (e) { return e.a; })));
      if (!ids.length) { toast(dirOut ? 'No callees in view' : 'No callers in view'); return; }
      cycle = { dirOut: dirOut, ids: ids, i: -1 };
    }
    cycle.i = (cycle.i + step + cycle.ids.length) % cycle.ids.length;
    var target = V.nodes[cycle.ids[cycle.i]];
    var keep = cycle;
    select(target.id, !onScreenV(target));
    cycle = keep;
  }
  function moveDirectional(dx, dy) {
    var v = selectedV();
    if (!v) return false;
    var best = null, bestScore = Infinity;
    var nbrs = new Set(v.outE.map(function (e) { return e.b; }).concat(v.inE.map(function (e) { return e.a; })));
    nbrs.forEach(function (i) {
      var w = V.nodes[i], ox = w.x - v.x, oy = w.y - v.y, d = Math.hypot(ox, oy) || 1;
      var cos = (ox * dx + oy * dy) / d;
      if (cos < 0.35) return;
      var score = d * (2 - cos);
      if (score < bestScore) { bestScore = score; best = w; }
    });
    if (!best) { toast('No neighbour that way'); return true; }
    select(best.id, !onScreenV(best));
    return true;
  }
  document.addEventListener('keydown', function (ev) {
    var tag = (ev.target.tagName || '').toLowerCase();
    var typing = tag === 'input' || tag === 'select' || tag === 'textarea';
    if (ev.key === '/' && !typing) { ev.preventDefault(); search.focus(); search.select(); return; }
    if (typing || ev.metaKey || ev.ctrlKey) return;
    if (ev.altKey) {
      if (ev.key === 'ArrowLeft') { ev.preventDefault(); goBack(); }
      else if (ev.key === 'ArrowRight') { ev.preventDefault(); goForward(); }
      return;
    }
    if (ev.key === '?') { openHelp(); return; }
    if (st.level === 'app') { appKey(ev); return; }
    if ((ev.key === 'a' || ev.key === 'A') && HAS_APPS && !ev.shiftKey) { setLevel('app'); return; }
    if (ev.key === 'Escape') {
      if (st.picking) { st.picking = false; drawOverlay(); return; }
      if (st.isolate && !st.selected) { clearIsolate(); return; }
      clearSelection();
      return;
    }
    var onCanvas = ev.target === canvas || ev.target === document.body;
    if (!onCanvas) return;
    var step = 60, k = ev.key;
    if ((k === 'ArrowLeft' || k === 'ArrowRight' || k === 'ArrowUp' || k === 'ArrowDown') && !ev.shiftKey && selectedV()) {
      ev.preventDefault();
      moveDirectional(k === 'ArrowLeft' ? -1 : k === 'ArrowRight' ? 1 : 0, k === 'ArrowUp' ? -1 : k === 'ArrowDown' ? 1 : 0);
      return;
    }
    switch (k) {
      case 'ArrowLeft': st.cam.x += step; break;
      case 'ArrowRight': st.cam.x -= step; break;
      case 'ArrowUp': st.cam.y += step; break;
      case 'ArrowDown': st.cam.y -= step; break;
      case '+': case '=': zoomAt(W / 2, H / 2, 1.25); return;
      case '-': case '_': zoomAt(W / 2, H / 2, 0.8); return;
      case 'f': case 'F': case '0': fit(true); return;
      case 'l': case 'L': setLayout(st.layout === 'force' ? 'layered' : 'force'); return;
      case 'm': case 'M': setView(st.view === 'graph' ? 'matrix' : 'graph'); return;
      case ']': neighbourStep(true, 1); return;
      case '[': neighbourStep(false, 1); return;
      case 'Enter': if (selectedV()) expand(selectedV()); return;
      case 'c': case 'C': collapseGroupOf(selectedV()); return;
      case 'i': case 'I': isolate(); return;
      case 'p': case 'P': togglePin(selectedV()); return;
      case 'u': case 'U': if (selectedV()) { st.mode = 'up'; computeHighlight(); renderPanel(); draw(); } return;
      case 'd': case 'D': if (selectedV()) { st.mode = 'down'; computeHighlight(); renderPanel(); draw(); } return;
      case '1': case '2': case '3': case '4':
        st.mode = 'hops'; st.hops = +k; computeHighlight(); renderPanel(); draw(); return;
      default: return;
    }
    ev.preventDefault();
    st.userMoved = true;
    markInteracting();
    requestDraw();
  });

  // -------------------------------------------------------------- app view
  // Applications, libraries and infrastructure as an architecture sheet:
  // cards in bands by role (front ends, services, workers and tools,
  // libraries, infra), dependencies flowing down, every link named in
  // words. DOM rather than canvas: there are few cards, and each is a
  // focusable element with real text.
  var KIND_LABEL = {
    ui: 'UI', desktop: 'desktop app', api: 'API', service: 'service', worker: 'worker', cli: 'CLI',
    lib: 'library', loose: 'loose files', data: 'data', infra: 'infra', test: 'tests', example: 'example',
    docs: 'docs', tooling: 'tooling'
  };
  var BANDS = [
    { id: 'front', label: 'Front ends', kinds: { ui: 1, desktop: 1 } },
    { id: 'svc', label: 'APIs, services, workers and tools', kinds: { api: 1, service: 1, worker: 1, cli: 1 } },
    { id: 'lib', label: 'Libraries and shared code', kinds: { lib: 1, loose: 1 } },
    { id: 'infra', label: 'Infrastructure and data', kinds: { infra: 1, data: 1 } },
    { id: 'hidden', label: 'Tests, examples, docs and tooling', kinds: { test: 1, example: 1, docs: 1, tooling: 1 } },
    { id: 'pkg', label: 'Shared external packages', kinds: {} }
  ];
  // Drawn connectors take the style of their most telling part.
  var APP_FAM = {
    http: { label: 'HTTP', rank: 0 },
    rpc: { label: 'RPC', rank: 1 },
    bus: { label: 'message bus', rank: 2 },
    config: { label: 'config keys and secrets', rank: 3 },
    declares: { label: 'proto declares RPC', rank: 4 },
    deploys: { label: 'deploys', rank: 5 },
    code: { label: 'calls and uses (code)', rank: 6 },
    depends: { label: 'declared reference', rank: 7 },
    package: { label: 'uses package', rank: 8 }
  };
  var APP_DASH = {
    code: '', depends: '6 4', http: '11 3 2 3', rpc: '3 3', bus: '11 4', config: '0.5 5', declares: '2 3', deploys: '9 3', package: '1 3'
  };
  var APP_TOK = {
    code: 'edge', depends: 'edge-3', http: 'bridge', rpc: 'bridge', bus: 'bridge', config: 'bridge', declares: 'edge-4', deploys: 'edge-4', package: 'ink-3'
  };
  var LANG_NAME = {
    csharp: 'C#', python: 'Python', typescript: 'TypeScript', tsx: 'TSX', javascript: 'JavaScript', rust: 'Rust', go: 'Go',
    proto: 'Protobuf', sql: 'SQL', postgres: 'PostgreSQL', bicep: 'Bicep', yaml: 'YAML', markdown: 'Markdown', lua: 'Lua'
  };
  var NON_CODE_LANG = { markdown: 1, yaml: 1, json: 1 };
  var CARD_W = { app: 264, lib: 216, infra: 240, open: 344 };
  var LANE_W = 168;
  var ast = {
    // Families drawn only around the card in focus: every app reads config
    // and is deployed, so drawn always they would bury the rest. The card
    // text says what they link to.
    quiet: { config: true, deploys: true },
    on: {}, showKind: { test: false, example: false, docs: false, tooling: false },
    packages: false, undeclared: false, selected: null, selConn: null, hover: null,
    open: new Set(), cam: { k: 1, x: 0, y: 0 }, built: false, userMoved: false,
    pos: {}, conns: [], lists: new Set()
  };
  Object.keys(APP_FAM).forEach(function (f) { ast.on[f] = true; });
  var appsEl = document.getElementById('apps'), appsWorld = document.getElementById('apps-world');
  var appsSvg = document.getElementById('apps-edges'), appsCards = document.getElementById('apps-cards');
  var zedges = document.getElementById('apps-zedges');
  var FILE_NODE = new Map();
  N.forEach(function (n) { if (n.file && !FILE_NODE.has(n.file)) FILE_NODE.set(n.file, n.i); });

  function langName(l) { return LANG_NAME[l] || l; }
  function unitKind(u) { return KIND_LABEL[u.kind] || u.kind; }
  function unitVisible(u) { return !u.hidden || !!ast.showKind[u.kind]; }
  function linkVisible(e) {
    return ast.on[e.family] && unitVisible(AU[e.from]) && unitVisible(AU[e.to]) && (e.declared || ast.undeclared);
  }
  function keyName(k) { var i = k.indexOf('://'); return i >= 0 ? k.slice(i + 3) : k; }
  function codeLangs(u) {
    var l = u.langs.filter(function (x) { return !NON_CODE_LANG[x[0]]; });
    return (l.length ? l : u.langs).slice(0, 2).map(function (x) { return x[0]; });
  }
  function exposesList(u) {
    var x = u.exposes, out = [];
    if (u.kind === 'infra') {
      // Infra defines topics and routes rather than serving them.
      var t = x.bus.total + u.consumes.topics.total;
      if (t) out.push('defines ' + plural(t, 'topic or subscription', 'topics and subscriptions'));
      if (x.http.total) out.push('defines ' + plural(x.http.total, 'route', 'routes'));
      if (x.provides.total) out.push('provides ' + plural(x.provides.total, 'key or secret', 'keys and secrets'));
      return out;
    }
    if (x.http.total) out.push(plural(x.http.total, 'HTTP route', 'HTTP routes'));
    if (x.rpc.total) out.push(plural(x.rpc.total, 'RPC method', 'RPC methods') + (u.services.length ? ' in ' + plural(u.services.length, 'service', 'services') : ''));
    if (x.bus.total) out.push('subscribes to ' + plural(x.bus.total, 'topic', 'topics'));
    if (x.declares.total) out.push('declares ' + plural(x.declares.total, 'RPC method', 'RPC methods'));
    if (x.provides.total) out.push('provides ' + plural(x.provides.total, 'key or secret', 'keys and secrets'));
    if (u.entries.length) out.push('CLI ' + u.entries.slice(0, 3).join(', ') + (u.entries.length > 3 ? ' +' + (u.entries.length - 3) : ''));
    return out;
  }
  function consumesList(u) {
    var c = u.consumes, out = [];
    if (u.kind === 'infra') {
      if (c.secrets.total) out.push('reads ' + plural(c.secrets.total, 'secret', 'secrets'));
      if (c.config.total) out.push('reads ' + plural(c.config.total, 'config key', 'config keys'));
      return out;
    }
    if (c.config.total) out.push(plural(c.config.total, 'config key', 'config keys'));
    if (c.secrets.total) out.push(plural(c.secrets.total, 'secret', 'secrets'));
    if (c.topics.total) out.push('publishes to ' + plural(c.topics.total, 'topic', 'topics'));
    var ext = c.calls.items.filter(function (it) { return it[3]; }).length;
    if (ext) out.push('calls ' + plural(ext, 'external endpoint', 'external endpoints'));
    return out;
  }
  function relName(u, p) { return u.dir && p.indexOf(u.dir) === 0 ? p.slice(u.dir.length) : p; }
  function deploysOf(u) {
    return AE.filter(function (e) { return e.from === u.id && e.family === 'deploys'; }).map(function (e) { return AU[e.to].name; });
  }

  function cardHtml(u) {
    var open = ast.open.has(u.id);
    var shape = u.app ? 'app' : (u.kind === 'infra' || u.kind === 'data') ? 'infra' : 'lib';
    var cls = 'card ' + (shape === 'app' ? 'is-app' : shape) + (u.hidden ? ' hidden-kind' : '') + (open ? ' open' : '');
    var langs = codeLangs(u);
    var dots = langs.map(function (l) {
      return '<i style="background:' + esc(langColor(l)) + '"></i>' + esc(langName(l));
    }).join(' ');
    var kind = unitKind(u) + (u.kind === 'infra' && u.why ? ' · ' + u.why : '');
    if (open && zoomable(u)) return zoomCardHtml(u, cls, kind, u.kind !== 'infra' ? dots : '');
    var html = '<article role="listitem" class="' + cls + '" data-key="u' + u.id + '" tabindex="0" title="' + esc(u.root) + '" aria-expanded="' + open + '" aria-label="' +
      esc(u.name + ', ' + unitKind(u) + (langs.length ? ', ' + langs.map(langName).join(' and ') : '')) + '">';
    html += '<div class="k"><span>' + esc(kind) + '</span>' + (u.kind !== 'infra' && dots ? '<span class="lg">' + dots + '</span>' : '') + '</div>';
    if (zoomable(u) && u.files) html += '<button type="button" class="zexp" data-zexp="' + u.id + '" aria-label="Expand ' + esc(u.name) + ' in place" title="Expand in place: folders, files, symbols (Enter)">Expand</button>';
    html += '<h3>' + esc(u.name) + '</h3>';
    // The path is in the tooltip and the panel: on the sheet it costs a line
    // per card and says less than the name.
    if (open) html += '<div class="root">' + esc(u.root) + '</div>';
    var ex = exposesList(u), co = consumesList(u), dep = deploysOf(u), dl = '';
    if (ex.length) dl += '<dt>Exposes</dt><dd>' + esc(ex.join(' · ')) + '</dd>';
    if (co.length) dl += '<dt>Uses</dt><dd>' + esc(co.join(' · ')) + '</dd>';
    if (dep.length) dl += '<dt>Deploys</dt><dd>' + esc(dep.slice(0, 4).join(', ') + (dep.length > 4 ? ' +' + (dep.length - 4) : '')) + '</dd>';
    if (dl) html += '<dl>' + dl + '</dl>';
    var mods = u.modules;
    if (open) {
      html += '<ul class="modlist" aria-label="Folders in ' + esc(u.name) + '">' + (mods.length ? mods.map(function (m) {
        return '<li><button type="button" data-folder="' + esc(m[0]) + '"><span>' + esc(relName(u, m[0])) + '</span><span class="n">' +
          esc(plural(m[1], 'file', 'files')) + ' · ' + fmt(m[2]) + '</span></button></li>';
      }).join('') : '<li class="more">All ' + esc(plural(u.files, 'file sits', 'files sit')) + ' at its root.</li>') + '</ul>';
      html += '<div class="open-row"><button type="button" data-folder="' + esc(u.dir) + '">Open in folders</button>' +
        '<button type="button" data-close-card="' + u.id + '">Close</button></div>';
    } else if (mods.length) {
      var top = mods.slice(0, 3).map(function (m) { return relName(u, m[0]); });
      html += '<p class="mods"><b>' + esc(top.join(' · ')) + '</b>' + (mods.length > 3 ? ' +' + (mods.length - 3) : '') + '</p>';
    }
    html += '</article>';
    return html;
  }
  function chipHtml(c, i) {
    return '<article role="listitem" class="card chip" data-key="p' + i + '" tabindex="0" aria-label="' + esc(c.name + ', external package used by ' + plural(c.users.length, 'unit', 'units')) + '">' +
      '<h3>' + esc(c.name) + '</h3><div class="root">' + esc(plural(c.users.length, 'user', 'users')) + '</div></article>';
  }

  // Connectors: one per ordered pair of cards, merging that pair's links.
  function buildConns() {
    var map = new Map(), out = [];
    AE.forEach(function (e, i) {
      if (!linkVisible(e)) return;
      var k = e.from + '>' + e.to, c = map.get(k);
      if (!c) { c = { a: 'u' + e.from, b: 'u' + e.to, parts: [] }; map.set(k, c); out.push(c); }
      c.parts.push(i);
    });
    if (ast.packages) {
      CHIPS.forEach(function (ch, ci) {
        ch.users.forEach(function (u) { if (unitVisible(AU[u])) out.push({ a: 'u' + u, b: 'p' + ci, parts: [], pkg: ci }); });
      });
    }
    out.forEach(function (c, i) {
      c.i = i;
      c.parts.sort(function (x, y) { return APP_FAM[AE[x].family].rank - APP_FAM[AE[y].family].rank; });
      c.fam = c.parts.length ? AE[c.parts[0]].family : 'package';
      c.weight = c.parts.reduce(function (m, j) { return Math.max(m, AE[j].weight); }, 1);
      c.label = c.parts.length ? c.parts.map(function (j) { return AE[j].label; }).join(' · ') : 'uses';
    });
    return out;
  }
  function bandIndex(key) {
    if (key.charAt(0) === 'p') return BANDS.length - 1;
    var u = AU[+key.slice(1)];
    for (var b = 0; b < BANDS.length; b++) if (BANDS[b].kinds[u.kind]) return b;
    return 2;
  }
  // Measured size, or an estimate when the page is not laid out (headless
  // checks, a hidden stage).
  function cardSize(key, el) {
    if (key.charAt(0) === 'u' && ast.open.has(+key.slice(1)) && zoomable(AU[+key.slice(1)])) return zcardSize(+key.slice(1));
    if (el && el.offsetWidth > 0) return { w: el.offsetWidth, h: el.offsetHeight };
    if (key.charAt(0) === 'p') return { w: 150, h: 40 };
    var u = AU[+key.slice(1)], open = ast.open.has(u.id);
    var w = open ? CARD_W.open : u.app ? CARD_W.app : u.kind === 'infra' || u.kind === 'data' ? CARD_W.infra : CARD_W.lib;
    var lines = 3 + exposesList(u).length + consumesList(u).length + (u.modules.length ? 1 : 0) + (open ? Math.min(14, u.modules.length) + 1 : 0);
    return { w: w, h: 22 + lines * 17 };
  }

  // Layered layout: bands top to bottom; libraries split into rows so a
  // library sits above the libraries it depends on; rows ordered by the
  // barycentre of their neighbours, then wrapped to the sheet width.
  function layoutApps(keys, sizes, conns) {
    var band = {}, layerOf = {}, adj = {};
    keys.forEach(function (k) { band[k] = bandIndex(k); adj[k] = []; });
    conns.forEach(function (c) {
      if (band[c.a] === undefined || band[c.b] === undefined) return;
      adj[c.a].push(c.b); adj[c.b].push(c.a);
    });
    // Library height: 1 + the tallest library it reaches.
    var libBand = 2, height = {};
    function libHeight(k, seen) {
      if (height[k] !== undefined) return height[k];
      if (seen[k]) return 0;
      seen[k] = 1;
      var h = 0;
      conns.forEach(function (c) {
        if (c.a === k && band[c.b] === libBand && c.b !== k && c.fam !== 'package') h = Math.max(h, 1 + libHeight(c.b, seen));
      });
      seen[k] = 0;
      height[k] = h;
      return h;
    }
    var maxH = 0;
    keys.forEach(function (k) { if (band[k] === libBand) maxH = Math.max(maxH, libHeight(k, {})); });
    var layerKeys = [];
    keys.forEach(function (k) {
      var b = band[k];
      var sub = b === libBand ? Math.min(3, maxH - height[k]) : 0;
      var lk = b * 10 + Math.max(0, Math.min(sub, 9));
      layerOf[k] = lk;
      if (layerKeys.indexOf(lk) < 0) layerKeys.push(lk);
    });
    layerKeys.sort(function (a, b) { return a - b; });
    var layers = layerKeys.map(function (lk) { return keys.filter(function (k) { return layerOf[k] === lk; }); });
    // Initial order: apps by size, then name.
    layers.forEach(function (L) {
      L.sort(function (a, b) {
        var ua = a.charAt(0) === 'u' ? AU[+a.slice(1)] : null, ub = b.charAt(0) === 'u' ? AU[+b.slice(1)] : null;
        var sa = ua ? ua.symbols : 0, sb = ub ? ub.symbols : 0;
        return sb - sa || (a < b ? -1 : 1);
      });
    });
    var posIn = {};
    function index() { layers.forEach(function (L) { L.forEach(function (k, i) { posIn[k] = L.length > 1 ? i / (L.length - 1) : 0.5; }); }); }
    index();
    for (var sweep = 0; sweep < 8; sweep++) {
      var order = sweep % 2 ? layers.slice().reverse() : layers;
      order.forEach(function (L) {
        var bary = {};
        L.forEach(function (k) {
          var ns = adj[k].filter(function (o) { return layerOf[o] !== layerOf[k]; });
          bary[k] = ns.length ? ns.reduce(function (s, o) { return s + posIn[o]; }, 0) / ns.length : posIn[k];
        });
        L.sort(function (a, b) { return bary[a] - bary[b] || posIn[a] - posIn[b]; });
        L.forEach(function (k, i) { posIn[k] = L.length > 1 ? i / (L.length - 1) : 0.5; });
      });
    }
    var GAP_X = 28, GAP_ROW = 30, GAP_LAYER = 76;
    var sw = stage.getBoundingClientRect().width || 1400;
    // A phone gets a narrow sheet it can scroll down; a desk a wide one.
    var maxW = sw < 760 ? 560 : Math.max(1100, Math.min(1800, Math.round(sw * 1.3)));
    var rows = [], P = {}, labels = [], y = 0, prevBand = -1, rowIdx = 0;
    layers.forEach(function (L) {
      var b = band[L[0]];
      if (b !== prevBand) {
        if (prevBand >= 0) y += GAP_LAYER - GAP_ROW;
        var count = keys.filter(function (k) { return band[k] === b; }).length;
        labels.push({ band: b, y: y, count: count });
        prevBand = b;
      } else y += GAP_LAYER - GAP_ROW;
      // Wrap into rows no wider than the sheet.
      var row = [], rw = 0;
      function flush() {
        if (!row.length) return;
        rows.push({ keys: row, w: rw, y: y });
        var rh = 0, padT = 0;
        row.forEach(function (k) { padT = Math.max(padT, sizes[k].padT || 0); });
        row.forEach(function (k) { rh = Math.max(rh, padT + sizes[k].h + (sizes[k].padB || 0)); });
        row.forEach(function (k) { P[k] = { x: 0, y: y + padT, w: sizes[k].w, h: sizes[k].h, row: rowIdx, band: b }; });
        rowIdx++;
        y += rh + GAP_ROW;
        row = []; rw = 0;
      }
      L.forEach(function (k) {
        var w = sizes[k].w;
        if (row.length && rw + GAP_X + w > maxW) flush();
        rw += (row.length ? GAP_X : 0) + w;
        row.push(k);
      });
      flush();
    });
    var sheet = rows.reduce(function (m, r) { return Math.max(m, r.w); }, 0);
    rows.forEach(function (r) {
      var x = Math.round((sheet - r.w) / 2);
      r.keys.forEach(function (k) { P[k].x = x; x += P[k].w + GAP_X; });
    });
    return { P: P, labels: labels, width: sheet, height: Math.max(0, y - GAP_ROW) };
  }

  // Connector geometry: down between rows (bottom to top), up the other
  // way, an arch above for cards in one row. Ports spread along each side
  // in the order of the other ends, so lines do not cross at the card.
  function routeConns(conns, P) {
    var att = {};
    conns.forEach(function (c) {
      var A = P[c.a], B = P[c.b];
      c.skip = !A || !B;
      if (c.skip) return;
      if (A.row < B.row) { c.sa = 'b'; c.sb = 't'; } else if (A.row > B.row) { c.sa = 't'; c.sb = 'b'; } else { c.sa = 't'; c.sb = 't'; }
      (att[c.a + c.sa] = att[c.a + c.sa] || []).push({ c: c, end: 'a', ox: B.x + B.w / 2 });
      (att[c.b + c.sb] = att[c.b + c.sb] || []).push({ c: c, end: 'b', ox: A.x + A.w / 2 });
    });
    Object.keys(att).forEach(function (k) {
      var list = att[k], key = k.slice(0, -1), side = k.slice(-1), box = P[key];
      list.sort(function (p, q) { return p.ox - q.ox; });
      list.forEach(function (p, i) {
        var f = (i + 1) / (list.length + 1);
        p.c[p.end + 'x'] = box.x + box.w * (0.12 + 0.76 * f);
        p.c[p.end + 'y'] = side === 't' ? box.y : box.y + box.h;
      });
    });
    conns.forEach(function (c) {
      if (c.skip) return;
      var x1 = c.ax, y1 = c.ay, x2 = c.bx, y2 = c.by, c1y, c2y;
      if (c.sa === 'b') { var d = Math.max(28, (y2 - y1) / 2); c1y = y1 + d; c2y = y2 - d; }
      else if (c.sb === 'b') { var u = Math.max(28, (y1 - y2) / 2); c1y = y1 - u; c2y = y2 + u; }
      else { var h = 34 + Math.abs(x2 - x1) * 0.12; c1y = y1 - h; c2y = y2 - h; }
      c.c1y = c1y; c.c2y = c2y;
      c.d = 'M' + r1(x1) + ' ' + r1(y1) + 'C' + r1(x1) + ' ' + r1(c1y) + ' ' + r1(x2) + ' ' + r1(c2y) + ' ' + r1(x2) + ' ' + r1(y2);
      c.mx = 0.125 * x1 + 0.375 * x1 + 0.375 * x2 + 0.125 * x2;
      c.my = 0.125 * y1 + 0.375 * c1y + 0.375 * c2y + 0.125 * y2;
      // Arrowhead at the target, along the last control leg.
      var tx = 0, ty = y2 - c2y, len = Math.abs(ty) || 1, s = 7;
      tx /= len; ty /= len;
      var bx = x2 - tx * s, by = y2 - ty * s, nx = -ty * s * 0.55, ny = tx * s * 0.55;
      c.ah = 'M' + r1(x2) + ' ' + r1(y2) + 'L' + r1(bx + nx) + ' ' + r1(by + ny) + 'L' + r1(bx - nx) + ' ' + r1(by - ny) + 'Z';
    });
  }
  function r1(v) { return Math.round(v * 10) / 10; }
  // Labels go at the middle of their link unless that spot is taken by a
  // card, a band heading or a label placed before (cross-process links
  // first, then by weight); then a little either way along the curve.
  function placeLabels(conns, P, headings) {
    var boxes = Object.keys(P).map(function (k) { var p = P[k]; return [p.x - 4, p.y - 4, p.x + p.w + 4, p.y + p.h + 4]; }).concat(headings);
    var taken = [];
    var order = conns.filter(function (c) { return !c.skip; }).sort(function (a, b) {
      return APP_FAM[a.fam].rank - APP_FAM[b.fam].rank || b.weight - a.weight;
    });
    order.forEach(function (c) {
      var text = c.label.length > 48 ? c.label.slice(0, 47) + '…' : c.label;
      var w = text.length * 6.3 + 6, h = 15, best = null;
      // Along the curve first, then beside it.
      var spots = [];
      [0.5, 0.4, 0.6, 0.3, 0.7, 0.22, 0.78].forEach(function (t) { spots.push([t, 0]); });
      [0.5, 0.35, 0.65].forEach(function (t) { spots.push([t, 1], [t, -1]); });
      spots.some(function (s) {
        var x = bez(c.ax, c.ax, c.bx, c.bx, s[0]) + s[1] * (w / 2 + 8), y = bez(c.ay, c.c1y, c.c2y, c.by, s[0]);
        var r = [x - w / 2, y - h / 2, x + w / 2, y + h / 2];
        var hit = function (b) { return r[0] < b[2] && r[2] > b[0] && r[1] < b[3] && r[3] > b[1]; };
        if (!best) best = { x: x, y: y, r: r };
        if (boxes.some(hit) || taken.some(hit)) return false;
        best = { x: x, y: y, r: r };
        return true;
      });
      c.mx = best.x; c.my = best.y; c.text = text;
      taken.push(best.r);
    });
  }
  function bez(p0, p1, p2, p3, t) {
    var u = 1 - t;
    return u * u * u * p0 + 3 * u * u * t * p1 + 3 * u * t * t * p2 + t * t * t * p3;
  }
  function connWidth(c) { return Math.min(5, 1 + Math.log2(Math.max(1, c.weight)) * 0.55); }

  function appKeys() {
    var keys = AU.filter(unitVisible).map(function (u) { return 'u' + u.id; });
    if (ast.packages) CHIPS.forEach(function (c, i) { keys.push('p' + i); });
    return keys;
  }
  function renderApps(keepCam) {
    ast.built = true;
    var keys = appKeys();
    var html = '';
    keys.forEach(function (k) { html += k.charAt(0) === 'p' ? chipHtml(CHIPS[+k.slice(1)], +k.slice(1)) : cardHtml(AU[+k.slice(1)]); });
    appsCards.innerHTML = html;
    var els = {};
    Array.prototype.forEach.call(appsCards.querySelectorAll('[data-key]'), function (el) { els[el.dataset.key] = el; });
    var sizes = {};
    keys.forEach(function (k) { sizes[k] = cardSize(k, els[k]); });
    keys.forEach(function (k) {
      if (els[k] && els[k].classList.contains('zoomed')) { els[k].style.width = sizes[k].w + 'px'; els[k].style.height = sizes[k].h + 'px'; }
    });
    var conns = buildConns();
    var lay = layoutApps(keys, sizes, conns);
    routeConns(conns, lay.P);
    placeLabels(conns, lay.P, []);
    // Cards are new elements each render: start each where it was (no
    // transition), then move it, so only a real move animates.
    var prev = ast.pos || {};
    ast.pos = lay.P; ast.conns = conns; ast.sheet = lay;
    var at = function (p) { return 'translate(' + p.x + 'px,' + p.y + 'px)'; };
    appsCards.classList.add('placing');
    keys.forEach(function (k) {
      var p = lay.P[k], el = els[k];
      if (el && p) el.style.transform = at(prev[k] || p);
    });
    void appsCards.offsetWidth;
    appsCards.classList.remove('placing');
    keys.forEach(function (k) {
      var p = lay.P[k], el = els[k];
      if (el && p && prev[k]) el.style.transform = at(p);
    });
    // Lane titles sit in the margin, ruled across the sheet between bands,
    // so link labels in the gaps never meet them.
    var labels = lay.labels.map(function (l, i) {
      var b = BANDS[l.band];
      return (i ? '<div class="band-rule" style="transform:translate(' + (-LANE_W) + 'px,' + r1(l.y - 38) + 'px);width:' + (lay.width + LANE_W) + 'px"></div>' : '') +
        '<div class="band-label" style="transform:translate(' + (-LANE_W) + 'px,' + l.y + 'px);width:' + (LANE_W - 28) + 'px">' + esc(b.label) +
        '<span>' + esc(fmt(l.count)) + '</span></div>';
    }).join('');
    appsCards.insertAdjacentHTML('afterbegin', labels);
    var svg = '';
    conns.forEach(function (c) {
      if (c.skip) return;
      var w = connWidth(c);
      // Cross-process links and deployments are few and say the most: their
      // labels always show; the rest on hover or selection.
      var named = c.fam === 'http' || c.fam === 'rpc' || c.fam === 'bus' || c.fam === 'declares' || c.fam === 'deploys';
      var quiet = ast.quiet[c.fam] && c.parts.every(function (j) { return ast.quiet[AE[j].family]; });
      // Next to an open card its own links, ending at its edge, say more.
      var under = c.a.charAt(0) === 'u' && c.b.charAt(0) === 'u' && (ast.open.has(+c.a.slice(1)) || ast.open.has(+c.b.slice(1))) && !!ZR;
      svg += '<g class="conn f-' + c.fam + (named && !under ? ' named' : '') + (quiet ? ' quiet' : '') + (under ? ' under' : '') + '" data-conn="' + c.i + '" style="--w:' + r1(w) + '">' +
        '<path class="ln" d="' + c.d + '" stroke-width="' + r1(w) + '"/><path class="ah" d="' + c.ah + '"/>' +
        '<path class="hit" d="' + c.d + '"/><text class="lbl" x="' + r1(c.mx) + '" y="' + r1(c.my) + '" text-anchor="middle" dy="0.35em">' +
        esc(c.text) + '</text></g>';
    });
    appsSvg.setAttribute('width', String(Math.max(1, lay.width)));
    appsSvg.setAttribute('height', String(Math.max(1, lay.height)));
    appsSvg.innerHTML = svg;
    var shown = conns.filter(function (c) { return !c.skip && !(ast.quiet[c.fam] && c.parts.every(function (j) { return ast.quiet[AE[j].family]; })); }).length;
    appsEl.classList.toggle('few', shown <= 16);
    buildZlinks();
    zRendered = null;
    updateAppEmphasis();
    if (!keepCam || !ast.userMoved) fitApps();
    else applyAppCam();
    renderZoomView(true);
    renderAppCrumbs();
    empty.hidden = keys.length > 0;
    if (!keys.length) {
      emptyText.innerHTML = '<strong>No applications to show.</strong> Every unit here is a test, example, docs or tooling folder; show them from the legend.';
      document.getElementById('empty-reset').hidden = true;
    }
    setStatus();
  }
  function applyAppCam() {
    appsWorld.style.transform = 'translate(' + r1(ast.cam.x) + 'px,' + r1(ast.cam.y) + 'px) scale(' + ast.cam.k + ')';
    if (ast.open.size) requestZoomView();
    setStatus();
  }
  function fitApps() {
    var lay = ast.sheet;
    if (!lay) return;
    var r = stage.getBoundingClientRect(), w = Math.max(1, r.width), h = Math.max(1, r.height);
    var full = lay.width + LANE_W;
    var phone = w < 760;
    var k = Math.min(1, (w - (phone ? 16 : 40)) / Math.max(1, full), phone ? 1 : (h - 40) / Math.max(1, lay.height));
    k = Math.max(ast.open.size ? 0.05 : 0.2, k);
    ast.cam.k = k;
    ast.cam.x = Math.round((w - full * k) / 2 + LANE_W * k);
    ast.cam.y = Math.round(phone ? 12 : Math.max(20, (h - lay.height * k) / 2));
    ast.userMoved = false;
    applyAppCam();
  }
  function zoomApps(px, py, f) {
    var k = Math.max(0.05, Math.min(3, ast.cam.k * f));
    ast.cam.x = px - (px - ast.cam.x) * (k / ast.cam.k);
    ast.cam.y = py - (py - ast.cam.y) * (k / ast.cam.k);
    ast.cam.k = k;
    ast.userMoved = true;
    applyAppCam();
  }
  function neighboursOf(key) {
    var s = new Set([key]);
    ast.conns.forEach(function (c) { if (c.a === key) s.add(c.b); if (c.b === key) s.add(c.a); });
    return s;
  }
  function updateAppEmphasis() {
    var key = ast.selected || ast.hover, conn = ast.selConn !== null ? ast.conns[ast.selConn] : null;
    var near = key ? neighboursOf(key) : conn ? new Set([conn.a, conn.b]) : null;
    appsEl.classList.toggle('focus', !!near);
    Array.prototype.forEach.call(appsCards.querySelectorAll('[data-key]'), function (el) {
      var k = el.dataset.key;
      el.classList.toggle('sel', k === ast.selected);
      el.classList.toggle('dim', !!near && !near.has(k));
    });
    Array.prototype.forEach.call(appsSvg.querySelectorAll('[data-conn]'), function (g) {
      var c = ast.conns[+g.dataset.conn];
      g.classList.toggle('on', !!c && (conn ? c === conn : !!key && (c.a === key || c.b === key)));
    });
  }
  function cardEl(key) { return appsCards.querySelector('[data-key="' + key + '"]'); }
  function selectUnit(key, centre) {
    ast.selected = key;
    ast.selConn = null;
    ast.zlink = null;
    if (!key) ast.zsel = null;
    ast.zpanelUnit = key && key.charAt(0) === 'u' ? +key.slice(1) : null;
    updateAppEmphasis();
    renderPanel();
    var u = key && key.charAt(0) === 'u' ? AU[+key.slice(1)] : null;
    if (u) live.textContent = u.name + ', ' + unitKind(u) + ' selected';
    if (centre && key && ast.pos[key]) {
      var r = stage.getBoundingClientRect(), p = ast.pos[key];
      var cx = (p.x + p.w / 2) * ast.cam.k + ast.cam.x, cy = (p.y + p.h / 2) * ast.cam.k + ast.cam.y;
      if (cx < 40 || cy < 40 || cx > r.width - 40 || cy > r.height - 40) {
        ast.cam.x += r.width / 2 - cx; ast.cam.y += r.height / 2 - cy; ast.userMoved = true; applyAppCam();
      }
    }
  }
  function selectConn(i) {
    ast.zlink = null;
    ast.selConn = i;
    ast.selected = null;
    updateAppEmphasis();
    renderPanel();
  }
  function toggleOpen(id) {
    if (zoomable(AU[id])) { if (ast.open.has(id)) zoomClose(id); else zoomOpen(id, []); return; }
    pushHistory();
    if (ast.open.has(id)) ast.open.delete(id); else ast.open.add(id);
    renderApps(true);
    var el = cardEl('u' + id);
    if (el) el.focus();
  }
  function revealUnit(id) {
    var u = AU[id];
    if (!unitVisible(u)) { ast.showKind[u.kind] = true; renderApps(true); renderLegend(); }
  }
  // Leave the sheet for the folder map, opened at `dir`.
  function openFolder(dir) {
    setLevel('folder');
    if (dir && dir !== './') {
      drillTo(dir);
      var c = V.byId.get('c:' + dir);
      if (c) select(c.id, true);
      else {
        var m = V.nodes.find(function (v) { return v.leaf && v.leaf.file && v.leaf.file.indexOf(dir) === 0; });
        if (m) select(m.id, true);
      }
    }
    canvas.focus();
  }
  function jumpToFile(file) {
    var i = FILE_NODE.get(file);
    if (i === undefined) {
      // A module-level page: the folder that holds the file.
      var best = -1;
      N.forEach(function (n) {
        if (n.kind === 'module' && file.indexOf(n.qual) === 0 && (best < 0 || n.qual.length > N[best].qual.length)) best = n.i;
      });
      if (best < 0) { toast('Not in this page: ' + file); return; }
      i = best;
    }
    setLevel('folder');
    focusLeaf(i);
    canvas.focus();
  }

  // ------------------------------------------------------- app view panel
  function symButtons(syms) {
    return '<ul>' + syms.map(function (s) {
      return '<li><button type="button" data-file="' + esc(s[1]) + '"><span class="nm">' + esc(s[0]) + '</span>' +
        '<span class="loc">' + esc(s[1] + (s[2] ? ':' + s[2] : '')) + '</span></button></li>';
    }).join('') + '</ul>';
  }
  function keySection(id, title, usage, external) {
    if (!usage || !usage.total) return '';
    var items = usage.items, limit = ast.lists.has(id) ? items.length : 12;
    var html = '<h3>' + esc(title) + ' · ' + fmt(usage.total) + '</h3><div class="edges">';
    items.slice(0, limit).forEach(function (it) {
      html += '<details class="key"><summary>' + esc(keyName(it[0])) + (external && it[3] ? '<span class="ext">external</span>' : '') +
        '<span class="n">' + (it[1] > 1 ? '×' + fmt(it[1]) : '') + '</span></summary>' + symButtons(it[2]) + '</details>';
    });
    if (items.length > limit) html += '<div class="more"><button type="button" data-alist="' + esc(id) + '">Show ' + fmt(items.length - limit) + ' more</button></div>';
    if (usage.total > items.length) html += '<div class="more">' + fmt(usage.total - items.length) + ' more not embedded</div>';
    return html + '</div>';
  }
  function famSwatch(f) {
    return '<svg viewBox="0 0 22 10" aria-hidden="true"><line x1="1" y1="5" x2="21" y2="5" stroke="' + esc(theme[APP_TOK[f]]) +
      '" stroke-width="2" stroke-dasharray="' + APP_DASH[f] + '" stroke-linecap="round"/></svg>';
  }
  // Links to shown units first; links to hidden units and resolver guesses
  // fold under a summary.
  function linkRows(list, other) {
    var row = function (e) {
      var o = AU[other(e)];
      return '<li><button type="button" class="lrow" data-unit="' + o.id + '">' + famSwatch(e.family) +
        '<span class="nm">' + esc(o.name) + '</span><span class="w">' + esc(e.label) + '</span></button></li>';
    };
    var main = list.filter(function (e) { return e.declared && unitVisible(AU[other(e)]); });
    var rest = list.filter(function (e) { return main.indexOf(e) < 0; });
    var html = '<ul class="unit-list">' + main.map(row).join('') + '</ul>';
    if (rest.length) {
      html += '<details class="rest"><summary>' + esc(plural(rest.length, 'more link', 'more links')) +
        ' to hidden units or guessed by name</summary><ul class="unit-list">' + rest.map(function (e) {
          var o = AU[other(e)];
          return row(e).replace('</span></button>', (e.declared ? '' : ' · guess') + (unitVisible(o) ? '' : ' · ' + esc(unitKind(o))) + '</span></button>');
        }).join('') + '</ul></details>';
    }
    return html;
  }
  function unitPanel(u) {
    var html = '<div class="panel-head"><h2>' + esc(u.name) + ' <span class="kind">' + esc(unitKind(u)) + '</span></h2>' +
      '<button type="button" data-act="aclose" aria-label="Close details">×</button></div>';
    html += '<dl class="facts"><dt>What</dt><dd>' + esc(u.why) + '</dd>';
    html += '<dt>Where</dt><dd><code>' + esc(u.root) + '</code></dd>';
    if (u.langs.length) html += '<dt>Languages</dt><dd>' + esc(u.langs.slice(0, 5).map(function (l) { return langName(l[0]) + ' ' + fmt(l[1]); }).join(', ')) + '</dd>';
    var sizeRoot = zoomable(u) ? ztree(u.id).root : null;
    html += '<dt>Size</dt><dd>' + esc(sizeRoot ? plural(sizeRoot.files, 'file', 'files') + ' · ' + plural(sizeRoot.syms, 'symbol', 'symbols') : plural(u.files, 'file', 'files') + ' · ' + plural(u.symbols, 'symbol', 'symbols')) + '</dd>';
    if (u.manifests.length) html += '<dt>Manifests</dt><dd>' + u.manifests.map(function (m) { return '<code>' + esc(m) + '</code>'; }).join('<br>') + '</dd>';
    if (u.dockerfiles.length) html += '<dt>Built by</dt><dd>' + u.dockerfiles.map(function (m) { return '<code>' + esc(m) + '</code>'; }).join('<br>') + '</dd>';
    if (u.entries.length) html += '<dt>Commands</dt><dd>' + esc(u.entries.join(', ')) + '</dd>';
    html += '</dl>';
    html += '<div class="actions"><button type="button" data-act="aopen">' + (ast.open.has(u.id) ? 'Collapse card' : zoomable(u) ? 'Expand in place' : 'Open card into folders') + '</button>' +
      '<button type="button" data-folder="' + esc(u.dir) + '">Show in folder map</button></div>';
    // What is inside comes first: it is how you move around an open card.
    if (zoomable(u)) html += treeSection(u);
    var inn = AE.filter(function (e) { return e.to === u.id; }), out = AE.filter(function (e) { return e.from === u.id; });
    var byW = function (a, b) { return APP_FAM[a.family].rank - APP_FAM[b.family].rank || b.weight - a.weight; };
    inn.sort(byW); out.sort(byW);
    var callers = inn.filter(function (e) { return e.family !== 'deploys'; }), deployers = inn.filter(function (e) { return e.family === 'deploys'; });
    var shownN = function (list, other) { return list.filter(function (e) { return e.declared && unitVisible(AU[other(e)]); }).length; };
    var fromOf = function (e) { return e.from; }, toOf = function (e) { return e.to; };
    if (callers.length) html += '<h3>Used by · ' + fmt(shownN(callers, fromOf)) + '</h3>' + linkRows(callers, fromOf);
    if (out.length) html += '<h3>Uses · ' + fmt(shownN(out, toOf)) + '</h3>' + linkRows(out, toOf);
    if (deployers.length) html += '<h3>Deployed by</h3>' + linkRows(deployers, function (e) { return e.from; });
    var ex = u.exposes, co = u.consumes;
    html += keySection('http' + u.id, 'HTTP routes it serves', ex.http);
    if (u.services.length) html += '<p class="lead">RPC services: ' + esc(u.services.slice(0, 12).join(', ') + (u.services.length > 12 ? ', +' + (u.services.length - 12) : '')) + '</p>';
    html += keySection('rpc' + u.id, 'RPC methods it implements', ex.rpc);
    html += keySection('bus' + u.id, 'Topics it subscribes to', ex.bus);
    html += keySection('dec' + u.id, 'RPC methods it declares', ex.declares);
    html += keySection('prov' + u.id, 'Keys and secrets it provides', ex.provides);
    html += keySection('top' + u.id, 'Topics it publishes to', co.topics);
    html += keySection('cfg' + u.id, 'Config keys it reads', co.config);
    html += keySection('sec' + u.id, 'Secrets it reads', co.secrets);
    html += keySection('call' + u.id, 'Endpoints it calls', co.calls, true);
    if (u.packages.length) {
      var names = u.packages.map(function (p) { return p[0]; });
      html += '<h3>External packages · ' + fmt(u.packages.reduce(function (s, p) { return s + p[1].length; }, 0)) + '</h3>' +
        '<p class="lead">Uses ' + esc(names.slice(0, 8).join(', ') + (names.length > 8 ? ', …' : '')) + '</p><div class="edges">' +
        u.packages.map(function (p) {
          return '<details><summary>' + esc(p[0]) + '<span class="n">' + fmt(p[1].length) + '</span></summary><ul>' +
            p[1].map(function (n) { return '<li class="loc" style="padding:2px 7px;font:12px var(--mono)">' + esc(n) + '</li>'; }).join('') + '</ul></details>';
        }).join('') + '</div>';
    }
    if (!zoomable(u) && u.modules.length) {
      html += '<h3>Folders · ' + fmt(u.modules.length) + '</h3><div class="edges"><ul>' + u.modules.slice(0, 20).map(function (m) {
        return '<li><button type="button" data-folder="' + esc(m[0]) + '"><span class="nm">' + esc(relName(u, m[0])) + '</span>' +
          '<span class="loc">' + esc(plural(m[1], 'file', 'files') + ' · ' + plural(m[2], 'symbol', 'symbols')) + '</span></button></li>';
      }).join('') + '</ul></div>';
    }
    return html;
  }
  function chipPanel(ci) {
    var c = CHIPS[ci];
    var html = '<div class="panel-head"><h2>' + esc(c.name) + ' <span class="kind">external packages</span></h2>' +
      '<button type="button" data-act="aclose" aria-label="Close details">×</button></div>';
    html += '<p class="lead">Declared by ' + esc(plural(c.users.length, 'unit', 'units')) + ': ' + esc(c.packages.join(', ')) + '</p>';
    html += '<ul class="unit-list">' + c.users.map(function (u) {
      return '<li><button type="button" data-unit="' + u + '" style="width:100%;text-align:left;background:none;border:0;padding:4px 7px">' + esc(AU[u].name) + '</button></li>';
    }).join('') + '</ul>';
    return html;
  }
  function connPanel(c) {
    var A = c.a.charAt(0) === 'u' ? AU[+c.a.slice(1)].name : CHIPS[+c.a.slice(1)].name;
    var B = c.b.charAt(0) === 'u' ? AU[+c.b.slice(1)].name : CHIPS[+c.b.slice(1)].name;
    var html = '<div class="panel-head"><h2>' + esc(A) + ' → ' + esc(B) + '</h2>' +
      '<button type="button" data-act="aclose" aria-label="Close details">×</button></div>';
    if (!c.parts.length) return html + '<p class="lead">' + esc(A) + ' declares packages from ' + esc(B) + '.</p>';
    c.parts.forEach(function (j) {
      var e = AE[j];
      html += '<h3 style="display:flex;gap:8px;align-items:center">' + famSwatch(e.family) + esc(e.label) + '</h3>';
      if (!e.declared) html += '<p class="lead">No manifest declares this dependency: the resolver matched these by name, so read them as guesses.</p>';
      html += '<div class="edges"><ul>' + e.evidence.map(function (ev) {
        return '<li><button type="button" data-file="' + esc(ev[1]) + '"><span class="nm">' + esc(ev[0]) + '</span>' +
          '<span class="loc">' + esc(ev[1] + (ev[2] ? ':' + ev[2] : '')) + '</span></button></li>';
      }).join('') + '</ul></div>';
      if (e.keys.length > e.evidence.length) html += '<div class="more">and ' + fmt(e.keys.length - e.evidence.length) + ' more keys</div>';
    });
    return html;
  }
  function renderAppPanel() {
    var c = ast.selConn !== null ? ast.conns[ast.selConn] : null;
    var k = ast.selected;
    if (!c && !k && !ast.zlink && !ast.zsel) { panel.hidden = true; return; }
    panel.hidden = false;
    if (ast.zlink) panel.innerHTML = ast.zlink.stub ? stubPanel(ast.zlink.stub) : zlinkPanel(ast.zlink);
    else if (c) panel.innerHTML = connPanel(c);
    else if (ast.zsel && (ast.zpanelUnit === null || ast.zpanelUnit === undefined)) panel.innerHTML = zitemPanel(ast.zsel.u, ast.zsel.id);
    else if (!k) { panel.hidden = true; return; }
    else panel.innerHTML = k.charAt(0) === 'p' ? chipPanel(+k.slice(1)) : unitPanel(AU[+k.slice(1)]);
    renderTree();
  }
  function appPanelClick(b) {
    if (b.dataset.unit !== undefined) { var id = +b.dataset.unit; revealUnit(id); selectUnit('u' + id, true); return true; }
    if (b.dataset.file !== undefined) { jumpToFile(b.dataset.file); return true; }
    if (b.dataset.folder !== undefined) { openFolder(b.dataset.folder); return true; }
    if (b.dataset.alist) { ast.lists.add(b.dataset.alist); renderPanel(); return true; }
    if (b.dataset.act === 'aclose') { ast.zsel = null; ast.zlink = null; ast.zpanelUnit = null; selectUnit(null); renderZoomView(true); writeHash(); appsEl.focus(); return true; }
    if (b.dataset.zend !== undefined) { var end = +b.dataset.zend, z = zdata(); ast.zlink = null; ast.zpanelUnit = null; zoomReveal(endUnit(z, end), end >= 0 ? 's' + end : 'f' + (-1 - end)); return true; }
    if (b.dataset.zunit !== undefined) { ast.zpanelUnit = +b.dataset.zunit; ast.zlink = null; ast.selected = 'u' + b.dataset.zunit; renderPanel(); writeHash(); return true; }
    if (b.dataset.zcopy !== undefined) { copyText(zdata().syms[+b.dataset.zcopy].qual); return true; }
    if (b.dataset.tgo !== undefined) { treeGo(+panel.querySelector('[data-vtree]').dataset.vtree, b.dataset.tgo); return true; }
    if (b.dataset.ttog !== undefined) { treeToggle(b.dataset.ttog); return true; }
    if (b.dataset.zopen !== undefined) { var zu = +b.dataset.zu; zoomOpen(zu, zchain(zu, b.dataset.zopen)); return true; }
    if (b.dataset.act === 'aopen' && ast.selected) { toggleOpen(+ast.selected.slice(1)); renderPanel(); return true; }
    return false;
  }

  // ------------------------------------------------------ app view legend
  function renderAppLegend() {
    var fc = {};
    AE.forEach(function (e) { if (unitVisible(AU[e.from]) && unitVisible(AU[e.to]) && (e.declared || ast.undeclared)) fc[e.family] = (fc[e.family] || 0) + 1; });
    var html = '<details data-sec="edges"' + (legendOpen.edges ? ' open' : '') + '><summary>Links</summary>';
    Object.keys(APP_FAM).forEach(function (f) {
      if (f === 'package') return;
      if (!fc[f] && ast.on[f]) return;
      html += '<label><input type="checkbox" data-afam="' + f + '"' + (ast.on[f] ? ' checked' : '') + '>' +
        famSwatch(f).replace('<svg ', '<svg class="key-line" ') + '<span' + (ast.on[f] ? '' : ' class="off"') + '>' + esc(APP_FAM[f].label) +
        (f in ast.quiet ? ' <button type="button" class="quiet-btn" data-aquiet="' + f + '" aria-pressed="' + !ast.quiet[f] + '" title="Draw these links all the time, not only around the card in focus">' +
          (ast.quiet[f] ? 'on focus' : 'always') + '</button>' : '') +
        '</span><span class="count">' + fmt(fc[f] || 0) + '</span></label>';
    });
    html += '<p class="hint">“On focus” links are drawn around the card you point at.</p></details>';
    var kinds = {};
    AU.forEach(function (u) { if (u.hidden) kinds[u.kind] = (kinds[u.kind] || 0) + 1; });
    var und = AE.filter(function (e) { return !e.declared && unitVisible(AU[e.from]) && unitVisible(AU[e.to]); }).length;
    html += '<details data-sec="show"' + (legendOpen.show ? ' open' : '') + '><summary>Show</summary>';
    [['test', 'Test projects'], ['example', 'Examples'], ['docs', 'Docs'], ['tooling', 'Tooling folders']].forEach(function (p) {
      if (!kinds[p[0]]) return;
      html += '<label class="plain"><input type="checkbox" data-akind="' + p[0] + '"' + (ast.showKind[p[0]] ? ' checked' : '') + '><span>' + esc(p[1]) +
        '</span><span class="count">' + fmt(kinds[p[0]]) + '</span></label>';
    });
    if (CHIPS.length) html += '<label class="plain"><input type="checkbox" data-apkg="1"' + (ast.packages ? ' checked' : '') + '><span>Shared external packages</span><span class="count">' + fmt(CHIPS.length) + '</span></label>';
    if (und) html += '<label class="plain"><input type="checkbox" data-aund="1"' + (ast.undeclared ? ' checked' : '') + '><span>Code links no manifest declares (resolver guesses)</span><span class="count">' + fmt(und) + '</span></label>';
    html += '</details>';
    html += '<details data-sec="shapes"' + (legendOpen.shapes ? ' open' : '') + '><summary>Cards</summary><div class="shape-key">' +
      keySvg('<rect x="1.5" y="2.5" width="19" height="13" rx="2" fill="' + esc(theme.paper) + '" stroke="' + esc(theme.ink) + '" stroke-width="1.6"/>') + '<span>application: something you deploy or run</span>' +
      keySvg('<rect x="1.5" y="2.5" width="19" height="13" rx="2" fill="' + esc(theme['paper-2']) + '" stroke="' + esc(theme['ink-3']) + '"/>') + '<span>library or shared code</span>' +
      keySvg('<rect x="1.5" y="2.5" width="19" height="13" rx="2" fill="none" stroke="' + esc(theme['ink-3']) + '" stroke-dasharray="3 2"/>') + '<span>infrastructure or data</span>' +
      '</div><p class="hint">' + (ZR ? 'Scroll into a card, or double-click it, to open its folders, files and symbols in place.' : 'Double-click a card to open its folders.') + '</p></details>';
    // Notes about the folder graph (dispatch hops, trims) do not apply here.
    legend.innerHTML = html;
  }
  if (legend) legend.addEventListener('click', function (ev) {
    var b = ev.target.closest('button[data-aquiet]');
    if (!b || st.level !== 'app') return;
    ev.preventDefault();
    ast.quiet[b.dataset.aquiet] = !ast.quiet[b.dataset.aquiet];
    renderApps(true);
    renderLegend();
  });
  function appLegendChange(t) {
    if (t.dataset.afam) ast.on[t.dataset.afam] = t.checked;
    else if (t.dataset.akind) ast.showKind[t.dataset.akind] = t.checked;
    else if (t.dataset.apkg) ast.packages = t.checked;
    else if (t.dataset.aund) ast.undeclared = t.checked;
    else return false;
    if (ast.selected && ast.selected.charAt(0) === 'u' && !unitVisible(AU[+ast.selected.slice(1)])) ast.selected = null;
    ast.selConn = null;
    renderApps(true);
    renderLegend();
    renderPanel();
    return true;
  }

  // ------------------------------------------------------ app view input
  var appDrag = null, appPointers = new Map();
  if (appsEl) {
    appsEl.addEventListener('pointerdown', function (ev) {
      appPointers.set(ev.pointerId, [ev.clientX, ev.clientY]);
      var onCard = ev.target.closest('.card');
      // An open card fills the view: it pans like the paper, except on its controls.
      if ((onCard && !onCard.classList.contains('zoomed')) || ev.target.closest('.hit') || ev.target.closest('.zh button, .zfh button')) return;
      appDrag = { x: ev.clientX, y: ev.clientY, cx: ast.cam.x, cy: ast.cam.y, moved: false, pinch: null, bg: !onCard };
      if (appPointers.size === 2) {
        var p = Array.from(appPointers.values());
        appDrag.pinch = { d: Math.hypot(p[0][0] - p[1][0], p[0][1] - p[1][1]), k: ast.cam.k };
      }
      appsEl.classList.add('dragging');
      if (appsEl.setPointerCapture) appsEl.setPointerCapture(ev.pointerId);
    });
    appsEl.addEventListener('pointermove', function (ev) {
      if (appPointers.has(ev.pointerId)) appPointers.set(ev.pointerId, [ev.clientX, ev.clientY]);
      if (appDrag) {
        if (appDrag.pinch && appPointers.size === 2) {
          var p = Array.from(appPointers.values()), r = appsEl.getBoundingClientRect();
          var d = Math.hypot(p[0][0] - p[1][0], p[0][1] - p[1][1]);
          zoomApps((p[0][0] + p[1][0]) / 2 - r.left, (p[0][1] + p[1][1]) / 2 - r.top, (appDrag.pinch.k * d / appDrag.pinch.d) / ast.cam.k);
          return;
        }
        var dx = ev.clientX - appDrag.x, dy = ev.clientY - appDrag.y;
        if (Math.abs(dx) + Math.abs(dy) > 3) { appDrag.moved = true; ast.dragged = true; }
        if (!appDrag.moved) return;
        ast.cam.x = appDrag.cx + dx; ast.cam.y = appDrag.cy + dy; ast.userMoved = true;
        applyAppCam();
        return;
      }
      var hit = ev.target.closest('.hit, .shit');
      var zs = hit && hit.parentNode.dataset.zs !== undefined ? +hit.parentNode.dataset.zs : null;
      if (zs !== ast.zhstub) markStub(zs);
      if (hit) {
        var r2 = stage.getBoundingClientRect();
        if (zs !== null) showTooltip(stubTooltip(ast.stubs[zs]), ev.clientX - r2.left, ev.clientY - r2.top);
        else if (hit.parentNode.dataset.zl !== undefined) showTooltip(zlinkTooltip(ast.zlinks[+hit.parentNode.dataset.zl]), ev.clientX - r2.left, ev.clientY - r2.top);
        else showTooltip(connTooltip(ast.conns[+hit.parentNode.dataset.conn]), ev.clientX - r2.left, ev.clientY - r2.top);
      } else hideTooltip();
      var zi = ev.target.closest('.zi');
      var hv = zi ? itemFromEl(zi) : null, hk = hv && hv.it ? { u: hv.u, id: hv.it.id } : null;
      if ((hk ? hk.u + hk.id : '') !== (ast.zhover ? ast.zhover.u + ast.zhover.id : '')) { ast.zhover = hk; if (zRendered) renderZlinks(zRendered); }
    });
    var endAppPointer = function (ev) {
      appPointers.delete(ev.pointerId);
      if (!appDrag) return;
      var wasDrag = appDrag.moved, appDrag0 = appDrag;
      appDrag = null;
      if (wasDrag) setTimeout(function () { ast.dragged = false; }, 0);
      appsEl.classList.remove('dragging');
      if (!wasDrag && ev.type === 'pointerup' && appDrag0.bg && (ast.selected || ast.selConn !== null || ast.zsel || ast.zlink)) { selectUnit(null); renderZoomView(true); writeHash(); }
    };
    appsEl.addEventListener('pointerup', endAppPointer);
    appsEl.addEventListener('pointercancel', endAppPointer);
    appsEl.addEventListener('pointerleave', function () { hideTooltip(); });
    appsEl.addEventListener('wheel', function (ev) {
      ev.preventDefault();
      var r = appsEl.getBoundingClientRect();
      var f = Math.exp(-ev.deltaY * (ev.deltaMode === 1 ? 0.05 : 0.0018));
      if (zAnim) { cancelAnimationFrame(zAnim); zAnim = null; }
      zoomApps(ev.clientX - r.left, ev.clientY - r.top, f);
      semanticWheel(ev.clientX - r.left, ev.clientY - r.top, f);
    }, { passive: false });
    appsEl.addEventListener('click', function (ev) {
      if (ast.dragged) return;
      var b = ev.target.closest('button');
      if (b && b.dataset.zexp !== undefined) { zoomOpen(+b.dataset.zexp, []); return; }
      if (b && b.dataset.zup !== undefined) { var uu = +b.dataset.zu; zoomTo(uu, (ast.zpath[uu] || []).indexOf(b.dataset.zup)); return; }
      if (b && b.dataset.zcrumb !== undefined) { zoomTo(+b.dataset.zcrumb, +b.dataset.depth); return; }
      if (b && appPanelClick(b)) return;
      if (b && b.dataset.closeCard !== undefined) { toggleOpen(+b.dataset.closeCard); return; }
      var zi = ev.target.closest('.zi');
      if (zi) { var h = itemFromEl(zi); if (h && h.it) zselect(h.u, h.it.id); return; }
      var hit = ev.target.closest('.hit, .shit');
      if (hit && hit.parentNode.dataset.zs !== undefined) { ast.zlink = { stub: ast.stubs[+hit.parentNode.dataset.zs] }; ast.selConn = null; renderPanel(); return; }
      if (hit && hit.parentNode.dataset.zl !== undefined) { ast.zlink = ast.zlinks[+hit.parentNode.dataset.zl]; ast.selConn = null; renderPanel(); return; }
      if (hit) { selectConn(+hit.parentNode.dataset.conn); return; }
      if (ev.target.closest('.card.zoomed')) return;
      var card = ev.target.closest('.card');
      if (card) selectUnit(card.dataset.key, false);
    });
    appsEl.addEventListener('dblclick', function (ev) {
      var zi = ev.target.closest('.zi');
      if (zi) { var h = itemFromEl(zi); if (h) zactivate(h.u, h.it); return; }
      if (ev.target.closest('.card.zoomed')) return;
      var card = ev.target.closest('.card');
      if (!card || ev.target.closest('button') || card.dataset.key.charAt(0) !== 'u') return;
      toggleOpen(+card.dataset.key.slice(1));
    });
    appsEl.addEventListener('mouseover', function (ev) {
      var card = ev.target.closest('.card');
      var k = card ? card.dataset.key : null;
      if (k !== ast.hover) { ast.hover = k; if (!ast.selected && ast.selConn === null) updateAppEmphasis(); }
    });
    appsEl.addEventListener('focusin', function (ev) {
      var card = ev.target.classList && ev.target.classList.contains('card') ? ev.target : null;
      if (card && card.dataset.key !== ast.selected) selectUnit(card.dataset.key, true);
    });
  }
  function connTooltip(c) {
    var A = c.a.charAt(0) === 'u' ? AU[+c.a.slice(1)].name : '', B = c.b.charAt(0) === 'u' ? AU[+c.b.slice(1)].name : CHIPS[+c.b.slice(1)].name;
    var html = '<div class="t-kind">' + esc(A) + ' → ' + esc(B) + '</div>';
    if (!c.parts.length) return html + '<div class="t-dim">declares these packages</div>';
    c.parts.forEach(function (j) {
      var e = AE[j];
      html += '<div style="margin-top:4px"><b>' + esc(e.label) + '</b>' + (e.declared ? '' : ' <span class="t-dim">(no manifest declares it)</span>') + '</div>';
      e.evidence.slice(0, 3).forEach(function (ev) {
        html += '<span class="t-ev">' + esc(ev[0]) + ' <span class="t-dim">' + esc(ev[1] + (ev[2] ? ':' + ev[2] : '')) + '</span></span>';
      });
    });
    return html;
  }
  // Arrow keys move between cards in that direction; Enter opens a card.
  function appKey(ev) {
    var k = ev.key, cur = ast.selected && ast.pos[ast.selected] ? ast.selected : null;
    if (k === 'Escape') {
      if (ast.zlink) { ast.zlink = null; renderPanel(); return; }
      // One level out of the card in focus, then the card itself.
      var fu = ast.zsel ? ast.zsel.u : ast.zfocus;
      if (fu !== null && fu !== undefined && ast.open.has(fu)) {
        if (ast.zsel && !(ast.zpath[fu] || []).length) { ast.zsel = null; renderZoomView(true); renderPanel(); writeHash(); var ce = cardEl('u' + fu); if (ce) ce.focus(); return; }
        zoomOut(fu); return;
      }
      if (ast.selected || ast.selConn !== null) selectUnit(null);
      return;
    }
    if (!appsEl.contains(ev.target) && ev.target !== document.body) return;
    var arrows = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
    if (arrows[k] && !ev.shiftKey && ast.zsel && zarrow(arrows[k][0], arrows[k][1])) { ev.preventDefault(); return; }
    if (k === 'Enter' && ast.zsel && ast.zitems[ast.zsel.u]) { ev.preventDefault(); zactivate(ast.zsel.u, ast.zitems[ast.zsel.u].byId[ast.zsel.id]); return; }
    // Into an open card with the arrows: its first box.
    if (arrows[k] && !ev.shiftKey && cur && cur.charAt(0) === 'u' && ast.open.has(+cur.slice(1)) && ast.zitems[+cur.slice(1)] && ast.zitems[+cur.slice(1)].list.length) {
      ev.preventDefault();
      var zu = +cur.slice(1), zl = ast.zitems[zu].list, path = ast.zpath[zu] || [];
      var deep = path.length ? path[path.length - 1] : null;
      var first = zl.find(function (it) { return it.type !== 'frame' && (deep ? it.parent === deep : it.depth === 0); }) || zl[0];
      zselect(zu, first.id);
      return;
    }
    if (arrows[k]) {
      ev.preventDefault();
      if (!cur || ev.shiftKey) {
        ast.cam.x -= arrows[k][0] * 60; ast.cam.y -= arrows[k][1] * 60; ast.userMoved = true; applyAppCam(); return;
      }
      var a = ast.pos[cur], ax = a.x + a.w / 2, ay = a.y + a.h / 2, best = null, bestS = Infinity;
      Object.keys(ast.pos).forEach(function (key) {
        if (key === cur) return;
        var p = ast.pos[key], dx = p.x + p.w / 2 - ax, dy = p.y + p.h / 2 - ay;
        var along = dx * arrows[k][0] + dy * arrows[k][1];
        if (along <= 1) return;
        var s = along + 2.5 * Math.abs(dx * arrows[k][1] + dy * arrows[k][0]);
        if (s < bestS) { bestS = s; best = key; }
      });
      if (best) { var el = cardEl(best); if (el) el.focus(); else selectUnit(best, true); }
      return;
    }
    if (k === 'Enter' && cur && cur.charAt(0) === 'u') { ev.preventDefault(); toggleOpen(+cur.slice(1)); return; }
    if (k === '+' || k === '=') { zoomApps(W / 2, H / 2, 1.25); return; }
    if (k === '-' || k === '_') { zoomApps(W / 2, H / 2, 0.8); return; }
    if (k === 'f' || k === 'F' || k === '0') { fitApps(); }
  }

  // ------------------------------------------------------ app view zoom
  // Semantic zoom: a card opens in place into its folders, a folder into its
  // files, a file into its symbols (classes with their members, free
  // functions, constants). One open path per card (the breadcrumb); several
  // cards can be open at once. Relations are drawn between whatever is
  // visible at that depth, aggregated with weights; links to other units end
  // at the card's edge, named, and go on to that card when it is shown.
  // Only the items in view are in the DOM.
  var ZR = APPS && APPS.zoom ? APPS.zoom : null;
  var ZD = null;
  var ZPAD = 12, ZHEAD = 84, ZGAP = 12;
  var ZBOX = { w: 196, h: 42 }, ZLEAF = { w: 236, h: 52 }, ZCONT_W = 300, ZROW = 20, ZSYM_HEAD = 44, ZFRAME_HEAD = 28, ZFRAME_PAD = 10;
  var KIND_GLYPH = {
    class: 'C', struct: 'S', interface: 'I', trait: 'T', enum: 'E', record: 'R', type: 'T', method: 'm', function: 'ƒ',
    const: 'K', static: 'K', field: 'f', property: 'p', variable: 'v', var: 'v', enum_value: 'e', message: 'M', service: 'S', rpc: 'r',
    impl: 'i', table: 't', procedure: 'P', resource: 'r', param: 'a'
  };
  ast.zpath = {};      // unit id -> [tree node ids] opened below the card
  ast.zsel = null;     // { u, id } selected inside an open card
  ast.zfocus = null;   // unit id the breadcrumb follows
  ast.zitems = {};     // unit id -> { list, byId, w, h }
  ast.zlinks = [];
  ast.stubs = [];     // where links leave an open level, one per target
  ast.zhstub = null;   // index of the stub under the pointer
  ast.treeQ = {};      // unit id -> tree filter text
  ast.treeOpen = new Set();
  var zRendered = null, zRaf = 0, zCooldown = 0, zAnim = null;
  var ZLINK_CAP = 600;

  function zoomable(u) { return !!ZR && !!u; }
  // Decode the columnar section once, on first use.
  function zdata() {
    if (ZD || !ZR) return ZD;
    var s = ZR.s, nf = ZR.f_path.length, ny = ZR.y_file.length;
    var files = new Array(nf), syms = new Array(ny);
    for (var f = 0; f < nf; f++) {
      files[f] = { i: f, unit: ZR.f_unit[f], path: s[ZR.f_path[f]], lang: s[ZR.f_lang[f]], syms: ZR.f_syms[f], emb: !!ZR.f_emb[f], top: [] };
    }
    for (var y = 0; y < ny; y++) {
      syms[y] = {
        i: y, file: ZR.y_file[y], kind: s[ZR.y_kind[y]], name: s[ZR.y_name[y]], qual: s[ZR.y_qual[y]], line: ZR.y_line[y],
        parent: ZR.y_parent[y], sig: ZR.y_sig[y] >= 0 ? s[ZR.y_sig[y]] : null, kids: []
      };
    }
    syms.forEach(function (y) {
      if (y.parent >= 0 && syms[y.parent].file === y.file) syms[y.parent].kids.push(y.i);
      else { y.parent = -1; files[y.file].top.push(y.i); }
    });
    var e = ZR.e, edges = [], bf = ZR.bridge_from || 0;
    for (var j = 0; j * 4 < e.length; j++) {
      edges.push({ a: e[j * 4], b: e[j * 4 + 1], kind: s[e[j * 4 + 2]], w: e[j * 4 + 3], keys: j >= bf ? ZR.keys[j - bf] || [] : null });
    }
    var omitted = {};
    (ZR.omitted || []).forEach(function (o) { omitted[o[0]] = { syms: o[1], files: o[2] }; });
    ZD = { files: files, syms: syms, edges: edges, trees: {}, omitted: omitted };
    return ZD;
  }
  function endUnit(z, end) { return end >= 0 ? z.files[z.syms[end].file].unit : z.files[-1 - end].unit; }

  // A unit's tree: folders (single-child chains merged), files, symbols.
  // Node ids: 'u<id>' root, 'd<id>:<rel>' folder, 'f<i>' file, 's<i>' symbol.
  function ztree(uid) {
    var z = zdata();
    if (!z) return null;
    if (z.trees[uid]) return z.trees[uid];
    var u = AU[uid], nodes = {}, rel = {};
    var root = { id: 'u' + uid, type: 'unit', name: u.name, kids: [], parent: null, files: 0, syms: 0, rel: '' };
    nodes[root.id] = root;
    function dir(path) {
      var id = 'd' + uid + ':' + path;
      if (nodes[id]) return nodes[id];
      var cut = path.slice(0, -1).lastIndexOf('/');
      var parent = cut < 0 ? root : dir(path.slice(0, cut + 1));
      var n = { id: id, type: 'dir', name: path.slice(cut + 1), kids: [], parent: parent, files: 0, syms: 0, rel: path };
      nodes[id] = n; parent.kids.push(n);
      return n;
    }
    z.files.forEach(function (f) {
      if (f.unit !== uid) return;
      var r = u.dir && f.path.indexOf(u.dir) === 0 ? f.path.slice(u.dir.length) : f.path;
      var cut = r.lastIndexOf('/');
      var parent = cut < 0 ? root : dir(r.slice(0, cut + 1));
      var n = { id: 'f' + f.i, type: 'file', name: r.slice(cut + 1), kids: [], parent: parent, file: f, files: 1, syms: f.syms, rel: r };
      nodes[n.id] = n; parent.kids.push(n);
    });
    // Merge a folder holding only one folder into it: src/main/java/ is one box.
    function merge(n) {
      n.kids.forEach(merge);
      n.kids = n.kids.map(function (k) {
        while (k.type === 'dir' && k.kids.length === 1 && k.kids[0].type === 'dir') {
          var only = k.kids[0];
          rel[k.rel] = only;
          only.name = k.name + only.name; only.parent = k.parent;
          delete nodes[k.id];
          k = only;
        }
        return k;
      });
    }
    merge(root);
    function sum(n) {
      if (n.type === 'file') return;
      n.kids.forEach(function (k) { sum(k); n.files += k.files; n.syms += k.syms; });
      n.kids.sort(function (a, b) { return (a.type === 'file') - (b.type === 'file') || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0); });
    }
    sum(root);
    Object.keys(nodes).forEach(function (id) { if (nodes[id].type === 'dir') rel[nodes[id].rel] = nodes[id]; });
    var t = { root: root, nodes: nodes, rel: rel, uid: uid };
    z.trees[uid] = t;
    return t;
  }
  // Tree node for an id, symbols included.
  function znode(uid, id) {
    var t = ztree(uid);
    if (!t) return null;
    if (t.nodes[id]) return t.nodes[id];
    if (id.charAt(0) === 's') {
      var y = zdata().syms[+id.slice(1)];
      if (y && zdata().files[y.file].unit === uid) return symNode(y);
    }
    return null;
  }
  function symNode(y) {
    return { id: 's' + y.i, type: 'sym', name: y.name, sym: y, kids: y.kids, fileId: 'f' + y.file };
  }
  // Ancestors of a node from the card down (the card itself excluded).
  function zchain(uid, id) {
    var z = zdata(), t = ztree(uid), out = [];
    if (!t) return out;
    var n = t.nodes[id];
    if (!n && id.charAt(0) === 's') {
      var y = z.syms[+id.slice(1)];
      while (y.parent >= 0) y = z.syms[y.parent];
      n = t.nodes['f' + y.file];
    }
    for (; n && n.type !== 'unit'; n = n.parent) out.unshift(n.id);
    return out;
  }
  function openable(n) { return n && (n.type === 'dir' || (n.type === 'file' && n.file.emb && n.file.top.length > 0)); }

  // ----- layout of an open card: shelves of boxes, the open one in place.
  function zmeasure(uid, n, depth) {
    var path = ast.zpath[uid] || [];
    var openId = path[depth];
    var kids;
    if (n.type === 'file') kids = n.file.top.map(function (i) { return symNode(zdata().syms[i]); });
    else kids = n.kids;
    // Above the deepest open level, the boxes beside the path shrink to
    // name tags: context, not content.
    var compact = depth < path.length;
    var lays = kids.map(function (k) {
      if (k.type === 'sym') return symLay(k);
      if (k.id === openId && openable(k)) return frameLay(uid, k, depth + 1);
      if (compact) return { node: k, w: Math.min(190, Math.max(70, k.name.length * 7.4 + 22)), h: 24, compact: true };
      return { node: k, w: ZBOX.w, h: ZBOX.h, kids: [] };
    });
    return shelf(lays);
  }
  function frameLay(uid, n, depth) {
    var inner = zmeasure(uid, n, depth);
    return { node: n, frame: true, w: Math.max(ZBOX.w, inner.w + 2 * ZFRAME_PAD), h: inner.h + ZFRAME_HEAD + ZFRAME_PAD, inner: inner };
  }
  function symRows(y, depth, out) {
    y.kids.forEach(function (i) {
      var k = zdata().syms[i];
      out.push({ sym: k, depth: depth });
      if (depth < 4) symRows(k, depth + 1, out);
    });
    return out;
  }
  function symLay(n) {
    var rows = symRows(n.sym, 0, []);
    if (!rows.length) return { node: n, w: ZLEAF.w, h: ZLEAF.h, rows: rows };
    return { node: n, w: ZCONT_W, h: ZSYM_HEAD + rows.length * ZROW + 6, rows: rows };
  }
  // Pack boxes into rows about as wide as they are tall overall.
  function shelf(lays) {
    if (!lays.length) return { w: ZBOX.w, h: 34, place: [], empty: true };
    var area = 0, widest = 0;
    lays.forEach(function (l) { area += (l.w + ZGAP) * (l.h + ZGAP); widest = Math.max(widest, l.w); });
    var target = Math.max(widest, Math.min(9000, Math.sqrt(area * 1.9)));
    var x = 0, y = 0, rowH = 0, w = 0, place = [];
    lays.forEach(function (l) {
      if (x > 0 && x + l.w > target) { y += rowH + ZGAP; x = 0; rowH = 0; }
      place.push({ lay: l, x: x, y: y });
      x += l.w + ZGAP; rowH = Math.max(rowH, l.h); w = Math.max(w, x - ZGAP);
    });
    return { w: w, h: y + rowH, place: place };
  }
  // Flatten to items with offsets from the card's content origin.
  function zflatten(uid) {
    var t = ztree(uid);
    var top = zmeasure(uid, t.root, 0);
    var list = [], byId = {};
    function add(it) { it.k = list.length; list.push(it); if (!byId[it.id]) byId[it.id] = it; return it; }
    function walk(sh, ox, oy, depth, parentId) {
      sh.place.forEach(function (p) {
        var l = p.lay, x = ox + p.x, y = oy + p.y, n = l.node;
        if (l.frame) {
          add({ id: n.id, type: 'frame', node: n, x: x, y: y, w: l.w, h: l.h, depth: depth, parent: parentId });
          walk(l.inner, x + ZFRAME_PAD, y + ZFRAME_HEAD, depth + 1, n.id);
        } else if (n.type === 'sym') {
          add({ id: n.id, type: 'sym', node: n, x: x, y: y, w: l.w, h: l.h, depth: depth, parent: parentId, rows: l.rows.length });
          l.rows.forEach(function (r, i) {
            add({ id: 's' + r.sym.i, type: 'row', sym: r.sym, x: x + 4, y: y + ZSYM_HEAD + i * ZROW, w: l.w - 8, h: ZROW, depth: depth + 1, indent: r.depth, parent: n.id });
          });
        } else {
          add({ id: n.id, type: n.type, node: n, x: x, y: y, w: l.w, h: l.h, depth: depth, parent: parentId, compact: !!l.compact });
        }
      });
    }
    walk(top, 0, 0, 0, t.root.id);
    var z = { list: list, byId: byId, w: top.w, h: top.h, empty: !!top.empty };
    ast.zitems[uid] = z;
    return z;
  }
  function zcardSize(uid) {
    var z = zflatten(uid);
    // Room above and below for the names of links that leave the card.
    return { w: Math.max(360, z.w + 2 * ZPAD), h: ZHEAD + z.h + ZPAD + (zdata().omitted[uid] ? 22 : 0), padT: 40, padB: 70 };
  }
  function zorigin(uid) {
    var p = ast.pos['u' + uid];
    return p ? { x: p.x + ZPAD, y: p.y + ZHEAD } : null;
  }
  function itemBox(uid, it) {
    var o = zorigin(uid);
    return o ? { x: o.x + it.x, y: o.y + it.y, w: it.w, h: it.h } : null;
  }

  // ----- what an edge end is drawn as: the deepest visible box holding it.
  function zrep(end) {
    var z = zdata(), uid = endUnit(z, end);
    if (!ast.open.has(uid)) return { card: uid };
    var items = ast.zitems[uid];
    if (!items) return { card: uid };
    var id, fileIx;
    if (end >= 0) {
      id = 's' + end;
      if (items.byId[id]) return { u: uid, it: items.byId[id] };
      fileIx = z.syms[end].file;
    } else fileIx = -1 - end;
    var t = ztree(uid), n = t.nodes['f' + fileIx];
    for (; n; n = n.parent) {
      if (n.type === 'unit') return { u: uid, it: null };
      if (items.byId[n.id]) return { u: uid, it: items.byId[n.id] };
    }
    return { u: uid, it: null };
  }
  function repKey(r) { return r.card !== undefined ? 'u' + r.card : r.it ? r.u + ':' + r.it.k : r.u + ':root'; }
  // Is item `a` inside item `b` (or the same) in one open card?
  function within(uid, a, b) {
    var items = ast.zitems[uid];
    for (var it = a; it; it = it.parent && items.byId[it.parent]) { if (it === b) return true; if (!it.parent || it.parent === it.id) break; }
    return false;
  }
  function inFocus(r) {
    if (r.card !== undefined || !r.it) return false;
    var path = ast.zpath[r.u] || [];
    if (!path.length) return true;
    var items = ast.zitems[r.u], deep = items.byId[path[path.length - 1]];
    return !!deep && r.it !== deep && within(r.u, r.it, deep);
  }
  function zfam(kind) {
    if (kind === 'VIA_RPC' || kind === 'DECLARES_RPC') return 'rpc';
    if (kind === 'VIA_HTTP') return 'http';
    if (kind === 'VIA_BUS') return 'bus';
    if (kind === 'VIA_CONFIG') return 'config';
    if (kind === 'IMPLEMENTS' || kind === 'EXTENDS') return 'depends';
    return 'code';
  }
  var ZKIND_WORD = {
    CALLS: 'calls', USES: 'uses', IMPORTS: 'imports', IMPORTS_FILE: 'imports', REFERENCES: 'references', IMPLEMENTS: 'implements',
    EXTENDS: 'extends', XREF: 'refers to', CONFIG_BIND: 'binds config', VIA_RPC: 'RPC', VIA_HTTP: 'HTTP', VIA_BUS: 'message bus',
    VIA_CONFIG: 'reads config', DECLARES_RPC: 'declares RPC', DISPATCH: 'dispatches', MODULE_EXPORT: 'exports'
  };
  function zkindsLabel(kinds) {
    return Object.keys(kinds).sort(function (a, b) { return kinds[b] - kinds[a]; }).map(function (k) {
      return (ZKIND_WORD[k] || k.toLowerCase()) + ' ×' + fmt(kinds[k]);
    }).join(' · ');
  }
  // Links at the current depth of every open card. A link drawn as a line
  // joins two boxes inside one card's deepest open level (or, when both are
  // on screen, inside the deepest open levels of two cards). Every other
  // link leaves through a stub on the edge of that open level, one per
  // target, weighted and named: a file's members do not each draw a line to
  // a folder or card that is closed or out of view.
  function buildZlinks() {
    var z = zdata(), map = new Map(), out = [];
    // Stubs are re-made with the links: one under the pointer is no longer known.
    ast.zlinks = out; ast.stubs = []; ast.zhstub = null;
    if (!z || !ast.open.size) return;
    z.edges.forEach(function (e) {
      var ua = endUnit(z, e.a), ub = endUnit(z, e.b);
      if (!ast.open.has(ua) && !ast.open.has(ub)) return;
      var A = zrep(e.a), B = zrep(e.b);
      if (A.card !== undefined && B.card !== undefined) return;
      var ka = repKey(A), kb = repKey(B);
      if (ka === kb) return;
      if (A.u !== undefined && A.u === B.u && A.it && B.it && (within(A.u, A.it, B.it) || within(A.u, B.it, A.it))) return;
      if ((A.u !== undefined && !A.it) || (B.u !== undefined && !B.it)) return;
      // The current depth: at least one end in (or on) a card's deepest open level.
      var fa = focusEnd(A), fb = focusEnd(B);
      if (!fa && !fb) return;
      var k = ka + '>' + kb, l = map.get(k);
      if (!l) {
        l = { a: A, b: B, w: 0, kinds: {}, keys: [], fam: zfam(e.kind), edges: [], fa: fa, fb: fb };
        // Drawn end to end: both ends inside open levels, never a level's own frame.
        l.inA = fa && inFocus(A); l.inB = fb && inFocus(B);
        l.local = l.inA && l.inB && A.u === B.u;
        l.cross = l.inA && l.inB && A.u !== B.u;
        map.set(k, l); out.push(l);
      }
      l.w += e.w;
      l.kinds[e.kind] = (l.kinds[e.kind] || 0) + e.w;
      if (zfam(e.kind) !== 'code') l.fam = zfam(e.kind);
      if (e.keys) e.keys.forEach(function (x) { if (l.keys.indexOf(x) < 0 && l.keys.length < 8) l.keys.push(x); });
      if (l.edges.length < 200) l.edges.push(e);
    });
    // Geometry once per layout; the heaviest first when there are too many to draw.
    out.forEach(function (l) {
      var g = l.local || l.cross ? linkGeom(l) : null;
      l.g = g;
      if (!g) return;
      l.bb = { x: Math.min(g.x1, g.x2, g.c1x, g.c2x), y: Math.min(g.y1, g.y2, g.c1y, g.c2y) };
      l.bb.w = Math.max(g.x1, g.x2, g.c1x, g.c2x) - l.bb.x; l.bb.h = Math.max(g.y1, g.y2, g.c1y, g.c2y) - l.bb.y;
    });
    var order = out.map(function (l, i) { return i; });
    order.sort(function (x, y) { return (out[y].cross ? 1 : 0) - (out[x].cross ? 1 : 0) || out[y].w - out[x].w || x - y; });
    order.forEach(function (i, r) { out[i].rank = r; });
  }
  // The deepest open level of a card: its open folder or file, else the card.
  function focusItem(uid) {
    var path = ast.zpath[uid] || [], items = ast.zitems[uid];
    return path.length && items ? items.byId[path[path.length - 1]] || null : null;
  }
  function focusBoxOf(uid) {
    var it = focusItem(uid);
    return it ? itemBox(uid, it) : ast.pos['u' + uid] || null;
  }
  // In the deepest open level, or that level's own frame (a file's module scope).
  function focusEnd(r) {
    if (r.card !== undefined || !r.it) return false;
    return inFocus(r) || r.it === focusItem(r.u);
  }
  // What a stub names: an end inside another open level stands for that
  // level (or its card); anything else for itself.
  function stubTarget(r) {
    if (r.card !== undefined || !focusEnd(r)) return r;
    var it = focusItem(r.u);
    return it ? { u: r.u, it: it } : { card: r.u };
  }
  function boxInView(b, v) { return !!b && b.x < v.x1 && b.x + b.w > v.x0 && b.y < v.y1 && b.y + b.h > v.y0; }
  // Which links are lines and which leave through stubs, for the view `v`.
  function zstubs(v) {
    var map = new Map(), list = [];
    ast.zlinks.forEach(function (l, i) {
      l.line = !!l.local || (!!l.cross && boxInView(endBox(l.a), v) && boxInView(endBox(l.b), v));
      if (l.line) return;
      [[l.a, l.b, l.fa, true], [l.b, l.a, l.fb, false]].forEach(function (side) {
        var me = side[0], other = side[1];
        if (!side[2]) return;
        var t = stubTarget(other), key = me.u + '|' + repKey(t), s = map.get(key);
        if (!s) { s = { u: me.u, target: t, out: false, inn: false, w: 0, kinds: {}, keys: [], members: [], links: [], fam: 'code' }; map.set(key, s); list.push(s); }
        if (side[3]) s.out = true; else s.inn = true;
        s.w += l.w;
        Object.keys(l.kinds).forEach(function (kk) { s.kinds[kk] = (s.kinds[kk] || 0) + l.kinds[kk]; });
        l.keys.forEach(function (x) { if (s.keys.indexOf(x) < 0 && s.keys.length < 8) s.keys.push(x); });
        if (l.fam !== 'code') s.fam = l.fam;
        if (s.members.indexOf(me.it) < 0) s.members.push(me.it);
        s.links.push(i);
      });
    });
    // Two targets with one name (two internal/ folders) are told apart by path.
    var seen = {};
    list.forEach(function (s) { s.label = targetName(s); seen[s.u + '|' + s.label] = (seen[s.u + '|' + s.label] || 0) + 1; });
    list.forEach(function (s) {
      var t = s.target;
      if (seen[s.u + '|' + s.label] > 1 && t.it && t.it.node && t.it.node.rel) s.label = (t.u !== s.u ? AU[t.u].name + ' › ' : '') + t.it.node.rel;
    });
    return placeStubs(list, v);
  }
  var STUBS_PER_SIDE = 8;
  // Stubs sit on the side of the open level facing their target, spread
  // along the part of that edge in view, and stay in view while the level does.
  function placeStubs(list, v) {
    // m: room kept to the view's edge; mv: room for the stub and a label row.
    var bySide = {}, m = 30 / (v.k || 1), mv = 48 / (v.k || 1), out = [];
    list.forEach(function (s) {
      var A = focusBoxOf(s.u), B = endBox(s.target);
      s.box = A; s.tbox = B;
      if (!A) return;
      var ax = A.x + A.w / 2, ay = A.y + A.h / 2;
      var dx = B ? (B.x + B.w / 2 - ax) / Math.max(1, A.w / 2) : 0, dy = B ? (B.y + B.h / 2 - ay) / Math.max(1, A.h / 2) : 1;
      // Beside the box (overlapping it vertically) the stub goes on that side.
      var beside = B && B.y < A.y + A.h && B.y + B.h > A.y && (B.x >= A.x + A.w || B.x + B.w <= A.x);
      s.side = beside ? (dx < 0 ? 'l' : 'r') : Math.abs(dy) >= Math.abs(dx) || !B ? (dy < 0 ? 't' : 'b') : (dx < 0 ? 'l' : 'r');
      s.ox = B ? (s.side === 't' || s.side === 'b' ? B.x + B.w / 2 : B.y + B.h / 2) : 0;
      (bySide[s.u + s.side] = bySide[s.u + s.side] || []).push(s);
    });
    list.forEach(function (s) { if (!s.box) out.push(s); });
    Object.keys(bySide).forEach(function (key) {
      var group = bySide[key], A = group[0].box, side = group[0].side, horiz = side === 't' || side === 'b';
      // Past a handful on one side, the lightest share one stub: "+N more".
      if (group.length > STUBS_PER_SIDE) {
        group.sort(function (a, b) { return b.w - a.w || a.ox - b.ox; });
        var rest = group.slice(STUBS_PER_SIDE - 1), more = { u: rest[0].u, target: null, more: rest.length, out: false, inn: false, w: 0, kinds: {}, keys: [], members: [], links: [], fam: 'code', box: A, tbox: null, side: side, ox: Infinity };
        rest.forEach(function (s) {
          more.out = more.out || s.out; more.inn = more.inn || s.inn; more.w += s.w;
          Object.keys(s.kinds).forEach(function (kk) { more.kinds[kk] = (more.kinds[kk] || 0) + s.kinds[kk]; });
          s.members.forEach(function (it) { if (more.members.indexOf(it) < 0) more.members.push(it); });
          s.links.forEach(function (i) { if (more.links.indexOf(i) < 0) more.links.push(i); });
        });
        group = group.slice(0, STUBS_PER_SIDE - 1).concat([more]);
      }
      group.sort(function (a, b) { return a.ox - b.ox || b.w - a.w; });
      // The visible stretch of the edge, or all of it when none is visible.
      var lo = horiz ? Math.max(A.x, v.x0 + m) : Math.max(A.y, v.y0 + m), hi = horiz ? Math.min(A.x + A.w, v.x1 - m) : Math.min(A.y + A.h, v.y1 - m);
      if (hi - lo < 40) { lo = horiz ? A.x : A.y; hi = horiz ? A.x + A.w : A.y + A.h; }
      var at;
      if (side === 't') at = Math.min(Math.max(A.y, v.y0 + mv), A.y + A.h);
      else if (side === 'b') at = Math.max(Math.min(A.y + A.h, v.y1 - mv), A.y);
      else if (side === 'l') at = Math.min(Math.max(A.x, v.x0 + m), A.x + A.w);
      else at = Math.max(Math.min(A.x + A.w, v.x1 - m), A.x);
      group.forEach(function (s, i) {
        var f = (i + 1) / (group.length + 1), along = lo + (hi - lo) * f;
        if (horiz) { s.x = along; s.y = at; } else { s.x = at; s.y = along; }
        out.push(s);
      });
    });
    return out;
  }
  function stubName(s) {
    if (!s.target) return '+' + fmt(s.more) + ' more';
    return s.label || targetName(s);
  }
  function targetName(s) {
    var t = s.target;
    if (t.card !== undefined) return AU[t.card].name;
    var nm = t.it.type === 'row' ? t.it.sym.name : t.it.node.name;
    return t.u !== s.u ? AU[t.u].name + ' › ' + nm : nm;
  }
  var measureCtx = null;
  function labelWidth(text) {
    try {
      if (!measureCtx) { var cv = document.createElement('canvas'); measureCtx = cv && cv.getContext ? cv.getContext('2d') : false; }
      if (measureCtx && measureCtx.measureText) {
        measureCtx.font = '700 11px ' + SANS;
        var w = measureCtx.measureText(text).width;
        if (w > 0) return w + text.length * 0.25;
      }
    } catch (e) { /* no canvas: estimate */ }
    return text.length * 6.8;
  }
  function stubText(s) { return (s.out && s.inn ? '⇄ ' : s.out ? '→ ' : '← ') + stubName(s) + ' ×' + fmt(s.w); }
  function endBox(r) {
    if (r.card !== undefined) { var p = ast.pos['u' + r.card]; return p ? { x: p.x, y: p.y, w: p.w, h: p.h } : null; }
    return r.it ? itemBox(r.u, r.it) : null;
  }
  // A curve from box to box (or to a port point), leaving the facing sides.
  function zcurve(A, B, rowA, rowB) {
    var acx = A.x + A.w / 2, acy = A.y + A.h / 2, bcx = B.x + B.w / 2, bcy = B.y + B.h / 2;
    var dx = bcx - acx, dy = bcy - acy, x1, y1, x2, y2, c1x, c1y, c2x, c2y;
    var horiz = rowA || rowB || Math.abs(dx) > Math.abs(dy) * 1.2;
    if (horiz) {
      var right = dx >= 0;
      x1 = right ? A.x + A.w : A.x; y1 = acy; x2 = right ? B.x : B.x + B.w; y2 = bcy;
      if (rowA && rowB && Math.abs(dx) < Math.max(A.w, B.w)) { x1 = A.x + A.w; x2 = B.x + B.w; right = true; }
      var h = Math.max(24, Math.abs(x2 - x1) * 0.4);
      c1x = x1 + (right ? h : -h); c2x = (rowA && rowB && x2 === B.x + B.w && x1 === A.x + A.w) ? x2 + h : x2 + (right ? -h : h);
      c1y = y1; c2y = y2;
    } else {
      var down = dy >= 0;
      x1 = acx; y1 = down ? A.y + A.h : A.y; x2 = bcx; y2 = down ? B.y : B.y + B.h;
      var v = Math.max(24, Math.abs(y2 - y1) * 0.4);
      c1x = x1; c2x = x2; c1y = y1 + (down ? v : -v); c2y = y2 + (down ? -v : v);
    }
    return { x1: x1, y1: y1, x2: x2, y2: y2, c1x: c1x, c1y: c1y, c2x: c2x, c2y: c2y };
  }
  function curveD(g) {
    return 'M' + r1(g.x1) + ' ' + r1(g.y1) + 'C' + r1(g.c1x) + ' ' + r1(g.c1y) + ' ' + r1(g.c2x) + ' ' + r1(g.c2y) + ' ' + r1(g.x2) + ' ' + r1(g.y2);
  }
  function arrowAt(g, s) {
    var tx = g.x2 - g.c2x, ty = g.y2 - g.c2y, len = Math.hypot(tx, ty) || 1;
    tx /= len; ty /= len;
    var bx = g.x2 - tx * s, by = g.y2 - ty * s, nx = -ty * s * 0.55, ny = tx * s * 0.55;
    return 'M' + r1(g.x2) + ' ' + r1(g.y2) + 'L' + r1(bx + nx) + ' ' + r1(by + ny) + 'L' + r1(bx - nx) + ' ' + r1(by - ny) + 'Z';
  }
  function linkGeom(l) {
    var A = endBox(l.a), B = endBox(l.b);
    if (!A || !B) return null;
    var rowA = l.a.it && l.a.it.type === 'row', rowB = l.b.it && l.b.it.type === 'row';
    return zcurve(A, B, rowA, rowB);
  }

  // ----- DOM for an open card: header, then the items in view.
  function zcrumbs(uid) {
    var u = AU[uid], path = ast.zpath[uid] || [];
    var html = '<button type="button" data-zcrumb="' + uid + '" data-depth="0"' + (path.length ? '' : ' aria-current="location"') + '>' + esc(u.name) + '</button>';
    path.forEach(function (id, i) {
      var n = znode(uid, id);
      if (!n) return;
      html += '<span class="sep" aria-hidden="true">›</span><button type="button" data-zcrumb="' + uid + '" data-depth="' + (i + 1) + '"' +
        (i === path.length - 1 ? ' aria-current="location"' : '') + '>' + esc(n.name) + '</button>';
    });
    return html;
  }
  function zoomCardHtml(u, cls, kind, dots) {
    var om = zdata().omitted[u.id];
    var html = '<article role="listitem" class="' + cls + ' zoomed" data-key="u' + u.id + '" tabindex="0" aria-expanded="true" aria-label="' +
      esc(u.name + ', ' + unitKind(u) + ', open') + '" title="' + esc(u.root) + '">';
    html += '<div class="zh"><div class="k"><span>' + esc(kind) + '</span>' + (dots ? '<span class="lg">' + dots + '</span>' : '') + '</div>';
    var root = ztree(u.id).root;
    html += '<div class="zt"><h3>' + esc(u.name) + '</h3><span class="zn">' + esc(plural(root.files, 'file', 'files') + ' · ' + plural(root.syms, 'symbol', 'symbols')) + '</span>' +
      '<span class="zact"><button type="button" data-folder="' + esc(u.dir) + '" title="Show in the folder map">Folder map</button>' +
      '<button type="button" class="zclose" data-close-card="' + u.id + '" aria-label="Collapse ' + esc(u.name) + '" title="Collapse (Esc)">Collapse</button></span></div>';
    html += (ast.zpath[u.id] || []).length ? '<nav class="zcrumbs" aria-label="Path in ' + esc(u.name) + '">' + zcrumbs(u.id) + '</nav></div>'
      : '<p class="zhint">Scroll into a folder or file, or double-click it, to open it. Esc goes back up.</p></div>';
    html += '<div class="zin" data-zin="' + u.id + '"></div>';
    if (om) html += '<p class="zom">' + esc(plural(om.syms, 'symbol', 'symbols') + ' in ' + plural(om.files, 'file', 'files') + ' not embedded (--max-symbols ' + fmt(ZR.max_symbols) + '); see the panel') + '</p>';
    return html + '</article>';
  }
  function glyph(kind) { return KIND_GLYPH[kind] || (kind ? kind.charAt(0) : '·'); }
  function itemHtml(uid, it, far) {
    var sel = ast.zsel && ast.zsel.u === uid && ast.zsel.id === it.id;
    var st0 = 'transform:translate(' + r1(it.x) + 'px,' + r1(it.y) + 'px);width:' + r1(it.w) + 'px;height:' + r1(it.h) + 'px';
    var base = ' data-zu="' + uid + '" data-zk="' + it.k + '" tabindex="-1" style="' + st0 + '"';
    var cls = 'zi z-' + it.type + (sel ? ' zsel' : '');
    if (it.type === 'row') {
      var y = it.sym;
      return '<button type="button" class="' + cls + '"' + base + ' title="' + esc(y.kind + ' ' + y.qual + (y.sig ? '\n' + y.sig : '')) + '">' +
        '<span class="g" style="margin-left:' + (it.indent * 12) + 'px">' + esc(glyph(y.kind)) + '</span><span class="nm">' + esc(y.name) + '</span>' +
        (y.sig ? '<span class="sg">' + esc(y.sig) + '</span>' : '') + '</button>';
    }
    if (it.type === 'sym') {
      var s = it.node.sym;
      // Far out a box carries its name only: thousands stay cheap to draw.
      if (far) return '<div class="' + cls + (it.rows ? ' has-rows' : '') + '"' + base.replace('tabindex="-1"', 'tabindex="-1" role="button"') + ' title="' + esc(s.kind + ' ' + s.qual) + '"><div class="nm">' + esc(s.name) + '</div></div>';
      return '<div class="' + cls + (it.rows ? ' has-rows' : '') + '"' + base.replace('tabindex="-1"', 'tabindex="-1" role="button"') + ' title="' + esc(s.kind + ' ' + s.qual + (s.sig ? '\n' + s.sig : '')) + '">' +
        '<div class="zk"><span>' + esc(s.kind) + '</span>' + (it.rows ? '<span class="n">' + fmt(it.rows) + '</span>' : '') + '</div>' +
        // A box with members lists them under its name; its signature is in the tooltip.
        '<div class="nm">' + esc(s.name) + '</div>' + (s.sig && !it.rows ? '<div class="sg">' + esc(s.sig) + '</div>' : '') + '</div>';
    }
    var n = it.node, counts;
    if (n.type === 'file') {
      var f = n.file;
      counts = (f.emb ? plural(f.syms, 'symbol', 'symbols') : plural(f.syms, 'symbol', 'symbols') + ', not embedded') + ' · ' + langName(f.lang);
    } else counts = plural(n.files, 'file', 'files') + ' · ' + plural(n.syms, 'symbol', 'symbols');
    if (it.type === 'frame') {
      return '<div class="' + cls + ' f-' + n.type + '"' + base + '><div class="zfh"><button type="button" class="zup" data-zu="' + uid + '" data-zup="' + esc(n.id) +
        '" title="Close ' + esc(n.name) + '" aria-label="Close ' + esc(n.name) + '">' + esc(n.name) + '</button><span class="n">' + esc(counts) + '</span></div></div>';
    }
    var can = openable(n);
    if (it.compact) {
      return '<div class="' + cls + ' compact' + (can ? '' : ' leafbox') + '"' + base.replace('tabindex="-1"', 'tabindex="-1" role="button"') + ' title="' + esc((n.rel || n.name) + ' · ' + counts) + '">' +
        '<span class="nm">' + esc(n.name) + '</span></div>';
    }
    return '<div class="' + cls + (can ? '' : ' leafbox') + '"' + base.replace('tabindex="-1"', 'tabindex="-1" role="button"') + ' title="' + esc(n.rel || n.name) +
      (can ? '. Double-click or Enter to open' : '') + '">' +
      '<div class="nm">' + esc(n.name) + '</div><div class="n">' + esc(counts) + '</div></div>';
  }
  function viewRect(margin) {
    var r = stage.getBoundingClientRect(), k = ast.cam.k || 1, m = (margin || 0) / k;
    return { x0: -ast.cam.x / k - m, y0: -ast.cam.y / k - m, x1: (r.width - ast.cam.x) / k + m, y1: (r.height - ast.cam.y) / k + m, k: k };
  }
  function hitRect(a, b) { return a.x < b.x1 && a.x + a.w > b.x0 && a.y < b.y1 && a.y + a.h > b.y0; }
  // The items and links in (and around) the view. Re-rendered when the view
  // leaves the rendered region or the zoom changes a lot.
  function renderZoomView(force) {
    zRaf = 0;
    if (!ast.open.size || st.level !== 'app') { if (zedges) zedges.innerHTML = ''; zRendered = null; return; }
    var v = viewRect(0);
    if (!force && zRendered && v.x0 >= zRendered.x0 && v.y0 >= zRendered.y0 && v.x1 <= zRendered.x1 && v.y1 <= zRendered.y1 &&
        v.k < zRendered.k * 1.8 && v.k > zRendered.k / 1.8) { renderStubs(); return; }
    var R = viewRect(Math.max(300, (stage.getBoundingClientRect().width || 1400) * 0.5));
    zRendered = R;
    var shown = 0;
    ast.open.forEach(function (uid) {
      var el = appsCards.querySelector('[data-zin="' + uid + '"]'), items = ast.zitems[uid], o = zorigin(uid);
      if (!items || !o) return;
      var html = '', cap = 3000;
      items.list.forEach(function (it) {
        var sel = ast.zsel && ast.zsel.u === uid && ast.zsel.id === it.id;
        if (!sel && (cap <= 0 || !hitRect({ x: o.x + it.x, y: o.y + it.y, w: it.w, h: it.h }, R))) return;
        // Text under 4 px is noise: far out, boxes only.
        if (R.k < 0.18 && it.type === 'row' && !sel) return;
        html += itemHtml(uid, it, R.k < 0.3);
        cap--; shown++;
      });
      if (items.empty) html = '<p class="zempty">No files with symbols here.</p>';
      if (el) el.innerHTML = html;
    });
    ast.zshown = shown;
    renderZlinks(R);
  }
  function requestZoomView() { if (!zRaf) zRaf = requestAnimationFrame(function () { renderZoomView(false); }); }
  function zlinkWidth(w) { return Math.min(4.5, 0.9 + Math.log2(Math.max(1, w)) * 0.5); }
  function touches(r, sel) { return !!sel && r.u === sel.u && !!r.it && r.it.id === sel.id; }
  function zlinkOn(l) {
    var s = ast.zsel, h = ast.zhover;
    return touches(l.a, s) || touches(l.b, s) || touches(l.a, h) || touches(l.b, h);
  }
  function stubOn(s, i) {
    if (ast.zhstub === i) return true;
    var sel = ast.zsel, h = ast.zhover;
    return s.members.some(function (it) { return touches({ u: s.u, it: it }, sel) || touches({ u: s.u, it: it }, h); });
  }
  // Lines are re-drawn when the view leaves the rendered region; stubs, which
  // follow the view, on every move.
  function renderZlinks(R) {
    if (!zedges) return;
    var v = viewRect(0);
    ast.stubs = zstubs(v);
    ast.zlineKey = lineKey();
    var many = ast.zlinks.length > 120, svg = '';
    ast.zdrawn = 0;
    ast.zlinks.forEach(function (l, i) {
      var g = l.g;
      if (!l.line || !g || !hitRect(l.bb, R)) return;
      var on = zlinkOn(l), w = zlinkWidth(l.w), cross = l.cross;
      // Past the cap only the heaviest are drawn, and whatever touches the focus.
      if (!on && l.rank >= ZLINK_CAP) return;
      ast.zdrawn++;
      svg += '<g class="zl conn f-' + l.fam + (on ? ' on' : '') + (cross ? ' cross named' : '') + (many ? ' many' : '') + '" data-zl="' + i + '" style="--w:' + r1(w) + '">' +
        '<path class="ln" d="' + curveD(g) + '" stroke-width="' + r1(w) + '"/><path class="ah" d="' + arrowAt(g, 6) + '"/>' +
        '<path class="hit" d="' + curveD(g) + '"/>' +
        (on || cross ? '<text class="lbl" x="' + r1(bez(g.x1, g.c1x, g.c2x, g.x2, 0.5)) + '" y="' + r1(bez(g.y1, g.c1y, g.c2y, g.y2, 0.5)) + '" text-anchor="middle" dy="0.35em">' +
          esc(zkindsLabel(l.kinds)) + '</text>' : '') + '</g>';
    });
    zedges.setAttribute('width', String(Math.max(1, ast.sheet ? ast.sheet.width : 1)));
    zedges.setAttribute('height', String(Math.max(1, ast.sheet ? ast.sheet.height : 1)));
    zedges.innerHTML = '<g class="zlg">' + svg + '</g><g class="zsg">' + stubsSvg(v) + '</g>';
    zedges.classList.toggle('hasfocus', !!(ast.zsel || ast.zhover || ast.zhstub !== null));
  }
  // Which cross-card links are lines in this view: when it changes, the lines are re-drawn.
  function lineKey() {
    var key = '';
    ast.zlinks.forEach(function (l, i) { if (l.cross && l.line) key += i + ','; });
    return key;
  }
  function renderStubs() {
    var g = zedges && zedges.querySelector ? zedges.querySelector('.zsg') : null;
    var v = viewRect(0);
    ast.stubs = zstubs(v);
    if (!g || lineKey() !== ast.zlineKey) { renderZlinks(zRendered || viewRect(600)); return; }
    g.innerHTML = stubsSvg(v);
  }
  function stubsSvg(v) {
    var placed = [], svg = '', k = v.k || 1, L = 16;
    ast.stubs.forEach(function (s, i) {
      if (!s.box || s.x === undefined) return;
      var on = stubOn(s, i), text = stubText(s);
      var nx = s.side === 'l' ? -1 : s.side === 'r' ? 1 : 0, ny = s.side === 't' ? -1 : s.side === 'b' ? 1 : 0;
      var ex = s.x + nx * L, ey = s.y + ny * L;
      // The label beside the stub's end, else stepped outward and along the
      // edge until it clears the others, always inside the view.
      var tw = labelWidth(text) + 10, th = 16, box = null;
      var bx0 = nx < 0 ? ex - 4 - tw : nx > 0 ? ex + 4 : ex - tw / 2, by0 = ny < 0 ? ey - 2 - th : ny > 0 ? ey + 2 : ey - th / 2;
      var vm = 4 / k;
      search:
      for (var row = 0; row < 6; row++) {
        for (var j = 0; j < 5; j++) {
          var sh = j === 0 ? 0 : (j % 2 ? 1 : -1) * Math.ceil(j / 2);
          var cx = bx0 + (ny ? sh * (tw / 2 + 6) : nx * row * 24), cy = by0 + (ny ? ny * row * (th + 2) : sh * (th + 2));
          var cand = [cx, cy, cx + tw, cy + th];
          if (cand[0] < v.x0 + vm || cand[2] > v.x1 - vm || cand[1] < v.y0 + vm || cand[3] > v.y1 - vm) continue;
          if (!placed.some(function (q) { return cand[0] < q[2] + 2 && cand[2] + 2 > q[0] && cand[1] < q[3] + 2 && cand[3] + 2 > q[1]; })) { box = cand; break search; }
        }
      }
      if (box) placed.push(box);
      var w = zlinkWidth(s.w);
      svg += '<g class="zstub conn f-' + s.fam + (on ? ' on' : '') + '" data-zs="' + i + '" style="--w:' + r1(w) + '">';
      // On to the target, while hovered or selected and the target is in
      // view: one line for the whole bundle.
      if (on && s.tbox && boxInView(s.tbox, v)) {
        var g = s.out ? zcurve({ x: ex - 1, y: ey - 1, w: 2, h: 2 }, s.tbox, false, false) : zcurve(s.tbox, { x: ex - 1, y: ey - 1, w: 2, h: 2 }, false, false);
        svg += '<path class="ln on-card" d="' + curveD(g) + '" stroke-width="' + r1(w) + '"/>' + (s.out && s.inn ? '' : '<path class="ah" d="' + arrowAt(g, 6) + '"/>');
      } else if (!(s.out && s.inn)) {
        var gs = s.out ? { x1: s.x, y1: s.y, x2: ex, y2: ey, c1x: s.x, c1y: s.y, c2x: s.x, c2y: s.y } : { x1: ex, y1: ey, x2: s.x, y2: s.y, c1x: ex, c1y: ey, c2x: ex, c2y: ey };
        svg += '<path class="ah" d="' + arrowAt(gs, 6) + '"/>';
      }
      svg += '<path class="ln" d="M' + r1(s.x) + ' ' + r1(s.y) + 'L' + r1(ex) + ' ' + r1(ey) + '" stroke-width="' + r1(w) + '"/>' +
        '<circle class="pt" cx="' + r1(s.x) + '" cy="' + r1(s.y) + '" r="4"/>' +
        (box ? leader(ex, ey, box) + '<rect class="pbg" x="' + r1(box[0]) + '" y="' + r1(box[1]) + '" width="' + r1(box[2] - box[0]) + '" height="' + r1(box[3] - box[1]) + '" rx="3"/>' +
          '<text class="plbl" x="' + r1((box[0] + box[2]) / 2) + '" y="' + r1(box[3] - 4.5) + '" text-anchor="middle">' + esc(text) + '</text>' : '') +
        '<rect class="shit" x="' + r1(Math.min(s.x, ex, box ? box[0] : ex) - 4) + '" y="' + r1(Math.min(s.y, ey, box ? box[1] : ey) - 4) + '" width="' +
        r1(Math.max(s.x, ex, box ? box[2] : ex) - Math.min(s.x, ex, box ? box[0] : ex) + 8) + '" height="' + r1(Math.max(s.y, ey, box ? box[3] : ey) - Math.min(s.y, ey, box ? box[1] : ey) + 8) + '"/></g>';
    });
    return svg;
  }
  // A thin line from a stub's end to its label when the label had to move away.
  function leader(x, y, b) {
    var px = Math.max(b[0], Math.min(x, b[2])), py = Math.max(b[1], Math.min(y, b[3]));
    return Math.hypot(px - x, py - y) > 6 ? '<path class="ldr" d="M' + r1(x) + ' ' + r1(y) + 'L' + r1(px) + ' ' + r1(py) + '"/>' : '';
  }
  // Hovering a stub lights up the boxes its links come from (and its target).
  function markStub(i) {
    if (ast.zhstub === i) return;
    ast.zhstub = i;
    Array.prototype.forEach.call(appsCards.querySelectorAll('.zi.zhl'), function (el) { el.classList.remove('zhl'); });
    var s = i !== null ? ast.stubs[i] : null;
    if (s) {
      var mark = function (u, it) { var el = appsCards.querySelector('[data-zu="' + u + '"][data-zk="' + it.k + '"]'); if (el) el.classList.add('zhl'); };
      s.members.forEach(function (it) { mark(s.u, it); });
      if (s.target && s.target.it) mark(s.target.u, s.target.it);
    }
    if (zRendered) renderZlinks(zRendered);
  }

  // ----- state changes
  function zoomOpen(uid, path, opts) {
    opts = opts || {};
    if (!opts.noHistory) pushHistory();
    ast.open.add(uid);
    ast.zpath[uid] = (path || []).slice();
    ast.zfocus = uid;
    if (opts.sel !== undefined) ast.zsel = opts.sel;
    renderApps(true);
    renderPanel();
    renderAppCrumbs();
    var target = opts.sel ? (ast.zitems[uid] && ast.zitems[uid].byId[opts.sel.id]) : null;
    var deepest = ast.zpath[uid].length ? ast.zitems[uid].byId[ast.zpath[uid][ast.zpath[uid].length - 1]] : null;
    if (opts.fit !== false) {
      if (target) fitItem(uid, target, deepest);
      else if (deepest) fitBox(itemBox(uid, deepest), 0.9);
      else fitBox(ast.pos['u' + uid], 0.9);
    }
    zCooldown = performance.now() + 450;
    writeHash();
    // Focus lost with the re-rendered card goes back to it.
    var ae = document.activeElement;
    if (!ae || ae === document.body) { var ce0 = cardEl('u' + uid); if (ce0 && ce0.focus) ce0.focus({ preventScroll: true }); }
    var lbl = (ast.zpath[uid].length ? znode(uid, ast.zpath[uid][ast.zpath[uid].length - 1]).name : AU[uid].name);
    live.textContent = 'Opened ' + lbl;
  }
  function zoomClose(uid, opts) {
    opts = opts || {};
    if (!opts.noHistory) pushHistory();
    ast.open.delete(uid);
    delete ast.zpath[uid];
    delete ast.zitems[uid];
    if (ast.zsel && ast.zsel.u === uid) ast.zsel = null;
    if (ast.zfocus === uid) ast.zfocus = ast.open.size ? Array.from(ast.open).pop() : null;
    renderApps(true);
    renderPanel();
    renderAppCrumbs();
    if (opts.fit !== false) fitBox(ast.pos['u' + uid], 0.5);
    zCooldown = performance.now() + 450;
    writeHash();
    var el = cardEl('u' + uid);
    if (el && opts.focus !== false) el.focus();
    live.textContent = 'Collapsed ' + AU[uid].name;
  }
  // Up to `depth` levels of the card's path stay open.
  function zoomTo(uid, depth, opts) {
    var path = (ast.zpath[uid] || []).slice(0, depth);
    if (ast.zsel && ast.zsel.u === uid) {
      // Drop a selection that the closed levels now hide.
      var chain = zchain(uid, ast.zsel.id), sn = znode(uid, ast.zsel.id);
      if (!sn || (sn.type === 'sym' ? chain.length : chain.length - 1) > depth) ast.zsel = null;
    }
    zoomOpen(uid, path, opts);
  }
  // One level out: the deepest open folder or file, else the card.
  function zoomOut(uid) {
    if (uid === null || uid === undefined) return false;
    var path = ast.zpath[uid];
    if (!path) return false;
    if (path.length) zoomTo(uid, path.length - 1);
    else zoomClose(uid);
    return true;
  }
  // Show a node: open the card down to it and select it.
  function zoomReveal(uid, id) {
    var n = znode(uid, id);
    if (!n) return false;
    revealUnit(uid);
    var chain = zchain(uid, id);
    var path = n.type === 'sym' ? chain : openable(n) ? chain : chain.slice(0, -1);
    zoomOpen(uid, path, { sel: { u: uid, id: id } });
    ast.selected = 'u' + uid;
    renderPanel();
    return true;
  }
  function zselect(uid, id) {
    ast.zsel = id ? { u: uid, id: id } : null;
    ast.zfocus = uid;
    ast.selected = 'u' + uid; ast.selConn = null; ast.zpanelUnit = null; ast.zlink = null;
    updateAppEmphasis();
    renderZoomView(true);
    renderPanel();
    writeHash();
    var it = id && ast.zitems[uid] && ast.zitems[uid].byId[id];
    if (it) {
      var el = appsCards.querySelector('[data-zu="' + uid + '"][data-zk="' + it.k + '"]');
      if (el && el.focus) el.focus({ preventScroll: true });
      var n = it.type === 'row' ? it.sym : it.node.sym || it.node;
      live.textContent = (n.kind || n.type) + ' ' + n.name + ' selected';
    }
  }
  function zactivate(uid, it) {
    if (!it) return;
    if (it.type === 'frame') { zoomTo(uid, (ast.zpath[uid] || []).indexOf(it.id)); return; }
    if ((it.type === 'dir' || it.type === 'file') && openable(it.node)) {
      zoomOpen(uid, zchain(uid, it.id));
      return;
    }
    zselect(uid, it.id);
  }
  // Fit onto a selected item: a member row is shown in its class box, at a
  // reading scale, never as a strip filling the screen.
  function fitItem(uid, target, deepest) {
    if (target.type !== 'row') { fitBox(itemBox(uid, target), 0.8, deepest ? itemBox(uid, deepest) : null); return; }
    var box = itemBox(uid, ast.zitems[uid].byId[target.parent] || target), row = itemBox(uid, target);
    var r = stage.getBoundingClientRect(), w = Math.max(1, r.width), h = Math.max(1, r.height);
    var k = Math.max(0.3, Math.min(1.3, w * 0.7 / box.w, h * 0.85 / box.h));
    animateCam({ k: k, x: w / 2 - (box.x + box.w / 2) * k, y: box.h * k > h * 0.9 ? h / 2 - (row.y + row.h / 2) * k : h / 2 - (box.y + box.h / 2) * k });
  }
  // Animated camera fit onto a world box.
  function fitBox(b, frac, ctx) {
    if (!b) return;
    var r = stage.getBoundingClientRect(), w = Math.max(1, r.width), h = Math.max(1, r.height);
    var k = Math.max(0.15, Math.min(1.4, Math.min(w * frac / Math.max(1, b.w), h * frac / Math.max(1, b.h))));
    if (ctx) {
      // A box inside an open file: the file around it, while it stays readable.
      var kc = Math.min(w * 0.9 / Math.max(1, ctx.w), h * 0.9 / Math.max(1, ctx.h));
      k = Math.max(Math.min(k, kc), Math.min(k, 0.7));
    }
    var to = { k: k, x: w / 2 - (b.x + b.w / 2) * k, y: h / 2 - (b.y + b.h / 2) * k };
    // A tall box starts at its top, with room above for the stubs on its edge.
    if (b.h * k > h * 0.95) to.y = 56 - b.y * k;
    animateCam(to);
  }
  function animateCam(to) {
    var from = { k: ast.cam.k, x: ast.cam.x, y: ast.cam.y }, t0 = performance.now(), dur = REDUCE ? 0 : 280;
    ast.userMoved = true;
    if (zAnim) cancelAnimationFrame(zAnim);
    function step(now) {
      var t = dur ? Math.min(1, ((now || performance.now()) - t0) / dur) : 1, e = 1 - Math.pow(1 - t, 3);
      // Interpolate the scale in log space so the zoom feels even.
      ast.cam.k = Math.exp(Math.log(from.k) + (Math.log(to.k) - Math.log(from.k)) * e);
      ast.cam.x = from.x + (to.x - from.x) * e; ast.cam.y = from.y + (to.y - from.y) * e;
      applyAppCam();
      zAnim = t < 1 ? requestAnimationFrame(step) : null;
    }
    if (!dur) { step(); return; }
    zAnim = requestAnimationFrame(step);
  }
  // Wheel past a threshold opens what is under the pointer, or closes the
  // deepest open level once it is small on screen.
  function semanticWheel(px, py, f) {
    if (!ZR || performance.now() < zCooldown) return;
    var r = stage.getBoundingClientRect(), k = ast.cam.k;
    var wx = (px - ast.cam.x) / k, wy = (py - ast.cam.y) / k;
    var hit = zhitAt(wx, wy);
    if (f > 1) {
      if (hit && hit.card !== undefined && !ast.open.has(hit.card)) {
        var P = ast.pos['u' + hit.card];
        if (P.w * k > r.width * 0.42 || P.h * k > r.height * 0.6) zoomOpen(hit.card, []);
      } else if (hit && hit.it && (hit.it.type === 'dir' || hit.it.type === 'file') && openable(hit.it.node)) {
        if (hit.it.w * k > r.width * 0.3 || hit.it.h * k > r.height * 0.4) zoomOpen(hit.u, zchain(hit.u, hit.it.id));
      }
      return;
    }
    var uid = hit && hit.u !== undefined ? hit.u : ast.zfocus;
    if (uid === null || uid === undefined || !ast.open.has(uid)) return;
    var path = ast.zpath[uid] || [], box;
    if (path.length) box = itemBox(uid, ast.zitems[uid].byId[path[path.length - 1]]);
    else box = ast.pos['u' + uid];
    if (box && box.w * k < r.width * 0.22 && box.h * k < r.height * 0.22) zoomOut(uid);
  }
  // The deepest thing at a world point: an item of an open card, else a card.
  function zhitAt(wx, wy) {
    var best = null;
    Object.keys(ast.pos).forEach(function (key) {
      if (key.charAt(0) !== 'u') return;
      var p = ast.pos[key];
      if (wx < p.x || wy < p.y || wx > p.x + p.w || wy > p.y + p.h) return;
      var uid = +key.slice(1);
      best = { card: uid };
      var items = ast.zitems[uid], o = zorigin(uid);
      if (!ast.open.has(uid) || !items) return;
      best = { u: uid, it: null };
      items.list.forEach(function (it) {
        var x = o.x + it.x, y = o.y + it.y;
        if (wx >= x && wy >= y && wx <= x + it.w && wy <= y + it.h && (!best.it || it.depth >= best.it.depth)) best.it = it;
      });
    });
    return best;
  }
  function itemFromEl(el) {
    var uid = +el.dataset.zu, items = ast.zitems[uid];
    return items ? { u: uid, it: items.list[+el.dataset.zk] } : null;
  }
  // Arrow keys move between the boxes of the open level in that direction.
  function zarrow(dx, dy) {
    var s = ast.zsel;
    if (!s || !ast.zitems[s.u]) return false;
    var items = ast.zitems[s.u], cur = items.byId[s.id];
    if (!cur) return false;
    var best = null, bestS = Infinity;
    items.list.forEach(function (it) {
      if (it === cur || it.type === 'frame') return;
      var ox = it.x + it.w / 2 - (cur.x + cur.w / 2), oy = it.y + it.h / 2 - (cur.y + cur.h / 2);
      var along = ox * dx + oy * dy;
      if (along <= 1) return;
      var sc = along + 2.5 * Math.abs(ox * dy + oy * dx);
      if (sc < bestS) { bestS = sc; best = it; }
    });
    if (!best) return true;
    zselect(s.u, best.id);
    var b = itemBox(s.u, best), r = stage.getBoundingClientRect(), k = ast.cam.k;
    var x0 = b.x * k + ast.cam.x, y0 = b.y * k + ast.cam.y;
    if (x0 < 20 || y0 < 20 || x0 + b.w * k > r.width - 20 || y0 + b.h * k > r.height - 20) {
      ast.cam.x += r.width / 2 - (x0 + b.w * k / 2); ast.cam.y += r.height / 2 - (y0 + b.h * k / 2); applyAppCam();
    }
    return true;
  }

  // ----- breadcrumb in the header: Apps › card › folder › file
  function renderAppCrumbs() {
    if (st.level !== 'app') return;
    var uid = ast.zfocus;
    if (uid === null || uid === undefined || !ast.open.has(uid)) { crumbs.innerHTML = ''; crumbs.hidden = true; return; }
    crumbs.hidden = false;
    crumbs.innerHTML = '<button type="button" data-zall="1" title="Collapse every card">Apps</button><span class="sep" aria-hidden="true">›</span>' +
      zcrumbs(uid).replace(/<span class="sep" aria-hidden="true">›<\/span>/g, '<span class="sep" aria-hidden="true">›</span>');
  }
  crumbs.addEventListener('click', function (ev) {
    if (st.level !== 'app') return;
    var b = ev.target.closest('button');
    if (!b) return;
    ev.stopPropagation();
    if (b.dataset.zall) { pushHistory(); Array.from(ast.open).forEach(function (uid) { zoomClose(uid, { noHistory: true, fit: false, focus: false }); }); fitApps(); return; }
    if (b.dataset.zcrumb !== undefined) zoomTo(+b.dataset.zcrumb, +b.dataset.depth);
  }, true);

  // ----- panels for what is selected inside a card
  function symLinks(uid, sym, outward) {
    var z = zdata(), rows = [];
    z.edges.forEach(function (e) {
      if (outward ? e.a !== sym : e.b !== sym) return;
      var o = outward ? e.b : e.a;
      rows.push({ end: o, kind: e.kind, w: e.w });
    });
    rows.sort(function (a, b) { return b.w - a.w; });
    return rows;
  }
  function endName(end) {
    var z = zdata();
    return end >= 0 ? z.syms[end].name : z.files[-1 - end].path.split('/').pop();
  }
  function endLoc(end) {
    var z = zdata();
    if (end >= 0) { var y = z.syms[end]; return z.files[y.file].path + ':' + y.line; }
    return z.files[-1 - end].path;
  }
  function endRows(list, limit) {
    var z = zdata();
    return '<div class="edges"><ul>' + list.slice(0, limit).map(function (r) {
      var uo = endUnit(z, r.end);
      return '<li><button type="button" data-zend="' + r.end + '"><span class="nm">' + esc(endName(r.end)) +
        '<span class="w"> · ' + esc((ZKIND_WORD[r.kind] || r.kind.toLowerCase()) + (r.w > 1 ? ' ×' + fmt(r.w) : '')) + '</span></span>' +
        '<span class="loc">' + esc(AU[uo].name + ' · ' + endLoc(r.end)) + '</span></button></li>';
    }).join('') + '</ul>' + (list.length > limit ? '<div class="more">and ' + fmt(list.length - limit) + ' more</div>' : '') + '</div>';
  }
  function zitemPanel(uid, id) {
    var z = zdata(), n = znode(uid, id), u = AU[uid];
    if (!n) return unitPanel(u);
    var html = '<div class="panel-head"><h2>' + esc(n.name) + ' <span class="kind">' + esc(n.type === 'sym' ? n.sym.kind : n.type === 'dir' ? 'folder' : 'file') + '</span></h2>' +
      '<button type="button" data-act="aclose" aria-label="Close details">×</button></div>';
    html += '<p class="lead"><button type="button" class="linkish" data-zunit="' + uid + '">‹ ' + esc(u.name) + '</button> · ' + esc(unitKind(u)) + '</p>';
    if (n.type === 'sym') {
      var y = n.sym, f = z.files[y.file];
      html += '<div class="qual"><code>' + esc(y.qual) + '</code><button type="button" data-zcopy="' + y.i + '" title="Copy qualname">Copy</button></div>';
      html += '<dl class="facts"><dt>Location</dt><dd><code>' + esc(f.path + ':' + y.line) + '</code></dd><dt>Language</dt><dd>' + esc(langName(f.lang)) + '</dd>';
      if (y.parent >= 0) html += '<dt>In</dt><dd><button type="button" class="linkish" data-zend="' + y.parent + '">' + esc(z.syms[y.parent].name) + '</button></dd>';
      if (y.kids.length) html += '<dt>Members</dt><dd>' + fmt(y.kids.length) + '</dd>';
      html += '</dl>';
      if (y.sig) html += '<pre class="sig">' + esc(y.sig) + '</pre>';
      var outs = symLinks(uid, y.i, true), ins = symLinks(uid, y.i, false);
      if (outs.length) html += '<h3>Uses · ' + fmt(outs.length) + '</h3>' + endRows(outs, 30);
      if (ins.length) html += '<h3>Used by · ' + fmt(ins.length) + '</h3>' + endRows(ins, 30);
      if (!outs.length && !ins.length) html += '<p class="more">No relations to other embedded symbols.</p>';
      html += '<p class="more">Its full neighbourhood, callers and callees across the repo:</p><p class="cmd">lidx graph --seed ' + esc(shellQuote(y.qual)) + '</p>';
      return html;
    }
    html += '<dl class="facts"><dt>Path</dt><dd><code>' + esc((u.dir || '') + (n.rel || '')) + '</code></dd>';
    if (n.type === 'file') {
      html += '<dt>Language</dt><dd>' + esc(langName(n.file.lang)) + '</dd><dt>Symbols</dt><dd>' + fmt(n.file.syms) + (n.file.emb ? '' : ', not embedded') + '</dd></dl>';
      if (!n.file.emb) html += '<p class="more">This file\'s symbols were left out by --max-symbols ' + fmt(ZR.max_symbols) + '. Regenerate with a higher --max-symbols to zoom into it.</p>';
    } else html += '<dt>Holds</dt><dd>' + esc(plural(n.files, 'file', 'files') + ' · ' + plural(n.syms, 'symbol', 'symbols')) + '</dd></dl>';
    if (openable(n)) html += '<div class="actions"><button type="button" data-zopen="' + esc(n.id) + '" data-zu="' + uid + '">Open ' + esc(n.type === 'dir' ? 'folder' : 'file') + '</button></div>';
    return html;
  }

  // ----- the panel's tree of everything in a unit, filtered, virtualised
  var TREE_ROW = 24, TREE_H = 384;
  function treeRows(uid) {
    var t = ztree(uid), z = zdata();
    if (!t) return [];
    var q = (ast.treeQ[uid] || '').trim().toLowerCase(), rows = [];
    if (!q) {
      (function walk(n, depth) {
        var kids = n.type === 'file' ? n.file.top.map(function (i) { return symNode(z.syms[i]); }) : n.type === 'sym' ? n.sym.kids.map(function (i) { return symNode(z.syms[i]); }) : n.kids;
        kids.forEach(function (k) {
          var hasKids = k.type === 'dir' || (k.type === 'file' && k.file.top.length) || (k.type === 'sym' && k.sym.kids.length);
          var open = ast.treeOpen.has(uid + ':' + k.id);
          rows.push({ node: k, depth: depth, kids: hasKids, open: open });
          if (hasKids && open) walk(k, depth + 1);
        });
      })(t.root, 0);
      return rows;
    }
    // Matches and the folders, files and classes around them.
    var keep = new Set(), match = new Set();
    function mark(id) {
      for (var c = zchain(uid, id), i = 0; i < c.length; i++) keep.add(c[i]);
      keep.add(id);
    }
    Object.keys(t.nodes).forEach(function (id) {
      var n = t.nodes[id];
      if (n.type !== 'unit' && (n.rel || n.name).toLowerCase().indexOf(q) >= 0) { match.add(id); mark(id); }
    });
    z.syms.forEach(function (y) {
      if (z.files[y.file].unit !== uid) return;
      if (y.name.toLowerCase().indexOf(q) >= 0 || y.qual.toLowerCase().indexOf(q) >= 0) {
        match.add('s' + y.i); mark('s' + y.i);
        for (var p = y.parent; p >= 0; p = z.syms[p].parent) keep.add('s' + p);
      }
    });
    (function walk(n, depth) {
      var kids = n.type === 'file' ? n.file.top.map(function (i) { return symNode(z.syms[i]); }) : n.type === 'sym' ? n.sym.kids.map(function (i) { return symNode(z.syms[i]); }) : n.kids;
      kids.forEach(function (k) {
        if (!keep.has(k.id)) return;
        rows.push({ node: k, depth: depth, kids: false, open: true, hit: match.has(k.id) });
        walk(k, depth + 1);
      });
    })(t.root, 0);
    rows.forEach(function (r, i) { r.kids = i + 1 < rows.length && rows[i + 1].depth > r.depth; });
    return rows;
  }
  function treeSection(u) {
    var t = ztree(u.id);
    if (!t) return '';
    var om = zdata().omitted[u.id];
    var html = '<h3>Inside · ' + esc(plural(t.root.files, 'file', 'files') + ' · ' + plural(t.root.syms, 'symbol', 'symbols')) + '</h3>';
    if (om) html += '<p class="more">' + esc(plural(om.syms, 'symbol', 'symbols') + ' in ' + plural(om.files, 'file', 'files') + ' not embedded (--max-symbols ' + fmt(ZR.max_symbols) +
      '). Run lidx graph --max-symbols ' + Math.max(ZR.max_symbols * 2, ZR.max_symbols + om.syms) + ' to include them, or lidx graph --seed <qualname> for one symbol.') + '</p>';
    html += '<label class="sr-only" for="tree-q">Filter ' + esc(u.name) + '</label><input id="tree-q" class="tree-q" type="search" autocomplete="off" spellcheck="false" placeholder="Filter folders, files and symbols" data-tree="' + u.id + '" value="' + esc(ast.treeQ[u.id] || '') + '">';
    html += '<div class="vtree" role="tree" aria-label="Folders, files and symbols in ' + esc(u.name) + '" data-vtree="' + u.id + '"><div class="vt-pad"><div class="vt-rows"></div></div></div><p class="more vt-count"></p>';
    return html;
  }
  var treeCache = { uid: null, q: null, rows: [] };
  function treeData(uid) {
    var q = ast.treeQ[uid] || '', key = q + '|' + ast.treeOpen.size;
    if (treeCache.uid !== uid || treeCache.key !== key) treeCache = { uid: uid, key: key, rows: treeRows(uid) };
    return treeCache.rows;
  }
  function renderTree(scrollTo) {
    var box = panel.querySelector ? panel.querySelector('[data-vtree]') : null;
    if (!box) return;
    var uid = +box.dataset.vtree, rows = treeData(uid);
    var pad = box.querySelector('.vt-pad'), list = box.querySelector('.vt-rows');
    var h = Math.min(TREE_H, Math.max(TREE_ROW, rows.length * TREE_ROW));
    box.style.height = h + 'px';
    pad.style.height = rows.length * TREE_ROW + 'px';
    if (scrollTo !== undefined && scrollTo >= 0) {
      var top = scrollTo * TREE_ROW;
      if (top < box.scrollTop || top > box.scrollTop + h - TREE_ROW) box.scrollTop = Math.max(0, top - h / 2);
    }
    var first = Math.max(0, Math.floor(box.scrollTop / TREE_ROW) - 8), last = Math.min(rows.length, first + Math.ceil(h / TREE_ROW) + 16);
    var cur = ast.zsel && ast.zsel.u === uid ? ast.zsel.id : null, html = '', filtered = !!(ast.treeQ[uid] || '').trim();
    for (var i = first; i < last; i++) {
      var r = rows[i], n = r.node, kind = n.type === 'sym' ? n.sym.kind : n.type === 'dir' ? 'folder' : 'file';
      var meta = n.type === 'sym' ? ':' + n.sym.line : n.type === 'dir' ? fmt(n.syms) : n.file.emb ? fmt(n.file.syms) : fmt(n.file.syms) + ' not embedded';
      html += '<div class="vt-row' + (r.hit ? ' hit' : '') + (n.id === cur ? ' cur' : '') + '" role="treeitem" aria-level="' + (r.depth + 1) + '"' +
        (r.kids ? ' aria-expanded="' + r.open + '"' : '') + ' style="padding-left:' + (4 + r.depth * 14) + 'px">' +
        (r.kids && !filtered ? '<button type="button" class="tw" data-ttog="' + esc(n.id) + '" tabindex="-1" aria-label="' + (r.open ? 'Collapse ' : 'Expand ') + esc(n.name) + '">' + (r.open ? '▾' : '▸') + '</button>'
          : '<span class="tw" aria-hidden="true">' + (r.kids ? '▾' : '') + '</span>') +
        '<button type="button" class="tn" data-tgo="' + esc(n.id) + '" data-ti="' + i + '"><span class="g">' + esc(n.type === 'sym' ? glyph(kind) : n.type === 'dir' ? '▭' : '▤') + '</span>' +
        '<span class="nm">' + esc(n.name) + '</span><span class="loc">' + esc(meta) + '</span></button></div>';
    }
    list.style.transform = 'translateY(' + first * TREE_ROW + 'px)';
    list.innerHTML = html;
    var count = panel.querySelector('.vt-count');
    if (count) count.textContent = ast.treeQ[uid] ? plural(rows.filter(function (r) { return r.hit; }).length, 'match', 'matches') : '';
  }
  var treeTimer = null;
  panel.addEventListener('input', function (ev) {
    var t = ev.target;
    if (!t || !t.dataset || t.dataset.tree === undefined) return;
    ast.treeQ[+t.dataset.tree] = t.value;
    clearTimeout(treeTimer);
    treeTimer = setTimeout(function () { var b = panel.querySelector('[data-vtree]'); if (b) b.scrollTop = 0; renderTree(); writeHash(); }, 90);
  });
  panel.addEventListener('scroll', function (ev) {
    if (ev.target && ev.target.classList && ev.target.classList.contains('vtree')) renderTree();
  }, true);
  panel.addEventListener('keydown', function (ev) {
    var b = ev.target.closest ? ev.target.closest('.tn') : null;
    if (ev.target.classList && ev.target.classList.contains('tree-q') && ev.key === 'ArrowDown') {
      ev.preventDefault();
      var f = panel.querySelector('.vt-row .tn');
      if (f) f.focus();
      return;
    }
    if (!b) return;
    var i = +b.dataset.ti, rows = treeData(+panel.querySelector('[data-vtree]').dataset.vtree), r = rows[i];
    var go = function (j) {
      if (j < 0 || j >= rows.length) return;
      renderTree(j);
      var nb = panel.querySelector('.tn[data-ti="' + j + '"]');
      if (nb) nb.focus();
    };
    if (ev.key === 'ArrowDown') { ev.preventDefault(); go(i + 1); }
    else if (ev.key === 'ArrowUp') { ev.preventDefault(); if (i === 0) { var q = panel.querySelector('.tree-q'); if (q) q.focus(); } else go(i - 1); }
    else if ((ev.key === 'ArrowRight' && r && r.kids && !r.open) || (ev.key === 'ArrowLeft' && r && r.kids && r.open)) {
      ev.preventDefault(); treeToggle(r.node.id); go(i);
    }
  });
  function treeToggle(id) {
    var box = panel.querySelector('[data-vtree]');
    if (!box) return;
    var key = box.dataset.vtree + ':' + id;
    if (ast.treeOpen.has(key)) ast.treeOpen.delete(key); else ast.treeOpen.add(key);
    treeCache.uid = null;
    renderTree();
  }
  // Click in the tree: zoom the sheet to the entry; the panel stays.
  function treeGo(uid, id) {
    var keep = ast.selected;
    zoomReveal(uid, id);
    ast.selected = keep || 'u' + uid;
    ast.zpanelUnit = uid;
    renderPanel();
    var rows = treeData(uid);
    for (var i = 0; i < rows.length; i++) if (rows[i].node.id === id) { renderTree(i); break; }
  }

  // ----- URL hash: #zoom=<unit>/<path>,...&sel=<unit>/<id>&panel=<unit>&filter=<text>
  function unitByName(name) { for (var i = 0; i < AU.length; i++) if (AU[i].name === name) return AU[i].id; return -1; }
  function nodeRef(uid, id) {
    var n = znode(uid, id);
    if (!n) return '';
    if (n.type === 'sym') return zdata().files[n.sym.file].path.slice((AU[uid].dir || '').length) + '#' + n.sym.qual;
    return n.rel;
  }
  function nodeFromRef(uid, ref) {
    var t = ztree(uid), z = zdata();
    if (!t) return null;
    var hashAt = ref.indexOf('#');
    if (hashAt >= 0) {
      var fileRel = ref.slice(0, hashAt), q = ref.slice(hashAt + 1), fn = null;
      Object.keys(t.nodes).forEach(function (id) { if (t.nodes[id].type === 'file' && t.nodes[id].rel === fileRel) fn = t.nodes[id]; });
      if (!fn) return null;
      var hit = null;
      z.syms.forEach(function (y) { if (!hit && y.file === fn.file.i && (y.qual === q || y.name === q)) hit = 's' + y.i; });
      return hit;
    }
    if (t.rel[ref]) return t.rel[ref].id;
    var found = null;
    Object.keys(t.nodes).forEach(function (id) { if (t.nodes[id].rel === ref) found = id; });
    return found;
  }
  function writeHash() {
    var hist = window.history, loc = window.location;
    if (!ZR || !hist || !hist.replaceState || !loc) return;
    var parts = [];
    var z = [];
    ast.open.forEach(function (uid) {
      var path = ast.zpath[uid] || [];
      z.push(encodeURIComponent(AU[uid].name) + (path.length ? '/' + encodeURIComponent(nodeRef(uid, path[path.length - 1])) : ''));
    });
    if (z.length) parts.push('zoom=' + z.join(','));
    if (ast.zsel) parts.push('sel=' + encodeURIComponent(AU[ast.zsel.u].name) + '/' + encodeURIComponent(nodeRef(ast.zsel.u, ast.zsel.id)));
    if (ast.selected && ast.selected.charAt(0) === 'u') {
      var pu = +ast.selected.slice(1);
      parts.push('panel=' + encodeURIComponent(AU[pu].name));
      if (ast.treeQ[pu]) parts.push('filter=' + encodeURIComponent(ast.treeQ[pu]));
    }
    try { hist.replaceState(null, '', parts.length ? '#' + parts.join('&') : loc.pathname + loc.search); } catch (e) { /* file: pages may refuse */ }
  }
  // A value from the fragment, or '' when its percent-encoding is malformed:
  // a bad part is skipped, the rest still applies.
  function dec(s) { try { return decodeURIComponent(s || ''); } catch (e) { return ''; } }
  function readHash() {
    var h = ((window.location && window.location.hash) || '').replace(/^#/, '');
    if (!ZR || !h) return false;
    var p = {};
    h.split('&').forEach(function (kv) { var i = kv.indexOf('='); if (i > 0) p[kv.slice(0, i)] = kv.slice(i + 1); });
    var applied = false, lastU = null;
    (p.zoom ? p.zoom.split(',') : []).forEach(function (entry) {
      var i = entry.indexOf('/'), name = dec(i >= 0 ? entry.slice(0, i) : entry), uid = unitByName(name);
      if (uid < 0) return;
      revealUnit(uid);
      var ref = i >= 0 ? dec(entry.slice(i + 1)) : '', id = ref ? nodeFromRef(uid, ref) : null;
      var path = id ? zchain(uid, id) : [];
      if (id && !openable(znode(uid, id)) && znode(uid, id).type !== 'sym') path = path.slice(0, -1);
      ast.open.add(uid); ast.zpath[uid] = path; ast.zfocus = uid; lastU = uid; applied = true;
    });
    if (p.sel) {
      var j = p.sel.indexOf('/'), su = j > 0 ? unitByName(dec(p.sel.slice(0, j))) : -1;
      if (su >= 0) {
        var sid = nodeFromRef(su, dec(p.sel.slice(j + 1)));
        if (sid) {
          var chain = zchain(su, sid), n = znode(su, sid);
          ast.open.add(su);
          if (!ast.zpath[su] || ast.zpath[su].length < chain.length - 1) ast.zpath[su] = n.type === 'sym' ? chain : chain.slice(0, -1);
          ast.zsel = { u: su, id: sid }; ast.zfocus = su; lastU = su; applied = true;
        }
      }
    }
    if (p.panel) {
      var pu = unitByName(dec(p.panel));
      if (pu >= 0) { ast.selected = 'u' + pu; ast.zpanelUnit = pu; applied = true; if (p.filter) ast.treeQ[pu] = dec(p.filter); }
    }
    ast.hashFocus = lastU;
    return applied;
  }

  function zendLabel(r) {
    if (r.card !== undefined) return AU[r.card].name;
    var it = r.it;
    if (!it) return AU[r.u].name;
    return it.type === 'row' ? it.sym.name : it.node.name;
  }
  function zlinkTooltip(l) {
    if (!l) return '';
    var html = '<div class="t-kind">' + esc(zendLabel(l.a)) + ' → ' + esc(zendLabel(l.b)) + '</div><div><b>' + esc(zkindsLabel(l.kinds)) + '</b></div>';
    if (l.keys.length) html += '<span class="t-ev">' + esc(l.keys.map(keyName).join(', ')) + '</span>';
    return html;
  }
  function stubTooltip(s) {
    if (!s) return '';
    var f = focusItem(s.u), here = f ? f.node.name : AU[s.u].name;
    var html = '<div class="t-kind">' + esc(here + ' ' + (s.out && s.inn ? '⇄' : s.out ? '→' : '←') + ' ' + stubName(s)) + '</div><div><b>' + esc(zkindsLabel(s.kinds)) + '</b></div>';
    html += '<div class="t-dim">' + esc('from ' + plural(s.members.length, 'box', 'boxes') + ' here: ' + s.members.slice(0, 6).map(function (it) { return it.type === 'row' ? it.sym.name : it.node.name; }).join(', ') + (s.members.length > 6 ? ', …' : '')) + '</div>';
    if (s.keys.length) html += '<span class="t-ev">' + esc(s.keys.map(keyName).join(', ')) + '</span>';
    return html;
  }
  // A stub: the links bundled into it, with the boxes they come from.
  function stubPanel(s) {
    var f = focusItem(s.u), here = f ? f.node.name : AU[s.u].name;
    var html = '<div class="panel-head"><h2>' + esc(here + ' ' + (s.out && s.inn ? '⇄' : s.out ? '→' : '←') + ' ' + stubName(s)) + '</h2>' +
      '<button type="button" data-act="aclose" aria-label="Close details">×</button></div>';
    html += '<p class="lead">' + esc(zkindsLabel(s.kinds)) + '</p>';
    if (s.keys.length) html += '<h3>Through</h3><div class="edges"><ul>' + s.keys.map(function (k) { return '<li class="loc" style="padding:2px 7px;font:12px var(--mono)">' + esc(keyName(k)) + '</li>'; }).join('') + '</ul></div>';
    var rows = [];
    s.links.forEach(function (i) { ast.zlinks[i].edges.forEach(function (e) { if (rows.length < 61) rows.push(e); }); });
    html += '<h3>Edges · ' + fmt(Math.min(rows.length, 60)) + (rows.length > 60 ? '+' : '') + '</h3><div class="edges"><ul>' + rows.slice(0, 60).map(function (e) {
      return '<li><button type="button" data-zend="' + e.a + '"><span class="nm">' + esc(endName(e.a)) + ' → ' + esc(endName(e.b)) +
        '<span class="w"> · ' + esc((ZKIND_WORD[e.kind] || e.kind.toLowerCase()) + (e.w > 1 ? ' ×' + fmt(e.w) : '')) + '</span></span><span class="loc">' + esc(endLoc(e.a)) + '</span></button></li>';
    }).join('') + '</ul></div>';
    return html;
  }
  function zlinkPanel(l) {
    var html = '<div class="panel-head"><h2>' + esc(zendLabel(l.a)) + ' → ' + esc(zendLabel(l.b)) + '</h2>' +
      '<button type="button" data-act="aclose" aria-label="Close details">×</button></div>';
    html += '<p class="lead">' + esc(zkindsLabel(l.kinds)) + '</p>';
    if (l.keys.length) html += '<h3>Through</h3><div class="edges"><ul>' + l.keys.map(function (k) { return '<li class="loc" style="padding:2px 7px;font:12px var(--mono)">' + esc(keyName(k)) + '</li>'; }).join('') + '</ul></div>';
    // The stored edges behind it, end to end.
    var z = zdata(), rows = [], ka = repKey(l.a), kb = repKey(l.b);
    z.edges.forEach(function (e) {
      if (rows.length > 60) return;
      var A = zrep(e.a), B = zrep(e.b);
      if (repKey(A) === ka && repKey(B) === kb) rows.push(e);
    });
    html += '<h3>Edges · ' + fmt(rows.length) + (rows.length > 60 ? '+' : '') + '</h3><div class="edges"><ul>' + rows.slice(0, 60).map(function (e) {
      return '<li><button type="button" data-zend="' + e.a + '"><span class="nm">' + esc(endName(e.a)) + ' → ' + esc(endName(e.b)) +
        '<span class="w"> · ' + esc((ZKIND_WORD[e.kind] || e.kind.toLowerCase()) + (e.w > 1 ? ' ×' + fmt(e.w) : '')) + '</span></span><span class="loc">' + esc(endLoc(e.a)) + '</span></button></li>';
    }).join('') + '</ul></div>';
    return html;
  }

  // ------------------------------------------------------------- levels
  // Apps (the sheet above), Folders (the folder map at its starting depth)
  // and Files (every folder open) over one page.
  var levelSeg = document.getElementById('level-seg');
  var lvlBtns = { app: document.getElementById('lvl-app'), folder: document.getElementById('lvl-folder'), file: document.getElementById('lvl-file') };
  var graphOnly = Array.prototype.slice.call(document.querySelectorAll('.graph-only'));
  var graphBooted = false, FOLDER_DEPTH = 1;
  function updateLevelButtons() {
    if (!lvlBtns.app) return;
    var lv = st.level === 'app' ? 'app' : st.depth === Infinity ? 'file' : 'folder';
    Object.keys(lvlBtns).forEach(function (k) { lvlBtns[k].setAttribute('aria-pressed', String(k === lv)); });
  }
  function setLevel(l, fromHistory) {
    if (l === 'app' && !HAS_APPS) return;
    var was = st.level;
    if (!fromHistory && l !== (was === 'app' ? 'app' : st.depth === Infinity ? 'file' : 'folder')) pushHistory();
    var app = l === 'app';
    st.level = app ? 'app' : 'graph';
    appsEl.hidden = !app;
    stage.classList.toggle('app-mode', app);
    graphOnly.forEach(function (el) { el.hidden = app; });
    crumbs.hidden = app;
    hideTooltip();
    if (!app && zedges) zedges.innerHTML = '';
    if (app) {
      empty.hidden = true;
      sim.running = false;
      renderApps(ast.built);
      renderAppCrumbs();
    } else {
      empty.hidden = true;
      ensureGraphBooted();
      renderCrumbs();
      if (!fromHistory) {
        var d = l === 'file' ? Infinity : FOLDER_DEPTH;
        if (was === 'app' && ((d === Infinity) === (st.depth === Infinity))) { resize(); if (!st.userMoved) fit(true); }
        else depthTo(d);
      } else resize();
      updateEmpty();
    }
    updateLevelButtons();
    renderLegend();
    renderPanel();
    setHeader();
    setStatus();
  }
  Object.keys(lvlBtns).forEach(function (k) { if (lvlBtns[k]) lvlBtns[k].addEventListener('click', function () { setLevel(k); }); });

  // Seeded views colour symbols by the app they belong to.
  var APP_HUES = (function () {
    var counts = {};
    N.forEach(function (n) { var u = NODE_UNIT[n.i]; if (u >= 0 && !has(n, F.halo)) counts[u] = (counts[u] || 0) + 1; });
    var order = Object.keys(counts).sort(function (a, b) { return counts[b] - counts[a] || a - b; });
    var hue = {};
    order.forEach(function (u, i) { hue[u] = Math.round((25 + i * 137.508) % 360); });
    return { hue: hue, order: order.map(Number), counts: counts };
  })();
  function appColor(u) {
    var h = APP_HUES.hue[u];
    if (h === undefined) return theme['l-other'];
    return effectiveDark() ? 'oklch(74% 0.13 ' + h + ')' : 'oklch(62% 0.15 ' + h + ')';
  }
  function nodeColor(n) {
    if (st.colorBy === 'app') { var u = NODE_UNIT[n.i]; if (u >= 0) return appColor(u); }
    return langColor(n.lang);
  }

  // ----------------------------------------------------------------- header
  function setHeader() {
    document.getElementById('repo-name').textContent = META.repo;
    var c = META.counts, parts = [];
    if (st.level === 'app') {
      var by = {};
      AU.forEach(function (u) { var g = u.app ? 'app' : u.hidden ? 'hidden' : u.kind === 'infra' || u.kind === 'data' ? 'infra' : 'lib'; by[g] = (by[g] || 0) + 1; });
      parts.push('<b>applications and libraries</b>');
      var counts = [];
      if (by.app) counts.push(plural(by.app, 'app', 'apps'));
      if (by.lib) counts.push(plural(by.lib, 'library or shared folder', 'libraries and shared folders'));
      if (by.infra) counts.push(plural(by.infra, 'infra unit', 'infra units'));
      parts.push(counts.join(' · ') + (by.hidden ? '<span class="long"> · ' + fmt(by.hidden) + ' tests, examples, docs and tooling hidden</span>' : ''));
    } else {
      parts.push('<b>' + esc(META.scope) + '</b>');
      parts.push(fmt(c.nodes) + ' nodes · ' + fmt(c.edges) + ' edges' + (c.halo_nodes ? ' · ' + fmt(c.halo_nodes) + ' outside the walk' : ''));
    }
    if (META.truncated) parts.push('<span class="warn">trimmed, see notes</span>');
    var when = META.generated_at ? new Date(META.generated_at * 1000).toISOString().slice(0, 16).replace('T', ' ') + ' UTC' : '';
    parts.push('<span class="long">' + (META.commit ? 'commit <code>' + esc(META.commit.slice(0, 10)) + '</code> · ' : '') +
      'graph v' + esc(META.graph_version) + ' · ' + esc(META.generated_by + (when ? ', ' + when : '')) + '</span>');
    document.getElementById('meta').innerHTML = parts.join(' · ');
  }
  var statusEl = document.getElementById('status');
  function setStatus() {
    if (st.level === 'app') {
      statusEl.textContent = plural(Object.keys(ast.pos).length, 'card', 'cards') + ' · ' +
        plural(ast.conns.filter(function (c) { return !c.skip; }).length, 'link', 'links') +
        (ast.open.size ? ' · ' + plural(ast.open.size, 'card', 'cards') + ' open, ' + plural(ast.zlinks.length, 'inner link', 'inner links') +
          (ast.zlinks.length > ZLINK_CAP ? ' (' + fmt(ZLINK_CAP) + ' heaviest drawn)' : '') : ZR ? ' · scroll into a card to open it' : '') +
        ' · ' + Math.round(ast.cam.k * 100) + '%';
      return;
    }
    var running = sim.running ? 'laying out · ' : '';
    var folded = st.folded.size ? ' · ' + plural(st.folded.size, 'folder', 'folders') + ' folded to stay under ' + fmt(BUDGET) : '';
    statusEl.textContent = running + fmt(V.nodes.length) + ' shown · ' + fmt(V.edges.length) + ' lines' + folded + ' · ' + Math.round(st.cam.k * 100) + '%';
  }

  // ------------------------------------------------------------------- boot
  var savedTheme = storageGet('lidx-graph-theme');
  if (savedTheme === 'dark' || savedTheme === 'light') document.documentElement.setAttribute('data-theme', savedTheme);
  readTheme();
  setHeader();
  if (window.matchMedia && window.matchMedia('(max-width: 760px)').matches) setLegend(false);
  if (!N.some(function (n) { return !has(n, F.scope_out); })) st.showNonCode = true;
  setDepth(META.collapse_modules ? 1 : Infinity);
  // A repo with one or two top-level folders says little at depth 1: go a
  // level deeper until the overview shows a handful of boxes.
  if (META.collapse_modules) {
    for (var d0 = 2; d0 <= MAX_DEPTH && countDrawn(N.filter(leafVisible)) < 4; d0++) setDepth(d0);
  }
  FOLDER_DEPTH = st.depth;
  rebuild();
  if (HAS_APPS) {
    levelSeg.hidden = false;
    search.placeholder = 'Find an app, symbol, file or folder';
  }
  applyTheme(null);
  renderCrumbs();
  // On the sheet, a panel opening or the window resizing keeps the zoom and
  // the view's centre; only Fit (or a first look) rescales.
  var lastStageW = stage.getBoundingClientRect().width;
  function onResize() {
    var w = stage.getBoundingClientRect().width;
    if (st.level === 'app') {
      if (!ast.sheet || !lastStageW) fitApps();
      else { ast.cam.x += (w - lastStageW) / 2; applyAppCam(); }
    } else resize();
    lastStageW = w;
  }
  if (typeof ResizeObserver !== 'undefined') new ResizeObserver(onResize).observe(stage);
  else window.addEventListener('resize', onResize);
  resize();
  layForce.setAttribute('aria-pressed', String(st.layout === 'force'));
  layLayer.setAttribute('aria-pressed', String(st.layout === 'layered'));
  // The first layout is computed at once: a headless or background page may
  // never run an animation frame. On the app sheet it waits until the
  // folder map is first shown.
  function ensureGraphBooted() {
    if (graphBooted) return;
    graphBooted = true;
    resize();
    fit(true);
    if (st.layout === 'layered') layered(true);
    else {
      sim.alpha = 1;
      if (!settleNow(REDUCE ? 4000 : 400)) startForce(sim.alpha);
    }
  }
  // A repository that is one unit opens its card: one closed box says
  // nothing the header does not.
  var shownUnits = AU.filter(function (u) { return !u.hidden; });
  // A malformed fragment (bad percent-encoding) is ignored, not fatal.
  var fromHash = false;
  try { fromHash = st.level === 'app' && readHash(); } catch (e) { fromHash = false; }
  if (!fromHash && shownUnits.length === 1) { ast.open.add(shownUnits[0].id); ast.zpath[shownUnits[0].id] = []; ast.zfocus = shownUnits[0].id; }
  if (st.level === 'app') setLevel('app', true);
  if (fromHash) {
    // Straight to the state the link names, without animating.
    var hf = ast.hashFocus, savedReduce = REDUCE;
    REDUCE = true;
    // The panel first, so the fit below sees the stage it leaves.
    renderPanel();
    onResize();
    if (ast.zsel && ast.zitems[ast.zsel.u] && ast.zitems[ast.zsel.u].byId[ast.zsel.id]) {
      var zp = ast.zpath[ast.zsel.u], deepIt = zp.length ? ast.zitems[ast.zsel.u].byId[zp[zp.length - 1]] : null;
      fitItem(ast.zsel.u, ast.zitems[ast.zsel.u].byId[ast.zsel.id], deepIt);
    } else if (ast.open.size > 1) {
      // Several open cards: their deepest open levels, together.
      var ub = null;
      ast.open.forEach(function (uid) {
        var p = ast.zpath[uid] || [], b = p.length ? itemBox(uid, ast.zitems[uid].byId[p[p.length - 1]]) : ast.pos['u' + uid];
        if (!b) return;
        if (!ub) ub = { x: b.x, y: b.y, x2: b.x + b.w, y2: b.y + b.h };
        else { ub.x = Math.min(ub.x, b.x); ub.y = Math.min(ub.y, b.y); ub.x2 = Math.max(ub.x2, b.x + b.w); ub.y2 = Math.max(ub.y2, b.y + b.h); }
      });
      if (ub) fitBox({ x: ub.x, y: ub.y, w: ub.x2 - ub.x, h: ub.y2 - ub.y }, 0.94); else fitApps();
    }
    else if (hf !== null && hf !== undefined && ast.open.has(hf)) {
      var hp = ast.zpath[hf], hb = hp.length ? itemBox(hf, ast.zitems[hf].byId[hp[hp.length - 1]]) : ast.pos['u' + hf];
      fitBox(hb, 0.9);
    }
    REDUCE = savedReduce;
    renderZoomView(true);
    renderPanel();
  }
  else { ensureGraphBooted(); updateLevelButtons(); }
  setStatus();
  // Exposed for automated checks (headless screenshots, tests).
  window.lidxGraph = {
    nodes: N.length, edges: E.length, bootMs: Math.round(performance.now() - BOOT_T0),
    visible: function () { return V.nodes.length; },
    settled: function () { return st.level === 'app' || (!sim.running && !anim); },
    svg: function () { return toSvg(); },
    json: function () { return toJson(); },
    clientXY: function (id) {
      var v = V.byId.get(id);
      if (!v) return null;
      var r = canvas.getBoundingClientRect();
      return [r.left + sx(v.x), r.top + sy(v.y)];
    },
    clusters: function () { return V.nodes.filter(function (v) { return v.cluster !== null; }).map(function (v) { return v.id; }); },
    layoutKey: function () { return V.nodes.map(function (v) { return v.id + '@' + Math.round(v.x) + ',' + Math.round(v.y); }).join(';'); },
    drawMs: function () { var t = performance.now(); draw(); return performance.now() - t; },
    tickMs: function () { var a = sim.alpha, t = performance.now(); sim.alpha = 0.1; tick(); sim.alpha = a; return performance.now() - t; },
    select: function (q) {
      var n = N.find(function (x) { return x.qual === q || x.name === q; });
      if (n) focusLeaf(n.i);
      return !!n;
    },
    setLayout: setLayout,
    setView: setView,
    level: function () { return st.level === 'app' ? 'app' : st.depth === Infinity ? 'file' : 'folder'; },
    setLevel: setLevel,
    colorBy: function () { return st.colorBy; },
    // The sheet: each card's band and box, and the drawn links.
    apps: function () {
      return {
        cards: Object.keys(ast.pos).map(function (k) {
          var p = ast.pos[k];
          return { key: k, name: k.charAt(0) === 'u' ? AU[+k.slice(1)].name : CHIPS[+k.slice(1)].name, band: BANDS[p.band].id, row: p.row, x: p.x, y: p.y, w: p.w, h: p.h };
        }),
        links: ast.conns.filter(function (c) { return !c.skip; }).map(function (c) {
          return { from: c.a, to: c.b, family: c.fam, label: c.label, up: c.sb === 'b' };
        })
      };
    },
    selectUnit: function (name) {
      var u = AU.find(function (x) { return x.name === name; });
      if (!u) return false;
      revealUnit(u.id); selectUnit('u' + u.id, true); return true;
    },
    openUnit: function (name) {
      var u = AU.find(function (x) { return x.name === name; });
      if (u) toggleOpen(u.id);
      return !!u;
    },
    toggleApps: function (what, on) {
      if (what === 'packages') ast.packages = on; else if (what === 'undeclared') ast.undeclared = on; else ast.showKind[what] = on;
      renderApps(true); renderLegend();
    },
    panelText: function () { return panel.hidden ? '' : panel.textContent; },
    // Semantic zoom: open cards, what each shows, the links drawn.
    zoom: {
      expand: function (name) { var id = unitByName(name); if (id < 0) return false; revealUnit(id); zoomOpen(id, ast.zpath[id] || []); return true; },
      // Open `name` down to the folder or file at `rel` (relative to the unit).
      open: function (name, rel) {
        var id = unitByName(name), nid = id >= 0 ? nodeFromRef(id, rel) : null;
        if (!nid) return false;
        zoomOpen(id, zchain(id, nid)); return true;
      },
      reveal: function (name, ref) { var id = unitByName(name), nid = id >= 0 ? nodeFromRef(id, ref) : null; return nid ? zoomReveal(id, nid) : false; },
      escape: function () { appKey({ key: 'Escape', target: document.body, preventDefault: function () {} }); },
      crumb: function (name, depth) { zoomTo(unitByName(name), depth); },
      open_cards: function () { return Array.from(ast.open).map(function (u) { return AU[u].name; }); },
      path: function (name) { var id = unitByName(name); return (ast.zpath[id] || []).map(function (n) { return znode(id, n).name; }); },
      crumbs: function () { var u = ast.zfocus; return u === null || u === undefined || !ast.open.has(u) ? [] : [AU[u].name].concat((ast.zpath[u] || []).map(function (n) { return znode(u, n).name; })); },
      items: function (name) {
        var id = unitByName(name), z = ast.zitems[id];
        return z ? z.list.map(function (it) {
          var n = it.type === 'row' ? it.sym : it.node.sym || it.node;
          return { type: it.type, name: n.name, kind: n.kind || n.type, sig: n.sig || null, line: n.line || null, depth: it.depth };
        }) : [];
      },
      rendered: function () { return ast.zshown || 0; },
      // Cost of one re-render after panning by (dx, dy) screen pixels.
      panMs: function (dx, dy) { ast.cam.x -= dx; ast.cam.y -= dy; applyAppCam(); var t = performance.now(); renderZoomView(true); return performance.now() - t; },
      links: function () {
        renderStubs();
        return ast.zlinks.map(function (l) {
          var side = function (r) { return r.card !== undefined ? { unit: AU[r.card].name, card: true } : { unit: AU[r.u].name, name: zendLabel(r), type: r.it.type, ref: nodeRef(r.u, r.it.id) }; };
          return { from: side(l.a), to: side(l.b), weight: l.w, kinds: l.kinds, keys: l.keys, line: !!l.line };
        });
      },
      // Where links leave each open level: target, direction, weight, the boxes behind it.
      stubs: function () {
        renderStubs();
        return ast.stubs.map(function (s) {
          var f = focusItem(s.u), t = s.target;
          return {
            unit: AU[s.u].name, at: f ? f.node.name : null, side: s.side, target: stubName(s), targetUnit: t ? AU[t.card !== undefined ? t.card : t.u].name : null,
            targetCard: !!t && t.card !== undefined, out: s.out, inn: s.inn, weight: s.w, kinds: s.kinds, keys: s.keys,
            members: s.members.map(function (it) { return it.type === 'row' ? it.sym.name : it.node.name; }).sort(), text: stubText(s)
          };
        });
      },
      // Lines and stubs as drawn, after moving the camera by (dx, dy) screen pixels.
      pan: function (dx, dy) { ast.cam.x -= dx; ast.cam.y -= dy; applyAppCam(); renderZoomView(true); },
      fitAll: function () { if (zAnim) { cancelAnimationFrame(zAnim); zAnim = null; } fitApps(); renderZoomView(true); },
      stubCap: function (n) { STUBS_PER_SIDE = n; renderZoomView(true); },
      // The camera at scale k on the centre of a card's deepest open level, at once.
      look: function (name, k) {
        var b = focusBoxOf(unitByName(name)), r = stage.getBoundingClientRect();
        if (!b) return false;
        if (zAnim) { cancelAnimationFrame(zAnim); zAnim = null; }
        ast.cam.k = k; ast.cam.x = r.width / 2 - (b.x + b.w / 2) * k; ast.cam.y = r.height / 2 - (b.y + b.h / 2) * k;
        applyAppCam(); renderZoomView(true); return true;
      },
      hoverStub: function (i) { markStub(i); return { on: ast.stubs.map(function (s, j) { return stubOn(s, j); }) }; },
      clickStub: function (i) { ast.zlink = { stub: ast.stubs[i] }; renderPanel(); return panel.textContent; },
      selected: function () { var s = ast.zsel; if (!s) return null; var n = znode(s.u, s.id); return { unit: AU[s.u].name, name: n.name, type: n.type }; },
      select: function (name, ref) { var id = unitByName(name), nid = nodeFromRef(id, ref); if (nid) zselect(id, nid); return !!nid; },
      tree: function (name, filter) {
        var id = unitByName(name);
        ast.treeQ[id] = filter || '';
        treeCache.uid = null;
        return treeRows(id).map(function (r) { return { name: r.node.name, type: r.node.type, depth: r.depth, hit: !!r.hit, id: r.node.id }; });
      },
      treeGo: function (name, id) { treeGo(unitByName(name), id); },
      hash: function () { return window.location ? window.location.hash : ''; },
      omitted: function () { return zdata() ? zdata().omitted : {}; }
    },
    back: goBack,
    selected: function () { return st.selected; },
    // A qualname drawn inside a folder the visible budget folded, if any.
    foldedSample: function () {
      var c = V.nodes.find(function (v) { return v.folded; });
      return c ? c.members[Math.floor(c.members.length / 2)].qual : null;
    },
    folded: function () { return st.folded.size; },
    // The drawn node a qualname is shown as: itself ('n<i>') or a folder.
    drawnAs: function (q) {
      var n = N.find(function (x) { return x.qual === q || x.name === q; });
      return n && V.leafTo[n.i] >= 0 ? V.nodes[V.leafTo[n.i]].id : null;
    },
    nodeId: function (q) {
      var n = N.find(function (x) { return x.qual === q || x.name === q; });
      return n ? 'n' + n.i : null;
    }
  };
})();
