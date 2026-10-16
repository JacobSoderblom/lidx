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
      ev: str(r[6]), evFile: str(r[7]), res: str(r[8])
    };
  });
  E.forEach(function (e) { N[e.s].out.push(e); N[e.t].inn.push(e); });
  var AGGREGATED = META.level !== 'symbol';
  var REDUCE = !!(window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches);

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
    return 'other';
  }
  var BRIDGE = { bus: 1, rpc: 1, http: 1, config: 1 };
  var FAMILIES = ['calls', 'imports', 'inherit', 'uses', 'xref', 'other', 'bus', 'rpc', 'http', 'config'];
  // Colour is never the only cue (WCAG 1.4.1): every family has its own dash.
  var STYLE = {
    calls: { tok: 'edge', dash: [], w: 1, label: 'calls', solid: true },
    imports: { tok: 'edge-2', dash: [1.5, 3], w: 0.85, label: 'imports' },
    inherit: { tok: 'edge-3', dash: [], w: 1.1, label: 'inherits / implements', hollow: true, solid: true },
    uses: { tok: 'edge-2', dash: [7, 3], w: 0.9, label: 'uses / references' },
    xref: { tok: 'edge-4', dash: [7, 3, 1.5, 3], w: 0.9, label: 'cross-reference' },
    other: { tok: 'edge-2', dash: [2, 2], w: 0.85, label: 'other' },
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
    if (v.cluster !== null) return Math.min(40, 12 + Math.sqrt(v.members.length) * 2.4);
    var n = v.leaf;
    if (has(n, F.external) && AGGREGATED) return 6;
    if (AGGREGATED) return Math.min(18, 4.5 + Math.sqrt(Math.max(1, n.size)) * 0.8);
    return Math.min(10, 4 + Math.log2(1 + v.deg) * 1.3);
  }

  // Degree of interest of a leaf: seeds and the selection first, then degree.
  function interest(n) {
    var sel = st.selected && st.selected.charAt(0) === 'n' && +st.selected.slice(1) === n.i;
    return (has(n, F.seed) ? 1e6 : 0) + (sel ? 2e6 : 0) + n.out.length + n.inn.length;
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
      if (BRIDGE[e.fam] && emph > 0 && derived(e.refs[0])) tags.push([e, g]);
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
        var lines = Math.min(kids.length, Math.floor((b.h - 40) / 14));
        c.font = '11px ' + SANS;
        c.textAlign = 'left';
        c.textBaseline = 'top';
        c.fillStyle = theme['ink-3'];
        for (var li2 = 0; li2 < lines; li2++) {
          var kt = kids[li2][0] + '  ' + fmt(kids[li2][1]);
          if (li2 === lines - 1 && kids.length > lines) kt = '+' + (kids.length - lines + 1) + ' more';
          c.fillText(kt.length > 34 ? kt.slice(0, 33) + '…' : kt, b.x + 8, b.y + 24 + li2 * 14);
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
      c.fillStyle = langColor(n.lang);
      c.fill();
      c.strokeStyle = theme['node-line'];
      c.stroke();
    } else {
      c.beginPath();
      c.arc(x, y, r, 0, Math.PI * 2);
      c.fillStyle = langColor(n.lang);
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
          if (tw + 14 <= b.w && b.h > 26) { x = b.x + 7; y = b.y + 12; }
          else { x = sx(v.x) - tw / 2; y = b.y + b.h + 10; }
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
  function fit(markUser, only) {
    if (!V.nodes.length) return;
    var x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    (only ? only.map(function (i) { return V.nodes[i]; }) : V.nodes).forEach(function (v) {
      var r = v.cluster !== null ? v.r * 1.3 : v.r;
      x0 = Math.min(x0, v.x - r); y0 = Math.min(y0, v.y - r);
      x1 = Math.max(x1, v.x + r + (v.cluster === null ? Math.min(230, v.label.length * 7) : 0));
      y1 = Math.max(y1, v.y + r + (v.cluster !== null ? 18 : 0));
    });
    var layeredView = st.layout === 'layered' && !only;
    if (layeredView) y0 -= 40;
    var avail = W;
    var bw = x1 - x0 + 80, bh = y1 - y0 + 60;
    var k = clampK(Math.min(avail / bw, H / bh, 2.2));
    st.cam.k = k;
    st.cam.x = layeredView ? 40 - x0 * k : avail / 2 - (x0 + x1) / 2 * k;
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
      showNonCode: st.showNonCode, showExternal: st.showExternal, showEndpoints: st.showEndpoints, showTests: st.showTests
    };
  }
  function pushHistory() {
    past.push(snapshot());
    if (past.length > 80) past.shift();
    future = [];
    updateHistoryButtons();
  }
  function restore(s) {
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
    if (st.picking && V.byId.get(id)) { finishPath(V.byId.get(id)); return; }
    select(id, true);
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
    var code = FAMILIES.filter(function (f) { return !BRIDGE[f]; }).map(fam).join('');
    var bridges = FAMILIES.filter(function (f) { return BRIDGE[f]; }).map(fam).join('');
    var html = '<details data-sec="edges"' + (legendOpen.edges ? ' open' : '') + '><summary>Edges</summary>' + (code || '<p class="hint">None in view</p>');
    if (bridges) html += '<p class="hint">Across processes, between the folders that own each side:</p>' + bridges;
    html += '<div class="row-actions"><button type="button" data-legend="all">All</button><button type="button" data-legend="calls">Calls only</button>' +
      (bridges ? '<button type="button" data-legend="bridges">Bridges + calls</button>' : '') + '</div></details>';
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
    closeMenu(); download(baseName + '-view.json', new Blob([JSON.stringify(toJson(), null, 1)], { type: 'application/json' }));
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
      var fill = has(n, F.endpoint) ? theme.bridge : has(n, F.external | F.unresolved) ? theme.canvas : langColor(n.lang);
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

  // ----------------------------------------------------------------- header
  function setHeader() {
    document.getElementById('repo-name').textContent = META.repo;
    var c = META.counts, parts = [];
    parts.push('<b>' + esc(META.scope) + '</b>');
    parts.push(fmt(c.nodes) + ' nodes · ' + fmt(c.edges) + ' edges' + (c.halo_nodes ? ' · ' + fmt(c.halo_nodes) + ' outside the walk' : ''));
    if (META.truncated) parts.push('<span class="warn">trimmed, see notes</span>');
    var when = META.generated_at ? new Date(META.generated_at * 1000).toISOString().slice(0, 16).replace('T', ' ') + ' UTC' : '';
    parts.push('<span class="long">' + (META.commit ? 'commit <code>' + esc(META.commit.slice(0, 10)) + '</code> · ' : '') +
      'graph v' + esc(META.graph_version) + ' · ' + esc(META.generated_by + (when ? ', ' + when : '')) + '</span>');
    document.getElementById('meta').innerHTML = parts.join(' · ');
  }
  var statusEl = document.getElementById('status');
  function setStatus() {
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
  rebuild();
  applyTheme(null);
  renderCrumbs();
  if (typeof ResizeObserver !== 'undefined') new ResizeObserver(resize).observe(stage);
  else window.addEventListener('resize', resize);
  resize();
  fit(true);
  layForce.setAttribute('aria-pressed', String(st.layout === 'force'));
  layLayer.setAttribute('aria-pressed', String(st.layout === 'layered'));
  // The first layout is computed at once: a headless or background page may
  // never run an animation frame.
  if (st.layout === 'layered') layered(true);
  else {
    sim.alpha = 1;
    if (!settleNow(REDUCE ? 4000 : 400)) startForce(sim.alpha);
  }
  setStatus();
  // Exposed for automated checks (headless screenshots, tests).
  window.lidxGraph = {
    nodes: N.length, edges: E.length, bootMs: Math.round(performance.now() - BOOT_T0),
    visible: function () { return V.nodes.length; },
    settled: function () { return !sim.running && !anim; },
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
    back: goBack
  };
})();
