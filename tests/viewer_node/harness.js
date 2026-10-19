// Runs a `lidx graph` page's viewer script under node with a minimal DOM
// stub, then a scenario against its `window.lidxGraph` test API, and prints
// the scenario's result as JSON. Driven by tests/graph_viewer_node.rs.
//
//   node harness.js <page.html> <scenario> [arg]
'use strict';
const fs = require('fs');
const vm = require('vm');

const [page, scenario, arg] = process.argv.slice(2);
const html = fs.readFileSync(page, 'utf8');
const tag = '<script type="application/json" id="lidx-graph-data">';
const start = html.indexOf(tag) + tag.length;
const data = html.slice(start, html.indexOf('</script>', start));
const jsStart = html.indexOf('<script>', start) + '<script>'.length;
const script = html.slice(jsStart, html.indexOf('</script>', jsStart));

// Anything the viewer touches that the stub does not model is a callable,
// infinitely chainable no-op.
function stub(name) {
  const f = function () { return stub(name + '()'); };
  return new Proxy(f, {
    get(t, p) {
      if (p in t) return t[p];
      if (p === Symbol.toPrimitive) return () => '';
      if (p === Symbol.iterator) return undefined;
      if (p === 'then') return undefined;
      return (t[p] = stub(name + '.' + String(p)));
    },
    set(t, p, v) { t[p] = v; return true; },
  });
}
function ctx2d() {
  const c = stub('ctx');
  c.measureText = (s) => ({ width: String(s).length * 7 });
  c.canvas = { width: 1400, height: 900 };
  return c;
}
const elements = new Map();
function element(id) {
  if (elements.has(id)) return elements.get(id);
  const e = stub('#' + id);
  e.id = id;
  e.hidden = false;
  e.textContent = id === 'lidx-graph-data' ? data : '';
  // innerHTML keeps a plain-text copy, so panel text can be read back.
  let html = '';
  Object.defineProperty(e, 'innerHTML', {
    get() { return html; },
    set(v) {
      html = String(v);
      e.textContent = html.replace(/<[^>]*>/g, '').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&amp;/g, '&');
    },
  });
  e.style = {};
  e.dataset = {};
  e.classList = { add() {}, remove() {}, toggle() {}, contains() { return false; } };
  // Listeners are kept, so a scenario can fire wheel and click events.
  e.listeners = {};
  e.addEventListener = (type, fn) => { (e.listeners[type] = e.listeners[type] || []).push(fn); };
  e.setAttribute = () => {};
  e.getAttribute = () => null;
  e.removeAttribute = () => {};
  // An open card's item layer is looked up by selector: give it an element
  // of its own so what is drawn there can be read back.
  e.querySelector = (sel) => (/data-zin/.test(String(sel)) ? element('qs:' + sel) : null);
  e.querySelectorAll = () => [];
  // A scenario may move or resize an element (a panel opening) through `rect`.
  e.rect = null;
  e.getBoundingClientRect = () => e.rect || { left: 0, top: 0, right: 1400, bottom: 900, width: 1400, height: 900 };
  e.getContext = () => ctx2d();
  e.offsetParent = null;
  e.focus = () => {};
  elements.set(id, e);
  return e;
}
const frames = [];
const cancelled = new Set();
let frameId = 0;
// The page's clock: real time, plus 17 ms for every round of frames run,
// so an animation finishes in a few rounds however fast they run.
let clockOffset = 0;
const clock = { now: () => performance.now() + clockOffset };
const windowListeners = {};
const sandbox = {
  console,
  performance: clock,
  setTimeout: (fn) => { frames.push(fn); return 0; },
  clearTimeout: () => {},
  // Cancelled frames do not run (a camera animation stopped by a later one).
  requestAnimationFrame: (fn) => { const id = ++frameId; const f = () => { if (!cancelled.has(id)) fn(clock.now()); }; frames.push(f); return id; },
  cancelAnimationFrame: (id) => { cancelled.add(id); },
  getComputedStyle: () => ({ getPropertyValue: () => '#808080' }),
  // REDUCE=1 in the environment: reduced motion, camera moves at once.
  matchMedia: (q) => ({ matches: !!process.env.REDUCE && String(q).indexOf('reduced-motion') >= 0, addEventListener() {} }),
  addEventListener: (type, fn) => { (windowListeners[type] = windowListeners[type] || []).push(fn); },
  devicePixelRatio: 1,
  // HASH=... starts the page at that URL fragment (the zoom state).
  location: { hash: process.env.HASH || '', pathname: '/page.html', search: '' },
  history: { replaceState(_s, _t, url) { sandbox.location.hash = String(url).charAt(0) === '#' ? String(url) : ''; } },
  navigator: {},
  localStorage: { getItem: () => null, setItem: () => {} },
  Blob: function () {},
  URL: { createObjectURL: () => '', revokeObjectURL: () => {} },
  DOMParser: function () {},
  MouseEvent: function () {},
  document: {
    getElementById: element,
    createElement: () => element('created'),
    body: element('body'),
    documentElement: element('html'),
    addEventListener: () => {},
    activeElement: null,
    execCommand: () => false,
    querySelectorAll: () => [],
  },
};
sandbox.window = sandbox;
vm.createContext(sandbox);
vm.runInContext(script, sandbox, { filename: 'viewer.js' });
// Let queued frames (layout, draws) run a few rounds.
function flush(rounds) {
  for (let r = 0; r < rounds && frames.length; r++) {
    clockOffset += 17;
    const batch = frames.splice(0, frames.length);
    batch.forEach((fn) => { try { fn(); } catch (err) { console.error(err); process.exit(2); } });
  }
}
flush(200);
const g = sandbox.lidxGraph;
let out;
if (scenario === 'focus') {
  const q = arg || g.foldedSample();
  out = { picked: q };
  if (!q) { process.stdout.write(JSON.stringify(out)); process.exit(0); }
  const before = g.drawnAs(q);
  g.select(q);
  flush(50);
  out = { picked: q, before, after: g.drawnAs(q), id: g.nodeId(q), selected: g.selected(), folded: g.folded(), visible: g.visible() };
} else if (scenario === 'focus-named') {
  const before = g.drawnAs(arg);
  g.select(arg);
  flush(50);
  out = { before, after: g.drawnAs(arg), id: g.nodeId(arg), selected: g.selected(), folded: g.folded(), visible: g.visible() };
} else if (scenario === 'lines') {
  out = g.json().edges;
} else if (scenario === 'apps') {
  const level = g.level();
  const sheet = g.apps();
  const selected = g.selectUnit(arg);
  flush(10);
  g.setLevel('folder');
  flush(50);
  out = { level, sheet, selected, after: g.level(), visible: g.visible() };
} else if (scenario === 'guesses') {
  const before = g.apps().links;
  g.toggleApps('undeclared', true);
  flush(10);
  out = { before, after: g.apps().links, cards: g.apps().cards };
} else if (scenario === 'colour') {
  out = { colorBy: g.colorBy(), level: g.level() };
} else if (scenario === 'zoom-drill') {
  // Open a card, then a file, select a method, and back out with Escape.
  const z = g.zoom, steps = {};
  z.expand(arg); flush(20);
  steps.top = { items: z.items(arg), crumbs: z.crumbs(), hash: z.hash(), links: z.links(), stubs: z.stubs() };
  const file = z.items(arg).find((i) => i.type === 'dir');
  steps.dirOpened = z.open(arg, file.name); flush(20);
  steps.dir = { items: z.items(arg), crumbs: z.crumbs(), links: z.links(), stubs: z.stubs() };
  const f = z.items(arg).find((i) => i.type === 'file');
  steps.fileOpened = z.open(arg, file.name + f.name); flush(20);
  steps.file = { items: z.items(arg), links: z.links(), stubs: z.stubs(), crumbs: z.crumbs(), rendered: z.rendered() };
  z.select(arg, file.name + f.name + '#render'); flush(10);
  steps.selected = { sel: z.selected(), panel: g.panelText(), hash: z.hash() };
  z.escape(); flush(10);
  steps.esc1 = z.crumbs();
  z.escape(); flush(10); z.escape(); flush(10);
  steps.esc3 = { open: z.open_cards(), crumbs: z.crumbs() };
  out = steps;
} else if (scenario === 'zoom-two') {
  // Two cards open down to files: the bus link joins the two functions.
  const z = g.zoom, [a, af, b, bf] = arg.split(',');
  z.open(a, af); flush(10);
  const closed = { links: z.links(), stubs: z.stubs() };
  z.open(b, bf); flush(10);
  // Both in view: the links run end to end.
  z.fitAll(); flush(10);
  const both = { links: z.links(), stubs: z.stubs() };
  // One far out of view: they leave through stubs, one per side.
  z.look(a, 6); flush(10);
  const away = { links: z.links(), stubs: z.stubs() };
  const i = away.stubs.findIndex((s) => s.unit === a && s.target.indexOf(b) === 0);
  const hover = i >= 0 ? z.hoverStub(i) : null;
  const panel = i >= 0 ? z.clickStub(i) : '';
  out = { closed, open: z.open_cards(), both, away, hover, panel };
} else if (scenario === 'zoom-cap') {
  // Stubs per side capped: the lightest share one "+N more" stub.
  const z = g.zoom, [unit, rel, cap] = arg.split(',');
  z.open(unit, rel); flush(10);
  const before = z.stubs();
  z.stubCap(+cap);
  out = { before, after: z.stubs() };
} else if (scenario === 'zoom-html') {
  // Everything drawn with a card open down to a file and a symbol selected.
  const z = g.zoom, [unit, rel, sym] = arg.split(',');
  z.open(unit, rel); flush(10);
  z.select(unit, rel + '#' + sym); flush(10);
  g.selectUnit(unit); flush(5);
  let html = '';
  elements.forEach((e) => { html += '\n' + e.innerHTML; });
  out = { html, items: z.items(unit), stubs: z.stubs() };
} else if (scenario === 'zoom-tree') {
  // The panel tree filtered, then a click on a match.
  const z = g.zoom, [unit, filter] = arg.split(',');
  g.selectUnit(unit); flush(5);
  const all = z.tree(unit, '');
  const rows = z.tree(unit, filter);
  const hit = rows.filter((r) => r.hit).pop();
  z.treeGo(unit, hit.id); flush(20);
  out = { all, rows, crumbs: z.crumbs(), sel: z.selected(), panel: g.panelText(), hash: z.hash() };
  // Back to the unit: the tree, its filter kept.
  out.back = z.panelBack(unit);
  out.backHash = z.hash();
} else if (scenario === 'zoom-clip') {
  // A card open at a folder: the boxes drawn and the link layer over them.
  const z = g.zoom, [unit, rel] = arg.split(',');
  if (rel) z.open(unit, rel); else z.expand(unit);
  flush(40);
  out = { boxes: z.boxes(), links: z.links(), svg: elements.get('apps-zedges').innerHTML };
} else if (scenario === 'zoom-search') {
  // Queries in the search box, then the first result of the last one picked.
  const qs = arg.split(';');
  out = { results: qs.map((q) => g.search(q)) };
  g.choose(0); flush(40);
  const z = g.zoom;
  out.crumbs = z.crumbs(); out.sel = z.selected(); out.panel = g.panelText(); out.level = g.level(); out.hash = z.hash();
} else if (scenario === 'zoom-wheel') {
  // A trackpad's scroll into a folder: wheel events a frame apart, past
  // the point the folder opens; then zoomed in far, and Fit pressed.
  const z = g.zoom, [unit, name] = arg.split(',');
  z.expand(unit); flush(40);
  const wheel = elements.get('apps').listeners.wheel[0];
  const ks = [];
  for (let i = 0; i < 45; i++) {
    const b = z.itemScreen(unit, name) || z.focusScreen(unit);
    wheel({ deltaY: -40, deltaMode: 0, clientX: b.x + b.w / 2, clientY: b.y + b.h / 2, preventDefault() {} });
    flush(1);
    ks.push(z.cam().k);
  }
  flush(40);
  out = { ks, crumbs: z.crumbs(), settled: z.cam(), level: z.focusScreen(unit) };
  z.look(unit, 3); flush(5);
  elements.get('fit').listeners.click.forEach((fn) => fn({}));
  flush(40);
  out.fit = z.cam(); out.fitLevel = z.focusScreen(unit);
} else if (scenario === 'zoom-panel-fit') {
  // A folder opens and, as the camera moves, a panel takes the stage's right side.
  const z = g.zoom, [unit, rel] = arg.split(',');
  z.expand(unit); flush(40);
  z.open(unit, rel); flush(2);
  const stage = elements.get('stage');
  stage.rect = { left: 0, top: 0, right: 1028, bottom: 900, width: 1028, height: 900 };
  (windowListeners.resize || []).forEach((fn) => fn({}));
  flush(40);
  out = { moved: z.cam(), level: z.focusScreen(unit), refit: z.fit(), refitLevel: z.focusScreen(unit) };
  // A phone: the panel is a sheet over the stage's lower part.
  stage.rect = null;
  g.selectUnit(unit); flush(5);
  const panel = elements.get('panel');
  panel.hidden = false;
  panel.rect = { left: 0, top: 500, right: 1400, bottom: 900, width: 1400, height: 400 };
  z.fit(); flush(5);
  out.sheet = z.focusScreen(unit);
} else if (scenario === 'zoom-stubs-ui') {
  // Stubs with the open level's top edge just above the view (`top`) or
  // its foot just above the status line (`bottom`), in screen px.
  const z = g.zoom, [unit, rel, k, top, bottom] = arg.split(',');
  z.open(unit, rel); flush(40);
  z.look(unit, +k, top ? +top : null, bottom ? +bottom : null);
  out = { stubs: z.stubs(), cam: z.cam() };
} else if (scenario === 'zoom-others') {
  // A folder whose files without symbols share one box; listed, then one picked.
  const z = g.zoom, [unit, rel, deeper] = arg.split(',');
  z.open(unit, rel); flush(40);
  const before = z.items(unit), boxes = z.boxes();
  const others = boxes.find((b) => b.type === 'others');
  const id = others ? z.itemScreen(unit, others.name).id : null;
  if (id) { z.activate(unit, id); flush(20); }
  const listed = z.items(unit), panel = g.panelText(), hash = z.hash();
  if (id) { z.activate(unit, id); flush(20); }
  out = { before, listed, panel, hash, folded: z.items(unit), links: z.links() };
  // Deeper in, the box is a name tag: activating it goes back up to its level, listed.
  if (deeper && id) {
    z.open(unit, deeper); flush(40);
    const tag = z.items(unit).find((i) => i.type === 'others');
    z.activate(unit, id); flush(40);
    out.tag = tag; out.backUp = { crumbs: z.crumbs(), items: z.items(unit), sel: z.selected() };
  }
} else if (scenario === 'zoom-hash') {
  // The page opened at a URL fragment (HASH in the environment).
  let html = '';
  elements.forEach((e) => { html += '\n' + e.innerHTML; });
  out = { open: g.zoom.open_cards(), crumbs: g.zoom.crumbs(), sel: g.zoom.selected(), panel: g.panelText(), html };
} else {
  throw new Error('unknown scenario ' + scenario);
}
process.stdout.write(JSON.stringify(out));
