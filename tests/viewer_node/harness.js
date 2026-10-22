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
// Whole-repo pages open on the landing page; the scenarios about Explore
// (the app sheet and its zoom) start there, one click away, without a
// history entry.
const landing = g.level();
if (!scenario.startsWith('glance') && landing === 'glance') { g.setLevel('app', true); flush(50); }
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
} else if (scenario === 'zoom-labels') {
  // Every label drawn with a card open at `rel`, optionally a symbol
  // selected, the stage narrowed to `stageW` px (a panel beside it) and the
  // camera at scale k with the open level's top (or foot) at a screen y,
  // or its left edge at a screen x.
  const z = g.zoom, [unit, rel, sel, stageW, k, top, bottom, left] = arg.split(',');
  if (stageW) elements.get('stage').rect = { left: 0, top: 0, right: +stageW, bottom: 900, width: +stageW, height: 900 };
  z.open(unit, rel); flush(40);
  if (sel) { z.select(unit, sel); flush(20); }
  if (k) z.look(unit, +k, top ? +top : null, bottom ? +bottom : null, left ? +left : null);
  out = { labels: z.labels(), cam: z.cam(), level: z.focusScreen(unit) };
} else if (scenario === 'zoom-hash') {
  // The page opened at a URL fragment (HASH in the environment).
  let html = '';
  elements.forEach((e) => { html += '\n' + e.innerHTML; });
  out = { open: g.zoom.open_cards(), crumbs: g.zoom.crumbs(), sel: g.zoom.selected(), panel: g.panelText(), html };
} else if (scenario === 'glance-landing') {
  // The landing page as it opens: the map, its links, the side column.
  const gl = g.glance;
  out = { level: g.level(), cards: gl.cards(), lanes: gl.lanes(), links: gl.links(), strip: gl.strip(), side: gl.side(), tours: gl.tours(), html: gl.html() };
} else if (scenario === 'glance-flow') {
  // Into a flow, stepped with the arrow keys, then out with Escape.
  const gl = g.glance, steps = [];
  gl.openFlow(+arg); flush(5);
  steps.push(gl.state());
  gl.key('ArrowRight'); flush(5); steps.push(gl.state());
  gl.key('ArrowRight'); flush(5); steps.push(gl.state());
  const side = gl.side(), html = gl.html();
  gl.key('ArrowLeft'); flush(5); steps.push(gl.state());
  for (let i = 0; i < 9; i++) gl.key('ArrowRight');
  flush(5); steps.push(gl.state());
  gl.key('Escape'); flush(5); steps.push(gl.state());
  out = { steps, side, html };
} else if (scenario === 'glance-seq') {
  const gl = g.glance;
  gl.openFlow(+arg); flush(5);
  gl.key('ArrowRight'); flush(5);
  gl.toggleSeq(); flush(5);
  const on = { state: gl.state(), lanes: gl.seqLanes(), html: gl.html().seq };
  gl.toggleSeq(); flush(5);
  out = { on, off: { state: gl.state(), lanes: gl.seqLanes(), cards: gl.cards().length } };
} else if (scenario === 'glance-tour') {
  const gl = g.glance, seen = [];
  gl.tourStart(); flush(5);
  seen.push({ state: gl.state(), tours: gl.tours(), side: gl.side() });
  gl.tourNext(); flush(5);
  seen.push({ state: gl.state(), tours: gl.tours(), side: gl.side() });
  gl.key('ArrowRight'); flush(5);
  seen.push({ state: gl.state(), tours: gl.tours(), side: gl.side() });
  gl.tourBack(); flush(5);
  seen.push({ state: gl.state() });
  gl.key('Escape'); flush(5);
  seen.push({ state: gl.state(), tours: gl.tours() });
  out = { seen, count: gl.data().tour.length };
} else if (scenario === 'glance-explore') {
  const gl = g.glance;
  const before = g.level();
  gl.explore(); flush(20);
  out = { before, after: g.level(), open: g.zoom.open_cards() };
  g.setLevel('glance'); flush(5);
  out.back = g.level();
} else if (scenario === 'glance-dblclick') {
  const gl = g.glance;
  const ok = gl.dblclick(arg); flush(40);
  out = { ok, level: g.level(), open: g.zoom.open_cards(), crumbs: g.zoom.crumbs() };
} else if (scenario === 'glance-select') {
  // A card, then a line, then a question: what stays lit and what the side says.
  const gl = g.glance;
  gl.select(arg); flush(5);
  const card = { state: gl.state(), side: gl.side() };
  gl.selectLink(0); flush(5);
  const link = { state: gl.state(), side: gl.side() };
  gl.question(0); flush(5);
  const q = { state: gl.state(), side: gl.side() };
  gl.key('Escape'); flush(5);
  out = { card, link, q, cleared: gl.state() };
} else if (scenario === 'glance-a11y') {
  // What a screen reader and the keyboard meet: the cards' roles and order,
  // what the live region says after each action, the legend.
  const gl = g.glance;
  const live = () => element('live').textContent;
  const html0 = gl.html();
  gl.select(arg); flush(5);
  const card = { live: live(), html: gl.html().cards, state: gl.state() };
  gl.selectLink(0); flush(5);
  const link = { live: live(), state: gl.state() };
  gl.question(0); flush(5);
  const question = { live: live() };
  gl.key('Escape'); flush(5);
  const closed = { key: gl.html().key, hidden: gl.html().keyHidden, head: gl.html().head, state: gl.state() };
  gl.toggleKey(); flush(5);
  const open = { key: gl.html().key, hidden: gl.html().keyHidden, head: gl.html().head, state: gl.state() };
  gl.toggleKey(); flush(5);
  const shut = { key: gl.html().key, hidden: gl.html().keyHidden, state: gl.state() };
  out = { cards: gl.cards(), lanes: gl.lanes(), order: html0.cards, card, link, question, closed, open, shut };
} else if (scenario === 'apps-a11y') {
  // The sheet's cards as the accessibility tree sees them, closed and open.
  const closed = element('apps-cards').innerHTML;
  const apps = g.apps();
  g.openUnit(arg); flush(40);
  out = { closed, apps, open: element('apps-cards').innerHTML, openApps: g.apps() };
} else if (scenario === 'glance-curves') {
  // The landing page's curves and scale, then those of each flow.
  const gl = g.glance, flows = [];
  const landing = { cam: gl.camera(), cards: gl.cards(), curves: gl.curves() };
  gl.data().flows.forEach((_, i) => { gl.openFlow(i); flush(2); flows.push({ cards: gl.cards(), curves: gl.curves() }); gl.key('Escape'); flush(2); });
  out = { landing, flows };
} else if (scenario === 'glance-deploy') {
  // "Where it runs" on and off: the lanes, the frames, the side column.
  const gl = g.glance;
  const before = { lanes: gl.lanes(), head: gl.html().head };
  gl.toggleDeploy(); flush(5);
  const on = { state: gl.state(), lanes: gl.lanes(), cards: gl.cards(), curves: gl.curves(), side: gl.side(), html: gl.html().cards };
  gl.toggleDeploy(); flush(5);
  out = { before, on, off: { state: gl.state(), lanes: gl.lanes() } };
} else if (scenario === 'glance-showall') {
  // Every resource past the cap, shown and hidden again.
  const gl = g.glance;
  const before = { lanes: gl.lanes(), cards: gl.cards().length };
  gl.showAll(); flush(5);
  out = { before, after: { lanes: gl.lanes(), cards: gl.cards().length } };
} else if (scenario === 'glance-hover') {
  // Lines with the pointer on each named card in turn (`arg`: names joined
  // by commas, an empty name for off every card), then with one selected.
  const gl = g.glance, steps = [];
  const look = () => ({ links: gl.links().map((l) => l.from + '>' + l.to), cards: gl.cards(), curves: gl.curves() });
  steps.push(look());
  arg.split(',').forEach((name) => { gl.hover(name === '' ? null : name); flush(2); steps.push(look()); });
  out = { steps, html: gl.html().cards };
} else if (scenario === 'glance-area') {
  // The landing page on a map area of `arg` ("width,height").
  const [w, h] = arg.split(',').map(Number);
  element('g-map').rect = { left: 0, top: 0, right: w, bottom: h, width: w, height: h };
  const gl = g.glance;
  gl.relayout(); flush(5);
  out = { cam: gl.camera(), cards: gl.cards(), lanes: gl.lanes(), links: gl.links(), curves: gl.curves(), html: gl.html().cards };
} else if (scenario === 'glance-area-hover') {
  // The landing page on a map area of "width,height", at rest and then
  // with the pointer on each card named after it ("w,h,name,name").
  const [w, h, ...names] = arg.split(',');
  element('g-map').rect = { left: 0, top: 0, right: +w, bottom: +h, width: +w, height: +h };
  const gl = g.glance, steps = [];
  gl.relayout(); flush(5);
  const look = () => ({ cam: gl.camera(), cards: gl.cards(), lanes: gl.lanes(), links: gl.links(), curves: gl.curves(), html: gl.html().cards });
  steps.push(look());
  names.forEach((name) => { gl.hover(name === '' ? null : name); flush(2); steps.push(look()); });
  out = { steps };
} else if (scenario === 'glance-route') {
  // A route between two boxes laid out by the test: `arg` is
  // {boxes, size, from, to} as JSON.
  const a = JSON.parse(arg);
  out = g.glance.route(a.boxes, a.size, a.from, a.to);
} else if (scenario === 'glance-everything') {
  // Every view of the landing page rendered once: each card, line,
  // question, flow step (on the map and as a sequence) and tour step. The
  // markup of all of them, to check that data stays text.
  const gl = g.glance, d = gl.data(), parts = [];
  const grab = () => { const h = gl.html(); parts.push(Object.keys(h).map((k) => h[k]).join('\n')); };
  grab();
  d.nodes.forEach((n) => { gl.select(n.name); flush(2); grab(); });
  d.links.forEach((_, i) => { gl.selectLink(i); flush(2); grab(); });
  d.questions.forEach((_, i) => { gl.question(i); flush(2); grab(); });
  gl.clear(); flush(2);
  d.flows.forEach((f, i) => {
    gl.openFlow(i); flush(2); grab();
    f.steps.forEach(() => { gl.key('ArrowRight'); flush(2); grab(); });
    gl.toggleSeq(); flush(2); grab(); gl.toggleSeq(); flush(2);
    gl.key('Escape'); flush(2);
  });
  if (d.tour.length) {
    gl.tourStart(); flush(2); grab();
    d.tour.forEach(() => { gl.tourNext(); flush(2); grab(); });
  }
  // The pointer on each card, then "Where it runs" with each card selected.
  gl.clear(); flush(2);
  d.nodes.forEach((n) => { gl.hover(n.name); flush(2); grab(); });
  gl.hover(null); flush(2);
  gl.toggleDeploy(); flush(2); grab();
  d.nodes.forEach((n) => { gl.select(n.name); flush(2); grab(); });
  gl.toggleDeploy(); flush(2);
  out = { html: parts.join('\n'), flows: d.flows.length, tour: d.tour.length, links: d.links.length };
} else {
  throw new Error('unknown scenario ' + scenario);
}
process.stdout.write(JSON.stringify(out));
