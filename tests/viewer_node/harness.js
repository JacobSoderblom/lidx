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
  e.innerHTML = '';
  e.style = {};
  e.dataset = {};
  e.classList = { add() {}, remove() {}, toggle() {}, contains() { return false; } };
  e.addEventListener = () => {};
  e.setAttribute = () => {};
  e.getAttribute = () => null;
  e.removeAttribute = () => {};
  e.querySelector = () => null;
  e.querySelectorAll = () => [];
  e.getBoundingClientRect = () => ({ left: 0, top: 0, width: 1400, height: 900 });
  e.getContext = () => ctx2d();
  e.offsetParent = null;
  e.focus = () => {};
  elements.set(id, e);
  return e;
}
const frames = [];
const sandbox = {
  console,
  performance,
  setTimeout: (fn) => { frames.push(fn); return 0; },
  clearTimeout: () => {},
  requestAnimationFrame: (fn) => { frames.push(() => fn(performance.now())); return frames.length; },
  cancelAnimationFrame: () => {},
  getComputedStyle: () => ({ getPropertyValue: () => '#808080' }),
  matchMedia: () => ({ matches: false, addEventListener() {} }),
  addEventListener: () => {},
  devicePixelRatio: 1,
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
} else if (scenario === 'colour') {
  out = { colorBy: g.colorBy(), level: g.level() };
} else {
  throw new Error('unknown scenario ' + scenario);
}
process.stdout.write(JSON.stringify(out));
