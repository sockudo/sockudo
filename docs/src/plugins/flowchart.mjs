// Build-time renderer for Mermaid flowcharts: parses the common flowchart subset,
// lays it out with ELK (orthogonal routing, edge labels), and emits a themeable,
// animated SVG. Throws on syntax it doesn't understand so callers can fall back.
import ELK from 'elkjs/lib/elk.bundled.js';

const elk = new ELK();
let seq = 0;

/* ── Parsing ─────────────────────────────────────────────────────────────── */

const SHAPES = [
  ['([', '])', 'stadium'],
  ['[(', ')]', 'cylinder'],
  ['((', '))', 'circle'],
  ['[[', ']]', 'subroutine'],
  ['{{', '}}', 'hexagon'],
  ['[/', '/]', 'box'],
  ['[\\', '\\]', 'box'],
  ['[', ']', 'box'],
  ['(', ')', 'round'],
  ['{', '}', 'diamond'],
  ['>', ']', 'box'],
];

const EDGE_PLAIN = /^(<?)(-{2,}>|-{3,}|-\.+->|-\.+-|={2,}>|={3,}|--[xo])(?:\|([^|]*)\|)?/;
const EDGE_TEXT = /^(<?)(--|-\.|==)\s*([^|>\n]+?)\s*(-{2,}>|-{3,}|\.+->|\.+-|={2,}>|={3,})/;

const clean = (t) =>
  t
    .trim()
    .replace(/^"(.*)"$/s, '$1')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/&quot;/g, '"')
    .replace(/&amp;/g, '&')
    .replace(/#quot;/g, '"');

export function parseFlowchart(src) {
  const lines = src.split('\n');
  const head = lines[0].trim().match(/^(?:flowchart|graph)\s+(LR|RL|TB|TD|BT)\s*;?$/);
  if (!head) throw new Error('unsupported flowchart header');
  const dir = head[1] === 'TD' ? 'TB' : head[1];
  const nodes = new Map();
  const edges = [];
  const groups = [];
  const stack = [];

  // Subgraph ids may be used as edge endpoints (Mermaid allows `A --> MyGroup`).
  const groupIds = new Set([...src.matchAll(/^\s*subgraph\s+([A-Za-z0-9_]+)/gm)].map((m) => m[1]));
  const touch = (id, text, shape) => {
    if (groupIds.has(id) && text === undefined) return { id: `grp_${id}` };
    let n = nodes.get(id);
    if (!n) {
      n = { id, text: id, shape: 'box', group: stack.at(-1) ?? null };
      nodes.set(id, n);
      stack.at(-1)?.children.push(id);
    }
    if (text !== undefined) {
      n.text = clean(text);
      n.shape = shape;
    }
    return n;
  };

  const statements = lines
    .slice(1)
    .flatMap((l) => l.split(';'))
    .map((l) => l.trim())
    .filter((l) => l && !l.startsWith('%%'));

  for (const st of statements) {
    if (/^(classDef|class|style|linkStyle|click|direction)\b/.test(st)) continue;
    let m;
    if ((m = st.match(/^subgraph\s+([A-Za-z0-9_]+)?\s*(?:\[(.+)\])?\s*(.*)$/))) {
      const id = m[1] ?? `sg${groups.length}`;
      const title = clean(m[2] ?? (m[3] || m[1] || ''));
      const g = { id: `grp_${id}`, title, children: [], parent: stack.at(-1) ?? null };
      groups.push(g);
      stack.push(g);
      continue;
    }
    if (st === 'end') {
      if (!stack.pop()) throw new Error('unbalanced end');
      continue;
    }

    let pos = 0;
    const rest = () => st.slice(pos);
    const ws = () => {
      while (st[pos] === ' ' || st[pos] === '\t') pos++;
    };
    const node = () => {
      ws();
      const idm = rest().match(/^[A-Za-z0-9_]+/);
      if (!idm) throw new Error(`expected node id in "${st}"`);
      const id = idm[0];
      pos += id.length;
      for (const [open, close, shape] of SHAPES) {
        if (rest().startsWith(open)) {
          const end = st.indexOf(close, pos + open.length);
          if (end < 0) throw new Error(`unclosed shape in "${st}"`);
          const text = st.slice(pos + open.length, end);
          pos = end + close.length;
          return touch(id, text, shape).id;
        }
      }
      return touch(id).id;
    };
    const group = () => {
      const ids = [node()];
      ws();
      while (st[pos] === '&') {
        pos++;
        ids.push(node());
        ws();
      }
      return ids;
    };

    let left = group();
    for (;;) {
      ws();
      if (pos >= st.length) break;
      let em = rest().match(EDGE_PLAIN);
      let label;
      let op;
      let back;
      if (em) {
        [, back, op, label] = em;
      } else if ((em = rest().match(EDGE_TEXT))) {
        back = em[1];
        label = em[3];
        op = em[2] + em[4];
      } else throw new Error(`unsupported syntax near "${rest()}"`);
      pos += em[0].length;
      const right = group();
      const style = op.includes('.') ? 'dotted' : op.includes('=') ? 'thick' : 'solid';
      const head = op.endsWith('>') ? 'arrow' : /x$/.test(op) ? 'cross' : /o$/.test(op) ? 'circle' : 'none';
      for (const a of left)
        for (const b of right)
          edges.push({ id: `e${edges.length}`, from: a, to: b, label: label ? clean(label) : '', style, head, both: back === '<' });
      left = right;
    }
  }
  if (stack.length) throw new Error('unclosed subgraph');
  if (!nodes.size) throw new Error('empty flowchart');
  return { dir, nodes: [...nodes.values()], edges, groups };
}

/* ── Measuring (approximate Geist metrics; no DOM at build time) ─────────── */

function charW(c) {
  if (/[iljtf.,:;'|!I]/.test(c)) return 0.3;
  if (/[mwMW]/.test(c)) return 0.86;
  if (/[A-Z]/.test(c)) return 0.66;
  if (/[0-9]/.test(c)) return 0.58;
  if (c === ' ') return 0.28;
  return 0.53;
}
const textW = (s, size) => [...s].reduce((w, c) => w + charW(c) * size, 0);

function wrap(text, max = 22) {
  return text.split('\n').flatMap((para) => {
    const out = [];
    let line = '';
    for (const word of para.split(/\s+/)) {
      if (line && (line + ' ' + word).length > max) {
        out.push(line);
        line = word;
      } else line = line ? `${line} ${word}` : word;
    }
    if (line) out.push(line);
    return out;
  });
}

/* ── Icons ──────────────────────────────────────────────────────────────── */

export function iconFor(label, shape) {
  const l = label.toLowerCase();
  if (shape === 'diamond') return null;
  if (/sockudo/.test(l) && !/push api|node/.test(l)) return 'logo';
  if (/load balancer|ingress|lb\b/.test(l)) return 'split';
  if (/redis|nats|kafka|rabbit|pulsar|iggy|adapter|bus|pub\/sub/.test(l)) return 'network';
  if (/queue/.test(l)) return 'inbox';
  if (/history|store|database|cache|postgres|mysql|dynamo/.test(l)) return 'database';
  if (/webhook/.test(l)) return 'webhook';
  if (/fcm|apns|web push|hms|wns|push|provider|notification/.test(l)) return 'bell';
  if (/agent|llm|model/.test(l)) return 'bot';
  if (/tool/.test(l)) return 'wrench';
  if (/metric|prometheus|grafana|observ/.test(l)) return 'activity';
  if (/auth|token|secret|sign/.test(l)) return 'key-round';
  if (/node/.test(l)) return 'server';
  if (/backend|server|api|worker/.test(l)) return 'server';
  if (/browser|web\b|dashboard/.test(l)) return 'monitor';
  if (/client|sdk|mobile|device|app\b|phone/.test(l)) return 'smartphone';
  if (/event|publish|trigger/.test(l)) return 'zap';
  if (/user|member|presence/.test(l)) return 'users';
  if (/v2|protocol|channel/.test(l)) return 'radio-tower';
  return null;
}

/* ── Layout + render ───────────────────────────────────────────────────── */

const FONT = 14;
const LINE = 19;
const ELABEL = 12;

function sizeNode(n) {
  const lines = wrap(n.text);
  const icon = iconFor(n.text, n.shape);
  const tw = Math.max(...lines.map((l) => textW(l, FONT)));
  let w = Math.max(n.shape === 'circle' ? 70 : 112, tw + 34 + (icon ? 30 : 0));
  let h = Math.max(46, lines.length * LINE + 26);
  if (n.shape === 'diamond') {
    w = Math.max(150, tw * 1.5 + 44);
    h = Math.max(84, lines.length * LINE + 56);
  } else if (n.shape === 'cylinder') h += 14;
  else if (n.shape === 'circle') w = h = Math.max(w, h);
  else if (n.shape === 'hexagon') w += 28;
  return { lines, icon, w: Math.round(w), h: Math.round(h) };
}

const ELK_DIR = { LR: 'RIGHT', RL: 'LEFT', TB: 'DOWN', BT: 'UP' };

async function layout(model, dir, extra = {}) {
  const meta = new Map(model.nodes.map((n) => [n.id, sizeNode(n)]));
  const elkNode = (n) => ({ id: n.id, width: meta.get(n.id).w, height: meta.get(n.id).h });
  const groupNode = (g) => ({
    id: g.id,
    layoutOptions: { 'elk.padding': '[top=40,left=18,bottom=18,right=18]' },
    children: [
      ...g.children.map((id) => elkNode(model.nodes.find((n) => n.id === id))),
      ...model.groups.filter((c) => c.parent === g).map(groupNode),
    ],
  });
  const graph = {
    id: 'root',
    layoutOptions: {
      'elk.algorithm': 'layered',
      'elk.direction': ELK_DIR[dir],
      'elk.edgeRouting': 'ORTHOGONAL',
      'elk.hierarchyHandling': 'INCLUDE_CHILDREN',
      'elk.layered.spacing.nodeNodeBetweenLayers': '64',
      'elk.layered.spacing.edgeNodeBetweenLayers': '22',
      'elk.layered.spacing.edgeEdgeBetweenLayers': '14',
      'elk.spacing.nodeNode': '30',
      'elk.spacing.edgeNode': '18',
      'elk.spacing.edgeEdge': '12',
      'elk.spacing.edgeLabel': '6',
      'elk.edgeLabels.placement': 'CENTER',
      'elk.layered.nodePlacement.strategy': 'BRANDES_KOEPF',
      'elk.layered.cycleBreaking.strategy': 'GREEDY_MODEL_ORDER',
      'elk.layered.considerModelOrder.strategy': 'NODES_AND_EDGES',
      'elk.padding': '[top=16,left=16,bottom=16,right=16]',
      ...extra,
    },
    children: [...model.nodes.filter((n) => !n.group).map(elkNode), ...model.groups.filter((g) => !g.parent).map(groupNode)],
    edges: model.edges.map((e) => ({
      id: e.id,
      sources: [e.from],
      targets: [e.to],
      labels: e.label ? [{ text: e.label, width: Math.ceil(textW(e.label, ELABEL) + 18), height: 22 }] : [],
    })),
  };
  const res = await elk.layout(graph);
  return { res, meta };
}

const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

function roundedPath(pts, r = 10) {
  let d = `M${pts[0].x},${pts[0].y}`;
  for (let i = 1; i < pts.length - 1; i++) {
    const p = pts[i - 1];
    const b = pts[i];
    const n = pts[i + 1];
    const d1 = Math.hypot(b.x - p.x, b.y - p.y);
    const d2 = Math.hypot(n.x - b.x, n.y - b.y);
    const rr = Math.min(r, d1 / 2, d2 / 2);
    const ax = b.x - ((b.x - p.x) / d1) * rr;
    const ay = b.y - ((b.y - p.y) / d1) * rr;
    const cx = b.x + ((n.x - b.x) / d2) * rr;
    const cy = b.y + ((n.y - b.y) / d2) * rr;
    d += ` L${ax.toFixed(1)},${ay.toFixed(1)} Q${b.x},${b.y} ${cx.toFixed(1)},${cy.toFixed(1)}`;
  }
  const last = pts.at(-1);
  return `${d} L${last.x},${last.y}`;
}

function shapeSvg(shape, w, h) {
  switch (shape) {
    case 'diamond':
      return `<polygon class="fc-shape" points="${w / 2},1 ${w - 1},${h / 2} ${w / 2},${h - 1} 1,${h / 2}"/>`;
    case 'cylinder': {
      const ry = 7;
      return `<path class="fc-shape" d="M1,${ry} A${w / 2 - 1},${ry} 0 0 1 ${w - 1},${ry} V${h - ry} A${w / 2 - 1},${ry} 0 0 1 1,${h - ry} Z"/><path class="fc-rim" d="M1,${ry} A${w / 2 - 1},${ry} 0 0 0 ${w - 1},${ry}"/>`;
    }
    case 'stadium':
    case 'round':
      return `<rect class="fc-shape" x="0.5" y="0.5" width="${w - 1}" height="${h - 1}" rx="${shape === 'stadium' ? h / 2 : 18}"/>`;
    case 'circle':
      return `<circle class="fc-shape" cx="${w / 2}" cy="${h / 2}" r="${w / 2 - 1}"/>`;
    case 'hexagon':
      return `<polygon class="fc-shape" points="14,1 ${w - 14},1 ${w - 1},${h / 2} ${w - 14},${h - 1} 14,${h - 1} 1,${h / 2}"/>`;
    case 'subroutine':
      return `<rect class="fc-shape" x="0.5" y="0.5" width="${w - 1}" height="${h - 1}" rx="12"/><path class="fc-rim" d="M10,1 V${h - 1} M${w - 10},1 V${h - 1}"/>`;
    default:
      return `<rect class="fc-shape" x="0.5" y="0.5" width="${w - 1}" height="${h - 1}" rx="13"/>`;
  }
}

export async function renderFlowchart(src, iconSvg) {
  const model = parseFlowchart(src);
  // Lay out in the authored direction and the perpendicular one, then keep whichever
  // reads best in a ~760px column: large text first, then not absurdly tall.
  const COLUMN = 760;
  const score = ({ res: r }) => {
    const scale = Math.min(1, COLUMN / r.width);
    return scale - Math.max(0, r.height * scale - 760) / 1600;
  };
  const authored = await layout(model, model.dir);
  const flipped = await layout(model, model.dir === 'LR' || model.dir === 'RL' ? 'TB' : 'LR');
  const candidates = [
    { l: authored, bias: 0.06 },
    { l: flipped, bias: 0 },
  ];
  let { res, meta } = candidates.reduce((best, c) => (score(c.l) + c.bias > score(best.l) + best.bias ? c : best)).l;
  const uid = `fc${++seq}`;
  const W = Math.ceil(res.width);
  const H = Math.ceil(res.height);

  const nodeOut = [];
  const groupOut = [];
  const abs = new Map();
  const walk = (n, ox, oy) => {
    for (const c of n.children ?? []) {
      const x = ox + c.x;
      const y = oy + c.y;
      abs.set(c.id, { x, y });
      if (c.id.startsWith('grp_')) {
        const g = model.groups.find((gg) => gg.id === c.id);
        groupOut.push(
          `<g class="fc-group"><rect x="${x}" y="${y}" width="${c.width}" height="${c.height}" rx="18"/><text x="${x + 16}" y="${y + 24}">${esc(g.title)}</text></g>`,
        );
        walk(c, x, y);
      } else nodeOut.push({ c, x, y });
    }
  };
  walk(res, 0, 0);

  const nodesSvg = nodeOut
    .map(({ c, x, y }, i) => {
      const n = model.nodes.find((nn) => nn.id === c.id);
      const m = meta.get(c.id);
      const w = c.width;
      const h = c.height;
      const icon = m.icon;
      // Icon and text are laid out as one block so the 8px gap between them stays exact
      // even when the estimated text width is a little off.
      const tw = Math.max(...m.lines.map((l) => textW(l, FONT)));
      const start = icon ? Math.max(12, (w - (22 + 8 + tw)) / 2) : 0;
      const textX = icon ? start + 30 : w / 2;
      const blockH = m.lines.length * LINE;
      const y0 = h / 2 - blockH / 2 + LINE * 0.72;
      const iconInner = !icon
        ? ''
        : icon === 'logo'
          ? `<image href="/sockudo-logo/sockudo-icon-color.svg" x="2.5" y="2.5" width="17" height="17" preserveAspectRatio="xMidYMid meet"/>`
          : `<g transform="translate(3,3)">${iconSvg(icon)}</g>`;
      const iconBlock = icon
        ? `<g class="fc-icon" transform="translate(${start.toFixed(1)},${(h / 2 - 11).toFixed(1)})"><rect class="fc-icon-bg" width="22" height="22" rx="7"/>${iconInner}</g>`
        : '';
      const anchor = icon ? 'start' : 'middle';
      const text = m.lines.map((l, k) => `<tspan x="${textX.toFixed(1)}" y="${(y0 + k * LINE).toFixed(1)}">${esc(l)}</tspan>`).join('');
      return `<g class="fc-node fc-${n.shape}" style="--i:${i}" transform="translate(${x},${y})">${shapeSvg(n.shape, w, h)}${iconBlock}<text class="fc-text" text-anchor="${anchor}">${text}</text></g>`;
    })
    .join('');

  const edgeById = new Map(model.edges.map((e) => [e.id, e]));
  const allEdges = [];
  const collect = (n) => {
    for (const e of n.edges ?? []) allEdges.push({ e, container: n.id });
    for (const c of n.children ?? []) collect(c);
  };
  collect(res);

  const edgesSvg = [];
  const labelsSvg = [];
  const packets = [];
  allEdges.forEach(({ e }, i) => {
    const def = edgeById.get(e.id);
    // ELK edge coordinates are relative to the edge's container
    const off = e.container && e.container !== 'root' ? abs.get(e.container) : null;
    const ox = off?.x ?? 0;
    const oy = off?.y ?? 0;
    for (const s of e.sections ?? []) {
      const pts = [s.startPoint, ...(s.bendPoints ?? []), s.endPoint].map((p) => ({ x: p.x + ox, y: p.y + oy }));
      const pid = `${uid}-p${i}`;
      const markEnd = def.head === 'arrow' ? ` marker-end="url(#${uid}-arrow)"` : def.head === 'cross' ? ` marker-end="url(#${uid}-cross)"` : def.head === 'circle' ? ` marker-end="url(#${uid}-dot)"` : '';
      const markStart = def.both ? ` marker-start="url(#${uid}-arrow-rev)"` : '';
      edgesSvg.push(`<path id="${pid}" class="fc-edge fc-${def.style}" d="${roundedPath(pts)}"${markEnd}${markStart}/>`);
      const len = pts.slice(1).reduce((acc, p, k) => acc + Math.hypot(p.x - pts[k].x, p.y - pts[k].y), 0);
      const dur = Math.max(1.2, Math.min(3.2, len / 140)).toFixed(2);
      const begin = ((i * 0.37) % 2.4).toFixed(2);
      packets.push(
        `<circle class="fc-packet" r="3.6"><animateMotion dur="${dur}s" begin="${begin}s" repeatCount="indefinite"${def.both ? '' : ''}><mpath href="#${pid}"/></animateMotion></circle>`,
      );
      if (def.both)
        packets.push(
          `<circle class="fc-packet fc-packet-alt" r="3.2"><animateMotion dur="${dur}s" begin="${(Number(begin) + Number(dur) / 2).toFixed(2)}s" repeatCount="indefinite" keyPoints="1;0" keyTimes="0;1" calcMode="linear"><mpath href="#${pid}"/></animateMotion></circle>`,
        );
    }
    for (const l of e.labels ?? []) {
      const x = l.x + ox;
      const y = l.y + oy;
      labelsSvg.push(
        `<g class="fc-elabel"><rect x="${x}" y="${y}" width="${l.width}" height="${l.height}" rx="${l.height / 2}"/><text x="${x + l.width / 2}" y="${y + l.height / 2 + 4}" text-anchor="middle">${esc(l.text)}</text></g>`,
      );
    }
  });

  const defs = `<defs>
<linearGradient id="${uid}-g-node" x1="0" y1="0" x2="0" y2="1"><stop class="s-node-a" offset="0"/><stop class="s-node-b" offset="1"/></linearGradient>
<linearGradient id="${uid}-g-db" x1="0" y1="0" x2="0" y2="1"><stop class="s-db-a" offset="0"/><stop class="s-db-b" offset="1"/></linearGradient>
<linearGradient id="${uid}-g-dec" x1="0" y1="0" x2="1" y2="1"><stop class="s-dec-a" offset="0"/><stop class="s-dec-b" offset="1"/></linearGradient>
<marker id="${uid}-arrow" viewBox="0 0 12 12" refX="10.5" refY="6" markerWidth="9" markerHeight="9" orient="auto"><path class="fc-head" d="M1,1.5 L10.5,6 L1,10.5 L3.4,6 Z"/></marker>
<marker id="${uid}-arrow-rev" viewBox="0 0 12 12" refX="1.5" refY="6" markerWidth="9" markerHeight="9" orient="auto"><path class="fc-head" d="M11,1.5 L1.5,6 L11,10.5 L8.6,6 Z"/></marker>
<marker id="${uid}-cross" viewBox="0 0 12 12" refX="6" refY="6" markerWidth="10" markerHeight="10" orient="auto"><path class="fc-head-x" d="M2,2 L10,10 M10,2 L2,10"/></marker>
<marker id="${uid}-dot" viewBox="0 0 12 12" refX="6" refY="6" markerWidth="8" markerHeight="8"><circle class="fc-head" cx="6" cy="6" r="4"/></marker>
</defs>`;

  const css = `#${uid} .fc-shape{fill:url(#${uid}-g-node)}#${uid} .fc-cylinder .fc-shape{fill:url(#${uid}-g-db)}#${uid} .fc-diamond .fc-shape{fill:url(#${uid}-g-dec)}`;
  const minW = Math.min(W, Math.max(320, Math.round(W * 0.66)));

  return `<figure class="flow not-prose" data-reveal data-diagram>
<div class="flow-scroll" data-lenis-prevent-horizontal>
<svg id="${uid}" class="flow-svg" viewBox="0 0 ${W} ${H}" width="${W}" style="--minw:${minW}px;max-width:${W}px" role="img" aria-label="Flowchart: ${esc(model.nodes.map((n) => n.text.replace(/\n/g, ' ')).join(', '))}">
<style>${css}</style>${defs}
<g class="fc-groups">${groupOut.join('')}</g>
<g class="fc-edges">${edgesSvg.join('')}</g>
<g class="fc-packets">${packets.join('')}</g>
<g class="fc-nodes">${nodesSvg}</g>
<g class="fc-elabels">${labelsSvg.join('')}</g>
</svg>
</div>
<div class="mermaid-tools"><button type="button" class="mermaid-zoom" data-diagram-zoom aria-label="Open diagram fullscreen">${iconSvg('maximize-2')}<span>Expand</span></button></div>
<pre class="sr-only">${esc(src)}</pre>
</figure>`;
}
