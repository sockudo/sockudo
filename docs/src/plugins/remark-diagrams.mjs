// Renders ```mermaid fences at build time where we can do better than Mermaid:
// sequence diagrams become animated HTML; everything else becomes a placeholder
// that the client renders with Mermaid (see src/scripts/site.ts).
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { renderFlowchart } from './flowchart.mjs';

const lucideDir = join(dirname(fileURLToPath(import.meta.resolve('@lucide/astro'))), 'icons');
const iconCache = new Map();

function icon(name) {
  if (iconCache.has(name)) return iconCache.get(name);
  const src = readFileSync(join(lucideDir, `${name}.ts`), 'utf8');
  const data = JSON.parse(src.match(/iconData: LucideIconData = (\{.*\});/)[1]);
  const inner = data.node
    .map(([tag, attrs]) => `<${tag} ${Object.entries(attrs).map(([k, v]) => `${k}="${v}"`).join(' ')}/>`)
    .join('');
  const svg = `<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${inner}</svg>`;
  iconCache.set(name, svg);
  return svg;
}

const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');

function actorIcon(label) {
  const l = label.toLowerCase();
  if (/sockudo/.test(l)) return '<img src="/sockudo-logo/sockudo-icon-color.svg" alt="" width="18" height="18"/>';
  if (/history|store|database|cache/.test(l)) return icon('database');
  if (/provider|push|apns|fcm/.test(l)) return icon('bell');
  if (/agent/.test(l)) return icon('bot');
  if (/tool/.test(l)) return icon('wrench');
  if (/backend|api|server/.test(l)) return icon('server');
  if (/web|browser/.test(l)) return icon('monitor');
  if (/client|app|device|mobile/.test(l)) return icon('smartphone');
  return icon('user');
}

const ARROWS = [
  ['-->>', 'dashed', 'arrow'],
  ['--x', 'dashed', 'cross'],
  ['--)', 'dashed', 'open'],
  ['-->', 'dashed', 'none'],
  ['->>', 'solid', 'arrow'],
  ['-x', 'solid', 'cross'],
  ['-)', 'solid', 'open'],
  ['->', 'solid', 'none'],
];
const arrowRe = new RegExp(
  `^([A-Za-z0-9_]+)\\s*(${ARROWS.map(([a]) => a.replace(/[-)>]/g, (c) => `\\${c}`)).join('|')})\\s*([+-]?)([A-Za-z0-9_]+)\\s*:\\s*(.*)$`,
);

export function parseSequence(src) {
  const actors = [];
  const byId = new Map();
  const ensure = (id, label) => {
    if (!byId.has(id)) {
      const a = { id, label: label ?? id };
      byId.set(id, a);
      actors.push(a);
    } else if (label) byId.get(id).label = label;
    return byId.get(id);
  };
  const steps = [];
  for (const raw of src.split('\n').slice(1)) {
    const line = raw.trim();
    if (!line || line.startsWith('%%') || line === 'autonumber') continue;
    let m;
    if ((m = line.match(/^(participant|actor)\s+([A-Za-z0-9_]+)(?:\s+as\s+(.+))?$/))) {
      ensure(m[2], m[3]?.trim());
    } else if ((m = line.match(arrowRe))) {
      const [, from, op, , to, text] = m;
      const [, style, head] = ARROWS.find(([a]) => a === op);
      ensure(from);
      ensure(to);
      steps.push({ type: 'msg', from, to, style, head, text });
    } else if ((m = line.match(/^Note\s+(over|left of|right of)\s+([A-Za-z0-9_]+)(?:\s*,\s*([A-Za-z0-9_]+))?\s*:\s*(.*)$/i))) {
      ensure(m[2]);
      if (m[3]) ensure(m[3]);
      steps.push({ type: 'note', pos: m[1].toLowerCase(), a: m[2], b: m[3] ?? m[2], text: m[4] });
    } else {
      throw new Error(`Unsupported sequence diagram line: "${line}"`);
    }
  }
  return { actors, steps };
}

export function renderSequence(src) {
  const { actors, steps } = parseSequence(src);
  const n = actors.length;
  const col = new Map(actors.map((a, i) => [a.id, i]));
  let k = 0;
  const msgCount = steps.filter((s) => s.type === 'msg').length;
  const rows = steps
    .map((s) => {
      if (s.type === 'note') {
        const a = col.get(s.a);
        const b = col.get(s.b);
        return `<div class="seq-note" style="--a:${Math.min(a, b)};--b:${Math.max(a, b)}"><span>${esc(s.text)}</span></div>`;
      }
      const a = col.get(s.from);
      const b = col.get(s.to);
      const i = k++;
      const dir = a === b ? 'self' : b > a ? 'r' : 'l';
      const lo = Math.min(a, b);
      const span = Math.max(1, Math.abs(b - a));
      return `<div class="seq-msg dir-${dir} ${s.style} head-${s.head}" style="--a:${lo};--span:${span};--k:${i}">
<span class="seq-label"><b>${i + 1}</b>${esc(s.text)}</span>
<span class="seq-line"><i class="seq-dot"></i></span>
</div>`;
    })
    .join('');
  const heads = actors
    .map((a, i) => `<div class="seq-actor" style="--c:${i}"><span class="seq-ico">${actorIcon(a.label)}</span><span>${esc(a.label)}</span></div>`)
    .join('');
  const lifelines = actors.map((_, i) => `<i style="--c:${i}"></i>`).join('');
  return `<figure class="seq not-prose" data-reveal style="--n:${n};--T:${(msgCount * 0.75 + 1.6).toFixed(2)}s" aria-label="Sequence diagram: ${esc(actors.map((a) => a.label).join(', '))}">
<div class="seq-scroll" data-lenis-prevent-horizontal><div class="seq-grid">
<div class="seq-heads">${heads}</div>
<div class="seq-rows"><div class="seq-lifelines" aria-hidden="true">${lifelines}</div>${rows}</div>
</div></div>
<pre class="sr-only">${esc(src)}</pre>
</figure>`;
}

const mermaidPlaceholder = (src) =>
  `<figure class="mermaid-figure not-prose" data-reveal data-diagram><div class="mermaid-stage" data-mermaid><pre class="mermaid-src">${esc(src)}</pre></div><div class="mermaid-tools"><button type="button" class="mermaid-zoom" data-diagram-zoom aria-label="Open diagram fullscreen">${icon('maximize-2')}<span>Expand</span></button></div></figure>`;

async function renderDiagram(src) {
  if (/^sequenceDiagram\b/.test(src)) return renderSequence(src);
  if (/^(flowchart|graph)\s/.test(src)) {
    try {
      return await renderFlowchart(src, icon);
    } catch (err) {
      // Syntax beyond our subset: let Mermaid render it in the browser.
      if (process.env.DIAGRAMS_STRICT) throw err;
    }
  }
  return mermaidPlaceholder(src);
}

export function remarkDiagrams() {
  return async (tree) => {
    const jobs = [];
    const visit = (node) => {
      for (const child of node.children ?? []) {
        if (child.type === 'code' && child.lang === 'mermaid') {
          const src = child.value.trim();
          jobs.push(
            renderDiagram(src).then((html) => {
              child.type = 'html';
              child.value = html;
              delete child.lang;
              delete child.meta;
            }),
          );
        } else visit(child);
      }
    };
    visit(tree);
    await Promise.all(jobs);
  };
}
