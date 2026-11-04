import Lenis from 'lenis';

const reduceMotion = matchMedia('(prefers-reduced-motion: reduce)').matches;
let lenis: Lenis | undefined;
let cleanups: (() => void)[] = [];

function onPage(fn: () => void | (() => void)) {
  const run = () => {
    const c = fn();
    if (c) cleanups.push(c);
  };
  document.addEventListener('astro:page-load', run);
}

document.addEventListener('astro:before-swap', () => {
  cleanups.forEach((c) => c());
  cleanups = [];
  document.body.removeAttribute('data-nav-open');
});

// Keep the chosen theme across client-side navigations.
document.addEventListener('astro:after-swap', () => {
  document.documentElement.classList.add('js');
  applyTheme(currentTheme());
  lenis?.scrollTo(0, { immediate: true });
});

function currentTheme(): 'light' | 'dark' {
  try {
    const t = localStorage.getItem('sockudo-theme');
    if (t === 'light' || t === 'dark') return t;
  } catch {}
  return matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}
function applyTheme(t: 'light' | 'dark') {
  document.documentElement.dataset.theme = t;
  document.querySelector('meta[name="theme-color"]')?.setAttribute('content', t === 'dark' ? '#07050c' : '#fbfaff');
  window.dispatchEvent(new CustomEvent('sockudo:theme', { detail: t }));
}

/* ── Smooth scrolling ── */
if (!reduceMotion) {
  lenis = new Lenis({ lerp: 0.11, wheelMultiplier: 1, smoothWheel: true, anchors: { offset: -88 } });
  const raf = (t: number) => {
    lenis!.raf(t);
    requestAnimationFrame(raf);
  };
  requestAnimationFrame(raf);
  (window as any).__lenis = lenis;
}

/* ── Global, persisted UI (bound once) ── */
document.addEventListener('click', (e) => {
  const t = e.target as HTMLElement;
  if (t.closest('[data-theme-toggle]')) {
    const next = document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark';
    try {
      localStorage.setItem('sockudo-theme', next);
    } catch {}
    const swap = () => applyTheme(next);
    if ('startViewTransition' in document && !reduceMotion) (document as any).startViewTransition(swap);
    else swap();
  }
  if (t.closest('[data-nav-toggle]')) {
    document.body.toggleAttribute('data-nav-open');
  } else if (document.body.hasAttribute('data-nav-open') && !t.closest('.sidebar')) {
    document.body.removeAttribute('data-nav-open');
  }
  const anchor = t.closest<HTMLAnchorElement>('[data-anchor]');
  if (anchor) {
    navigator.clipboard?.writeText(anchor.href).catch(() => {});
    anchor.dataset.copied = '';
    setTimeout(() => delete anchor.dataset.copied, 1400);
  }
  if (t.closest('[data-search-open]')) openSearch();
  if (t.matches('[data-search-dialog]')) closeSearch();
  const copy = t.closest<HTMLElement>('[data-copy-page]');
  if (copy) copyPage(copy);
  const choice = t.closest<HTMLElement>('[data-consent-choice]');
  if (choice) setConsent(choice.dataset.consentChoice as 'granted' | 'denied');
  if (t.closest('[data-consent-open]')) {
    const el = document.querySelector<HTMLElement>('[data-consent]');
    if (el) el.hidden = false;
  }
});

document.addEventListener('keydown', (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
    e.preventDefault();
    isSearchOpen() ? closeSearch() : openSearch();
  } else if (e.key === '/' && !isTyping(e.target)) {
    e.preventDefault();
    openSearch();
  } else if (e.key === 'Escape' && isSearchOpen()) {
    closeSearch();
  }
});

function isTyping(t: EventTarget | null) {
  const el = t as HTMLElement | null;
  return !!el && (el.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(el.tagName));
}

/* ── Consent (TypeUI insights) ── */
const consentKey = 'sockudo.typeui.analytics-consent';
function getConsent() {
  try {
    const v = localStorage.getItem(consentKey);
    return v === 'granted' || v === 'denied' ? v : null;
  } catch {
    return null;
  }
}
function setConsent(v: 'granted' | 'denied') {
  try {
    localStorage.setItem(consentKey, v);
  } catch {}
  const w = window as any;
  v === 'granted' ? w.enableTypeUIInsights?.() : w.disableTypeUIInsights?.();
  const el = document.querySelector<HTMLElement>('[data-consent]');
  if (el) el.hidden = true;
}
{
  const c = getConsent();
  if (c === 'granted') (window as any).enableTypeUIInsights?.();
  const el = document.querySelector<HTMLElement>('[data-consent]');
  if (el && c === null) setTimeout(() => (el.hidden = false), 2500);
}

/* ── Search (Pagefind) ── */
type PagefindResult = { data: () => Promise<{ url: string; excerpt: string; meta: { title: string } }> };
let pagefind: { search: (q: string) => Promise<{ results: PagefindResult[] }> } | null | undefined;
let selected = -1;

async function loadPagefind() {
  if (pagefind !== undefined) return pagefind;
  try {
    const path = '/pagefind/pagefind.js';
    pagefind = await import(/* @vite-ignore */ path);
    await (pagefind as any).init?.();
  } catch {
    pagefind = null;
  }
  return pagefind;
}
const dialog = () => document.querySelector<HTMLElement>('[data-search-dialog]');
function isSearchOpen() {
  return !!dialog()?.hasAttribute('data-open');
}
function openSearch() {
  const d = dialog();
  if (!d) return;
  d.setAttribute('data-open', '');
  lenis?.stop();
  const input = d.querySelector<HTMLInputElement>('[data-search-input]')!;
  input.focus();
  input.select();
  loadPagefind();
}
function closeSearch() {
  dialog()?.removeAttribute('data-open');
  lenis?.start();
}
{
  const d = dialog();
  const input = d?.querySelector<HTMLInputElement>('[data-search-input]');
  const results = d?.querySelector<HTMLElement>('[data-search-results]');
  let token = 0;
  input?.addEventListener('input', async () => {
    const q = input.value.trim();
    const my = ++token;
    if (!q) {
      results!.innerHTML = '<div class="search-empty">Type to search 80+ pages of guides, references, and SDKs.</div>';
      return;
    }
    const pf = await loadPagefind();
    if (my !== token) return;
    if (!pf) {
      results!.innerHTML = '<div class="search-empty">Search index is built with <code>npm run build</code>. Run a production build to search locally.</div>';
      return;
    }
    const res = await pf.search(q);
    const data = await Promise.all(res.results.slice(0, 10).map((r) => r.data()));
    if (my !== token) return;
    selected = data.length ? 0 : -1;
    results!.innerHTML = data.length
      ? data
          .map(
            (d, i) =>
              `<a href="${d.url.replace(/\.html$/, '').replace(/\/index$/, '').replace(/(.)\/$/, '$1')}" aria-selected="${i === 0}"><div class="r-title">${escapeHtml(d.meta.title)}</div><div class="r-excerpt">${d.excerpt}</div></a>`,
          )
          .join('')
      : `<div class="search-empty">No results for “${escapeHtml(q)}”.</div>`;
  });
  input?.addEventListener('keydown', (e) => {
    const items = [...(results?.querySelectorAll<HTMLAnchorElement>('a') ?? [])];
    if (!items.length) return;
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      selected = (selected + (e.key === 'ArrowDown' ? 1 : -1) + items.length) % items.length;
      items.forEach((a, i) => a.setAttribute('aria-selected', String(i === selected)));
      items[selected].scrollIntoView({ block: 'nearest' });
    } else if (e.key === 'Enter' && selected >= 0) {
      e.preventDefault();
      items[selected].click();
    }
  });
  results?.addEventListener('click', (e) => {
    if ((e.target as HTMLElement).closest('a')) closeSearch();
  });
}
function escapeHtml(s: string) {
  return s.replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]!);
}

/* ── Live GitHub stars ── */
let liveStars: number | undefined;
async function refreshStars() {
  if (liveStars === undefined) {
    try {
      const cached = sessionStorage.getItem('sockudo-stars');
      if (cached) liveStars = Number(cached);
    } catch {}
  }
  if (liveStars === undefined) {
    try {
      const r = await fetch('https://api.github.com/repos/sockudo/sockudo');
      if (r.ok) {
        liveStars = (await r.json()).stargazers_count;
        try {
          sessionStorage.setItem('sockudo-stars', String(liveStars));
        } catch {}
      }
    } catch {}
  }
  if (!liveStars) return;
  const fmt = liveStars >= 1000 ? `${(liveStars / 1000).toFixed(liveStars >= 10000 ? 0 : 1)}k` : String(liveStars);
  document.querySelectorAll<HTMLElement>('[data-stars]').forEach((el) => (el.textContent = fmt));
  document.querySelectorAll<HTMLElement>('[data-stars-full]').forEach((el) => (el.dataset.target = String(liveStars)));
}

/* ── Per-page behaviour ── */
onPage(() => {
  const isMac = /Mac|iPhone|iPad/.test(navigator.platform);
  document.querySelectorAll('[data-mod-key]').forEach((k) => (k.textContent = isMac ? '⌘ K' : 'Ctrl K'));
  refreshStars();

  // Header translucency on scroll
  const header = document.querySelector<HTMLElement>('.site-header');
  const onScroll = () => header?.toggleAttribute('data-scrolled', scrollY > 8);
  onScroll();
  addEventListener('scroll', onScroll, { passive: true });

  // Scroll reveals
  const io = new IntersectionObserver(
    (entries) =>
      entries.forEach((en) => {
        if (en.isIntersecting) {
          en.target.classList.add('in');
          io.unobserve(en.target);
        }
      }),
    { rootMargin: '0px 0px -8% 0px', threshold: 0.08 },
  );
  document.querySelectorAll('[data-reveal]').forEach((el) => io.observe(el));

  // Pointer-follow glow on cards
  const onMove = (e: PointerEvent) => {
    const card = (e.target as HTMLElement).closest<HTMLElement>('.card-link, [data-glow]');
    if (!card) return;
    const r = card.getBoundingClientRect();
    card.style.setProperty('--mx', `${e.clientX - r.left}px`);
    card.style.setProperty('--my', `${e.clientY - r.top}px`);
  };
  document.addEventListener('pointermove', onMove, { passive: true });

  // Reading progress
  const bar = document.querySelector<HTMLElement>('[data-progress]');
  const article = document.querySelector<HTMLElement>('.docs-article, .blog-post');
  const onProgress = () => {
    if (!bar || !article) return;
    const r = article.getBoundingClientRect();
    const p = Math.min(1, Math.max(0, -r.top / Math.max(1, r.height - innerHeight)));
    bar.style.transform = `scaleX(${p})`;
  };
  onProgress();
  addEventListener('scroll', onProgress, { passive: true });

  // Keep the active sidebar link in view
  document.querySelector('.sidebar [aria-current="page"]')?.scrollIntoView({ block: 'center' });

  const stopToc = initToc();
  renderMermaid();
  const stop3d = mountDocScenes();

  return () => {
    removeEventListener('scroll', onScroll);
    removeEventListener('scroll', onProgress);
    document.removeEventListener('pointermove', onMove);
    io.disconnect();
    stopToc?.();
    stop3d();
  };
});

/** Lazily mounts 3D diagrams embedded in MDX pages. */
function mountDocScenes() {
  const disposers: (() => void)[] = [];
  const hasGL = (() => {
    try {
      return !!document.createElement('canvas').getContext('webgl2');
    } catch {
      return false;
    }
  })();
  if (!hasGL) return () => {};
  const lazy = (el: Element, run: () => void) => {
    const io = new IntersectionObserver(
      ([e]) => {
        if (!e.isIntersecting) return;
        io.disconnect();
        run();
      },
      { rootMargin: '300px 0px' },
    );
    io.observe(el);
    disposers.push(() => io.disconnect());
  };

  document.querySelectorAll<HTMLCanvasElement>('[data-mini-hero]').forEach((canvas) =>
    lazy(canvas, () => import('./three/hero').then(({ mountHero }) => disposers.push(mountHero(canvas, { scrollLinked: false })))),
  );

  document.querySelectorAll<HTMLElement>('[data-diagram3d]').forEach((fig) =>
    lazy(fig, () =>
      import('./three/fanout').then(({ mountFanout }) => {
        const canvas = fig.querySelector('canvas')!;
        const labels = fig.querySelector<HTMLElement>('[data-fanout-labels]')!;
        const scene = mountFanout(canvas, labels);
        const btns = [...fig.querySelectorAll<HTMLButtonElement>('[data-d3-step]')];
        let step = Number(fig.dataset.start) || 0;
        let pausedUntil = 0;
        const show = (s: number) => {
          step = s;
          scene.setStep(s);
          btns.forEach((b, i) => b.setAttribute('aria-selected', String(i === s)));
        };
        btns.forEach((b, i) =>
          b.addEventListener('click', () => {
            pausedUntil = Date.now() + 15000;
            show(i);
          }),
        );
        show(step);
        const id = setInterval(() => {
          if (Date.now() > pausedUntil && !reduceMotion) show((step + 1) % btns.length);
        }, 5500);
        disposers.push(() => {
          clearInterval(id);
          scene.dispose();
        });
      }),
    ),
  );
  return () => disposers.forEach((d) => d());
}

function initToc() {
  const links = [...document.querySelectorAll<HTMLAnchorElement>('.toc li a')];
  const marker = document.querySelector<HTMLElement>('.toc .marker');
  if (!links.length || !marker) return;
  const heads = links.map((a) => document.getElementById(decodeURIComponent(a.hash.slice(1)))).filter(Boolean) as HTMLElement[];
  const update = () => {
    let active = 0;
    for (let i = 0; i < heads.length; i++) if (heads[i].getBoundingClientRect().top < 140) active = i;
    links.forEach((a, i) => a.classList.toggle('active', i === active));
    const a = links[active];
    if (a) {
      marker.style.top = `${a.offsetTop}px`;
      marker.style.height = `${a.offsetHeight}px`;
    }
  };
  update();
  addEventListener('scroll', update, { passive: true });
  return () => removeEventListener('scroll', update);
}

let mermaidMod: typeof import('mermaid').default | undefined;
let mermaidSeq = 0;

function mermaidTheme(dark: boolean) {
  return dark
    ? {
        background: 'transparent',
        primaryColor: '#1a1129',
        primaryTextColor: '#f5f1ff',
        primaryBorderColor: '#8b4dff',
        secondaryColor: '#24163a',
        tertiaryColor: '#120c1c',
        lineColor: '#a978ff',
        textColor: '#d9d0ec',
        clusterBkg: 'rgba(139,77,255,0.06)',
        clusterBorder: 'rgba(196,165,255,0.25)',
        edgeLabelBackground: '#120c1c',
        nodeTextColor: '#f5f1ff',
      }
    : {
        background: 'transparent',
        primaryColor: '#ffffff',
        primaryTextColor: '#1b1029',
        primaryBorderColor: '#8b4dff',
        secondaryColor: '#f6f0ff',
        tertiaryColor: '#ffffff',
        lineColor: '#7c3aed',
        textColor: '#3b3150',
        clusterBkg: 'rgba(124,58,237,0.04)',
        clusterBorder: 'rgba(124,58,237,0.25)',
        edgeLabelBackground: '#ffffff',
        nodeTextColor: '#1b1029',
      };
}

/** Adds gradients/glow to Mermaid's flowchart SVG and tags shapes for CSS. */
function decorateFlowchart(svg: SVGSVGElement, dark: boolean) {
  const ns = 'http://www.w3.org/2000/svg';
  const id = `g${++mermaidSeq}`;
  const defs = document.createElementNS(ns, 'defs');
  defs.innerHTML = dark
    ? `<linearGradient id="${id}-node" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#2a1945"/><stop offset="1" stop-color="#150d24"/></linearGradient>
       <linearGradient id="${id}-db" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#3b1745"/><stop offset="1" stop-color="#1c0d26"/></linearGradient>
       <linearGradient id="${id}-dec" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#4c1d95"/><stop offset="1" stop-color="#701a75"/></linearGradient>
       <filter id="${id}-glow" x="-30%" y="-30%" width="160%" height="160%"><feDropShadow dx="0" dy="6" stdDeviation="9" flood-color="#8b4dff" flood-opacity="0.35"/></filter>`
    : `<linearGradient id="${id}-node" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#ffffff"/><stop offset="1" stop-color="#f4eeff"/></linearGradient>
       <linearGradient id="${id}-db" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#fdf4ff"/><stop offset="1" stop-color="#f5e1ff"/></linearGradient>
       <linearGradient id="${id}-dec" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#ede5ff"/><stop offset="1" stop-color="#fae8ff"/></linearGradient>
       <filter id="${id}-glow" x="-30%" y="-30%" width="160%" height="160%"><feDropShadow dx="0" dy="6" stdDeviation="8" flood-color="#7c3aed" flood-opacity="0.16"/></filter>`;
  svg.prepend(defs);
  svg.querySelectorAll<SVGGElement>('g.node').forEach((node) => {
    const shapes = node.querySelectorAll<SVGElement>(':scope > rect, :scope > polygon, :scope > path, :scope > g > path, :scope > g > rect, :scope > circle');
    const isDb = node.classList.contains('cylinder') || !!node.querySelector('path.basic.label-container[d*="a"]') || /cyl|database/i.test(node.getAttribute('class') ?? '');
    const isDecision = !!node.querySelector(':scope > polygon');
    node.classList.add(isDecision ? 'is-decision' : isDb ? 'is-db' : 'is-box');
    shapes.forEach((sh) => {
      sh.style.fill = `url(#${id}-${isDecision ? 'dec' : isDb ? 'db' : 'node'})`;
      sh.style.filter = `url(#${id}-glow)`;
      if (sh.tagName === 'rect') {
        sh.setAttribute('rx', '12');
        sh.setAttribute('ry', '12');
      }
    });
  });
}

/** Sizes the SVG to its natural width, shrinking to fit but never below a readable scale. */
function fitMermaid(stage: HTMLElement) {
  const svg = stage.querySelector('svg');
  if (!svg) return;
  const vb = svg.viewBox.baseVal;
  const natural = vb?.width || svg.getBoundingClientRect().width;
  const avail = stage.clientWidth;
  const width = natural <= avail ? natural : Math.max(avail, natural * 0.7);
  svg.style.width = `${width}px`;
  svg.style.maxWidth = 'none';
  svg.style.height = 'auto';
  stage.classList.toggle('scrolls', width > avail + 1);
}

async function renderMermaid(force = false) {
  const stages = [...document.querySelectorAll<HTMLElement>('[data-mermaid]')];
  if (!stages.length) return;
  mermaidMod ??= (await import('mermaid')).default;
  const dark = document.documentElement.dataset.theme === 'dark';
  mermaidMod.initialize({
    startOnLoad: false,
    securityLevel: 'strict',
    theme: 'base',
    look: 'neo',
    fontFamily: 'Geist Variable, ui-sans-serif, system-ui',
    fontSize: 15,
    htmlLabels: true,
    flowchart: { curve: 'basis', padding: 16, nodeSpacing: 46, rankSpacing: 58, useMaxWidth: false, diagramPadding: 12 },
    themeVariables: { ...mermaidTheme(dark), fontSize: '15px' },
  });
  for (const stage of stages) {
    if (stage.dataset.rendered && !force) continue;
    const src = stage.dataset.source ?? stage.querySelector('.mermaid-src')?.textContent ?? '';
    stage.dataset.source = src;
    try {
      const { svg } = await mermaidMod.render(`mmd-${++mermaidSeq}`, src);
      stage.innerHTML = svg;
      const el = stage.querySelector('svg')!;
      el.removeAttribute('style');
      decorateFlowchart(el, dark);
      fitMermaid(stage);
      stage.dataset.rendered = 'true';
    } catch (err) {
      console.warn('mermaid render failed', err);
    }
  }
}

/* Fullscreen diagram viewer with wheel zoom and drag to pan. */
function openDiagram(fig: HTMLElement) {
  const svg = fig.querySelector<SVGSVGElement>('.flow-svg, [data-mermaid] svg');
  if (!svg) return;
  const overlay = document.createElement('div');
  overlay.className = 'diagram-viewer';
  overlay.setAttribute('role', 'dialog');
  overlay.setAttribute('aria-label', 'Diagram viewer');
  overlay.innerHTML = `<div class="dv-canvas flow in"></div><div class="dv-bar"><span>Scroll to zoom · drag to pan</span><button type="button" data-dv-reset>Reset</button><button type="button" data-dv-close aria-label="Close">Close <kbd>Esc</kbd></button></div>`;
  const canvas = overlay.querySelector<HTMLElement>('.dv-canvas')!;
  const clone = svg.cloneNode(true) as SVGSVGElement;
  clone.removeAttribute('style');

  clone.style.width = '100%';
  clone.style.height = '100%';
  canvas.appendChild(clone);
  document.body.appendChild(overlay);
  lenis?.stop();
  let scale = 1;
  let x = 0;
  let y = 0;
  const apply = () => (clone.style.transform = `translate(${x}px, ${y}px) scale(${scale})`);
  const close = () => {
    overlay.remove();
    lenis?.start();
    removeEventListener('keydown', onKey);
  };
  const onKey = (e: KeyboardEvent) => e.key === 'Escape' && close();
  addEventListener('keydown', onKey);
  overlay.addEventListener('click', (e) => {
    const t = e.target as HTMLElement;
    if (t.closest('[data-dv-close]') || t === overlay) close();
    if (t.closest('[data-dv-reset]')) {
      scale = 1;
      x = y = 0;
      apply();
    }
  });
  canvas.addEventListener(
    'wheel',
    (e) => {
      e.preventDefault();
      scale = Math.min(6, Math.max(0.5, scale * (e.deltaY < 0 ? 1.12 : 0.89)));
      apply();
    },
    { passive: false },
  );
  let drag: { px: number; py: number; x: number; y: number } | null = null;
  canvas.addEventListener('pointerdown', (e) => {
    drag = { px: e.clientX, py: e.clientY, x, y };
    canvas.setPointerCapture(e.pointerId);
  });
  canvas.addEventListener('pointermove', (e) => {
    if (!drag) return;
    x = drag.x + e.clientX - drag.px;
    y = drag.y + e.clientY - drag.py;
    apply();
  });
  canvas.addEventListener('pointerup', () => (drag = null));
}
document.addEventListener('click', (e) => {
  const fig = (e.target as HTMLElement).closest('[data-diagram-zoom]')?.closest<HTMLElement>('[data-diagram]');
  if (fig) openDiagram(fig);
});
addEventListener('resize', () => document.querySelectorAll<HTMLElement>('[data-mermaid][data-rendered]').forEach(fitMermaid));
window.addEventListener('sockudo:theme', () => renderMermaid(true));

async function copyPage(btn: HTMLElement) {
  const url = btn.dataset.copyPage!;
  try {
    const text = await (await fetch(url)).text();
    await navigator.clipboard.writeText(text);
    const label = btn.querySelector('span');
    if (label) {
      const prev = label.textContent;
      label.textContent = 'Copied!';
      setTimeout(() => (label.textContent = prev), 1600);
    }
  } catch {}
}
