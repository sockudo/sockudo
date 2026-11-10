import { gsap } from 'gsap';
import { ScrollTrigger } from 'gsap/ScrollTrigger';

gsap.registerPlugin(ScrollTrigger);

const reduce = () => matchMedia('(prefers-reduced-motion: reduce)').matches;

/** Wires every homepage interaction; returns nothing because teardown hooks into astro:before-swap. */
export function initHome() {
  const root = document.querySelector<HTMLElement>('main.home');
  if (!root) return;
  const disposers: (() => void)[] = [];
  const once = () => {
    disposers.forEach((d) => d());
    ScrollTrigger.getAll().forEach((t) => t.kill());
    document.removeEventListener('astro:before-swap', once);
  };
  document.addEventListener('astro:before-swap', once);

  mountScenes(disposers);
  heroIntro();
  floatingCards(disposers);
  installBox();
  tabs();
  tilt(disposers);
  chatDemo(disposers);
  tickers(disposers);
  starCountUp();
}

function mountScenes(disposers: (() => void)[]) {
  const webgl = (() => {
    try {
      const c = document.createElement('canvas');
      return !!(c.getContext('webgl2') || c.getContext('webgl'));
    } catch {
      return false;
    }
  })();
  if (!webgl) return;

  const heroCanvas = document.querySelector<HTMLCanvasElement>('[data-hero-canvas]');
  if (heroCanvas)
    import('./three/hero').then(({ mountHero }) => {
      const d = mountHero(heroCanvas);
      disposers.push(d);
      heroCanvas.animate([{ opacity: 0 }, { opacity: 1 }], { duration: 1600, easing: 'ease-out', fill: 'forwards' });
    });

  const fan = document.querySelector<HTMLElement>('[data-fanout]');
  if (fan) {
    // Load the second scene only when the section approaches.
    const io = new IntersectionObserver(
      ([e]) => {
        if (!e.isIntersecting) return;
        io.disconnect();
        import('./three/fanout').then(({ mountFanout }) => {
          const canvas = fan.querySelector<HTMLCanvasElement>('[data-fanout-canvas]')!;
          const labels = fan.querySelector<HTMLElement>('[data-fanout-labels]')!;
          const scene = mountFanout(canvas, labels);
          disposers.push(scene.dispose);
          const steps = [...fan.querySelectorAll<HTMLElement>('.step')];
          steps[0]?.classList.add('active');
          steps.forEach((el, i) =>
            ScrollTrigger.create({
              trigger: el,
              start: 'top 60%',
              end: 'bottom 40%',
              onToggle: (self) => {
                if (!self.isActive) return;
                steps.forEach((s, j) => s.classList.toggle('active', j === i));
                scene.setStep(i);
              },
            }),
          );
        });
      },
      { rootMargin: '600px 0px' },
    );
    io.observe(fan);
    disposers.push(() => io.disconnect());
  }

  const globe = document.querySelector<HTMLCanvasElement>('[data-globe-canvas]');
  if (globe) {
    const io = new IntersectionObserver(
      ([e]) => {
        if (!e.isIntersecting) return;
        io.disconnect();
        import('./three/globe').then(({ mountGlobe }) => disposers.push(mountGlobe(globe)));
      },
      { rootMargin: '600px 0px' },
    );
    io.observe(globe);
    disposers.push(() => io.disconnect());
  }
}

function heroIntro() {
  const lines = document.querySelectorAll('[data-hero-line] > span');
  const ins = document.querySelectorAll('[data-hero-in]');
  if (reduce()) return;
  const tl = gsap.timeline({ defaults: { ease: 'expo.out' } });
  tl.from(lines, { yPercent: 110, rotate: 4, duration: 1.3, stagger: 0.09 }, 0.1)
    .from(ins, { y: 24, opacity: 0, filter: 'blur(8px)', duration: 1.1, stagger: 0.08 }, 0.35)
    .from('.fcard', { y: 40, opacity: 0, scale: 0.92, duration: 1.4, stagger: 0.15 }, 0.7);

  // Parallax the hero copy away as the page scrolls.
  gsap.to('.hero-copy', {
    yPercent: -12,
    opacity: 0.2,
    ease: 'none',
    scrollTrigger: { trigger: '.hero', start: 'top top', end: 'bottom top', scrub: true },
  });
  gsap.utils.toArray<HTMLElement>('.sec-title').forEach((el) =>
    gsap.from(el, {
      backgroundPositionX: '100%',
      scrollTrigger: { trigger: el, start: 'top 85%' },
    }),
  );
}

function floatingCards(disposers: (() => void)[]) {
  const cards = [...document.querySelectorAll<HTMLElement>('[data-float]')];
  if (!cards.length || reduce()) return;
  let raf = 0;
  let tx = 0;
  let ty = 0;
  let x = 0;
  let y = 0;
  const onMove = (e: PointerEvent) => {
    tx = e.clientX / innerWidth - 0.5;
    ty = e.clientY / innerHeight - 0.5;
  };
  const loop = (t: number) => {
    x += (tx - x) * 0.06;
    y += (ty - y) * 0.06;
    cards.forEach((c, i) => {
      const depth = 14 + i * 10;
      const bob = Math.sin(t / 900 + i * 1.7) * 6;
      c.style.transform = `translate3d(${x * depth}px, ${y * depth + bob}px, 0) rotateX(${-y * 8}deg) rotateY(${x * 10}deg)`;
    });
    raf = requestAnimationFrame(loop);
  };
  raf = requestAnimationFrame(loop);
  addEventListener('pointermove', onMove, { passive: true });
  disposers.push(() => {
    cancelAnimationFrame(raf);
    removeEventListener('pointermove', onMove);
  });
}

function installBox() {
  const box = document.querySelector<HTMLElement>('[data-install]');
  if (!box) return;
  box.querySelectorAll<HTMLButtonElement>('[data-install-tab]').forEach((tab) =>
    tab.addEventListener('click', () => {
      const id = tab.dataset.installTab;
      box.querySelectorAll('[data-install-tab]').forEach((t) => t.setAttribute('aria-selected', String(t === tab)));
      box.querySelectorAll<HTMLElement>('[data-install-cmd]').forEach((c) => (c.hidden = c.dataset.installCmd !== id));
    }),
  );
  const copy = box.querySelector<HTMLButtonElement>('[data-install-copy]');
  copy?.addEventListener('click', async () => {
    const cmd = box.querySelector<HTMLElement>('[data-install-cmd]:not([hidden])')?.textContent?.trim() ?? '';
    try {
      await navigator.clipboard.writeText(cmd);
      copy.setAttribute('data-copied', '');
      setTimeout(() => copy.removeAttribute('data-copied'), 1600);
    } catch {}
  });
}

function tabs() {
  document.querySelectorAll<HTMLElement>('[data-tabs]').forEach((group) => {
    const btns = [...group.querySelectorAll<HTMLButtonElement>('[data-tab]')];
    const ink = group.querySelector<HTMLElement>('.tab-ink');
    const place = (b: HTMLElement) => {
      if (!ink) return;
      ink.style.width = `${b.offsetWidth}px`;
      ink.style.transform = `translateX(${b.offsetLeft}px)`;
    };
    const sel = btns.find((b) => b.getAttribute('aria-selected') === 'true') ?? btns[0];
    requestAnimationFrame(() => sel && place(sel));
    btns.forEach((b) =>
      b.addEventListener('click', () => {
        btns.forEach((x) => x.setAttribute('aria-selected', String(x === b)));
        group.querySelectorAll<HTMLElement>('[data-panel]').forEach((p) => (p.hidden = p.dataset.panel !== b.dataset.tab));
        place(b);
      }),
    );
  });
}

function tilt(disposers: (() => void)[]) {
  if (reduce() || matchMedia('(hover: none)').matches) return;
  const els = [...document.querySelectorAll<HTMLElement>('[data-tilt]')];
  const handlers = els.map((el) => {
    const move = (e: PointerEvent) => {
      const r = el.getBoundingClientRect();
      const px = (e.clientX - r.left) / r.width - 0.5;
      const py = (e.clientY - r.top) / r.height - 0.5;
      el.style.transform = `perspective(1000px) rotateX(${-py * 6}deg) rotateY(${px * 8}deg) translateY(-4px)`;
    };
    const leave = () => (el.style.transform = '');
    el.addEventListener('pointermove', move);
    el.addEventListener('pointerleave', leave);
    return () => {
      el.removeEventListener('pointermove', move);
      el.removeEventListener('pointerleave', leave);
    };
  });
  disposers.push(() => handlers.forEach((h) => h()));
}

function chatDemo(disposers: (() => void)[]) {
  const chat = document.querySelector<HTMLElement>('[data-chat]');
  if (!chat) return;
  const textEl = chat.querySelector<HTMLElement>('[data-chat-text]')!;
  const state = chat.querySelector<HTMLElement>('[data-chat-state]')!;
  const drop = chat.querySelector<HTMLElement>('[data-chat-drop]')!;
  const ver = chat.querySelector<HTMLElement>('[data-chat-version]')!;
  const apps = chat.querySelector<HTMLElement>('[data-chat-appends]')!;
  const ratio = chat.querySelector<HTMLElement>('[data-chat-ratio]')!;
  const answer =
    'The nightly sync hit the partner API rate limit at 02:14 and retried with the same idempotency key, so no duplicate orders were created. 3 orders are still pending — I can re-run just those now if you like.';
  const tokens = answer.match(/\S+\s*/g) ?? [];
  let i = 0;
  let appends = 0;
  let timer = 0;
  let running = false;

  const tick = () => {
    if (i === 11 && !drop.hasAttribute('data-show')) {
      drop.setAttribute('data-show', '');
      state.textContent = 'reconnecting…';
      state.style.color = '#fb7185';
      timer = window.setTimeout(() => {
        state.textContent = 'resumed · streaming…';
        state.style.color = '';
        tick();
      }, 1600);
      return;
    }
    if (i === 16) drop.removeAttribute('data-show');
    if (i >= tokens.length) {
      state.textContent = 'run complete';
      ver.textContent = `v${appends + 1}`;
      timer = window.setTimeout(reset, 4000);
      return;
    }
    const burst = 1 + Math.floor(Math.random() * 3);
    textEl.textContent += tokens.slice(i, i + burst).join('');
    i += burst;
    appends += burst;
    apps.textContent = String(appends);
    ratio.textContent = (appends / Math.max(1, Math.ceil(appends / 2.2))).toFixed(1);
    ver.textContent = `v${appends + 1}`;
    timer = window.setTimeout(tick, 70 + Math.random() * 90);
  };
  const reset = () => {
    i = 0;
    appends = 0;
    textEl.textContent = '';
    state.textContent = 'streaming…';
    drop.removeAttribute('data-show');
    tick();
  };
  const io = new IntersectionObserver(([e]) => {
    if (e.isIntersecting && !running) {
      running = true;
      if (reduce()) textEl.textContent = answer;
      else reset();
    }
  });
  io.observe(chat);
  disposers.push(() => {
    io.disconnect();
    clearTimeout(timer);
  });
}

function tickers(disposers: (() => void)[]) {
  const els = [...document.querySelectorAll<HTMLElement>('[data-ticker]')];
  if (!els.length || reduce()) return;
  const id = setInterval(() => {
    els.forEach((el) => {
      el.textContent = String(Number(el.textContent) + 1);
      el.animate([{ color: '#fff', textShadow: '0 0 8px #fbbf24' }, {}], { duration: 600 });
    });
  }, 1400);
  disposers.push(() => clearInterval(id));
}

function starCountUp() {
  const el = document.querySelector<HTMLElement>('[data-stars-full]');
  if (!el) return;
  const io = new IntersectionObserver(([e]) => {
    if (!e.isIntersecting) return;
    io.disconnect();
    const target = Number(el.dataset.target) || 0;
    if (reduce()) return;
    const obj = { v: 0 };
    gsap.to(obj, {
      v: target,
      duration: 2.2,
      ease: 'expo.out',
      onUpdate: () => (el.textContent = Math.round(obj.v).toLocaleString('en-US')),
    });
  });
  io.observe(el);
}
