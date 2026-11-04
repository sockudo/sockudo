import * as THREE from 'three';
import { EffectComposer } from 'three/examples/jsm/postprocessing/EffectComposer.js';
import { RenderPass } from 'three/examples/jsm/postprocessing/RenderPass.js';
import { UnrealBloomPass } from 'three/examples/jsm/postprocessing/UnrealBloomPass.js';
import { OutputPass } from 'three/examples/jsm/postprocessing/OutputPass.js';

export const reduceMotion = () => matchMedia('(prefers-reduced-motion: reduce)').matches;
export const isSmall = () => matchMedia('(max-width: 760px)').matches;

export const PALETTE = {
  violet: new THREE.Color('#8b4dff'),
  violetLight: new THREE.Color('#c4a5ff'),
  fuchsia: new THREE.Color('#e879f9'),
  cyan: new THREE.Color('#67e8f9'),
  white: new THREE.Color('#ffffff'),
  deep: new THREE.Color('#2e1065'),
};

let glowTex: THREE.Texture | undefined;
/** Soft radial sprite used for every glowing point. */
export function glowTexture() {
  if (glowTex) return glowTex;
  const c = document.createElement('canvas');
  c.width = c.height = 128;
  const g = c.getContext('2d')!;
  const grd = g.createRadialGradient(64, 64, 0, 64, 64, 64);
  grd.addColorStop(0, 'rgba(255,255,255,1)');
  grd.addColorStop(0.18, 'rgba(255,255,255,0.85)');
  grd.addColorStop(0.42, 'rgba(255,255,255,0.22)');
  grd.addColorStop(1, 'rgba(255,255,255,0)');
  g.fillStyle = grd;
  g.fillRect(0, 0, 128, 128);
  glowTex = new THREE.CanvasTexture(c);
  glowTex.colorSpace = THREE.SRGBColorSpace;
  return glowTex;
}

export interface StageOptions {
  canvas: HTMLCanvasElement;
  fov?: number;
  bloom?: { strength: number; radius: number; threshold: number } | false;
  clearAlpha?: number;
}

export interface Stage {
  scene: THREE.Scene;
  camera: THREE.PerspectiveCamera;
  renderer: THREE.WebGLRenderer;
  size: { w: number; h: number };
  onFrame: (fn: (t: number, dt: number) => void) => void;
  onResize: (fn: (w: number, h: number) => void) => void;
  renderOnce: () => void;
  dispose: () => void;
}

/**
 * Creates a renderer bound to a canvas that sizes to its parent, pauses when off-screen
 * or in a background tab, and tears everything down on dispose.
 */
export function createStage({ canvas, fov = 45, bloom = { strength: 1.1, radius: 0.7, threshold: 0.05 }, clearAlpha = 0 }: StageOptions): Stage {
  const renderer = new THREE.WebGLRenderer({ canvas, antialias: true, alpha: true, powerPreference: 'high-performance' });
  renderer.setPixelRatio(Math.min(devicePixelRatio, isSmall() ? 1.5 : 1.75));
  renderer.setClearColor(0x000000, clearAlpha);
  renderer.outputColorSpace = THREE.SRGBColorSpace;

  const scene = new THREE.Scene();
  const camera = new THREE.PerspectiveCamera(fov, 1, 0.1, 200);

  let composer: EffectComposer | undefined;
  let bloomPass: UnrealBloomPass | undefined;
  if (bloom) {
    composer = new EffectComposer(renderer);
    composer.addPass(new RenderPass(scene, camera));
    bloomPass = new UnrealBloomPass(new THREE.Vector2(256, 256), bloom.strength, bloom.radius, bloom.threshold);
    composer.addPass(bloomPass);
    composer.addPass(new OutputPass());
  }

  const frameFns: ((t: number, dt: number) => void)[] = [];
  const resizeFns: ((w: number, h: number) => void)[] = [];
  const size = { w: 1, h: 1 };

  const resize = () => {
    const parent = canvas.parentElement!;
    const w = Math.max(1, parent.clientWidth);
    const h = Math.max(1, parent.clientHeight);
    if (w === size.w && h === size.h) return;
    size.w = w;
    size.h = h;
    renderer.setSize(w, h, false);
    composer?.setSize(w, h);
    bloomPass?.resolution.set(w, h);
    camera.aspect = w / h;
    camera.updateProjectionMatrix();
    resizeFns.forEach((f) => f(w, h));
  };
  const ro = new ResizeObserver(resize);
  ro.observe(canvas.parentElement!);
  resize();

  const draw = () => (composer ? composer.render() : renderer.render(scene, camera));

  let visible = false;
  let raf = 0;
  let last = performance.now();
  let elapsed = 0;
  const loop = (now: number) => {
    raf = requestAnimationFrame(loop);
    const dt = Math.min(0.05, (now - last) / 1000);
    last = now;
    elapsed += dt;
    frameFns.forEach((f) => f(elapsed, dt));
    draw();
  };
  const start = () => {
    if (raf || reduceMotion()) return;
    last = performance.now();
    raf = requestAnimationFrame(loop);
  };
  const stop = () => {
    cancelAnimationFrame(raf);
    raf = 0;
  };
  const io = new IntersectionObserver(([e]) => {
    visible = e.isIntersecting;
    visible && !document.hidden ? start() : stop();
  });
  io.observe(canvas);
  const onVis = () => (visible && !document.hidden ? start() : stop());
  document.addEventListener('visibilitychange', onVis);

  return {
    scene,
    camera,
    renderer,
    size,
    onFrame: (fn) => frameFns.push(fn),
    onResize: (fn) => resizeFns.push(fn),
    renderOnce: () => {
      frameFns.forEach((f) => f(2.5, 0));
      draw();
    },
    dispose: () => {
      stop();
      io.disconnect();
      ro.disconnect();
      document.removeEventListener('visibilitychange', onVis);
      scene.traverse((o) => {
        const m = o as THREE.Mesh;
        m.geometry?.dispose();
        const mat = m.material as THREE.Material | THREE.Material[] | undefined;
        (Array.isArray(mat) ? mat : mat ? [mat] : []).forEach((x) => x.dispose());
      });
      composer?.dispose();
      renderer.dispose();
    },
  };
}

/** Point cloud of glowing sprites with per-point color and size. */
export function glowPoints(count: number, opts: { size?: number; opacity?: number } = {}) {
  const geo = new THREE.BufferGeometry();
  geo.setAttribute('position', new THREE.BufferAttribute(new Float32Array(count * 3), 3));
  geo.setAttribute('color', new THREE.BufferAttribute(new Float32Array(count * 3), 3));
  geo.setAttribute('size', new THREE.BufferAttribute(new Float32Array(count).fill(1), 1));
  const mat = new THREE.ShaderMaterial({
    uniforms: { map: { value: glowTexture() }, baseSize: { value: opts.size ?? 0.3 }, opacity: { value: opts.opacity ?? 1 }, pr: { value: Math.min(devicePixelRatio, 1.75) } },
    vertexShader: /* glsl */ `
      attribute float size;
      attribute vec3 color;
      varying vec3 vColor;
      uniform float baseSize;
      uniform float pr;
      void main() {
        vColor = color;
        vec4 mv = modelViewMatrix * vec4(position, 1.0);
        gl_PointSize = baseSize * size * pr * (300.0 / -mv.z);
        gl_Position = projectionMatrix * mv;
      }`,
    fragmentShader: /* glsl */ `
      uniform sampler2D map;
      uniform float opacity;
      varying vec3 vColor;
      void main() {
        vec4 t = texture2D(map, gl_PointCoord);
        gl_FragColor = vec4(vColor * t.rgb, t.a * opacity);
      }`,
    transparent: true,
    depthWrite: false,
    blending: THREE.AdditiveBlending,
  });
  const points = new THREE.Points(geo, mat);
  points.frustumCulled = false;
  return {
    points,
    pos: geo.attributes.position as THREE.BufferAttribute,
    col: geo.attributes.color as THREE.BufferAttribute,
    size: geo.attributes.size as THREE.BufferAttribute,
    material: mat,
  };
}

/** Fresnel-lit glass shell for the "server" cores. */
export function fresnelMaterial(color: THREE.Color, power = 2.2, intensity = 1.4) {
  return new THREE.ShaderMaterial({
    uniforms: { color: { value: color }, power: { value: power }, intensity: { value: intensity } },
    vertexShader: /* glsl */ `
      varying vec3 vN;
      varying vec3 vV;
      void main() {
        vec4 mv = modelViewMatrix * vec4(position, 1.0);
        vN = normalize(normalMatrix * normal);
        vV = normalize(-mv.xyz);
        gl_Position = projectionMatrix * mv;
      }`,
    fragmentShader: /* glsl */ `
      uniform vec3 color;
      uniform float power;
      uniform float intensity;
      varying vec3 vN;
      varying vec3 vV;
      void main() {
        float f = pow(1.0 - abs(dot(vN, vV)), power);
        gl_FragColor = vec4(color * f * intensity, f);
      }`,
    transparent: true,
    depthWrite: false,
    blending: THREE.AdditiveBlending,
  });
}
