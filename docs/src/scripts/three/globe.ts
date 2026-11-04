import * as THREE from 'three';
import { PALETTE, createStage, fresnelMaterial, glowPoints, isSmall, reduceMotion } from './stage';

/** Dotted globe with arcing message pulses between regions. */
export function mountGlobe(canvas: HTMLCanvasElement) {
  const small = isSmall();
  const stage = createStage({ canvas, fov: 35, bloom: { strength: 0.6, radius: 0.5, threshold: 0.12 } });
  const { scene, camera } = stage;
  camera.position.set(0, 0.6, 13);
  camera.lookAt(0, 0, 0);

  const globe = new THREE.Group();
  globe.rotation.z = 0.35;
  scene.add(globe);
  const R = 3.2;

  const D = small ? 1400 : 2600;
  const dots = glowPoints(D, { size: 0.32 });
  const golden = Math.PI * (3 - Math.sqrt(5));
  const surface: THREE.Vector3[] = [];
  for (let i = 0; i < D; i++) {
    const y = 1 - (i / (D - 1)) * 2;
    const r = Math.sqrt(1 - y * y);
    const v = new THREE.Vector3(Math.cos(golden * i) * r, y, Math.sin(golden * i) * r).multiplyScalar(R);
    surface.push(v);
    dots.pos.setXYZ(i, v.x, v.y, v.z);
    // pseudo "continents": brighten clusters using a cheap noise
    const n = Math.sin(v.x * 1.7) * Math.cos(v.y * 2.1) + Math.sin(v.z * 1.3 + v.y);
    const b = n > 0.35 ? 0.95 : 0.28;
    dots.col.setXYZ(i, 0.62 * b, 0.45 * b, 1 * b);
    dots.size.setX(i, n > 0.35 ? 1.25 : 0.8);
  }
  globe.add(dots.points);

  const atmo = new THREE.Mesh(new THREE.SphereGeometry(R * 1.04, 64, 64), fresnelMaterial(PALETTE.violet.clone(), 3.2, 0.9));
  globe.add(atmo);
  const occluder = new THREE.Mesh(new THREE.SphereGeometry(R * 0.985, 48, 48), new THREE.MeshBasicMaterial({ color: '#07050c', transparent: true, opacity: 0.88 }));
  globe.add(occluder);

  // Arcs between random hubs
  const hubs = Array.from({ length: 14 }, () => surface[Math.floor(Math.random() * D)]);
  const arcs: THREE.QuadraticBezierCurve3[] = [];
  const arcVerts: number[] = [];
  for (let i = 0; i < 22; i++) {
    const a = hubs[Math.floor(Math.random() * hubs.length)];
    let b = hubs[Math.floor(Math.random() * hubs.length)];
    if (a === b) b = hubs[(hubs.indexOf(a) + 3) % hubs.length];
    const mid = a.clone().add(b).multiplyScalar(0.5);
    const lift = R + a.distanceTo(b) * 0.45;
    const c = new THREE.QuadraticBezierCurve3(a, mid.normalize().multiplyScalar(lift), b);
    arcs.push(c);
    const pts = c.getPoints(40);
    for (let k = 0; k < pts.length - 1; k++) arcVerts.push(pts[k].x, pts[k].y, pts[k].z, pts[k + 1].x, pts[k + 1].y, pts[k + 1].z);
  }
  const arcGeo = new THREE.BufferGeometry();
  arcGeo.setAttribute('position', new THREE.Float32BufferAttribute(arcVerts, 3));
  globe.add(
    new THREE.LineSegments(arcGeo, new THREE.LineBasicMaterial({ color: PALETTE.violetLight, transparent: true, opacity: 0.22, blending: THREE.AdditiveBlending, depthWrite: false })),
  );

  const hubPts = glowPoints(hubs.length, { size: 0.9 });
  hubs.forEach((h, i) => {
    hubPts.pos.setXYZ(i, h.x, h.y, h.z);
    const c = i % 3 === 0 ? PALETTE.fuchsia : PALETTE.violetLight;
    hubPts.col.setXYZ(i, c.r, c.g, c.b);
  });
  globe.add(hubPts.points);

  const TRAIL = 8;
  const pulses = arcs.map((c) => ({ c, t: Math.random(), s: 0.25 + Math.random() * 0.35 }));
  const pp = glowPoints(pulses.length * TRAIL, { size: 0.6 });
  globe.add(pp.points);
  const v = new THREE.Vector3();

  let px = 0;
  let tx = 0;
  const onPointer = (e: PointerEvent) => (tx = (e.clientX / innerWidth) * 2 - 1);
  addEventListener('pointermove', onPointer, { passive: true });

  stage.onFrame((t, dt) => {
    px += (tx - px) * 0.03;
    globe.rotation.y = t * 0.09 + px * 0.4;
    pulses.forEach((p, i) => {
      p.t = (p.t + dt * p.s) % 1;
      for (let k = 0; k < TRAIL; k++) {
        p.c.getPoint(Math.max(0, p.t - k * 0.02), v);
        const f = 1 - k / TRAIL;
        pp.pos.setXYZ(i * TRAIL + k, v.x, v.y, v.z);
        pp.col.setXYZ(i * TRAIL + k, f, 0.75 * f, f);
        pp.size.setX(i * TRAIL + k, (k === 0 ? 1 : 0.6) * f);
      }
    });
    pp.pos.needsUpdate = pp.col.needsUpdate = pp.size.needsUpdate = true;
    hubPts.material.uniforms.baseSize.value = 0.9 + Math.sin(t * 2) * 0.12;
  });

  if (reduceMotion()) stage.renderOnce();

  return () => {
    removeEventListener('pointermove', onPointer);
    stage.dispose();
  };
}
