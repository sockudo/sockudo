import * as THREE from 'three';
import { PALETTE, createStage, fresnelMaterial, glowPoints, isSmall, reduceMotion } from './stage';

/**
 * Hero scene: a glowing Sockudo core fanning messages out to a shell of client sockets.
 * Inbound packets (publishes) travel to the core; outbound packets fan out to clients.
 */
export function mountHero(canvas: HTMLCanvasElement, { scrollLinked = true } = {}) {
  const small = isSmall();
  const stage = createStage({ canvas, fov: 42, bloom: { strength: small ? 0.55 : 0.7, radius: 0.55, threshold: 0.12 } });
  const { scene, camera } = stage;
  camera.position.set(0, 0, small ? 15 : 12.5);

  const world = new THREE.Group();
  scene.add(world);
  const rig = new THREE.Group();
  world.add(rig);

  /* Core */
  const core = new THREE.Group();
  rig.add(core);
  const ico = new THREE.LineSegments(
    new THREE.EdgesGeometry(new THREE.IcosahedronGeometry(1.25, 1)),
    new THREE.LineBasicMaterial({ color: PALETTE.violetLight, transparent: true, opacity: 0.55, blending: THREE.AdditiveBlending }),
  );
  core.add(ico);
  const shell = new THREE.Mesh(new THREE.IcosahedronGeometry(1.32, 4), fresnelMaterial(PALETTE.violet.clone(), 2.4, 1.1));
  core.add(shell);
  const innerGeo = new THREE.IcosahedronGeometry(0.62, 0);
  const inner = new THREE.Mesh(innerGeo, new THREE.MeshBasicMaterial({ color: new THREE.Color('#d8c2ff'), wireframe: true, transparent: true, opacity: 0.5 }));
  core.add(inner);
  const heart = glowPoints(1, { size: 4 });
  heart.col.setXYZ(0, 0.55, 0.38, 0.9);
  core.add(heart.points);

  /* Orbit rings with satellites */
  const rings: { mesh: THREE.Mesh; speed: number; sat: THREE.Object3D }[] = [];
  const ringDefs = [
    { r: 2.0, tilt: [1.2, 0.2, 0], speed: 0.5, color: PALETTE.violetLight },
    { r: 2.45, tilt: [0.4, 0.9, 0.3], speed: -0.35, color: PALETTE.fuchsia },
    { r: 2.9, tilt: [1.7, -0.5, 0.6], speed: 0.25, color: PALETTE.cyan },
  ];
  for (const d of ringDefs) {
    const pivot = new THREE.Group();
    pivot.rotation.set(d.tilt[0], d.tilt[1], d.tilt[2]);
    const mesh = new THREE.Mesh(
      new THREE.TorusGeometry(d.r, 0.006, 6, 220),
      new THREE.MeshBasicMaterial({ color: d.color, transparent: true, opacity: 0.35, blending: THREE.AdditiveBlending }),
    );
    pivot.add(mesh);
    const sat = glowPoints(1, { size: 1.2 });
    sat.col.setXYZ(0, d.color.r, d.color.g, d.color.b);
    sat.pos.setXYZ(0, d.r, 0, 0);
    const satPivot = new THREE.Group();
    satPivot.add(sat.points);
    pivot.add(satPivot);
    rig.add(pivot);
    rings.push({ mesh, speed: d.speed, sat: satPivot });
  }

  /* Client sockets on a fibonacci shell */
  const N = small ? 110 : 190;
  const nodes = glowPoints(N, { size: 0.5 });
  const nodePos: THREE.Vector3[] = [];
  const nodeBase: number[] = [];
  const golden = Math.PI * (3 - Math.sqrt(5));
  for (let i = 0; i < N; i++) {
    const y = 1 - (i / (N - 1)) * 2;
    const rad = Math.sqrt(1 - y * y);
    const th = golden * i;
    const r = 3.6 + Math.random() * 2.2;
    const v = new THREE.Vector3(Math.cos(th) * rad * r, y * r * 0.82, Math.sin(th) * rad * r);
    nodePos.push(v);
    nodes.pos.setXYZ(i, v.x, v.y, v.z);
    const c = Math.random() < 0.14 ? PALETTE.fuchsia : Math.random() < 0.1 ? PALETTE.cyan : PALETTE.violetLight;
    nodes.col.setXYZ(i, c.r, c.g, c.b);
    const s = 0.6 + Math.random() * 0.8;
    nodeBase.push(s);
    nodes.size.setX(i, s);
  }
  rig.add(nodes.points);

  /* Curved links from core to a subset of sockets */
  const linkCount = small ? 46 : 84;
  const curves: { curve: THREE.QuadraticBezierCurve3; node: number }[] = [];
  const linkVerts: number[] = [];
  const linkCols: number[] = [];
  for (let i = 0; i < linkCount; i++) {
    const node = Math.floor((i / linkCount) * N + Math.random() * (N / linkCount));
    const end = nodePos[node % N];
    const dir = end.clone().normalize();
    const start = dir.clone().multiplyScalar(1.3);
    const ctrl = start
      .clone()
      .lerp(end, 0.5)
      .add(new THREE.Vector3().randomDirection().multiplyScalar(0.9))
      .multiplyScalar(1.12);
    const curve = new THREE.QuadraticBezierCurve3(start, ctrl, end);
    curves.push({ curve, node: node % N });
    const pts = curve.getPoints(24);
    for (let k = 0; k < pts.length - 1; k++) {
      linkVerts.push(pts[k].x, pts[k].y, pts[k].z, pts[k + 1].x, pts[k + 1].y, pts[k + 1].z);
      const a = 1 - k / pts.length;
      const b = 1 - (k + 1) / pts.length;
      linkCols.push(0.55 * a, 0.35 * a, 1 * a, 0.55 * b, 0.35 * b, 1 * b);
    }
  }
  const linkGeo = new THREE.BufferGeometry();
  linkGeo.setAttribute('position', new THREE.Float32BufferAttribute(linkVerts, 3));
  linkGeo.setAttribute('color', new THREE.Float32BufferAttribute(linkCols, 3));
  const links = new THREE.LineSegments(
    linkGeo,
    new THREE.LineBasicMaterial({ vertexColors: true, transparent: true, opacity: 0.35, blending: THREE.AdditiveBlending, depthWrite: false }),
  );
  rig.add(links);

  /* Packets with trails */
  const P = small ? 36 : 64;
  const TRAIL = 6;
  const packets = glowPoints(P * TRAIL, { size: 0.55 });
  rig.add(packets.points);
  type Packet = { curve: number; t: number; speed: number; inbound: boolean; color: THREE.Color };
  const pk: Packet[] = [];
  const spawn = (p?: Packet): Packet => {
    const inbound = Math.random() < 0.28;
    const np: Packet = {
      curve: Math.floor(Math.random() * curves.length),
      t: p ? 0 : Math.random(),
      speed: 0.35 + Math.random() * 0.5,
      inbound,
      color: inbound ? PALETTE.cyan : Math.random() < 0.3 ? PALETTE.fuchsia : PALETTE.white,
    };
    return np;
  };
  for (let i = 0; i < P; i++) pk.push(spawn());
  const flash = new Float32Array(N);
  const tmp = new THREE.Vector3();

  /* Starfield */
  const S = small ? 600 : 1400;
  const stars = glowPoints(S, { size: 0.3, opacity: 0.7 });
  for (let i = 0; i < S; i++) {
    tmp.randomDirection().multiplyScalar(18 + Math.random() * 40);
    stars.pos.setXYZ(i, tmp.x, tmp.y, tmp.z);
    const c = 0.35 + Math.random() * 0.5;
    stars.col.setXYZ(i, c * 0.85, c * 0.75, c);
    stars.size.setX(i, 0.4 + Math.random() * 1.4);
  }
  scene.add(stars.points);

  /* Interaction */
  const pointer = { x: 0, y: 0, tx: 0, ty: 0 };
  const onPointer = (e: PointerEvent) => {
    pointer.tx = (e.clientX / innerWidth) * 2 - 1;
    pointer.ty = (e.clientY / innerHeight) * 2 - 1;
  };
  addEventListener('pointermove', onPointer, { passive: true });

  const layout = (w: number) => {
    world.position.x = w > 1100 ? 3.4 : w > 760 ? 1.8 : 0;
    world.position.y = w > 760 ? 0 : 1.2;
    world.scale.setScalar(w > 760 ? 1 : 0.85);
  };
  layout(stage.size.w);
  stage.onResize(layout);

  let scrollP = 0;
  const onScroll = () => (scrollP = Math.min(1, scrollY / (canvas.parentElement!.clientHeight || 1)));
  if (scrollLinked) addEventListener('scroll', onScroll, { passive: true });

  stage.onFrame((t, dt) => {
    pointer.x += (pointer.tx - pointer.x) * 0.04;
    pointer.y += (pointer.ty - pointer.y) * 0.04;

    rig.rotation.y = t * 0.06 + pointer.x * 0.35 + scrollP * 1.2;
    rig.rotation.x = 0.18 + pointer.y * 0.2 + scrollP * 0.4;
    camera.position.z = (small ? 15 : 12.5) + scrollP * 5;
    stars.points.rotation.y = t * 0.008 + pointer.x * 0.05;
    stars.points.rotation.x = pointer.y * 0.03;

    ico.rotation.y = t * 0.25;
    ico.rotation.z = t * 0.12;
    inner.rotation.x = -t * 0.6;
    inner.rotation.y = t * 0.4;
    const pulse = 1 + Math.sin(t * 2.2) * 0.04;
    shell.scale.setScalar(pulse);
    heart.size.setX(0, 1 + Math.sin(t * 3) * 0.18);
    heart.size.needsUpdate = true;
    rings.forEach((r) => (r.sat.rotation.z = t * r.speed * 2));

    for (let i = 0; i < P; i++) {
      const p = pk[i];
      p.t += dt * p.speed;
      if (p.t >= 1) {
        if (!p.inbound) flash[curves[p.curve].node] = 1;
        pk[i] = spawn(p);
        continue;
      }
      const curve = curves[p.curve].curve;
      for (let k = 0; k < TRAIL; k++) {
        const tt = Math.max(0, p.t - k * 0.018);
        curve.getPoint(p.inbound ? 1 - tt : tt, tmp);
        const idx = i * TRAIL + k;
        packets.pos.setXYZ(idx, tmp.x, tmp.y, tmp.z);
        const fade = 1 - k / TRAIL;
        packets.col.setXYZ(idx, p.color.r * fade, p.color.g * fade, p.color.b * fade);
        packets.size.setX(idx, (k === 0 ? 1.15 : 0.75) * fade);
      }
    }
    packets.pos.needsUpdate = packets.col.needsUpdate = packets.size.needsUpdate = true;

    for (let i = 0; i < N; i++) {
      flash[i] *= 0.93;
      nodes.size.setX(i, nodeBase[i] * (1 + flash[i] * 2.4) * (1 + Math.sin(t * 1.5 + i) * 0.08));
    }
    nodes.size.needsUpdate = true;
  });

  if (reduceMotion()) stage.renderOnce();

  return () => {
    removeEventListener('pointermove', onPointer);
    removeEventListener('scroll', onScroll);
    stage.dispose();
  };
}
