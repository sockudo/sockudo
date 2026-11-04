import * as THREE from 'three';
import { PALETTE, createStage, fresnelMaterial, glowPoints, isSmall, reduceMotion } from './stage';

/**
 * Scroll-driven 3D architecture diagram: HTTP publish → node → adapter bus →
 * every node → every socket, plus durable-history recovery for a reconnecting client.
 * Steps are cumulative; `setStep` is called by the page as the narrative scrolls.
 */
export function mountFanout(canvas: HTMLCanvasElement, labelsRoot: HTMLElement) {
  const small = isSmall();
  const stage = createStage({ canvas, fov: 38, bloom: { strength: 0.6, radius: 0.5, threshold: 0.12 } });
  const { scene, camera } = stage;

  const root = new THREE.Group();
  scene.add(root);

  /* Floor grid */
  const grid = new THREE.GridHelper(30, 30, PALETTE.violet, PALETTE.deep);
  (grid.material as THREE.Material).transparent = true;
  (grid.material as THREE.Material).opacity = 0.18;
  grid.position.y = -2.6;
  root.add(grid);

  const ringY = -1.7;
  const ringR = 3.6;
  const ringC = new THREE.Vector3(0.4, ringY, 0);

  /* Nodes: hexagonal prisms with fresnel glass */
  const nodeAngles = [Math.PI * 0.95, Math.PI * 0.28, -Math.PI * 0.38];
  const nodeNames = ['node-a', 'node-b', 'node-c'];
  const nodes: { pos: THREE.Vector3; mesh: THREE.Group; angle: number; core: THREE.Mesh }[] = [];
  for (const a of nodeAngles) {
    const pos = new THREE.Vector3(ringC.x + Math.cos(a) * ringR, 0, Math.sin(a) * ringR);
    const g = new THREE.Group();
    g.position.copy(pos);
    const geo = new THREE.CylinderGeometry(0.8, 0.8, 1.1, 6);
    const glass = new THREE.Mesh(geo, fresnelMaterial(PALETTE.violet.clone(), 1.6, 1.6));
    const edges = new THREE.LineSegments(
      new THREE.EdgesGeometry(geo),
      new THREE.LineBasicMaterial({ color: PALETTE.violetLight, transparent: true, opacity: 0.85 }),
    );
    const core = new THREE.Mesh(new THREE.OctahedronGeometry(0.28), new THREE.MeshBasicMaterial({ color: '#e9dcff', wireframe: true }));
    g.add(glass, edges, core);
    root.add(g);
    // vertical tether to the adapter ring
    const tether = new THREE.Mesh(
      new THREE.CylinderGeometry(0.015, 0.015, Math.abs(ringY) - 0.55, 6),
      new THREE.MeshBasicMaterial({ color: PALETTE.violetLight, transparent: true, opacity: 0.4 }),
    );
    tether.position.set(pos.x, (ringY - 0.55) / 2, pos.z);
    root.add(tether);
    nodes.push({ pos, mesh: g, angle: a, core });
  }

  /* Adapter bus ring */
  const ring = new THREE.Mesh(
    new THREE.TorusGeometry(ringR, 0.05, 12, 200),
    new THREE.MeshBasicMaterial({ color: PALETTE.fuchsia, transparent: true, opacity: 0.55 }),
  );
  ring.rotation.x = Math.PI / 2;
  ring.position.copy(ringC);
  root.add(ring);
  const ringGlow = new THREE.Mesh(
    new THREE.TorusGeometry(ringR, 0.22, 12, 200),
    new THREE.MeshBasicMaterial({ color: PALETTE.violet, transparent: true, opacity: 0.08, blending: THREE.AdditiveBlending, depthWrite: false }),
  );
  ringGlow.rotation.x = Math.PI / 2;
  ringGlow.position.copy(ringC);
  root.add(ringGlow);

  /* Publisher box */
  const pubPos = new THREE.Vector3(-6.6, 1.4, 1.2);
  const pubGeo = new THREE.BoxGeometry(1.3, 0.8, 0.8);
  const pub = new THREE.Group();
  pub.position.copy(pubPos);
  pub.add(new THREE.Mesh(pubGeo, fresnelMaterial(PALETTE.cyan.clone(), 1.8, 1.4)));
  pub.add(new THREE.LineSegments(new THREE.EdgesGeometry(pubGeo), new THREE.LineBasicMaterial({ color: PALETTE.cyan, transparent: true, opacity: 0.9 })));
  root.add(pub);

  /* Durable history store: stacked discs */
  const storePos = new THREE.Vector3(ringC.x + ringR + 3, -1.3, -0.4);
  const store = new THREE.Group();
  store.position.copy(storePos);
  for (let i = 0; i < 3; i++) {
    const dg = new THREE.CylinderGeometry(0.75, 0.75, 0.32, 40);
    const disc = new THREE.Mesh(dg, fresnelMaterial(PALETTE.fuchsia.clone(), 1.5, 1.3));
    disc.position.y = i * 0.42;
    const e = new THREE.LineSegments(new THREE.EdgesGeometry(dg, 20), new THREE.LineBasicMaterial({ color: PALETTE.fuchsia, transparent: true, opacity: 0.7 }));
    e.position.y = i * 0.42;
    store.add(disc, e);
  }
  root.add(store);
  store.visible = false;

  /* Clients: a halo of sockets above each node */
  const perNode = small ? 5 : 7;
  const clients = glowPoints(nodes.length * perNode, { size: 0.6 });
  const clientPos: THREE.Vector3[] = [];
  nodes.forEach((n, ni) => {
    for (let k = 0; k < perNode; k++) {
      const a = (k / perNode) * Math.PI * 2 + ni;
      const v = new THREE.Vector3(n.pos.x + Math.cos(a) * 1.35, 2.0 + Math.sin(a * 2) * 0.18, n.pos.z + Math.sin(a) * 1.35);
      clientPos.push(v);
      const i = ni * perNode + k;
      clients.pos.setXYZ(i, v.x, v.y, v.z);
      clients.col.setXYZ(i, PALETTE.violetLight.r, PALETTE.violetLight.g, PALETTE.violetLight.b);
    }
  });
  root.add(clients.points);
  const clientLinks = new THREE.LineSegments(
    new THREE.BufferGeometry().setFromPoints(clientPos.flatMap((c, i) => [nodes[Math.floor(i / perNode)].pos.clone().setY(0.55), c])),
    new THREE.LineBasicMaterial({ color: PALETTE.violetLight, transparent: true, opacity: 0.16, blending: THREE.AdditiveBlending }),
  );
  root.add(clientLinks);

  /* Paths */
  const ringPoint = (a: number, y = ringY) => new THREE.Vector3(ringC.x + Math.cos(a) * ringR, y, Math.sin(a) * ringR);
  const arc = (a0: number, a1: number, steps = 18) => {
    let d = a1 - a0;
    while (d > Math.PI) d -= Math.PI * 2;
    while (d < -Math.PI) d += Math.PI * 2;
    return Array.from({ length: steps + 1 }, (_, i) => ringPoint(a0 + (d * i) / steps));
  };
  const A = nodes[0];
  const paths = {
    publish: new THREE.CatmullRomCurve3([pubPos.clone().add(new THREE.Vector3(0.65, 0, 0)), new THREE.Vector3(-5, 1.5, 1.4), A.pos.clone().add(new THREE.Vector3(-0.3, 0.3, 0))]),
    fanout: nodes.slice(1).map(
      (n) => new THREE.CatmullRomCurve3([A.pos.clone().setY(-0.55), ...arc(A.angle, n.angle), n.pos.clone().setY(-0.55)], false, 'catmullrom', 0.1),
    ),
    deliver: clientPos.map((c, i) => {
      const n = nodes[Math.floor(i / perNode)].pos;
      return new THREE.QuadraticBezierCurve3(n.clone().setY(0.55), n.clone().lerp(c, 0.5).setY(1.9), c);
    }),
    recover: [] as THREE.Curve<THREE.Vector3>[],
  };
  const lostClient = 2 * perNode + 2;
  {
    const c = clientPos[lostClient];
    paths.recover.push(
      new THREE.CatmullRomCurve3([storePos.clone().add(new THREE.Vector3(0, 1.1, 0)), new THREE.Vector3((storePos.x + c.x) / 2, 3.2, (storePos.z + c.z) / 2), c]),
    );
  }

  /* Packet pool */
  const MAX = 160;
  const TRAIL = 5;
  const pk = glowPoints(MAX * TRAIL, { size: 0.55 });
  root.add(pk.points);
  type P = { curve: THREE.Curve<THREE.Vector3>; t: number; speed: number; color: THREE.Color; onDone?: () => void };
  const live: P[] = [];
  const emit = (curve: THREE.Curve<THREE.Vector3>, color: THREE.Color, speed = 0.7, onDone?: () => void) => {
    if (live.length < MAX) live.push({ curve, t: 0, speed, color, onDone });
  };

  let step = -1;
  let acc = 0;
  let lostState = 0; // 0 connected, 1 dropped, 2 recovering
  let lostTimer = 0;
  const clientFlash = new Float32Array(clientPos.length);

  const publishCycle = () => {
    emit(paths.publish, PALETTE.cyan, 0.75, () => {
      flashNode(0);
      if (step >= 1)
        paths.fanout.forEach((c, i) =>
          emit(c, PALETTE.fuchsia, 0.62, () => {
            flashNode(i + 1);
            if (step >= 2) deliverFrom(i + 1);
          }),
        );
      if (step >= 2) deliverFrom(0);
    });
  };
  const nodeFlash = [0, 0, 0];
  const flashNode = (i: number) => (nodeFlash[i] = 1);
  const deliverFrom = (ni: number) => {
    for (let k = 0; k < perNode; k++) {
      const idx = ni * perNode + k;
      if (idx === lostClient && lostState === 1) continue;
      emit(paths.deliver[idx], PALETTE.white, 1.3 + Math.random() * 0.4, () => (clientFlash[idx] = 1));
    }
  };

  /* Camera choreography */
  const cams = small
    ? [
        { p: new THREE.Vector3(-2, 6, 16), l: new THREE.Vector3(-1, 0, 0) },
        { p: new THREE.Vector3(1, 10, 15), l: new THREE.Vector3(0.5, -1, 0) },
        { p: new THREE.Vector3(3, 7, 15), l: new THREE.Vector3(0.5, 0.5, 0) },
        { p: new THREE.Vector3(6, 7, 15), l: new THREE.Vector3(2.5, 0, 0) },
      ]
    : [
        { p: new THREE.Vector3(-5.6, 4.4, 11.5), l: new THREE.Vector3(-3.9, 0.3, 0.4) },
        { p: new THREE.Vector3(0.5, 8.5, 10.5), l: new THREE.Vector3(0.4, -1, 0) },
        { p: new THREE.Vector3(3, 5.2, 11.5), l: new THREE.Vector3(0.6, 0.6, 0) },
        { p: new THREE.Vector3(7, 5, 10.5), l: new THREE.Vector3(3.4, 0, 0) },
      ];
  const camTarget = { p: cams[0].p.clone(), l: cams[0].l.clone() };
  const look = cams[0].l.clone();
  camera.position.copy(cams[0].p);

  /* HTML labels projected from 3D anchors */
  const anchors: Record<string, THREE.Vector3> = {
    publisher: pubPos.clone().add(new THREE.Vector3(0, 0.75, 0)),
    'node-a': A.pos.clone().add(new THREE.Vector3(0, -1.0, 0)),
    'node-b': nodes[1].pos.clone().add(new THREE.Vector3(0, -1.0, 0)),
    'node-c': nodes[2].pos.clone().add(new THREE.Vector3(0, -1.0, 0)),
    adapter: ringPoint(Math.PI / 2 + 0.3, ringY - 0.35),
    clients: clientPos[perNode + 3].clone().add(new THREE.Vector3(0, 0.5, 0)),
    history: storePos.clone().add(new THREE.Vector3(0, -0.6, 0)),
  };
  void nodeNames;
  const labelEls = [...labelsRoot.querySelectorAll<HTMLElement>('[data-anchor]')];
  const v = new THREE.Vector3();

  stage.onFrame((t, dt) => {
    // camera
    camera.position.lerp(camTarget.p, 0.035);
    look.lerp(camTarget.l, 0.035);
    camera.position.x += Math.sin(t * 0.3) * 0.004;
    camera.lookAt(look);

    nodes.forEach((n, i) => {
      n.core.rotation.y = t * 1.2 + i;
      n.core.rotation.x = t * 0.7;
      nodeFlash[i] *= 0.92;
      n.mesh.scale.setScalar(1 + nodeFlash[i] * 0.12);
    });
    ring.rotation.z = t * 0.1;
    (ringGlow.material as THREE.MeshBasicMaterial).opacity = 0.06 + (step >= 1 ? 0.06 + Math.sin(t * 3) * 0.03 : 0);

    // emission
    if (step >= 0) {
      acc += dt;
      const every = 1.25;
      if (acc > every) {
        acc = 0;
        publishCycle();
      }
    }

    // recovery choreography
    if (step >= 3) {
      lostTimer += dt;
      if (lostState === 0 && lostTimer > 1.2) {
        lostState = 1;
        lostTimer = 0;
      } else if (lostState === 1 && lostTimer > 2.2) {
        lostState = 2;
        lostTimer = 0;
        for (let k = 0; k < 4; k++) setTimeout(() => emit(paths.recover[0], PALETTE.fuchsia, 0.9, () => (clientFlash[lostClient] = 1)), k * 140);
      } else if (lostState === 2 && lostTimer > 2.6) {
        lostState = 0;
        lostTimer = -1.5;
      }
    } else {
      lostState = 0;
      lostTimer = 0;
    }

    // packets
    let w = 0;
    for (let i = live.length - 1; i >= 0; i--) {
      const p = live[i];
      p.t += dt * p.speed;
      if (p.t >= 1) {
        live.splice(i, 1);
        p.onDone?.();
      }
    }
    for (const p of live) {
      for (let k = 0; k < TRAIL; k++) {
        p.curve.getPoint(Math.max(0, p.t - k * 0.025), v);
        const f = 1 - k / TRAIL;
        pk.pos.setXYZ(w, v.x, v.y, v.z);
        pk.col.setXYZ(w, p.color.r * f, p.color.g * f, p.color.b * f);
        pk.size.setX(w, (k === 0 ? 1.1 : 0.7) * f);
        w++;
      }
    }
    for (; w < MAX * TRAIL; w++) pk.size.setX(w, 0);
    pk.pos.needsUpdate = pk.col.needsUpdate = pk.size.needsUpdate = true;

    // clients
    for (let i = 0; i < clientPos.length; i++) {
      clientFlash[i] *= 0.9;
      let c = PALETTE.violetLight;
      let s = 0.8 + clientFlash[i] * 1.4;
      if (i === lostClient && step >= 3) {
        if (lostState === 1) {
          c = new THREE.Color('#ff5d8f');
          s = 0.55 + Math.sin(t * 10) * 0.15;
        } else if (lostState === 2) c = PALETTE.fuchsia;
      }
      clients.col.setXYZ(i, c.r, c.g, c.b);
      clients.size.setX(i, s);
    }
    clients.col.needsUpdate = clients.size.needsUpdate = true;

    // labels
    for (const el of labelEls) {
      const a = anchors[el.dataset.anchor!];
      if (!a) continue;
      v.copy(a).applyMatrix4(root.matrixWorld).project(camera);
      const x = (v.x * 0.5 + 0.5) * stage.size.w;
      const y = (-v.y * 0.5 + 0.5) * stage.size.h;
      el.style.transform = `translate3d(${x}px, ${y}px, 0) translate(-50%, -50%)`;
    }
  });

  const setStep = (s: number) => {
    if (s === step) return;
    step = s;
    store.visible = s >= 3;
    const c = cams[Math.max(0, Math.min(cams.length - 1, s))];
    camTarget.p.copy(c.p);
    camTarget.l.copy(c.l);
    labelsRoot.dataset.step = String(s);
    if (reduceMotion()) {
      camera.position.copy(c.p);
      camera.lookAt(c.l);
      stage.renderOnce();
    }
  };
  setStep(0);

  return { setStep, dispose: () => stage.dispose() };
}
