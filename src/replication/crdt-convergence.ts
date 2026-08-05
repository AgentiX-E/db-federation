/**
 * CRDT Convergence Verifier — Property-based testing framework.
 *
 * Verifies the three CRDT laws for all CRDT types:
 *   - Associativity: merge(a, merge(b, c)) === merge(merge(a, b), c)
 *   - Commutativity: merge(a, b) === merge(b, a)
 *   - Idempotence: merge(a, a) === a
 *
 * Uses quickcheck-style random state generation with SeededRNG
 * for deterministic and reproducible convergence proofs.
 */

import {
  GCounter,
  PNCounter,
  LWWRegister,
  MVRegister,
  ORSet,
} from "./crdt-types";
import type {
  GCounterState,
  PNCounterState,
  LWWRegisterState,
  MVRegisterState,
  ORSetState,
} from "./crdt-types";

// ── Seeded RNG (Xorshift128+) ──────────────────────────────

export class SeededRNG {
  private s0: number;
  private s1: number;

  constructor(seed: number) {
    // Seed a 64-bit split into two 32-bit parts
    this.s0 = (seed * 0x9e3779b9) >>> 0;
    this.s1 = (seed * 0x85ebca6b) >>> 0;
    // Warm up
    for (let i = 0; i < 5; i++) this.next();
  }

  /** Returns a pseudo-random number in [0, 1). */
  next(): number {
    let s1 = this.s0;
    const s0 = this.s1;
    this.s0 = s0;
    s1 ^= s1 << 23;
    s1 ^= s1 >>> 17;
    s1 ^= s0;
    s1 ^= s0 >>> 26;
    this.s1 = s1;
    // Convert to [0,1)
    return ((s0 + s1) >>> 0) / 4294967296;
  }

  /** Returns an integer in [min, max] inclusive. */
  int(min: number, max: number): number {
    return Math.floor(this.next() * (max - min + 1)) + min;
  }

  /** Pick a random element from an array. */
  pick<T>(arr: T[]): T {
    return arr[this.int(0, arr.length - 1)]!;
  }

  /** Generate a random node ID. */
  nodeId(): string {
    const nodes = ["node-a", "node-b", "node-c", "node-d", "node-e"];
    return this.pick(nodes);
  }

  /** Generate a random element name. */
  element(): string {
    const prefixes = ["key", "item", "elem", "doc", "record"];
    return `${this.pick(prefixes)}-${this.int(0, 99)}`;
  }

  /** Generate a random tag (unique ID). */
  tag(): string {
    return `${this.nodeId()}-${this.int(1000, 9999)}-${this.int(0, 999)}`;
  }
}

// ── Convergence Verifier ────────────────────────────────────

export interface ConvergenceReport {
  crdtType: string;
  trials: number;
  associativePassed: number;
  associativeFailed: number;
  commutativePassed: number;
  commutativeFailed: number;
  idempotentPassed: number;
  idempotentFailed: number;
  failures: Array<{
    law: string;
    details: string;
  }>;
}

export class ConvergenceVerifier {
  private rng: SeededRNG;
  private readonly trials: number;

  constructor(seed = 42, trials = 500) {
    this.rng = new SeededRNG(seed);
    this.trials = trials;
  }

  // ── GCounter ──

  verifyGCounter(): ConvergenceReport {
    const report: ConvergenceReport = {
      crdtType: "GCounter",
      trials: this.trials,
      associativePassed: 0,
      associativeFailed: 0,
      commutativePassed: 0,
      commutativeFailed: 0,
      idempotentPassed: 0,
      idempotentFailed: 0,
      failures: [],
    };

    for (let i = 0; i < this.trials; i++) {
      const a = this.randomGCounter();
      const b = this.randomGCounter();
      const c = this.randomGCounter();

      // Associativity: merge(a, merge(b, c)) === merge(merge(a, b), c)
      const g1 = GCounter.fromState(a);
      g1.merge(b);
      g1.merge(c);
      const g2 = GCounter.fromState({ counts: {} });
      g2.merge(a);
      const g2b = GCounter.fromState(b);
      g2b.merge(c);
      g2.merge(g2b.toState());

      if (this.countersEqual(g1, g2)) {
        report.associativePassed++;
      } else {
        report.associativeFailed++;
        report.failures.push({ law: "associativity", details: `trial ${i}` });
      }

      // Commutativity: merge(a, b) === merge(b, a)
      const c1 = GCounter.fromState(a);
      c1.merge(b);
      const c2 = GCounter.fromState(b);
      c2.merge(a);
      if (this.countersEqual(c1, c2)) {
        report.commutativePassed++;
      } else {
        report.commutativeFailed++;
        report.failures.push({ law: "commutativity", details: `trial ${i}` });
      }

      // Idempotence: merge(a, a) === a
      const i1 = GCounter.fromState(a);
      i1.merge(a);
      if (this.countersEqual(i1, GCounter.fromState(a))) {
        report.idempotentPassed++;
      } else {
        report.idempotentFailed++;
        report.failures.push({ law: "idempotence", details: `trial ${i}` });
      }
    }

    return report;
  }

  // ── PNCounter ──

  verifyPNCounter(): ConvergenceReport {
    const report: ConvergenceReport = {
      crdtType: "PNCounter",
      trials: this.trials,
      associativePassed: 0,
      associativeFailed: 0,
      commutativePassed: 0,
      commutativeFailed: 0,
      idempotentPassed: 0,
      idempotentFailed: 0,
      failures: [],
    };

    for (let i = 0; i < this.trials; i++) {
      const a = this.randomPNCounter();
      const b = this.randomPNCounter();
      const c = this.randomPNCounter();

      // Associativity
      const g1 = PNCounter.fromState(a);
      g1.merge(b);
      g1.merge(c);
      const g2 = PNCounter.fromState({ p: {}, n: {} });
      g2.merge(a);
      const g2b = PNCounter.fromState(b);
      g2b.merge(c);
      g2.merge(g2b.toState());
      if (this.pnCountersEqual(g1, g2)) {
        report.associativePassed++;
      } else {
        report.associativeFailed++;
    }

      // Commutativity
      const c1 = PNCounter.fromState(a);
      c1.merge(b);
      const c2 = PNCounter.fromState(b);
      c2.merge(a);
      if (this.pnCountersEqual(c1, c2)) report.commutativePassed++;
      else report.commutativeFailed++;

      // Idempotence
      const i1 = PNCounter.fromState(a);
      i1.merge(a);
      if (this.pnCountersEqual(i1, PNCounter.fromState(a))) report.idempotentPassed++;
      else report.idempotentFailed++;
    }

    return report;
  }

  // ── LWWRegister ──

  verifyLWWRegister(): ConvergenceReport {
    const report: ConvergenceReport = {
      crdtType: "LWWRegister",
      trials: this.trials,
      associativePassed: 0,
      associativeFailed: 0,
      commutativePassed: 0,
      commutativeFailed: 0,
      idempotentPassed: 0,
      idempotentFailed: 0,
      failures: [],
    };

    for (let i = 0; i < this.trials; i++) {
      const a = this.randomLWWRegister();
      const b = this.randomLWWRegister();
      const c = this.randomLWWRegister();

      // Associativity
      const g1 = LWWRegister.fromState(a);
      g1.merge(b);
      g1.merge(c);
      const g2 = LWWRegister.fromState(a);
      const g2tmp = LWWRegister.fromState(b);
      g2tmp.merge(c);
      g2.merge(g2tmp.toState());
      if (g1.value === g2.value) report.associativePassed++;
      else report.associativeFailed++;

      // Commutativity
      const c1 = LWWRegister.fromState(a);
      c1.merge(b);
      const c2 = LWWRegister.fromState(b);
      c2.merge(a);
      if (c1.value === c2.value) report.commutativePassed++;
      else report.commutativeFailed++;

      // Idempotence
      const i1 = LWWRegister.fromState(a);
      i1.merge(a);
      if (i1.value === a.value) report.idempotentPassed++;
      else report.idempotentFailed++;
    }

    return report;
  }

  // ── MVRegister ──

  verifyMVRegister(): ConvergenceReport {
    const report: ConvergenceReport = {
      crdtType: "MVRegister",
      trials: this.trials,
      associativePassed: 0,
      associativeFailed: 0,
      commutativePassed: 0,
      commutativeFailed: 0,
      idempotentPassed: 0,
      idempotentFailed: 0,
      failures: [],
    };

    for (let i = 0; i < this.trials; i++) {
      const a = this.randomMVRegister();
      const b = this.randomMVRegister();
      const c = this.randomMVRegister();

      // Associativity
      const g1 = MVRegister.fromState(a);
      g1.merge(b);
      g1.merge(c);
      const g2 = MVRegister.fromState(a);
      const g2tmp = MVRegister.fromState(b);
      g2tmp.merge(c);
      g2.merge(g2tmp.toState());
      if (this.mvRegistersEqual(g1, g2)) report.associativePassed++;
      else report.associativeFailed++;

      // Commutativity
      const c1 = MVRegister.fromState(a);
      c1.merge(b);
      const c2 = MVRegister.fromState(b);
      c2.merge(a);
      if (this.mvRegistersEqual(c1, c2)) report.commutativePassed++;
      else report.commutativeFailed++;

      // Idempotence
      const i1 = MVRegister.fromState(a);
      i1.merge(a);
      if (this.mvRegistersEqual(i1, MVRegister.fromState(a))) report.idempotentPassed++;
      else report.idempotentFailed++;
    }

    return report;
  }

  // ── ORSet ──

  verifyORSet(): ConvergenceReport {
    const report: ConvergenceReport = {
      crdtType: "ORSet",
      trials: this.trials,
      associativePassed: 0,
      associativeFailed: 0,
      commutativePassed: 0,
      commutativeFailed: 0,
      idempotentPassed: 0,
      idempotentFailed: 0,
      failures: [],
    };

    for (let i = 0; i < this.trials; i++) {
      const a = this.randomORSet();
      const b = this.randomORSet();
      const c = this.randomORSet();

      // Associativity
      const g1 = ORSet.fromState(a);
      g1.merge(b);
      g1.merge(c);
      const g2 = ORSet.fromState(a);
      const g2tmp = ORSet.fromState(b);
      g2tmp.merge(c);
      g2.merge(g2tmp.toState());
      if (this.orSetsEqual(g1, g2)) report.associativePassed++;
      else report.associativeFailed++;

      // Commutativity
      const c1 = ORSet.fromState(a);
      c1.merge(b);
      const c2 = ORSet.fromState(b);
      c2.merge(a);
      if (this.orSetsEqual(c1, c2)) report.commutativePassed++;
      else report.commutativeFailed++;

      // Idempotence
      const i1 = ORSet.fromState(a);
      i1.merge(a);
      if (this.orSetsEqual(i1, ORSet.fromState(a))) report.idempotentPassed++;
      else report.idempotentFailed++;
    }

    return report;
  }

  // ── Run all verifications ──

  verifyAll(): Record<string, ConvergenceReport> {
    return {
      GCounter: this.verifyGCounter(),
      PNCounter: this.verifyPNCounter(),
      LWWRegister: this.verifyLWWRegister(),
      MVRegister: this.verifyMVRegister(),
      ORSet: this.verifyORSet(),
    };
  }

  // ── Helpers ──

  private randomGCounter(): GCounterState {
    const counts: Record<string, number> = {};
    const nodeCount = this.rng.int(1, 4);
    for (let i = 0; i < nodeCount; i++) {
      counts[this.rng.nodeId()] = this.rng.int(0, 100);
    }
    return { counts };
  }

  private randomPNCounter(): PNCounterState {
    const p: Record<string, number> = {};
    const n: Record<string, number> = {};
    const nodeCount = this.rng.int(1, 4);
    for (let i = 0; i < nodeCount; i++) {
      const node = this.rng.nodeId();
      p[node] = this.rng.int(0, 50);
      n[node] = this.rng.int(0, 20);
    }
    return { p, n };
  }

  private randomLWWRegister(): LWWRegisterState {
    return {
      value: this.rng.element(),
      hlcWallTime: this.rng.int(0, 10000),
      hlcLogical: this.rng.int(0, 100),
      nodeId: this.rng.nodeId(),
    };
  }

  private randomMVRegister(): MVRegisterState {
    const entries: MVRegisterState["entries"] = [];
    const entryCount = this.rng.int(0, 5);
    for (let i = 0; i < entryCount; i++) {
      entries.push({
        value: this.rng.element(),
        wallTime: this.rng.int(0, 5000),
        logical: this.rng.int(0, 50),
        nodeId: this.rng.nodeId(),
      });
    }
    return { entries };
  }

  private randomORSet(): ORSetState {
    const elements: Record<string, string[]> = {};
    const tombstones: Record<string, string[]> = {};
    const elemCount = this.rng.int(0, 10);
    for (let i = 0; i < elemCount; i++) {
      const elem = this.rng.element();
      const tagCount = this.rng.int(1, 3);
      elements[elem] = Array.from({ length: tagCount }, () => this.rng.tag());
    }
    // Randomly tombstone some elements
    for (const [elem, tags] of Object.entries(elements)) {
      if (this.rng.next() < 0.2) {
        tombstones[elem] = [...tags];
        delete elements[elem];
      }
    }
    return { elements, tombstones };
  }

  private countersEqual(a: GCounter, b: GCounter): boolean {
    const allKeys = new Set([
      ...Object.keys(a.counts),
      ...Object.keys(b.counts),
    ]);
    for (const k of allKeys) {
      if ((a.counts[k] ?? 0) !== (b.counts[k] ?? 0)) return false;
    }
    return true;
  }

  private pnCountersEqual(a: PNCounter, b: PNCounter): boolean {
    const as = a.toState();
    const bs = b.toState();
    const allNodes = new Set([
      ...Object.keys(as.p), ...Object.keys(as.n),
      ...Object.keys(bs.p), ...Object.keys(bs.n),
    ]);
    for (const n of allNodes) {
      if ((as.p[n] ?? 0) !== (bs.p[n] ?? 0)) return false;
      if ((as.n[n] ?? 0) !== (bs.n[n] ?? 0)) return false;
    }
    return true;
  }

  private mvRegistersEqual(a: MVRegister, b: MVRegister): boolean {
    const aVals = new Set(a.values());
    const bVals = new Set(b.values());
    if (aVals.size !== bVals.size) return false;
    for (const v of aVals) {
      if (!bVals.has(v)) return false;
    }
    return true;
  }

  private orSetsEqual(a: ORSet, b: ORSet): boolean {
    const aMems = new Set(a.members());
    const bMems = new Set(b.members());
    return aMems.size === bMems.size && [...aMems].every((m) => bMems.has(m));
  }
}
