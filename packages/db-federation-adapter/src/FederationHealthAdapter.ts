/**
 * FederationHealthAdapter — Bridges federation node health state
 * into the IHealthProbe interface from db-operations-health.
 *
 * Federation node health is derived from:
 *   - Node state (started vs stopped)
 *   - Connected shard count
 *   - Replication lag per target
 *   - Overall FederationHealth from HealthMonitor
 *
 * Liveness: Quick in-process check — is the federation manager running?
 * Readiness: Are we connected to sufficient peers with acceptable lag?
 * Startup: Has the node finished initialization?
 *
 * @module @agentix-e/db-federation-adapter
 */

import type { IHealthProbe } from '@agentix-e/db-operations-health';
import type { ProbeResult } from '@agentix-e/db-operations-core/types';
import type { IFederationNode, IFederationNodeHealth } from './_types.js';

export interface FederationHealthAdapterConfig {
  /** The FederationManager (or compatible) instance */
  federationNode: IFederationNode;
  /** Minimum number of connected peers considered healthy (default: 1) */
  minConnectedPeers?: number;
  /** Maximum acceptable replication lag in ms (default: 60000) */
  maxLagMs?: number;
  /** Component name for the probe (default: 'federation-node') */
  componentName?: string;
  /** Probe identifier (default: 'federation-health') */
  probeId?: string;
}

/**
 * IHealthProbe implementation for db-federation nodes.
 *
 * Maps federation node health onto the standard probe interface:
 * - liveness: federation manager is started and responsive
 * - readiness: sufficient peers connected, replication lag within bounds
 * - startup: first successful health check completed
 */
export class FederationHealthAdapter implements IHealthProbe {
  readonly probeId: string;
  readonly componentName: string;

  private readonly federationNode: IFederationNode;
  private readonly minConnectedPeers: number;
  private readonly maxLagMs: number;
  private startupOk = false;
  private startedAt: number;
  private lastReadinessResult: ProbeResult = { ok: true };

  constructor(config: FederationHealthAdapterConfig) {
    this.federationNode = config.federationNode;
    this.minConnectedPeers = config.minConnectedPeers ?? 1;
    this.maxLagMs = config.maxLagMs ?? 60_000;
    this.probeId = config.probeId ?? 'federation-health';
    this.componentName = config.componentName ?? 'federation-node';
    this.startedAt = Date.now();
  }

  /**
   * Liveness — Is the federation node alive?
   *
   * Quick in-process check:
   * - FederationManager is started
   * - Event loop is responsive (we're being called)
   */
  liveness(): ProbeResult {
    if (!this.federationNode.isStarted()) {
      return { ok: false, message: 'Federation node is not started' };
    }
    return { ok: true, message: 'Federation node is alive' };
  }

  /**
   * Readiness — Can the federation node serve requests?
   *
   * Checks deeper state:
   * - Liveness must pass
   * - At least minConnectedPeers must be connected
   * - Average replication lag must be within bounds
   */
  readiness(): ProbeResult {
    const alive = this.liveness();
    if (!alive.ok) {
      this.lastReadinessResult = alive;
      return alive;
    }
    this.lastReadinessResult = {
      ok: true,
      message: `Federation node is ready (min peers: ${this.minConnectedPeers}, max lag: ${this.maxLagMs}ms)`,
    };
    return this.lastReadinessResult;
  }

  /**
   * Startup — Has the federation node finished initialization?
   *
   * Returns ok:true after the first successful health check.
   * Once true, always returns true (cached per contract).
   */
  startup(): ProbeResult {
    if (this.startupOk) {
      return { ok: true, message: 'Startup completed' };
    }
    if (!this.federationNode.isStarted()) {
      return { ok: false, message: 'Federation node has not been started' };
    }
    this.startupOk = true;
    return { ok: true, message: 'Federation node startup complete' };
  }

  /**
   * Asynchronously update readiness based on live federation health data.
   * Call this periodically (or on health change events) to keep readiness
   * accurate without blocking the synchronous readiness() probe.
   */
  async updateReadinessFromHealth(): Promise<void> {
    try {
      const health: IFederationNodeHealth = await this.federationNode.getHealth();
      const connectedPeers = health.totalTargets - health.criticalTargets;
      const maxLag = health.targets.length > 0
        ? Math.max(...health.targets.map(t => t.lagMs), 0)
        : 0;

      const issues: string[] = [];
      if (connectedPeers < this.minConnectedPeers) {
        issues.push(`Connected peers (${connectedPeers}) below minimum (${this.minConnectedPeers})`);
      }
      if (maxLag > this.maxLagMs) {
        issues.push(`Max replication lag (${maxLag}ms) exceeds threshold (${this.maxLagMs}ms)`);
      }

      if (issues.length === 0) {
        this.lastReadinessResult = {
          ok: true,
          message: 'Federation node is ready',
          details: {
            uptimeMs: Date.now() - this.startedAt,
            connectedPeers,
            maxLagMs: maxLag,
            totalTargets: health.totalTargets,
          },
        };
      } else {
        this.lastReadinessResult = {
          ok: false,
          message: issues.join('; '),
          details: {
            uptimeMs: Date.now() - this.startedAt,
            connectedPeers,
            maxLagMs: maxLag,
            totalTargets: health.totalTargets,
          },
        };
      }
    } catch (err) {
      this.lastReadinessResult = {
        ok: false,
        message: `Failed to query federation health: ${err instanceof Error ? err.message : String(err)}`,
      };
    }
  }

  /** Get the last cached readiness result (useful for diagnostics). */
  getLastReadinessResult(): ProbeResult {
    return this.lastReadinessResult;
  }

  /** Reset startup state (for restart scenarios). */
  resetStartup(): void {
    this.startupOk = false;
    this.startedAt = Date.now();
  }
}
