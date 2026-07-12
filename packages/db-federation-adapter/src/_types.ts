/**
 * Local type declarations for db-federation types used by adapters.
 *
 * These define the narrow contract that db-federation-adapter
 * expects from its federation node dependency. In production,
 * FederationManager implements IFederationNode.
 *
 * This indirection avoids cross-project rootDir TypeScript issues
 * while keeping the adapter fully type-safe.
 */

/** Minimal federation node contract consumed by adapters. */
export interface IFederationNode {
  /** Whether the federation manager has started. */
  isStarted(): boolean;
  /** Get the node's DC/region identifier. */
  getDc(): string;
  /** Get the node's unique identifier. */
  getNodeId(): string;
  /** Get the current federation cluster health. */
  getHealth(): Promise<IFederationNodeHealth>;
}

/** Per-target health from the federation health monitor. */
export interface ITargetHealth {
  targetNodeId: string;
  connected: boolean;
  lagMs: number;
  pendingCount: number;
  lastSuccessAt: number | null;
  lastErrorAt: number | null;
  lastErrorMessage: string | null;
}

/** Aggregate federation health status. */
export interface IFederationNodeHealth {
  overall: 'healthy' | 'degraded' | 'critical' | 'unknown';
  targets: ITargetHealth[];
  totalTargets: number;
  healthyTargets: number;
  criticalTargets: number;
  lastError: string | null;
  checkedAt: number;
}
