/**
 * @agentix-e/db-federation-adapter — db-operations adapters for db-federation.
 *
 * Exports four adapter classes that bridge db-federation types into
 * the db-operations canonical interfaces.
 *
 * @module @agentix-e/db-federation-adapter
 */

export {
  FederationHealthAdapter,
  type FederationHealthAdapterConfig,
} from './FederationHealthAdapter.js';

export {
  FederationMetricAdapter,
  type FederationMetricAdapterConfig,
} from './FederationMetricAdapter.js';

export {
  FederationAuditAdapter,
  type FederationAuditAdapterConfig,
  type FederationAuditEventType,
  type FederationAuditEventBase,
} from './FederationAuditAdapter.js';

export {
  FederationTracerAdapter,
  type FederationTracerAdapterConfig,
} from './FederationTracerAdapter.js';

export type {
  IFederationNode,
  IFederationNodeHealth,
  ITargetHealth,
} from './_types.js';
