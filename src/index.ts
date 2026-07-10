// Transport
export type { ITransport } from "./interfaces/transport/ITransport";
export type { TransportMessage } from "./interfaces/transport/TransportMessage";
export type { InboundMessage } from "./interfaces/transport/InboundMessage";
export { InProcessTransport } from "./transport/InProcessTransport";
export type { InProcessTransportConfig } from "./transport/InProcessTransport";

// Outbox
export type { IOutboxStore } from "./interfaces/outbox/IOutboxStore";
export type { OutboxEntry, OutboxStatus } from "./interfaces/outbox/OutboxEntry";
export { OutboxStore } from "./storage/OutboxStore";
export type { OutboxStoreConfig } from "./storage/OutboxStore";

// HLC
export type { IHLCTimestamp } from "./interfaces/hlc/IHLCTimestamp";
export { HLC, compareHLC, happenedBefore, hlcToKey, keyToHlc, makeHLC } from "./hlc/HLC";

// Shared types
export type { PeerInfo, TransportStats, OutboxStats, HLCStatus } from "./interfaces/types";

// Replication
export type { TopologyType } from "./interfaces/replication/IMultiLeaderReplication";
export type { ReplicationTarget } from "./interfaces/replication/IMultiLeaderReplication";
export type { ReplicationConfig } from "./interfaces/replication/IMultiLeaderReplication";
export type { ReplicationStatus } from "./interfaces/replication/IMultiLeaderReplication";
export type { IMultiLeaderReplication } from "./interfaces/replication/IMultiLeaderReplication";
export { MultiLeaderReplication } from "./replication/MultiLeaderReplication";
export type { MultiLeaderReplicationOptions } from "./replication/MultiLeaderReplication";

export type { CDCChangeEvent } from "./interfaces/replication/ICDCManager";
export type { CDCSubscription } from "./interfaces/replication/ICDCManager";
export type { ICDCEngine } from "./interfaces/replication/ICDCManager";
export type { ICDCPublication } from "./interfaces/replication/ICDCManager";
export type { ICDCManager } from "./interfaces/replication/ICDCManager";

export type { ReplicationSlot } from "./interfaces/replication/IReplicationSlotManager";
export type { IReplicationSlotManager } from "./interfaces/replication/IReplicationSlotManager";

export type { AntiEntropyConfig } from "./interfaces/replication/IAntiEntropy";
export type { SyncResult } from "./interfaces/replication/IAntiEntropy";
export type { IAntiEntropy } from "./interfaces/replication/IAntiEntropy";

// DedupRing
export { DedupRing } from "./replication/DedupRing";
export type { DedupRingConfig } from "./replication/DedupRing";

// Topology
export { TopologyManager } from "./topology/TopologyManager";
export type { TopologyConfig, TopologyChangeEvent } from "./topology/TopologyManager";

// HealthMonitor
export { HealthMonitor } from "./monitoring/HealthMonitor";
export type { HealthMonitorConfig, TargetHealth, FederationHealth } from "./monitoring/HealthMonitor";

// FederationManager
export { FederationManager, NOT_IMPLEMENTED_YET } from "./core/FederationManager";
export type {
  FederationManagerConfig,
  IFederationConsumer,
  ReplicatedEntry,
  ConflictEntry,
} from "./core/FederationManager";
