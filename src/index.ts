// Transport
export type { ITransport } from "./interfaces/transport/ITransport";
export type { TransportMessage } from "./interfaces/transport/TransportMessage";
export type { InboundMessage } from "./interfaces/transport/InboundMessage";

// Outbox
export type { IOutboxStore } from "./interfaces/outbox/IOutboxStore";
export type { OutboxEntry, OutboxStatus } from "./interfaces/outbox/OutboxEntry";

// HLC
export type { IHLCTimestamp } from "./interfaces/hlc/IHLCTimestamp";

// Shared types
export type { PeerInfo, TransportStats, OutboxStats, HLCStatus } from "./interfaces/types";

// Replication
export type { TopologyType } from "./interfaces/replication/IMultiLeaderReplication";
export type { ReplicationTarget } from "./interfaces/replication/IMultiLeaderReplication";
export type { ReplicationConfig } from "./interfaces/replication/IMultiLeaderReplication";
export type { ReplicationStatus } from "./interfaces/replication/IMultiLeaderReplication";
export type { IMultiLeaderReplication } from "./interfaces/replication/IMultiLeaderReplication";

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
