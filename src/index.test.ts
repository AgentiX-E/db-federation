import { describe, it, expect, vi } from "vitest";
import type {
  ITransport,
  TransportMessage,
  InboundMessage,
  IOutboxStore,
  OutboxEntry,
  IHLCTimestamp,
  PeerInfo,
  TransportStats,
  OutboxStats,
  HLCStatus,
} from "./index";

describe("@agentix-e/db-federation types", () => {
  it("should export ITransport interface", () => {
    const transport: ITransport = {
      send: vi.fn().mockResolvedValue(true),
      broadcast: vi.fn().mockResolvedValue(1),
      onMessage: vi.fn().mockReturnValue(vi.fn()),
      getConnectedPeers: vi.fn().mockReturnValue([]),
      getStats: vi.fn().mockReturnValue({} as TransportStats),
    };
    expect(typeof transport.send).toBe("function");
    expect(typeof transport.broadcast).toBe("function");
    expect(typeof transport.onMessage).toBe("function");
    expect(typeof transport.getConnectedPeers).toBe("function");
    expect(typeof transport.getStats).toBe("function");
  });

  it("should export IOutboxStore interface", () => {
    const store: IOutboxStore = {
      add: vi.fn(),
      fetchOldestPending: vi.fn().mockResolvedValue([]),
      markDelivered: vi.fn().mockResolvedValue(undefined),
      getPendingCount: vi.fn().mockResolvedValue(0),
      getStats: vi.fn().mockReturnValue({} as OutboxStats),
    };
    expect(typeof store.add).toBe("function");
    expect(typeof store.fetchOldestPending).toBe("function");
    expect(typeof store.markDelivered).toBe("function");
    expect(typeof store.getPendingCount).toBe("function");
    expect(typeof store.getStats).toBe("function");
  });

  it("should validate TransportMessage structure", () => {
    const msg: TransportMessage = {
      messageId: "msg-1",
      sourcePeerId: "peer-a",
      targetPeerId: "peer-b",
      payload: new Uint8Array([1, 2, 3]),
      timestamp: { wallTime: BigInt(0), logical: 0, nodeId: "n1" } as IHLCTimestamp,
      createdAt: Date.now(),
    };
    expect(msg.messageId).toBe("msg-1");
    expect(msg.payload).toBeInstanceOf(Uint8Array);
  });

  it("should validate InboundMessage extends TransportMessage", () => {
    const msg: InboundMessage = {
      messageId: "msg-2",
      sourcePeerId: "peer-a",
      targetPeerId: "peer-b",
      payload: new Uint8Array([]),
      timestamp: { wallTime: BigInt(0), logical: 0, nodeId: "n1" } as IHLCTimestamp,
      createdAt: Date.now(),
      receivedAt: Date.now(),
    };
    expect(msg.receivedAt).toBeGreaterThan(0);
  });

  it("should validate OutboxEntry structure", () => {
    const entry: OutboxEntry = {
      id: "entry-1",
      targetPeerIds: ["peer-b"],
      payload: new Uint8Array([0xde, 0xad]),
      timestamp: { wallTime: BigInt(0), logical: 0, nodeId: "n1" } as IHLCTimestamp,
      type: "replication",
      status: "pending",
      createdAt: Date.now(),
      lastAttemptAt: null,
      attemptCount: 0,
      lastError: null,
    };
    expect(entry.status).toBe("pending");
    expect(entry.attemptCount).toBe(0);
  });

  it("should validate IHLCTimestamp interface", () => {
    const ts: IHLCTimestamp = {
      wallTime: BigInt(1736000000000),
      logical: 0,
      nodeId: "node-1",
      compare: vi.fn().mockReturnValue(0),
      serialize: vi.fn().mockReturnValue("1736000000000:0:node-1"),
      deserialize: vi.fn(),
    };
    expect(typeof ts.compare).toBe("function");
    expect(typeof ts.serialize).toBe("function");
    expect(typeof ts.deserialize).toBe("function");
    expect(ts.serialize()).toBe("1736000000000:0:node-1");
  });

  it("should validate shared types", () => {
    const peer: PeerInfo = {
      peerId: "peer-z",
      address: "tcp://127.0.0.1:5555",
      connectedAt: Date.now(),
      latencyMs: 5,
    };
    expect(peer.latencyMs).toBeGreaterThanOrEqual(0);

    const tStats: TransportStats = {
      peersConnected: 0,
      messagesSent: 10,
      messagesReceived: 8,
      bytesSent: 1024,
      bytesReceived: 512,
      uptimeMs: 3600000,
    };
    expect(tStats.uptimeMs).toBe(3600000);

    const oStats: OutboxStats = {
      totalEntries: 100,
      pendingCount: 3,
      deliveredCount: 97,
      oldestPendingTimestamp: Date.now(),
    };
    expect(oStats.deliveredCount).toBe(97);

    const hlc: HLCStatus = {
      wallClockTime: BigInt(Date.now()),
      logicalCounter: 0,
      nodeId: "node-1",
    };
    expect(hlc.nodeId).toBe("node-1");
  });
});
