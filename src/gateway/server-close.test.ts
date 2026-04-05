import { describe, expect, it, vi } from "vitest";
import { createGatewayCloseHandler } from "./server-close.js";

const { triggerInternalHook } = vi.hoisted(() => ({
  triggerInternalHook: vi.fn(async () => undefined),
}));

vi.mock("../hooks/internal-hooks.js", async () => {
  const actual = await vi.importActual<typeof import("../hooks/internal-hooks.js")>(
    "../hooks/internal-hooks.js",
  );
  return {
    ...actual,
    triggerInternalHook,
  };
});

vi.mock("../channels/plugins/index.js", () => ({
  listChannelPlugins: () => [],
}));

vi.mock("../hooks/gmail-watcher.js", () => ({
  stopGmailWatcher: vi.fn(async () => undefined),
}));

describe("createGatewayCloseHandler", () => {
  it("unsubscribes lifecycle listeners during shutdown", async () => {
    const lifecycleUnsub = vi.fn();
    const close = createGatewayCloseHandler({
      bonjourStop: null,
      tailscaleCleanup: null,
      canvasHost: null,
      canvasHostServer: null,
      stopChannel: vi.fn(async () => undefined),
      pluginServices: null,
      cron: { stop: vi.fn() },
      heartbeatRunner: { stop: vi.fn() } as never,
      updateCheckStop: null,
      nodePresenceTimers: new Map(),
      broadcast: vi.fn(),
      tickInterval: setInterval(() => undefined, 60_000),
      healthInterval: setInterval(() => undefined, 60_000),
      dedupeCleanup: setInterval(() => undefined, 60_000),
      mediaCleanup: null,
      agentUnsub: null,
      heartbeatUnsub: null,
      transcriptUnsub: null,
      lifecycleUnsub,
      chatRunState: { clear: vi.fn() },
      clients: new Set(),
      configReloader: { stop: vi.fn(async () => undefined) },
      wss: { close: (cb: () => void) => cb() } as never,
      httpServer: {
        close: (cb: (err?: Error | null) => void) => cb(null),
        closeIdleConnections: vi.fn(),
      } as never,
    });

    await close({ reason: "test shutdown" });

    expect(lifecycleUnsub).toHaveBeenCalledTimes(1);
  });

  it("emits gateway shutdown + pre-restart hooks", async () => {
    triggerInternalHook.mockClear();

    const close = createGatewayCloseHandler({
      bonjourStop: null,
      tailscaleCleanup: null,
      canvasHost: null,
      canvasHostServer: null,
      stopChannel: vi.fn(async () => undefined),
      pluginServices: null,
      cron: { stop: vi.fn() },
      heartbeatRunner: { stop: vi.fn() } as never,
      updateCheckStop: null,
      nodePresenceTimers: new Map(),
      broadcast: vi.fn(),
      tickInterval: setInterval(() => undefined, 60_000),
      healthInterval: setInterval(() => undefined, 60_000),
      dedupeCleanup: setInterval(() => undefined, 60_000),
      mediaCleanup: null,
      agentUnsub: null,
      heartbeatUnsub: null,
      transcriptUnsub: null,
      lifecycleUnsub: null,
      chatRunState: { clear: vi.fn() },
      clients: new Set(),
      configReloader: { stop: vi.fn(async () => undefined) },
      wss: { close: (cb: () => void) => cb() } as never,
      httpServer: {
        close: (cb: (err?: Error | null) => void) => cb(null),
        closeIdleConnections: vi.fn(),
      } as never,
    });

    await close({ reason: "gateway restarting", restartExpectedMs: 123 });

    const shutdownEvent = triggerInternalHook.mock.calls.find(
      (call) => call[0]?.type === "gateway" && call[0]?.action === "shutdown",
    )?.[0];
    const preRestartEvent = triggerInternalHook.mock.calls.find(
      (call) => call[0]?.type === "gateway" && call[0]?.action === "pre-restart",
    )?.[0];

    expect(shutdownEvent?.context).toMatchObject({
      reason: "gateway restarting",
      restartExpectedMs: 123,
    });
    expect(preRestartEvent?.context).toMatchObject({
      reason: "gateway restarting",
      restartExpectedMs: 123,
    });
  });
});
