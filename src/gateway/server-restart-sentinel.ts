import { resolveAnnounceTargetFromKey } from "../agents/tools/sessions-send-helpers.js";
import { normalizeChannelId } from "../channels/plugins/index.js";
import type { CliDeps } from "../cli/deps.js";
import { resolveMainSessionKeyFromConfig } from "../config/sessions.js";
import { parseSessionThreadInfo } from "../config/sessions/delivery-info.js";
import { requestHeartbeatNow } from "../infra/heartbeat-wake.js";
import { deliverOutboundPayloads } from "../infra/outbound/deliver.js";
import { ackDelivery, enqueueDelivery, failDelivery } from "../infra/outbound/delivery-queue.js";
import { buildOutboundSessionContext } from "../infra/outbound/session-context.js";
import { resolveOutboundTarget } from "../infra/outbound/targets.js";
import {
  consumeRestartSentinel,
  formatRestartSentinelMessage,
  summarizeRestartSentinel,
  type RestartOutboxTask,
} from "../infra/restart-sentinel.js";
import { enqueueSystemEvent } from "../infra/system-events.js";
import { createSubsystemLogger } from "../logging/subsystem.js";
import { deliveryContextFromSession, mergeDeliveryContext } from "../utils/delivery-context.js";
import { loadSessionEntry } from "./session-utils.js";

const log = createSubsystemLogger("gateway/restart-sentinel");
const OUTBOUND_RETRY_DELAY_MS = 750;
const OUTBOUND_MAX_ATTEMPTS = 2;

export type RestartWakeReport = {
  restartId?: string;
  correlationId?: string;
  initiator?: string;
  suppressPrimaryNotice: boolean;
  primaryNoticeDelivered: boolean;
  outboxTotal: number;
  outboxExecuted: number;
};

function normalizeNonEmptyString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function enqueueRestartSentinelWake(
  message: string,
  sessionKey: string,
  deliveryContext?: {
    channel?: string;
    to?: string;
    accountId?: string;
    threadId?: string | number;
  },
) {
  enqueueSystemEvent(message, {
    sessionKey,
    ...(deliveryContext ? { deliveryContext } : {}),
  });
  requestHeartbeatNow({ reason: "wake", sessionKey });
}

async function waitForOutboundRetry(delayMs: number) {
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, delayMs);
    timer.unref?.();
  });
}

async function deliverRestartSentinelNotice(params: {
  deps: CliDeps;
  cfg: ReturnType<typeof loadSessionEntry>["cfg"];
  sessionKey: string;
  summary: string;
  message: string;
  channel: string;
  to: string;
  accountId?: string;
  replyToId?: string;
  threadId?: string;
  session: ReturnType<typeof buildOutboundSessionContext>;
}) {
  const payloads = [{ text: params.message }];
  // Persist one recoverable notice across the whole retry loop so a transient
  // failure does not leave behind a stale duplicate queue entry.
  const queueId = await enqueueDelivery({
    channel: params.channel,
    to: params.to,
    accountId: params.accountId,
    replyToId: params.replyToId,
    threadId: params.threadId,
    payloads,
    bestEffort: false,
  }).catch(() => null);
  for (let attempt = 1; attempt <= OUTBOUND_MAX_ATTEMPTS; attempt += 1) {
    try {
      const results = await deliverOutboundPayloads({
        cfg: params.cfg,
        channel: params.channel,
        to: params.to,
        accountId: params.accountId,
        replyToId: params.replyToId,
        threadId: params.threadId,
        payloads,
        session: params.session,
        deps: params.deps,
        bestEffort: false,
        skipQueue: true,
      });
      if (results.length > 0) {
        if (queueId) {
          await ackDelivery(queueId).catch(() => {});
        }
        return;
      }
      throw new Error("outbound delivery returned no results");
    } catch (err) {
      const retrying = attempt < OUTBOUND_MAX_ATTEMPTS;
      const suffix = retrying ? `; retrying in ${OUTBOUND_RETRY_DELAY_MS}ms` : "";
      log.warn(`${params.summary}: outbound delivery failed${suffix}: ${String(err)}`, {
        channel: params.channel,
        to: params.to,
        sessionKey: params.sessionKey,
        attempt,
        maxAttempts: OUTBOUND_MAX_ATTEMPTS,
      });
      if (!retrying) {
        if (queueId) {
          await failDelivery(queueId, err instanceof Error ? err.message : String(err)).catch(
            () => {
              // Best-effort queue bookkeeping.
            },
          );
        }
        return;
      }
      await waitForOutboundRetry(OUTBOUND_RETRY_DELAY_MS);
    }
  }
}

async function deliverRestartNoticeForSession(params: {
  deps: CliDeps;
  sessionKey: string;
  summary: string;
  message: string;
  deliveryContext?: {
    channel?: string;
    to?: string;
    accountId?: string;
  };
  threadId?: string;
}) {
  const sessionKey = normalizeNonEmptyString(params.sessionKey);
  if (!sessionKey) {
    return;
  }

  const wakeDeliveryContext = mergeDeliveryContext(
    params.threadId != null
      ? { ...params.deliveryContext, threadId: params.threadId }
      : params.deliveryContext,
    undefined,
  );
  enqueueRestartSentinelWake(params.message, sessionKey, wakeDeliveryContext);

  const { baseSessionKey, threadId: sessionThreadId } = parseSessionThreadInfo(sessionKey);

  const { cfg, entry } = loadSessionEntry(sessionKey);
  const parsedTarget = resolveAnnounceTargetFromKey(baseSessionKey ?? sessionKey);

  // Prefer delivery context from sentinel/outbox (captured at restart) over session store
  // Handles race condition where store wasn't flushed before restart
  const sentinelContext = params.deliveryContext;
  let sessionDeliveryContext = deliveryContextFromSession(entry);
  if (!sessionDeliveryContext && baseSessionKey && baseSessionKey !== sessionKey) {
    const { entry: baseEntry } = loadSessionEntry(baseSessionKey);
    sessionDeliveryContext = deliveryContextFromSession(baseEntry);
  }

  const origin = mergeDeliveryContext(
    sentinelContext,
    mergeDeliveryContext(sessionDeliveryContext, parsedTarget ?? undefined),
  );

  const channelRaw = origin?.channel;
  const channel = channelRaw ? normalizeChannelId(channelRaw) : null;
  const to = origin?.to;
  if (!channel || !to) {
    return;
  }

  const resolved = resolveOutboundTarget({
    channel,
    to,
    cfg,
    accountId: origin?.accountId,
    mode: "implicit",
  });
  if (!resolved.ok) {
    return;
  }

  const threadId =
    params.threadId ??
    parsedTarget?.threadId ?? // From resolveAnnounceTargetFromKey (extracts :topic:N)
    sessionThreadId ??
    (origin?.threadId != null ? String(origin.threadId) : undefined);

  // Slack uses replyToId (thread_ts) for threading, not threadId.
  // The reply path does this mapping but deliverOutboundPayloads does not,
  // so we must convert here to ensure post-restart notifications land in
  // the originating Slack thread. See #17716.
  const isSlack = channel === "slack";
  const replyToId = isSlack && threadId != null && threadId !== "" ? String(threadId) : undefined;
  const resolvedThreadId = isSlack ? undefined : threadId;
  const outboundSession = buildOutboundSessionContext({
    cfg,
    sessionKey,
  });

  await deliverRestartSentinelNotice({
    deps: params.deps,
    cfg,
    sessionKey,
    summary: params.summary,
    message: params.message,
    channel,
    to: resolved.to,
    accountId: origin?.accountId,
    replyToId,
    threadId: resolvedThreadId,
    session: outboundSession,
  });
}

async function processRestartMessageTask(params: {
  deps: CliDeps;
  summary: string;
  message: string;
  sessionKey?: string;
  deliveryContext?: {
    channel?: string;
    to?: string;
    accountId?: string;
  };
  threadId?: string;
}) {
  const sessionKey = normalizeNonEmptyString(params.sessionKey);
  if (!sessionKey) {
    const mainSessionKey = resolveMainSessionKeyFromConfig();
    enqueueSystemEvent(params.message, { sessionKey: mainSessionKey });
    return;
  }
  await deliverRestartNoticeForSession({
    deps: params.deps,
    sessionKey,
    summary: params.summary,
    message: params.message,
    deliveryContext: params.deliveryContext,
    threadId: params.threadId,
  });
}

function processSystemEventTask(params: {
  message: string;
  sessionKey?: string;
  deliveryContext?: {
    channel?: string;
    to?: string;
    accountId?: string;
  };
  threadId?: string;
}) {
  const sessionKey =
    normalizeNonEmptyString(params.sessionKey) ?? resolveMainSessionKeyFromConfig();
  const context = mergeDeliveryContext(
    params.threadId != null
      ? { ...params.deliveryContext, threadId: params.threadId }
      : params.deliveryContext,
    undefined,
  );
  enqueueSystemEvent(params.message, {
    sessionKey,
    ...(context ? { deliveryContext: context } : {}),
  });
  requestHeartbeatNow({ reason: "wake", sessionKey });
}

function shouldRunOutboxTask(params: {
  task: RestartOutboxTask;
  restartId?: string;
  correlationId?: string;
}): boolean {
  const taskRestartId = normalizeNonEmptyString(params.task.restartId);
  if (taskRestartId && params.restartId && taskRestartId !== params.restartId) {
    return false;
  }
  const taskCorrelationId = normalizeNonEmptyString(params.task.correlationId);
  if (taskCorrelationId && params.correlationId && taskCorrelationId !== params.correlationId) {
    return false;
  }
  return true;
}

function resolveOutboxTaskKind(task: RestartOutboxTask): "message" | "system_event" {
  const rawKind = normalizeNonEmptyString((task as { kind?: unknown }).kind);
  return rawKind === "system_event" ? "system_event" : "message";
}

export async function scheduleRestartSentinelWake(params: {
  deps: CliDeps;
}): Promise<RestartWakeReport | null> {
  const sentinel = await consumeRestartSentinel();
  if (!sentinel) {
    return null;
  }
  const payload = sentinel.payload;
  const summary = summarizeRestartSentinel(payload);
  const message = formatRestartSentinelMessage(payload);
  const restartId = normalizeNonEmptyString(payload.restartId);
  const correlationId = normalizeNonEmptyString(payload.correlationId) ?? restartId;
  const initiator = normalizeNonEmptyString(payload.initiator);

  const suppressPrimaryNotice = payload.suppressPrimaryNotice === true;
  let primaryNoticeDelivered = false;
  if (!suppressPrimaryNotice) {
    await processRestartMessageTask({
      deps: params.deps,
      summary,
      message,
      sessionKey: payload.sessionKey,
      deliveryContext: payload.deliveryContext,
      threadId: payload.threadId,
    });
    primaryNoticeDelivered = true;
  }

  const outbox = Array.isArray(payload.outbox) ? payload.outbox : [];
  let outboxExecuted = 0;

  for (const task of outbox) {
    if (!task || typeof task !== "object") {
      continue;
    }
    if (!shouldRunOutboxTask({ task, restartId, correlationId })) {
      continue;
    }
    const taskMessage = normalizeNonEmptyString(task.message);
    if (!taskMessage) {
      continue;
    }

    const taskKind = resolveOutboxTaskKind(task);
    const sessionKey = normalizeNonEmptyString(task.sessionKey) ?? payload.sessionKey;
    const deliveryContext =
      task.deliveryContext && typeof task.deliveryContext === "object"
        ? {
            channel: normalizeNonEmptyString(task.deliveryContext.channel),
            to: normalizeNonEmptyString(task.deliveryContext.to),
            accountId: normalizeNonEmptyString(task.deliveryContext.accountId),
          }
        : payload.deliveryContext;
    const threadId = normalizeNonEmptyString(task.threadId) ?? payload.threadId;

    if (taskKind === "system_event") {
      processSystemEventTask({
        message: taskMessage,
        sessionKey,
        deliveryContext,
        threadId,
      });
      outboxExecuted += 1;
      continue;
    }

    await processRestartMessageTask({
      deps: params.deps,
      summary: `${summary} (outbox)`,
      message: taskMessage,
      sessionKey,
      deliveryContext,
      threadId,
    });
    outboxExecuted += 1;
  }

  return {
    ...(restartId ? { restartId } : {}),
    ...(correlationId ? { correlationId } : {}),
    ...(initiator ? { initiator } : {}),
    suppressPrimaryNotice,
    primaryNoticeDelivered,
    outboxTotal: outbox.length,
    outboxExecuted,
  };
}

export function shouldWakeFromRestartSentinel() {
  return !process.env.VITEST && process.env.NODE_ENV !== "test";
}
