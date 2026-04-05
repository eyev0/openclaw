/**
 * restart-notify.hook.ts
 *
 * Example internal hook that queues a post-restart notice via gateway outbox tasks.
 *
 * Register for: gateway:pre-restart
 */

import type { HookHandler } from "../../../src/hooks/hooks.js";

const restartNotify: HookHandler = async (event) => {
  if (event.type !== "gateway" || event.action !== "pre-restart") {
    return;
  }

  const ctx = event.context as {
    restartId?: string;
    correlationId?: string;
    outbox?: Array<Record<string, unknown>>;
  };

  if (!Array.isArray(ctx.outbox)) {
    return;
  }

  // Route to a known session that already owns delivery context (channel/to/thread).
  const sessionKey = process.env.OPENCLAW_RESTART_NOTIFY_SESSION_KEY?.trim();
  if (!sessionKey) {
    return;
  }

  ctx.outbox.push({
    message: "🔁 Gateway restarted. Back online.",
    sessionKey,
    restartId: ctx.restartId,
    correlationId: ctx.correlationId,
  });
};

export default restartNotify;
