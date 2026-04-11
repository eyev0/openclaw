import type { Dispatcher } from "undici";
import { loadUndiciRuntimeDeps } from "./undici-runtime.js";

export type DispatcherAwareRequestInit = RequestInit & { dispatcher?: Dispatcher };

type FetchLike = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

type RuntimeFetchDeps = ReturnType<typeof loadUndiciRuntimeDeps>;

function normalizeMultipartBodyForRuntimeFetch(
  init: DispatcherAwareRequestInit | undefined,
  deps: RuntimeFetchDeps,
): DispatcherAwareRequestInit | undefined {
  if (!init?.body || typeof FormData === "undefined" || !(init.body instanceof FormData)) {
    return init;
  }

  if (typeof deps.FormData !== "function") {
    return init;
  }

  if (init.body instanceof deps.FormData) {
    return init;
  }

  const runtimeForm = new deps.FormData();
  for (const [key, value] of init.body.entries()) {
    if (typeof value === "string") {
      runtimeForm.append(key, value);
      continue;
    }
    const name = typeof value.name === "string" && value.name.trim() ? value.name : undefined;
    if (name) {
      runtimeForm.append(key, value, name);
    } else {
      runtimeForm.append(key, value);
    }
  }

  return { ...init, body: runtimeForm };
}

export function isMockedFetch(fetchImpl: FetchLike | undefined): boolean {
  if (typeof fetchImpl !== "function") {
    return false;
  }
  return typeof (fetchImpl as FetchLike & { mock?: unknown }).mock === "object";
}

export async function fetchWithRuntimeDispatcher(
  input: RequestInfo | URL,
  init?: DispatcherAwareRequestInit,
): Promise<Response> {
  const runtimeDeps = loadUndiciRuntimeDeps();
  const runtimeFetch = runtimeDeps.fetch as unknown as (
    input: RequestInfo | URL,
    init?: DispatcherAwareRequestInit,
  ) => Promise<unknown>;
  const normalizedInit = normalizeMultipartBodyForRuntimeFetch(init, runtimeDeps);
  return (await runtimeFetch(input, normalizedInit)) as Response;
}
