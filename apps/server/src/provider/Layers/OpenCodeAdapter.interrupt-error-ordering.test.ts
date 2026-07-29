import * as NodeAssert from "node:assert/strict";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  OpenCodeSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import type { OpenCodeAdapterShape } from "../Services/OpenCodeAdapter.ts";
import {
  OpenCodeRuntime,
  OpenCodeRuntimeError,
  type OpenCodeRuntimeShape,
} from "../opencodeRuntime.ts";
import { makeOpenCodeAdapter } from "./OpenCodeAdapter.ts";

class OpenCodeAdapter extends Context.Service<OpenCodeAdapter, OpenCodeAdapterShape>()(
  "t3/provider/Layers/OpenCodeAdapter.interrupt-error-ordering.test/OpenCodeAdapter",
) {}

const sessionId = "ses_interrupt_error_ordering";
const EVENT_STREAM_END = Symbol("OpenCodeAdapter.interrupt-error-ordering.test/end");
const drainFibers = Effect.forEach(Array.from({ length: 75 }), () => Effect.yieldNow, {
  discard: true,
});

function makeEventBus() {
  const buffered: Array<unknown> = [];
  const waiters: Array<(event: unknown | typeof EVENT_STREAM_END) => void> = [];

  const push = (event: unknown): void => {
    const waiter = waiters.shift();
    if (waiter) {
      waiter(event);
      return;
    }
    buffered.push(event);
  };

  const stream = (signal?: AbortSignal): AsyncIterable<unknown> => ({
    [Symbol.asyncIterator]() {
      return {
        next: async (): Promise<IteratorResult<unknown>> => {
          const immediate = buffered.shift();
          if (immediate !== undefined) return { done: false, value: immediate };

          const event = await new Promise<unknown | typeof EVENT_STREAM_END>((resolve) => {
            let settled = false;
            const finish = (value: unknown | typeof EVENT_STREAM_END) => {
              if (settled) return;
              settled = true;
              signal?.removeEventListener("abort", onAbort);
              const index = waiters.indexOf(finish);
              if (index >= 0) waiters.splice(index, 1);
              resolve(value);
            };
            const onAbort = () => finish(EVENT_STREAM_END);
            waiters.push(finish);
            signal?.addEventListener("abort", onAbort, { once: true });
            if (signal?.aborted) onAbort();
          });

          return event === EVENT_STREAM_END
            ? { done: true, value: undefined }
            : { done: false, value: event };
        },
      };
    },
  });

  return { push, stream };
}

function makeHarness() {
  const events = makeEventBus();
  const runtime: OpenCodeRuntimeShape = {
    startOpenCodeServerProcess: () =>
      Effect.succeed({ url: "http://127.0.0.1:4301", exitCode: Effect.never }),
    connectToOpenCodeServer: ({ serverUrl }) =>
      Effect.succeed({
        url: serverUrl ?? "http://127.0.0.1:4301",
        exitCode: null,
        external: true,
      }),
    runOpenCodeCommand: () => Effect.succeed({ stdout: "", stderr: "", code: 0 }),
    createOpenCodeSdkClient: () =>
      ({
        session: {
          get: async ({ sessionID }: { sessionID: string }) => ({ data: { id: sessionID } }),
          update: async ({ sessionID }: { sessionID: string }) => ({ data: { id: sessionID } }),
          promptAsync: async () => undefined,
          abort: async () => undefined,
        },
        event: {
          subscribe: async (_request?: unknown, options?: { readonly signal?: AbortSignal }) => ({
            stream: events.stream(options?.signal),
          }),
        },
      }) as unknown as ReturnType<OpenCodeRuntimeShape["createOpenCodeSdkClient"]>,
    loadOpenCodeInventory: () =>
      Effect.fail(
        new OpenCodeRuntimeError({
          operation: "loadOpenCodeInventory",
          detail: "not used in this test",
          cause: null,
        }),
      ),
    loadInventoryFromCli: () =>
      Effect.fail(
        new OpenCodeRuntimeError({
          operation: "loadInventoryFromCli",
          detail: "not used in this test",
          cause: null,
        }),
      ),
  };

  const providerSessionDirectoryTestLayer = Layer.succeed(ProviderSessionDirectory, {
    upsert: () => Effect.void,
    getProvider: () =>
      Effect.die(new Error("ProviderSessionDirectory.getProvider is not used in test")),
    getBinding: () => Effect.succeed(Option.none()),
    listThreadIds: () => Effect.succeed([]),
    listBindings: () => Effect.succeed([]),
  });

  const settings = Schema.decodeSync(OpenCodeSettings)({
    binaryPath: "fake-opencode",
    serverUrl: "http://127.0.0.1:9999",
    serverPassword: "secret-password",
  });

  const layer = Layer.effect(OpenCodeAdapter, makeOpenCodeAdapter(settings)).pipe(
    Layer.provideMerge(Layer.succeed(OpenCodeRuntime, runtime)),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(ServerSettingsService.layerTest()),
    Layer.provideMerge(providerSessionDirectoryTestLayer),
    Layer.provideMerge(NodeServices.layer),
  );

  return { events, layer };
}

const sendTurn = Effect.fn("OpenCodeAdapter.interrupt-error-ordering.test/sendTurn")(function* (
  adapter: OpenCodeAdapterShape,
  threadId: ThreadId,
  input: string,
) {
  return yield* adapter.sendTurn({
    threadId,
    input,
    modelSelection: {
      instanceId: ProviderInstanceId.make("opencode"),
      model: "openai/gpt-5",
    },
  });
});

const pushStatus = (
  events: ReturnType<typeof makeEventBus>,
  status: { readonly type: "busy" | "idle" },
): void =>
  events.push({
    type: "session.status",
    properties: {
      sessionID: sessionId,
      status,
    },
  });

const pushAbortedError = (events: ReturnType<typeof makeEventBus>): void =>
  events.push({
    type: "session.error",
    properties: {
      sessionID: sessionId,
      error: {
        name: "MessageAbortedError",
        data: { message: "Aborted" },
      },
    },
  });

const watchTerminals = (
  adapter: OpenCodeAdapterShape,
  threadId: ThreadId,
  sink: Array<ProviderRuntimeEvent>,
) =>
  adapter.streamEvents.pipe(
    Stream.filter(
      (event) =>
        event.threadId === threadId &&
        (event.type === "turn.aborted" || event.type === "turn.completed"),
    ),
    Stream.runForEach((event) => Effect.sync(() => sink.push(event))),
    Effect.forkChild,
  );

it.effect("ignores a delayed abort error that precedes provider activity for a newer turn", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-delayed-abort-error");
    const terminals: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* watchTerminals(adapter, threadId, terminals);

    yield* adapter.startSession({
      provider: ProviderDriverKind.make("opencode"),
      threadId,
      runtimeMode: "full-access",
      resumeCursor: { schemaVersion: 1, sessionId },
    });

    const firstTurn = yield* sendTurn(adapter, threadId, "first turn");
    pushStatus(harness.events, { type: "busy" });
    pushStatus(harness.events, { type: "idle" });
    yield* drainFibers;
    NodeAssert.equal(terminals.filter((event) => event.turnId === firstTurn.turnId).length, 1);

    const secondTurn = yield* sendTurn(adapter, threadId, "second turn");
    pushAbortedError(harness.events);
    yield* drainFibers;
    yield* Fiber.interrupt(watcher);

    NodeAssert.equal(terminals.filter((event) => event.turnId === secondTurn.turnId).length, 0);
    const sessions = yield* adapter.listSessions();
    const session = sessions.find((entry) => entry.threadId === threadId);
    NodeAssert.equal(session?.status, "running");
    NodeAssert.equal(session?.activeTurnId, secondTurn.turnId);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("classifies an abort error after current provider activity as interrupted", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-current-abort-error");
    const terminals: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* watchTerminals(adapter, threadId, terminals);

    yield* adapter.startSession({
      provider: ProviderDriverKind.make("opencode"),
      threadId,
      runtimeMode: "full-access",
      resumeCursor: { schemaVersion: 1, sessionId },
    });

    const turn = yield* sendTurn(adapter, threadId, "provider-originated cancellation");
    pushStatus(harness.events, { type: "busy" });
    pushAbortedError(harness.events);
    yield* drainFibers;
    yield* Fiber.interrupt(watcher);

    const exact = terminals.filter((event) => event.turnId === turn.turnId);
    NodeAssert.equal(exact.length, 1);
    const terminal = exact[0];
    if (terminal?.type === "turn.completed") {
      NodeAssert.equal(terminal.payload.state, "interrupted");
    } else {
      NodeAssert.equal(terminal?.type, "turn.aborted");
    }

    const sessions = yield* adapter.listSessions();
    const session = sessions.find((entry) => entry.threadId === threadId);
    NodeAssert.equal(session?.status, "ready");
    NodeAssert.equal(session?.activeTurnId, undefined);
  }).pipe(Effect.provide(harness.layer));
});
