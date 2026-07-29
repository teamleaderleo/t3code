import * as NodeAssert from "node:assert/strict";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
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
  TurnId,
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
  "t3/provider/Layers/OpenCodeAdapter.interrupt.test/OpenCodeAdapter",
) {}

const resumedSessionId = "ses_interrupt";
const EVENT_STREAM_END = Symbol("OpenCodeAdapter.interrupt.test/event-stream-end");
const drainFibers = Effect.forEach(Array.from({ length: 50 }), () => Effect.yieldNow, {
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
  const state = {
    abortCalls: [] as Array<string>,
    promptCalls: [] as Array<unknown>,
    abortError: null as Error | null,
    emitIdleDuringAbort: false,
  };

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
          promptAsync: async (request: unknown) => {
            state.promptCalls.push(request);
          },
          abort: async ({ sessionID }: { sessionID: string }) => {
            state.abortCalls.push(sessionID);
            if (state.emitIdleDuringAbort) {
              events.push({
                type: "session.status",
                properties: { sessionID, status: { type: "idle" } },
              });
            }
            if (state.abortError) throw state.abortError;
          },
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

  return { events, layer, state };
}

const startTurn = Effect.fn("OpenCodeAdapter.interrupt.test/startTurn")(function* (
  adapter: OpenCodeAdapterShape,
  threadId: ThreadId,
) {
  yield* adapter.startSession({
    provider: ProviderDriverKind.make("opencode"),
    threadId,
    runtimeMode: "full-access",
    resumeCursor: { schemaVersion: 1, sessionId: resumedSessionId },
  });

  return yield* adapter.sendTurn({
    threadId,
    input: "keep working until interrupted",
    modelSelection: {
      instanceId: ProviderInstanceId.make("opencode"),
      model: "openai/gpt-5",
    },
  });
});

const watchTerminalEvents = (
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

function exactTerminalEvents(
  events: ReadonlyArray<ProviderRuntimeEvent>,
  turnId: TurnId,
): ReadonlyArray<ProviderRuntimeEvent> {
  return events.filter(
    (event) =>
      event.turnId === turnId && (event.type === "turn.aborted" || event.type === "turn.completed"),
  );
}

function assertInterruptedTerminal(events: ReadonlyArray<ProviderRuntimeEvent>, turnId: TurnId): void {
  const exact = exactTerminalEvents(events, turnId);
  NodeAssert.equal(exact.length, 1);
  const event = exact[0];
  if (event?.type === "turn.completed") {
    NodeAssert.equal(event.payload.state, "interrupted");
  } else {
    NodeAssert.equal(event?.type, "turn.aborted");
  }
}

it.effect("settles the exact interrupted turn and clears local state after abort succeeds", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-interrupt-no-idle");
    const turn = yield* startTurn(adapter, threadId);
    const terminals: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* watchTerminalEvents(adapter, threadId, terminals);

    yield* adapter.interruptTurn(threadId, turn.turnId);
    yield* drainFibers;
    yield* Fiber.interrupt(watcher);

    NodeAssert.deepEqual(harness.state.abortCalls, [resumedSessionId]);
    assertInterruptedTerminal(terminals, turn.turnId);
    const sessions = yield* adapter.listSessions();
    const session = sessions.find((entry) => entry.threadId === threadId);
    NodeAssert.equal(session?.status, "ready");
    NodeAssert.equal(session?.activeTurnId, undefined);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("rejects a stale explicit turn id before aborting the current turn", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-interrupt-stale-id");
    const turn = yield* startTurn(adapter, threadId);
    const staleTurnId = TurnId.make("turn-stale-interrupt-request");

    const result = yield* adapter.interruptTurn(threadId, staleTurnId).pipe(Effect.result);

    NodeAssert.equal(result._tag, "Failure");
    NodeAssert.deepEqual(harness.state.abortCalls, []);
    const sessions = yield* adapter.listSessions();
    const session = sessions.find((entry) => entry.threadId === threadId);
    NodeAssert.equal(session?.status, "running");
    NodeAssert.equal(session?.activeTurnId, turn.turnId);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("settles an abort-idle race once and never labels the interrupted turn completed", () => {
  const harness = makeHarness();
  harness.state.emitIdleDuringAbort = true;

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-interrupt-idle-race");
    const turn = yield* startTurn(adapter, threadId);
    const terminals: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* watchTerminalEvents(adapter, threadId, terminals);

    yield* adapter.interruptTurn(threadId, turn.turnId);
    yield* drainFibers;
    yield* Fiber.interrupt(watcher);

    assertInterruptedTerminal(terminals, turn.turnId);
    NodeAssert.deepEqual(harness.state.abortCalls, [resumedSessionId]);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("coalesces concurrent duplicate interrupts into one abort and one terminal event", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-concurrent-interrupts");
    const turn = yield* startTurn(adapter, threadId);
    const terminals: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* watchTerminalEvents(adapter, threadId, terminals);

    yield* Effect.all(
      [
        Effect.exit(adapter.interruptTurn(threadId, turn.turnId)),
        Effect.exit(adapter.interruptTurn(threadId, turn.turnId)),
      ],
      { concurrency: "unbounded" },
    );
    yield* drainFibers;
    yield* Fiber.interrupt(watcher);

    NodeAssert.deepEqual(harness.state.abortCalls, [resumedSessionId]);
    assertInterruptedTerminal(terminals, turn.turnId);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("preserves the active turn and emits no terminal result when abort fails", () => {
  const harness = makeHarness();
  harness.state.abortError = new Error("abort transport failed", { cause: { status: 503 } });

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-interrupt-failure");
    const turn = yield* startTurn(adapter, threadId);
    const terminals: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* watchTerminalEvents(adapter, threadId, terminals);

    const exit = yield* Effect.exit(adapter.interruptTurn(threadId, turn.turnId));
    yield* drainFibers;
    yield* Fiber.interrupt(watcher);

    NodeAssert.equal(Exit.isFailure(exit), true);
    NodeAssert.deepEqual(terminals, []);
    const sessions = yield* adapter.listSessions();
    const session = sessions.find((entry) => entry.threadId === threadId);
    NodeAssert.equal(session?.status, "running");
    NodeAssert.equal(session?.activeTurnId, turn.turnId);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("does not let a delayed duplicate idle from the previous turn close a newer turn", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-delayed-idle-new-turn");
    const firstTurn = yield* startTurn(adapter, threadId);
    const terminals: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* watchTerminalEvents(adapter, threadId, terminals);

    harness.events.push({
      type: "session.status",
      properties: { sessionID: resumedSessionId, status: { type: "idle" } },
    });
    yield* drainFibers;
    NodeAssert.equal(exactTerminalEvents(terminals, firstTurn.turnId).length, 1);

    const secondTurn = yield* adapter.sendTurn({
      threadId,
      input: "start a genuinely new turn",
      modelSelection: {
        instanceId: ProviderInstanceId.make("opencode"),
        model: "openai/gpt-5",
      },
    });
    NodeAssert.notEqual(secondTurn.turnId, firstTurn.turnId);

    harness.events.push({
      type: "session.status",
      properties: { sessionID: resumedSessionId, status: { type: "idle" } },
    });
    yield* drainFibers;
    yield* Fiber.interrupt(watcher);

    NodeAssert.equal(exactTerminalEvents(terminals, secondTurn.turnId).length, 0);
    const sessions = yield* adapter.listSessions();
    const session = sessions.find((entry) => entry.threadId === threadId);
    NodeAssert.equal(session?.status, "running");
    NodeAssert.equal(session?.activeTurnId, secondTurn.turnId);
  }).pipe(Effect.provide(harness.layer));
});
