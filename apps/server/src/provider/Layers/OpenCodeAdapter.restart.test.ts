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
  ThreadId,
  TurnId,
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
  "t3/provider/Layers/OpenCodeAdapter.restart.test/OpenCodeAdapter",
) {}

const resumedSessionId = "ses_persisted";
const persistedTurnId = TurnId.make("turn-persisted-before-restart");
const persistedProviderMessageId = "msg_t3_persisted_before_restart";
const EVENT_STREAM_END = Symbol("OpenCodeAdapter.restart.test/event-stream-end");

type TestStatus =
  | { readonly type: "idle" }
  | { readonly type: "busy" }
  | { readonly type: "retry"; readonly attempt: number; readonly message: string; readonly next: number };

type TestMessage = {
  readonly info: {
    readonly id: string;
    readonly role: "user" | "assistant";
    readonly parentID?: string;
    readonly time?: {
      readonly created?: number;
      readonly completed?: number;
    };
    readonly error?: unknown;
  };
  readonly parts: ReadonlyArray<unknown>;
};

type RecoveryInput = Parameters<OpenCodeAdapterShape["startSession"]>[0] & {
  readonly recoveryActiveTurnId: TurnId;
  readonly recoveryProviderMessageIds: ReadonlyArray<string>;
};

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
          if (immediate !== undefined) {
            return { done: false, value: immediate };
          }

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

function makeRecoveryInput(
  threadId: ThreadId,
  turnId: TurnId = persistedTurnId,
  providerMessageIds: ReadonlyArray<string> = [persistedProviderMessageId],
): RecoveryInput {
  return {
    provider: ProviderDriverKind.make("opencode"),
    threadId,
    runtimeMode: "full-access",
    resumeCursor: { schemaVersion: 1, sessionId: resumedSessionId },
    recoveryActiveTurnId: turnId,
    recoveryProviderMessageIds: providerMessageIds,
  };
}

function makeHarness(input?: {
  readonly status?: TestStatus;
  readonly messages?: ReadonlyArray<TestMessage>;
  readonly statusError?: Error;
  readonly messagesError?: Error;
}) {
  const events = makeEventBus();
  const state = {
    status: input?.status ?? ({ type: "idle" } as const),
    messages: [...(input?.messages ?? [])],
    statusError: input?.statusError ?? null,
    messagesError: input?.messagesError ?? null,
    statusCalls: [] as Array<unknown>,
    messageCalls: [] as Array<Record<string, unknown>>,
    createCalls: [] as Array<unknown>,
    abortCalls: [] as Array<string>,
  };

  const runtime: OpenCodeRuntimeShape = {
    startOpenCodeServerProcess: () =>
      Effect.succeed({
        url: "http://127.0.0.1:4301",
        exitCode: Effect.never,
      }),
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
          create: async (request: unknown) => {
            state.createCalls.push(request);
            return { data: { id: "ses_unexpected_fresh" } };
          },
          get: async ({ sessionID }: { sessionID: string }) => ({
            data: { id: sessionID },
          }),
          update: async ({ sessionID }: { sessionID: string }) => ({
            data: { id: sessionID },
          }),
          status: async (request?: unknown) => {
            state.statusCalls.push(request);
            if (state.statusError) throw state.statusError;
            return {
              data: {
                [resumedSessionId]: state.status,
              },
            };
          },
          messages: async (request: Record<string, unknown>) => {
            state.messageCalls.push(request);
            if (state.messagesError) throw state.messagesError;
            return { data: state.messages };
          },
          abort: async ({ sessionID }: { sessionID: string }) => {
            state.abortCalls.push(sessionID);
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

const collectExactCompletion = (
  adapter: OpenCodeAdapterShape,
  threadId: ThreadId,
  turnId: TurnId,
) =>
  adapter.streamEvents.pipe(
    Stream.filter(
      (event) =>
        event.threadId === threadId && event.type === "turn.completed" && event.turnId === turnId,
    ),
    Stream.take(1),
    Stream.runCollect,
    Effect.forkChild,
  );

function assertBoundedHistoryReads(calls: ReadonlyArray<Record<string, unknown>>): void {
  NodeAssert.ok(calls.length > 0, "recovery must inspect provider history when status is idle");
  for (const call of calls) {
    NodeAssert.equal(call.sessionID, resumedSessionId);
    NodeAssert.equal(Number.isInteger(call.limit), true, "history reads must set a finite page limit");
    const limit = call.limit as number;
    NodeAssert.ok(limit > 0 && limit <= 100, `history page limit must be bounded, received ${limit}`);
  }
}

it.effect(
  "settles the exact persisted turn as interrupted when bounded idle history has no matching terminal evidence",
  () => {
    const harness = makeHarness({
      messages: [
        {
          info: { id: "msg_unrelated_user", role: "user", time: { created: 1 } },
          parts: [],
        },
        {
          info: {
            id: "msg_unrelated_assistant",
            role: "assistant",
            parentID: "msg_unrelated_user",
            time: { created: 2, completed: 3 },
          },
          parts: [],
        },
      ],
    });

    return Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = ThreadId.make("thread-opencode-resumed-idle-no-match");
      const completionFiber = yield* collectExactCompletion(adapter, threadId, persistedTurnId);

      yield* adapter.startSession(makeRecoveryInput(threadId));

      const completions = Array.from(
        yield* Fiber.join(completionFiber).pipe(Effect.timeout("1 second")),
      );
      NodeAssert.equal(completions.length, 1);
      const completion = completions[0];
      NodeAssert.equal(completion?.type, "turn.completed");
      if (completion?.type === "turn.completed") {
        NodeAssert.equal(completion.turnId, persistedTurnId);
        NodeAssert.equal(completion.payload.state, "interrupted");
      }
      NodeAssert.equal(harness.state.statusCalls.length, 1);
      assertBoundedHistoryReads(harness.state.messageCalls);
      NodeAssert.deepEqual(harness.state.createCalls, []);
    }).pipe(Effect.provide(harness.layer));
  },
);

it.effect("classifies matching terminal assistant history as completed", () => {
  const harness = makeHarness({
    messages: [
      {
        info: {
          id: persistedProviderMessageId,
          role: "user",
          time: { created: 1 },
        },
        parts: [],
      },
      {
        info: {
          id: "msg_matching_assistant_success",
          role: "assistant",
          parentID: persistedProviderMessageId,
          time: { created: 2, completed: 3 },
        },
        parts: [],
      },
    ],
  });

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-resumed-idle-success");
    const completionFiber = yield* collectExactCompletion(adapter, threadId, persistedTurnId);

    yield* adapter.startSession(makeRecoveryInput(threadId));

    const completions = Array.from(
      yield* Fiber.join(completionFiber).pipe(Effect.timeout("1 second")),
    );
    const completion = completions[0];
    NodeAssert.equal(completion?.type, "turn.completed");
    if (completion?.type === "turn.completed") {
      NodeAssert.equal(completion.payload.state, "completed");
    }
    assertBoundedHistoryReads(harness.state.messageCalls);
    NodeAssert.deepEqual(harness.state.createCalls, []);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("classifies matching terminal assistant errors as failed", () => {
  const harness = makeHarness({
    messages: [
      {
        info: {
          id: persistedProviderMessageId,
          role: "user",
          time: { created: 1 },
        },
        parts: [],
      },
      {
        info: {
          id: "msg_matching_assistant_failure",
          role: "assistant",
          parentID: persistedProviderMessageId,
          time: { created: 2, completed: 3 },
          error: {
            name: "APIError",
            data: {
              message: "provider failed after restart",
              isRetryable: false,
            },
          },
        },
        parts: [],
      },
    ],
  });

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-resumed-idle-failed");
    const completionFiber = yield* collectExactCompletion(adapter, threadId, persistedTurnId);

    yield* adapter.startSession(makeRecoveryInput(threadId));

    const completions = Array.from(
      yield* Fiber.join(completionFiber).pipe(Effect.timeout("1 second")),
    );
    const completion = completions[0];
    NodeAssert.equal(completion?.type, "turn.completed");
    if (completion?.type === "turn.completed") {
      NodeAssert.equal(completion.payload.state, "failed");
    }
    assertBoundedHistoryReads(harness.state.messageCalls);
    NodeAssert.deepEqual(harness.state.createCalls, []);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("restores an exact busy turn and lets a later idle event settle that turn once", () => {
  const harness = makeHarness({ status: { type: "busy" } });

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-resumed-busy");
    const completionFiber = yield* collectExactCompletion(adapter, threadId, persistedTurnId);

    const session = yield* adapter.startSession(makeRecoveryInput(threadId));
    NodeAssert.equal(session.status, "running");
    NodeAssert.equal(session.activeTurnId, persistedTurnId);
    NodeAssert.deepEqual(harness.state.messageCalls, []);

    harness.events.push({
      type: "session.status",
      properties: {
        sessionID: resumedSessionId,
        status: { type: "idle" },
      },
    });

    const completions = Array.from(
      yield* Fiber.join(completionFiber).pipe(Effect.timeout("1 second")),
    );
    NodeAssert.equal(completions.length, 1);
    const completion = completions[0];
    NodeAssert.equal(completion?.type, "turn.completed");
    if (completion?.type === "turn.completed") {
      NodeAssert.equal(completion.turnId, persistedTurnId);
      NodeAssert.equal(completion.payload.state, "completed");
    }
  }).pipe(Effect.provide(harness.layer));
});

it.effect("propagates a status snapshot failure without silently creating an empty session", () => {
  const harness = makeHarness({
    statusError: new Error("status endpoint unavailable", { cause: { status: 503 } }),
  });

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-recovery-status-failure");
    const exit = yield* Effect.exit(adapter.startSession(makeRecoveryInput(threadId)));

    NodeAssert.equal(Exit.isFailure(exit), true);
    NodeAssert.equal(harness.state.statusCalls.length, 1);
    NodeAssert.deepEqual(harness.state.messageCalls, []);
    NodeAssert.deepEqual(harness.state.createCalls, []);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("propagates a bounded history failure without silently creating an empty session", () => {
  const harness = makeHarness({
    messagesError: new Error("history endpoint timed out", { cause: { status: 504 } }),
  });

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-recovery-history-failure");
    const exit = yield* Effect.exit(adapter.startSession(makeRecoveryInput(threadId)));

    NodeAssert.equal(Exit.isFailure(exit), true);
    NodeAssert.equal(harness.state.statusCalls.length, 1);
    assertBoundedHistoryReads(harness.state.messageCalls);
    NodeAssert.deepEqual(harness.state.createCalls, []);
  }).pipe(Effect.provide(harness.layer));
});
