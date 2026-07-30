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
  ApprovalRequestId,
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
  "t3/provider/Layers/OpenCodeAdapter.pending-request.test/OpenCodeAdapter",
) {}

const sessionId = "ses_pending_requests";
const permissionId = ApprovalRequestId.make("per_pending_skill");
const questionId = ApprovalRequestId.make("que_pending_question");
const EVENT_STREAM_END = Symbol("OpenCodeAdapter.pending-request.test/event-stream-end");
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
    permissionReplyCalls: [] as Array<unknown>,
    questionReplyCalls: [] as Array<unknown>,
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
          promptAsync: async () => undefined,
          abort: async ({ sessionID }: { sessionID: string }) => {
            state.abortCalls.push(sessionID);
          },
        },
        permission: {
          reply: async (request: unknown) => {
            state.permissionReplyCalls.push(request);
            return { data: true };
          },
        },
        question: {
          reply: async (request: unknown) => {
            state.questionReplyCalls.push(request);
            return { data: true };
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

const startTurn = Effect.fn("OpenCodeAdapter.pending-request.test/startTurn")(function* (
  adapter: OpenCodeAdapterShape,
  threadId: ThreadId,
) {
  yield* adapter.startSession({
    provider: ProviderDriverKind.make("opencode"),
    threadId,
    runtimeMode: "approval-required",
    resumeCursor: { schemaVersion: 1, sessionId },
  });

  return yield* adapter.sendTurn({
    threadId,
    input: "load a skill and ask a question",
    modelSelection: {
      instanceId: ProviderInstanceId.make("opencode"),
      model: "openai/gpt-5",
    },
  });
});

const watchThreadEvents = (
  adapter: OpenCodeAdapterShape,
  threadId: ThreadId,
  sink: Array<ProviderRuntimeEvent>,
) =>
  adapter.streamEvents.pipe(
    Stream.filter((event) => event.threadId === threadId),
    Stream.runForEach((event) => Effect.sync(() => sink.push(event))),
    Effect.forkChild,
  );

const pushSkillPermission = (events: ReturnType<typeof makeEventBus>): void =>
  events.push({
    type: "permission.asked",
    properties: {
      id: permissionId,
      sessionID: sessionId,
      permission: "skill",
      patterns: ["restoring-agent-sessions"],
      metadata: {},
      always: ["restoring-agent-sessions"],
      tool: {
        messageID: "msg_pending_permission",
        callID: "call_pending_permission",
      },
    },
  });

const pushQuestion = (events: ReturnType<typeof makeEventBus>): void =>
  events.push({
    type: "question.asked",
    properties: {
      id: questionId,
      sessionID: sessionId,
      questions: [
        {
          question: "Continue?",
          header: "Confirmation",
          options: [
            { label: "Yes", description: "Continue the task." },
            { label: "No", description: "Stop the task." },
          ],
          multiple: false,
        },
      ],
      tool: {
        messageID: "msg_pending_question",
        callID: "call_pending_question",
      },
    },
  });

it.effect("maps OpenCode skill permission requests to a visible dynamic tool approval", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-skill-permission");
    const observed: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* watchThreadEvents(adapter, threadId, observed);

    yield* startTurn(adapter, threadId);
    pushSkillPermission(harness.events);
    yield* drainFibers;
    yield* Fiber.interrupt(watcher);

    const opened = observed.find(
      (event) => event.type === "request.opened" && event.requestId === permissionId,
    );
    NodeAssert.equal(opened?.type, "request.opened");
    if (opened?.type === "request.opened") {
      NodeAssert.equal(opened.payload.requestType, "dynamic_tool_call");
      NodeAssert.equal(opened.payload.detail, "restoring-agent-sessions");
    }
  }).pipe(Effect.provide(harness.layer));
});

it.effect("expires pending permission and question handles when the owning turn is interrupted", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-pending-request-interrupt");
    const observed: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* watchThreadEvents(adapter, threadId, observed);

    const turn = yield* startTurn(adapter, threadId);
    pushSkillPermission(harness.events);
    pushQuestion(harness.events);
    yield* drainFibers;

    yield* adapter.interruptTurn(threadId, turn.turnId);
    yield* drainFibers;

    const stalePermission = yield* adapter
      .respondToRequest(threadId, permissionId, "decline")
      .pipe(Effect.result);
    const staleQuestion = yield* adapter
      .respondToUserInput(threadId, questionId, {})
      .pipe(Effect.result);
    yield* Fiber.interrupt(watcher);

    NodeAssert.equal(stalePermission._tag, "Failure");
    NodeAssert.equal(staleQuestion._tag, "Failure");
    NodeAssert.deepEqual(harness.state.permissionReplyCalls, []);
    NodeAssert.deepEqual(harness.state.questionReplyCalls, []);
    NodeAssert.deepEqual(harness.state.abortCalls, [sessionId]);

    const permissionResolved = observed.find(
      (event) => event.type === "request.resolved" && event.requestId === permissionId,
    );
    NodeAssert.equal(permissionResolved?.type, "request.resolved");
    if (permissionResolved?.type === "request.resolved") {
      NodeAssert.equal(permissionResolved.payload.decision, undefined);
      NodeAssert.deepEqual(permissionResolved.payload.resolution, {
        status: "expired",
        reason: "turn_interrupted",
      });
    }

    const questionResolved = observed.find(
      (event) => event.type === "user-input.resolved" && event.requestId === questionId,
    );
    NodeAssert.equal(questionResolved?.type, "user-input.resolved");
    if (questionResolved?.type === "user-input.resolved") {
      const payload = questionResolved.payload as {
        readonly answers: Readonly<Record<string, unknown>>;
        readonly resolution?: unknown;
      };
      NodeAssert.deepEqual(payload.answers, {});
      NodeAssert.deepEqual(payload.resolution, {
        status: "expired",
        reason: "turn_interrupted",
      });
    }
  }).pipe(Effect.provide(harness.layer));
});