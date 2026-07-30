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
  "t3/provider/Layers/OpenCodeAdapter.pending-request-completion.test/OpenCodeAdapter",
) {}

const sessionId = "ses_pending_request_completion";
const permissionId = ApprovalRequestId.make("per_pending_completion");
const questionId = ApprovalRequestId.make("que_pending_completion");
const STREAM_END = Symbol("pending-request-completion/stream-end");
const drainFibers = Effect.forEach(Array.from({ length: 60 }), () => Effect.yieldNow, {
  discard: true,
});

function makeEventBus() {
  const buffered: Array<unknown> = [];
  const waiters: Array<(event: unknown | typeof STREAM_END) => void> = [];
  const push = (event: unknown): void => {
    const waiter = waiters.shift();
    if (waiter) waiter(event);
    else buffered.push(event);
  };
  const stream = (signal?: AbortSignal): AsyncIterable<unknown> => ({
    [Symbol.asyncIterator]() {
      return {
        next: async (): Promise<IteratorResult<unknown>> => {
          const immediate = buffered.shift();
          if (immediate !== undefined) return { done: false, value: immediate };
          const event = await new Promise<unknown | typeof STREAM_END>((resolve) => {
            let settled = false;
            const finish = (value: unknown | typeof STREAM_END) => {
              if (settled) return;
              settled = true;
              signal?.removeEventListener("abort", onAbort);
              const index = waiters.indexOf(finish);
              if (index >= 0) waiters.splice(index, 1);
              resolve(value);
            };
            const onAbort = () => finish(STREAM_END);
            waiters.push(finish);
            signal?.addEventListener("abort", onAbort, { once: true });
            if (signal?.aborted) onAbort();
          });
          return event === STREAM_END
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
    permissionReplyCalls: [] as Array<unknown>,
    questionReplyCalls: [] as Array<unknown>,
  };
  const runtime: OpenCodeRuntimeShape = {
    startOpenCodeServerProcess: () =>
      Effect.succeed({ url: "http://127.0.0.1:4305", exitCode: Effect.never }),
    connectToOpenCodeServer: ({ serverUrl }) =>
      Effect.succeed({
        url: serverUrl ?? "http://127.0.0.1:4305",
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
          detail: "not used in test",
          cause: null,
        }),
      ),
    loadInventoryFromCli: () =>
      Effect.fail(
        new OpenCodeRuntimeError({
          operation: "loadInventoryFromCli",
          detail: "not used in test",
          cause: null,
        }),
      ),
  };
  const directoryLayer = Layer.succeed(ProviderSessionDirectory, {
    upsert: () => Effect.void,
    getProvider: () => Effect.die(new Error("getProvider is not used in test")),
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
    Layer.provideMerge(directoryLayer),
    Layer.provideMerge(NodeServices.layer),
  );
  return { events, layer, state };
}

it.effect("expires stale pending handles before normal turn completion is projected", () => {
  const harness = makeHarness();
  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-pending-request-completion");
    const observed: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* adapter.streamEvents.pipe(
      Stream.filter((event) => event.threadId === threadId),
      Stream.runForEach((event) => Effect.sync(() => observed.push(event))),
      Effect.forkChild,
    );

    yield* adapter.startSession({
      provider: ProviderDriverKind.make("opencode"),
      threadId,
      runtimeMode: "approval-required",
      resumeCursor: { schemaVersion: 1, sessionId },
    });
    const turn = yield* adapter.sendTurn({
      threadId,
      input: "finish while stale request handles remain",
      modelSelection: {
        instanceId: ProviderInstanceId.make("opencode"),
        model: "openai/gpt-5",
      },
    });

    harness.events.push({
      type: "session.status",
      properties: { sessionID: sessionId, status: { type: "busy" } },
    });
    harness.events.push({
      type: "permission.asked",
      properties: {
        id: permissionId,
        sessionID: sessionId,
        permission: "skill",
        patterns: ["restoring-agent-sessions"],
        metadata: {},
        always: ["restoring-agent-sessions"],
        tool: { messageID: "msg_complete_permission", callID: "call_complete_permission" },
      },
    });
    harness.events.push({
      type: "question.asked",
      properties: {
        id: questionId,
        sessionID: sessionId,
        questions: [
          {
            question: "Continue?",
            header: "Confirmation",
            options: [
              { label: "Yes", description: "Continue." },
              { label: "No", description: "Stop." },
            ],
            multiple: false,
          },
        ],
        tool: { messageID: "msg_complete_question", callID: "call_complete_question" },
      },
    });
    yield* drainFibers;
    harness.events.push({
      type: "session.status",
      properties: { sessionID: sessionId, status: { type: "idle" } },
    });
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

    const permissionResolvedIndex = observed.findIndex(
      (event) =>
        event.type === "request.resolved" &&
        event.requestId === permissionId &&
        (event.payload.resolution as { readonly reason?: unknown } | undefined)?.reason ===
          "turn_completed",
    );
    const questionResolvedIndex = observed.findIndex(
      (event) =>
        event.type === "user-input.resolved" &&
        event.requestId === questionId &&
        (event.payload.resolution as { readonly reason?: unknown } | undefined)?.reason ===
          "turn_completed",
    );
    const completedIndex = observed.findIndex(
      (event) =>
        event.type === "turn.completed" &&
        event.turnId === turn.turnId &&
        event.payload.state === "completed",
    );

    NodeAssert.equal(permissionResolvedIndex >= 0, true);
    NodeAssert.equal(questionResolvedIndex >= 0, true);
    NodeAssert.equal(completedIndex > permissionResolvedIndex, true);
    NodeAssert.equal(completedIndex > questionResolvedIndex, true);
  }).pipe(Effect.provide(harness.layer));
});