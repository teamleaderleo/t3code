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
  "t3/provider/Layers/OpenCodeAdapter.interrupt-owner-cancellation.test/OpenCodeAdapter",
) {}

const sessionId = "ses_interrupt_owner_cancellation";
const drainFibers = Effect.forEach(Array.from({ length: 100 }), () => Effect.yieldNow, {
  discard: true,
});

function makeHarness() {
  const state = {
    abortCalls: [] as Array<string>,
    abortResolvers: [] as Array<() => void>,
    promptCalls: [] as Array<unknown>,
  };

  const releaseAborts = (): void => {
    for (const resolve of state.abortResolvers.splice(0)) resolve();
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
            await new Promise<void>((resolve) => state.abortResolvers.push(resolve));
          },
        },
        event: {
          subscribe: async () => ({
            stream: (async function* () {
              // The interrupt must settle without relying on provider SSE.
            })(),
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

  return { layer, releaseAborts, state };
}

const startTurn = Effect.fn("OpenCodeAdapter.interrupt-owner-cancellation.test/startTurn")(
  function* (adapter: OpenCodeAdapterShape, threadId: ThreadId) {
    yield* adapter.startSession({
      provider: ProviderDriverKind.make("opencode"),
      threadId,
      runtimeMode: "full-access",
      resumeCursor: { schemaVersion: 1, sessionId },
    });
    return yield* adapter.sendTurn({
      threadId,
      input: "cancel the caller while provider abort is in flight",
      modelSelection: {
        instanceId: ProviderInstanceId.make("opencode"),
        model: "openai/gpt-5",
      },
    });
  },
);

it.effect("continues the owned provider abort after the initiating caller fiber is cancelled", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-interrupt-owner-cancelled");
    const terminals: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* adapter.streamEvents.pipe(
      Stream.filter(
        (event) =>
          event.threadId === threadId &&
          (event.type === "turn.aborted" || event.type === "turn.completed"),
      ),
      Stream.runForEach((event) => Effect.sync(() => terminals.push(event))),
      Effect.forkChild,
    );

    const turn = yield* startTurn(adapter, threadId);
    const caller = yield* adapter.interruptTurn(threadId, turn.turnId).pipe(Effect.forkChild);
    yield* drainFibers;
    NodeAssert.deepEqual(harness.state.abortCalls, [sessionId]);

    yield* Fiber.interrupt(caller);
    harness.releaseAborts();
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

it.effect("rejects a new prompt while the provider abort operation owns the active turn", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-send-during-interrupt");
    const turn = yield* startTurn(adapter, threadId);
    const caller = yield* adapter.interruptTurn(threadId, turn.turnId).pipe(Effect.forkChild);
    yield* drainFibers;

    const result = yield* adapter
      .sendTurn({
        threadId,
        input: "this prompt must not race the abort",
        modelSelection: {
          instanceId: ProviderInstanceId.make("opencode"),
          model: "openai/gpt-5",
        },
      })
      .pipe(Effect.result);

    harness.releaseAborts();
    yield* Fiber.join(caller);

    NodeAssert.equal(result._tag, "Failure");
    NodeAssert.deepEqual(harness.state.abortCalls, [sessionId]);
    NodeAssert.equal(harness.state.promptCalls.length, 1);
  }).pipe(Effect.provide(harness.layer));
});

it.effect("reuses the in-flight provider abort when stopSession races the interrupt", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-stop-during-interrupt");
    const turn = yield* startTurn(adapter, threadId);
    const interruptCaller = yield* adapter
      .interruptTurn(threadId, turn.turnId)
      .pipe(Effect.forkChild);
    yield* drainFibers;

    const stopCaller = yield* adapter.stopSession(threadId).pipe(Effect.forkChild);
    yield* drainFibers;
    const abortCallsBeforeRelease = [...harness.state.abortCalls];

    harness.releaseAborts();
    yield* Fiber.join(interruptCaller).pipe(Effect.ignore);
    yield* Fiber.join(stopCaller);

    NodeAssert.deepEqual(abortCallsBeforeRelease, [sessionId]);
    NodeAssert.equal(yield* adapter.hasSession(threadId), false);
  }).pipe(Effect.provide(harness.layer));
});
