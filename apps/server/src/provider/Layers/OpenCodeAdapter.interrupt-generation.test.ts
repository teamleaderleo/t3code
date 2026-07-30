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
  "t3/provider/Layers/OpenCodeAdapter.interrupt-generation.test/OpenCodeAdapter",
) {}

const oldSessionId = "ses_interrupt_generation_old";
const newSessionId = "ses_interrupt_generation_new";
const drainFibers = Effect.forEach(Array.from({ length: 100 }), () => Effect.yieldNow, {
  discard: true,
});

function makeHarness() {
  const state = {
    abortCalls: [] as Array<string>,
    firstAbortResolvers: [] as Array<() => void>,
  };

  const releaseFirstAbort = (): void => {
    for (const resolve of state.firstAbortResolvers.splice(0)) resolve();
  };

  const runtime: OpenCodeRuntimeShape = {
    startOpenCodeServerProcess: () =>
      Effect.succeed({ url: "http://127.0.0.1:4304", exitCode: Effect.never }),
    connectToOpenCodeServer: ({ serverUrl }) =>
      Effect.succeed({
        url: serverUrl ?? "http://127.0.0.1:4304",
        exitCode: null,
        external: true,
      }),
    runOpenCodeCommand: () => Effect.succeed({ stdout: "", stderr: "", code: 0 }),
    createOpenCodeSdkClient: () =>
      ({
        session: {
          get: async ({ sessionID }: { sessionID: string }) => ({
            data: { id: sessionID },
          }),
          update: async ({ sessionID }: { sessionID: string }) => ({ data: { id: sessionID } }),
          promptAsync: async () => undefined,
          abort: async ({ sessionID }: { sessionID: string }) => {
            state.abortCalls.push(sessionID);
            if (state.abortCalls.length === 1) {
              await new Promise<void>((resolve) => state.firstAbortResolvers.push(resolve));
            }
          },
        },
        event: {
          subscribe: async () => ({
            stream: (async function* () {
              // Settlement and replacement ordering are owned locally.
            })(),
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

  return { layer, releaseFirstAbort, state };
}

it.effect("does not let session replacement overtake an abort owned by the old generation", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-interrupt-generation");
    const observed: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* adapter.streamEvents.pipe(
      Stream.filter((event) => event.threadId === threadId),
      Stream.runForEach((event) => Effect.sync(() => observed.push(event))),
      Effect.forkChild,
    );

    yield* adapter.startSession({
      provider: ProviderDriverKind.make("opencode"),
      threadId,
      runtimeMode: "full-access",
      resumeCursor: { schemaVersion: 1, sessionId: oldSessionId },
    });
    const oldTurn = yield* adapter.sendTurn({
      threadId,
      input: "interrupt the old provider generation",
      modelSelection: {
        instanceId: ProviderInstanceId.make("opencode"),
        model: "openai/gpt-5",
      },
    });

    const interruptFiber = yield* adapter
      .interruptTurn(threadId, oldTurn.turnId)
      .pipe(Effect.forkChild);
    yield* drainFibers;
    NodeAssert.deepEqual(harness.state.abortCalls, [oldSessionId]);

    const replacementFiber = yield* adapter
      .startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: newSessionId },
      })
      .pipe(Effect.forkChild);
    yield* drainFibers;

    NodeAssert.equal(replacementFiber.pollUnsafe(), undefined);
    NodeAssert.deepEqual(harness.state.abortCalls, [oldSessionId]);

    harness.releaseFirstAbort();
    yield* Fiber.join(interruptFiber);
    const replacement = yield* Fiber.join(replacementFiber);
    yield* drainFibers;
    yield* Fiber.interrupt(watcher);

    NodeAssert.equal(replacement.resumeCursor?.sessionId, newSessionId);
    NodeAssert.deepEqual(harness.state.abortCalls, [oldSessionId]);

    const oldCompletionIndex = observed.findIndex(
      (event) =>
        event.type === "turn.completed" &&
        event.turnId === oldTurn.turnId &&
        event.payload.state === "interrupted",
    );
    const startedIndices = observed
      .map((event, index) => (event.type === "session.started" ? index : -1))
      .filter((index) => index >= 0);
    const replacementStartedIndex = startedIndices.at(-1) ?? -1;

    NodeAssert.equal(oldCompletionIndex >= 0, true);
    NodeAssert.equal(replacementStartedIndex > oldCompletionIndex, true);

    const sessions = yield* adapter.listSessions();
    NodeAssert.equal(sessions.length, 1);
    NodeAssert.equal(sessions[0]?.resumeCursor?.sessionId, newSessionId);
    NodeAssert.equal(sessions[0]?.activeTurnId, undefined);
  }).pipe(Effect.provide(harness.layer));
});