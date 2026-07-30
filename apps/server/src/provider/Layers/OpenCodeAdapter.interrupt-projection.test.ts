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
  "t3/provider/Layers/OpenCodeAdapter.interrupt-projection.test/OpenCodeAdapter",
) {}

const sessionId = "ses_interrupt_projection";
const drainFibers = Effect.forEach(Array.from({ length: 50 }), () => Effect.yieldNow, {
  discard: true,
});

function makeHarness() {
  const state = { abortCalls: [] as Array<string> };
  const runtime: OpenCodeRuntimeShape = {
    startOpenCodeServerProcess: () =>
      Effect.succeed({ url: "http://127.0.0.1:4303", exitCode: Effect.never }),
    connectToOpenCodeServer: ({ serverUrl }) =>
      Effect.succeed({
        url: serverUrl ?? "http://127.0.0.1:4303",
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
        event: {
          subscribe: async () => ({
            stream: (async function* () {
              // Successful interrupt must settle without relying on later SSE.
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

  return { layer, state };
}

it.effect("emits exact turn.completed interrupted so orchestration can close the run", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-interrupt-projection");
    const terminalEvents: Array<ProviderRuntimeEvent> = [];
    const watcher = yield* adapter.streamEvents.pipe(
      Stream.filter(
        (event) =>
          event.threadId === threadId &&
          (event.type === "turn.aborted" || event.type === "turn.completed"),
      ),
      Stream.runForEach((event) => Effect.sync(() => terminalEvents.push(event))),
      Effect.forkChild,
    );

    yield* adapter.startSession({
      provider: ProviderDriverKind.make("opencode"),
      threadId,
      runtimeMode: "full-access",
      resumeCursor: { schemaVersion: 1, sessionId },
    });
    const turn = yield* adapter.sendTurn({
      threadId,
      input: "interrupt me without a later provider event",
      modelSelection: {
        instanceId: ProviderInstanceId.make("opencode"),
        model: "openai/gpt-5",
      },
    });

    yield* adapter.interruptTurn(threadId, turn.turnId);
    yield* drainFibers;
    yield* Fiber.interrupt(watcher);

    NodeAssert.deepEqual(harness.state.abortCalls, [sessionId]);
    NodeAssert.equal(terminalEvents.length, 1);
    const terminal = terminalEvents[0];
    NodeAssert.equal(terminal?.type, "turn.completed");
    if (terminal?.type === "turn.completed") {
      NodeAssert.equal(terminal.turnId, turn.turnId);
      NodeAssert.equal(terminal.payload.state, "interrupted");
    }

    const sessions = yield* adapter.listSessions();
    const session = sessions.find((entry) => entry.threadId === threadId);
    NodeAssert.equal(session?.status, "ready");
    NodeAssert.equal(session?.activeTurnId, undefined);
  }).pipe(Effect.provide(harness.layer));
});