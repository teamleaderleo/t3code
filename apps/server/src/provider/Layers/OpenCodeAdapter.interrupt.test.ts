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
const abortCalls: string[] = [];

const OpenCodeRuntimeTestDouble: OpenCodeRuntimeShape = {
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
        get: async ({ sessionID }: { sessionID: string }) => ({ data: { id: sessionID } }),
        update: async ({ sessionID }: { sessionID: string }) => ({ data: { id: sessionID } }),
        promptAsync: async () => undefined,
        abort: async ({ sessionID }: { sessionID: string }) => {
          abortCalls.push(sessionID);
          // OpenCode's abort endpoint waits for its runner cancellation path,
          // whose idle callback updates provider status before the response
          // returns. This test deliberately withholds SSE delivery so T3 cannot
          // depend on a later status event for lifecycle settlement.
        },
      },
      event: {
        subscribe: async () => ({
          stream: (async function* () {
            // No provider event is delivered after abort.
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

const openCodeAdapterTestSettings = Schema.decodeSync(OpenCodeSettings)({
  binaryPath: "fake-opencode",
  serverUrl: "http://127.0.0.1:9999",
  serverPassword: "secret-password",
});

const OpenCodeAdapterTestLayer = Layer.effect(
  OpenCodeAdapter,
  makeOpenCodeAdapter(openCodeAdapterTestSettings),
).pipe(
  Layer.provideMerge(Layer.succeed(OpenCodeRuntime, OpenCodeRuntimeTestDouble)),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(providerSessionDirectoryTestLayer),
  Layer.provideMerge(NodeServices.layer),
);

it.layer(OpenCodeAdapterTestLayer)("OpenCodeAdapter interrupt settlement", (it) => {
  it.effect("settles the exact interrupted turn after abort succeeds without an idle event", () =>
    Effect.gen(function* () {
      abortCalls.length = 0;
      const adapter = yield* OpenCodeAdapter;
      const threadId = ThreadId.make("thread-opencode-interrupt");

      yield* adapter.startSession({
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access",
        resumeCursor: { schemaVersion: 1, sessionId: resumedSessionId },
      });

      const turn = yield* adapter.sendTurn({
        threadId,
        input: "keep working until interrupted",
        modelSelection: {
          instanceId: ProviderInstanceId.make("opencode"),
          model: "openai/gpt-5",
        },
      });

      const completionFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.threadId === threadId &&
            event.type === "turn.completed" &&
            event.turnId === turn.turnId,
        ),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );

      yield* adapter.interruptTurn(threadId, turn.turnId);

      const events = Array.from(
        yield* Fiber.join(completionFiber).pipe(Effect.timeout("1 second")),
      );
      NodeAssert.deepEqual(abortCalls, [resumedSessionId]);
      NodeAssert.equal(events.length, 1);
      NodeAssert.equal(events[0]?.type, "turn.completed");
      if (events[0]?.type === "turn.completed") {
        NodeAssert.equal(events[0].turnId, turn.turnId);
        NodeAssert.equal(events[0].payload.state, "interrupted");
      }
    }),
  );
});
