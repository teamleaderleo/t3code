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
        status: async () => ({
          data: {
            [resumedSessionId]: { type: "idle" },
          },
        }),
        abort: async () => undefined,
      },
      event: {
        subscribe: async () => ({
          stream: (async function* () {
            // Recovery must not depend on a fresh status event. OpenCode exposes
            // an authoritative session-status snapshot for this purpose.
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

it.layer(OpenCodeAdapterTestLayer)("OpenCodeAdapter restart reconciliation", (it) => {
  it.effect("completes the exact persisted turn when a resumed session is idle", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = ThreadId.make("thread-opencode-resumed-idle");
      const completionFiber = yield* adapter.streamEvents.pipe(
        Stream.filter(
          (event) =>
            event.threadId === threadId &&
            event.type === "turn.completed" &&
            event.turnId === persistedTurnId,
        ),
        Stream.take(1),
        Stream.runCollect,
        Effect.forkChild,
      );

      // This extra field describes the intended internal recovery contract
      // between ProviderService and provider adapters. It deliberately does not
      // change the public websocket start-session schema.
      const recoveryInput = {
        provider: ProviderDriverKind.make("opencode"),
        threadId,
        runtimeMode: "full-access" as const,
        resumeCursor: { schemaVersion: 1, sessionId: resumedSessionId },
        recoveryActiveTurnId: persistedTurnId,
      } satisfies Parameters<OpenCodeAdapterShape["startSession"]>[0] & {
        recoveryActiveTurnId: TurnId;
      };

      yield* adapter.startSession(recoveryInput);

      const events = Array.from(
        yield* Fiber.join(completionFiber).pipe(Effect.timeout("1 second")),
      );
      NodeAssert.equal(events.length, 1);
      NodeAssert.equal(events[0]?.type, "turn.completed");
      if (events[0]?.type === "turn.completed") {
        NodeAssert.equal(events[0].turnId, persistedTurnId);
        NodeAssert.equal(events[0].payload.state, "completed");
      }
    }),
  );
});
