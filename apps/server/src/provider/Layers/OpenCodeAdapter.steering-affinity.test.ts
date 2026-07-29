import * as NodeAssert from "node:assert/strict";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

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
  "t3/provider/Layers/OpenCodeAdapter.steering-affinity.test/OpenCodeAdapter",
) {}

const sessionId = "ses_steering_affinity";

function makeHarness() {
  const state = {
    promptCalls: [] as Array<Record<string, unknown>>,
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
          get: async ({ sessionID }: { sessionID: string }) => ({ data: { id: sessionID } }),
          update: async ({ sessionID }: { sessionID: string }) => ({ data: { id: sessionID } }),
          promptAsync: async (request: Record<string, unknown>) => {
            state.promptCalls.push(request);
          },
          abort: async () => undefined,
        },
        event: {
          subscribe: async () => ({
            stream: (async function* () {
              // No provider lifecycle event is required for this request-affinity test.
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

  return { layer, state };
}

it.effect("assigns a distinct provider user-message id to every prompt in one steered turn", () => {
  const harness = makeHarness();

  return Effect.gen(function* () {
    const adapter = yield* OpenCodeAdapter;
    const threadId = ThreadId.make("thread-opencode-steering-message-affinity");
    const modelSelection = {
      instanceId: ProviderInstanceId.make("opencode"),
      model: "openai/gpt-5",
    } as const;

    yield* adapter.startSession({
      provider: ProviderDriverKind.make("opencode"),
      threadId,
      runtimeMode: "full-access",
      resumeCursor: { schemaVersion: 1, sessionId },
    });

    const initial = yield* adapter.sendTurn({
      threadId,
      input: "initial instruction",
      modelSelection,
    });
    const firstSteer = yield* adapter.sendTurn({
      threadId,
      input: "first steering instruction",
      modelSelection,
    });
    const secondSteer = yield* adapter.sendTurn({
      threadId,
      input: "second steering instruction",
      modelSelection,
    });

    NodeAssert.equal(firstSteer.turnId, initial.turnId);
    NodeAssert.equal(secondSteer.turnId, initial.turnId);
    NodeAssert.equal(harness.state.promptCalls.length, 3);

    const providerMessageIds = harness.state.promptCalls.map((request) => request.messageID);
    NodeAssert.equal(
      providerMessageIds.every(
        (messageId) => typeof messageId === "string" && messageId.startsWith("msg"),
      ),
      true,
      "every OpenCode prompt must carry a caller-generated provider message id",
    );
    NodeAssert.equal(
      new Set(providerMessageIds).size,
      providerMessageIds.length,
      "steering prompts must not reuse provider message identity",
    );

    const sessions = yield* adapter.listSessions();
    const session = sessions.find((entry) => entry.threadId === threadId);
    NodeAssert.equal(session?.status, "running");
    NodeAssert.equal(session?.activeTurnId, initial.turnId);
  }).pipe(Effect.provide(harness.layer));
});
