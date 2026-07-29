import * as NodeAssert from "node:assert/strict";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import { ProviderSessionReaper } from "../Services/ProviderSessionReaper.ts";
import { ProviderService, type ProviderServiceShape } from "../Services/ProviderService.ts";
import { makeProviderSessionReaperLive } from "./ProviderSessionReaper.ts";

const provider = ProviderDriverKind.make("opencode");
const instanceId = ProviderInstanceId.make("opencode");
const threadId = ThreadId.make("thread-provider-reaper-check-stop-race");
const activeTurnId = TurnId.make("turn-started-after-reaper-snapshot");
const drainFibers = Effect.forEach(Array.from({ length: 100 }), () => Effect.yieldNow, {
  discard: true,
});

const unsupported = () => Effect.die(new Error("Unsupported provider call in test")) as never;

function makeThreadShell(activeTurn: TurnId | null) {
  return {
    id: threadId,
    projectId: "project-reaper-race",
    title: "Reaper race",
    modelSelection: { instanceId, model: "openai/gpt-5" },
    interactionMode: "default",
    runtimeMode: "full-access",
    branch: null,
    worktreePath: null,
    createdAt: "1969-12-31T23:59:00.000Z",
    updatedAt: "1969-12-31T23:59:00.000Z",
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    latestTurn: null,
    messages: [],
    session: {
      threadId,
      status: activeTurn ? "running" : "ready",
      providerName: "opencode",
      runtimeMode: "full-access",
      activeTurnId: activeTurn,
      lastError: null,
      updatedAt: "1969-12-31T23:59:00.000Z",
    },
    activities: [],
    proposedPlans: [],
    checkpoints: [],
    deletedAt: null,
  } as never;
}

it.effect(
  "does not stop a provider session when a new turn starts after the reaper's idle snapshot",
  () => {
    let providerActiveTurn: TurnId | null = null;
    let projectionReads = 0;
    const stopCalls: Array<ThreadId> = [];

    const directoryLayer = Layer.succeed(ProviderSessionDirectory, {
      upsert: () => Effect.void,
      getProvider: () => Effect.succeed(provider),
      getBinding: () => Effect.succeed(Option.none()),
      listThreadIds: () => Effect.succeed([threadId]),
      listBindings: () =>
        Effect.succeed([
          {
            threadId,
            provider,
            providerInstanceId: instanceId,
            runtimeMode: "full-access",
            status: "running",
            lastSeenAt: "1969-12-31T23:59:00.000Z",
            resumeCursor: { schemaVersion: 1, sessionId: "ses_reaper_race" },
            runtimePayload: { activeTurnId: null },
          } as never,
        ]),
    });

    const providerService: ProviderServiceShape = {
      startSession: () => unsupported(),
      sendTurn: () => unsupported(),
      interruptTurn: () => unsupported(),
      respondToRequest: () => unsupported(),
      respondToUserInput: () => unsupported(),
      stopSession: ({ threadId: requestedThreadId }) =>
        Effect.sync(() => {
          stopCalls.push(requestedThreadId);
          providerActiveTurn = null;
        }),
      listSessions: () => Effect.succeed([]),
      getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
      getInstanceInfo: () =>
        Effect.succeed({
          instanceId,
          driverKind: provider,
          displayName: undefined,
          enabled: true,
          continuationIdentity: {
            driverKind: provider,
            continuationKey: "opencode:instance:opencode",
          },
        }),
      rollbackConversation: () => unsupported(),
      streamEvents: Stream.empty,
    };

    const projectionLayer = Layer.succeed(ProjectionSnapshotQuery, {
      getCommandReadModel: () => Effect.die("unused"),
      getSnapshot: () => Effect.die("unused"),
      getShellSnapshot: () => Effect.die("unused"),
      getArchivedShellSnapshot: () => Effect.die("unused"),
      getSnapshotSequence: () => Effect.succeed({ snapshotSequence: 0 }),
      getCounts: () => Effect.die("unused"),
      getActiveProjectByWorkspaceRoot: () => Effect.die("unused"),
      getProjectShellById: () => Effect.die("unused"),
      getFirstActiveThreadIdByProjectId: () => Effect.die("unused"),
      getThreadCheckpointContext: () => Effect.die("unused"),
      getFullThreadDiffContext: () => Effect.die("unused"),
      getThreadShellById: () =>
        Effect.sync(() => {
          projectionReads += 1;
          if (projectionReads === 1) {
            // The snapshot says idle, but a provider turn starts immediately
            // after that snapshot is materialized and before stopSession runs.
            providerActiveTurn = activeTurnId;
            return Option.some(makeThreadShell(null));
          }
          return Option.some(makeThreadShell(providerActiveTurn));
        }),
      getThreadDetailById: () => Effect.die("unused"),
      getThreadDetailSnapshot: () => Effect.die("unused"),
    });

    const layer = makeProviderSessionReaperLive({
      inactivityThresholdMs: 1,
      sweepIntervalMs: 60_000,
    }).pipe(
      Layer.provideMerge(directoryLayer),
      Layer.provideMerge(Layer.succeed(ProviderService, providerService)),
      Layer.provideMerge(projectionLayer),
      Layer.provideMerge(NodeServices.layer),
    );

    return Effect.gen(function* () {
      const reaper = yield* ProviderSessionReaper;
      yield* reaper.start();
      yield* drainFibers;

      NodeAssert.ok(projectionReads >= 1, "the reaper must inspect the stale binding");
      NodeAssert.deepEqual(stopCalls, []);
      NodeAssert.equal(providerActiveTurn, activeTurnId);
    }).pipe(Effect.provide(layer), Effect.scoped);
  },
);
