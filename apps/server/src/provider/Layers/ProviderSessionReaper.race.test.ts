// @effect-diagnostics nodeBuiltinImport:off
import * as NodeAssert from "node:assert/strict";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderSendTurnInput,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../Services/ProviderAdapterRegistry.ts";
import { ProviderSessionReaper } from "../Services/ProviderSessionReaper.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import { makeAdapterRegistryMock } from "../testUtils/providerAdapterRegistryMock.ts";
import * as ProviderEventLoggers from "./ProviderEventLoggers.ts";
import { makeProviderServiceLive } from "./ProviderService.ts";
import { makeProviderSessionReaperLive } from "./ProviderSessionReaper.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";

const provider = ProviderDriverKind.make("opencode");
const instanceId = ProviderInstanceId.make("opencode");
const threadId = ThreadId.make("thread-provider-reaper-check-stop-race");
const activeTurnId = TurnId.make("turn-started-after-reaper-snapshot");
const drainFibers = Effect.forEach(Array.from({ length: 50 }), () => Effect.yieldNow, {
  discard: true,
});

function makeRaceAdapter() {
  const sessions = new Map<ThreadId, ProviderSession>();
  const runtimeEvents = Effect.runSync(PubSub.unbounded<ProviderRuntimeEvent>());
  const state = {
    stopCalls: [] as Array<ThreadId>,
  };

  const adapter: ProviderAdapterShape<never> = {
    provider,
    capabilities: {
      sessionModelSwitch: "in-session",
    },
    startSession: (input: ProviderSessionStartInput) =>
      Effect.sync(() => {
        const now = "2026-07-30T00:00:00.000Z";
        const session: ProviderSession = {
          provider,
          providerInstanceId: input.providerInstanceId,
          status: "ready",
          runtimeMode: input.runtimeMode,
          threadId: input.threadId,
          resumeCursor: { schemaVersion: 1, sessionId: "ses_reaper_race" },
          createdAt: now,
          updatedAt: now,
        };
        sessions.set(input.threadId, session);
        return session;
      }),
    sendTurn: (input: ProviderSendTurnInput) =>
      Effect.sync(() => {
        const existing = sessions.get(input.threadId);
        if (!existing) throw new Error("test session missing");
        sessions.set(input.threadId, {
          ...existing,
          status: "running",
          activeTurnId,
          updatedAt: "2026-07-30T00:00:01.000Z",
        });
        return {
          threadId: input.threadId,
          turnId: activeTurnId,
          resumeCursor: existing.resumeCursor,
        };
      }),
    interruptTurn: () => Effect.void,
    respondToRequest: () => Effect.void,
    respondToUserInput: () => Effect.void,
    stopSession: (requestedThreadId) =>
      Effect.sync(() => {
        state.stopCalls.push(requestedThreadId);
        sessions.delete(requestedThreadId);
      }),
    listSessions: () => Effect.sync(() => Array.from(sessions.values())),
    hasSession: (requestedThreadId) => Effect.sync(() => sessions.has(requestedThreadId)),
    readThread: (requestedThreadId) =>
      Effect.succeed({
        threadId: requestedThreadId,
        turns: [],
      }),
    rollbackThread: (requestedThreadId) =>
      Effect.succeed({
        threadId: requestedThreadId,
        turns: [],
      }),
    stopAll: () =>
      Effect.sync(() => {
        sessions.clear();
      }),
    get streamEvents() {
      return Stream.fromPubSub(runtimeEvents);
    },
  };

  return { adapter, sessions, state };
}

function makeThreadShell(activeTurn: TurnId | null) {
  return {
    id: threadId,
    projectId: "project-reaper-race",
    title: "Reaper race",
    modelSelection: {
      instanceId,
      model: "openai/gpt-5",
    },
    interactionMode: "default",
    runtimeMode: "full-access",
    branch: null,
    worktreePath: null,
    createdAt: "2026-07-30T00:00:00.000Z",
    updatedAt: "2026-07-30T00:00:00.000Z",
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
      updatedAt: "2026-07-30T00:00:00.000Z",
    },
    activities: [],
    proposedPlans: [],
    checkpoints: [],
    deletedAt: null,
  } as never;
}

it.effect("does not stop a provider session when a new turn starts after the reaper's idle snapshot", () =>
  Effect.gen(function* () {
    const firstSnapshotRead = yield* Deferred.make<void>();
    const releaseFirstSnapshot = yield* Deferred.make<void>();
    const fake = makeRaceAdapter();
    let projectionReads = 0;

    const registry = makeAdapterRegistryMock({
      [provider]: fake.adapter,
    });
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(
      Layer.provide(runtimeRepositoryLayer),
    );
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
        Effect.gen(function* () {
          projectionReads += 1;
          if (projectionReads === 1) {
            yield* Deferred.succeed(firstSnapshotRead, undefined);
            yield* Deferred.await(releaseFirstSnapshot);
            return Option.some(makeThreadShell(null));
          }
          const active = fake.sessions.get(threadId)?.activeTurnId ?? null;
          return Option.some(makeThreadShell(active));
        }),
      getThreadDetailById: () => Effect.die("unused"),
      getThreadDetailSnapshot: () => Effect.die("unused"),
    });

    const providerAdapterLayer = Layer.succeed(
      ProviderAdapterRegistry.ProviderAdapterRegistry,
      registry,
    );
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(providerAdapterLayer),
      Layer.provide(directoryLayer),
      Layer.provide(ServerSettings.ServerSettingsService.layerTest()),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );
    const reaperLayer = makeProviderSessionReaperLive({
      inactivityThresholdMs: 1,
      sweepIntervalMs: 60_000,
    }).pipe(
      Layer.provide(providerLayer),
      Layer.provide(directoryLayer),
      Layer.provide(projectionLayer),
    );
    const fullLayer = Layer.mergeAll(
      providerLayer,
      reaperLayer,
      directoryLayer,
      runtimeRepositoryLayer,
      NodeServices.layer,
    );

    const scope = yield* Scope.make("sequential");
    const services = yield* Layer.buildWithScope(fullLayer, scope);
    const service = yield* ProviderService.ProviderService.pipe(Effect.provide(services));
    const reaper = yield* ProviderSessionReaper.pipe(Effect.provide(services));
    const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository.pipe(
      Effect.provide(services),
    );

    yield* service.startSession(threadId, {
      provider,
      providerInstanceId: instanceId,
      threadId,
      runtimeMode: "full-access",
    });
    yield* repository.upsert({
      threadId,
      providerName: provider,
      providerInstanceId: instanceId,
      adapterKey: provider,
      runtimeMode: "full-access",
      status: "running",
      lastSeenAt: "2026-01-01T00:00:00.000Z",
      resumeCursor: { schemaVersion: 1, sessionId: "ses_reaper_race" },
      runtimePayload: {
        activeTurnId: null,
      },
    });

    yield* reaper.start().pipe(Scope.provide(scope));
    yield* Deferred.await(firstSnapshotRead);

    const turn = yield* service.sendTurn({
      threadId,
      input: "start after the stale reaper snapshot",
      attachments: [],
      modelSelection: {
        instanceId,
        model: "openai/gpt-5",
      },
    });
    NodeAssert.equal(turn.turnId, activeTurnId);

    yield* Deferred.succeed(releaseFirstSnapshot, undefined);
    yield* drainFibers;

    NodeAssert.deepEqual(fake.state.stopCalls, []);
    NodeAssert.equal(fake.sessions.has(threadId), true);
    NodeAssert.equal(fake.sessions.get(threadId)?.activeTurnId, activeTurnId);

    const persisted = yield* repository.getByThreadId({ threadId });
    NodeAssert.equal(Option.isSome(persisted), true);
    if (Option.isSome(persisted)) {
      NodeAssert.equal(persisted.value.status, "running");
      NodeAssert.equal(
        (persisted.value.runtimePayload as { readonly activeTurnId?: unknown } | null)?.activeTurnId,
        activeTurnId,
      );
    }

    yield* Scope.close(scope, Exit.void);
  }),
);
