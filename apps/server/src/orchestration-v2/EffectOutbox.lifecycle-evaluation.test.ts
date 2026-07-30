import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import {
  EffectOutboxV2,
  layer as effectOutboxLayer,
  type PendingOrchestrationEffectV2,
} from "./EffectOutbox.ts";

const TestLayer = effectOutboxLayer.pipe(Layer.provide(SqlitePersistenceMemory));

function interruptEffect(input: {
  readonly id: string;
  readonly threadId: ThreadId;
  readonly ordinal: number;
  readonly availableAt: DateTime.Utc;
}): PendingOrchestrationEffectV2 {
  return {
    id: input.id,
    commandId: CommandId.make(`command:lifecycle-interrupt:${input.ordinal}`),
    threadId: input.threadId,
    request: {
      type: "provider-turn.interrupt",
      providerSessionId: ProviderSessionId.make(
        `provider-session:lifecycle-interrupt:${input.ordinal}`,
      ),
      providerThreadId: ProviderThreadId.make(
        `provider-thread:lifecycle-interrupt:${input.ordinal}`,
      ),
      providerTurnId: ProviderTurnId.make(`provider-turn:lifecycle-interrupt:${input.ordinal}`),
    },
    availableAt: input.availableAt,
  };
}

function cleanupEffect(input: {
  readonly id: string;
  readonly threadId: ThreadId;
  readonly ordinal: number;
  readonly availableAt: DateTime.Utc;
}): PendingOrchestrationEffectV2 {
  return {
    id: input.id,
    commandId: CommandId.make(`command:lifecycle-cleanup:${input.ordinal}`),
    threadId: input.threadId,
    request: { type: "terminal.cleanup" },
    availableAt: input.availableAt,
  };
}

function requireSome<A>(value: Option.Option<A>, detail: string): A {
  if (Option.isNone(value)) {
    throw new Error(detail);
  }
  return value.value;
}

it.effect("serializes provider interrupts per thread without blocking another thread", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutboxV2;
    const availableAt = yield* DateTime.now;
    const interruptedThreadId = ThreadId.make("thread:lifecycle-serialized-interrupt");
    const independentThreadId = ThreadId.make("thread:lifecycle-independent-cleanup");

    yield* outbox.enqueue([
      interruptEffect({
        id: "effect:lifecycle:01-first-interrupt",
        threadId: interruptedThreadId,
        ordinal: 1,
        availableAt,
      }),
      interruptEffect({
        id: "effect:lifecycle:02-second-interrupt",
        threadId: interruptedThreadId,
        ordinal: 2,
        availableAt,
      }),
      cleanupEffect({
        id: "effect:lifecycle:03-independent-cleanup",
        threadId: independentThreadId,
        ordinal: 1,
        availableAt,
      }),
    ]);

    const first = requireSome(
      yield* outbox.claimNext({ workerId: "worker:lifecycle:first", leaseDurationMs: 60_000 }),
      "expected the first interrupt to be claimable",
    );
    assert.equal(first.id, "effect:lifecycle:01-first-interrupt");

    const second = requireSome(
      yield* outbox.claimNext({ workerId: "worker:lifecycle:second", leaseDurationMs: 60_000 }),
      "expected another thread to remain claimable",
    );
    assert.equal(second.id, "effect:lifecycle:03-independent-cleanup");

    assert.isTrue(
      yield* outbox.succeed({
        effectId: first.id,
        workerId: "worker:lifecycle:first",
      }),
    );

    const third = requireSome(
      yield* outbox.claimNext({ workerId: "worker:lifecycle:third", leaseDurationMs: 60_000 }),
      "expected the second same-thread interrupt after the first settled",
    );
    assert.equal(third.id, "effect:lifecycle:02-second-interrupt");
  }).pipe(Effect.provide(TestLayer)),
);

it.effect("cancels process-bound interrupts after process loss and requeues replay-safe cleanup", () =>
  Effect.gen(function* () {
    const outbox = yield* EffectOutboxV2;
    const availableAt = yield* DateTime.now;
    const interruptId = "effect:lifecycle:01-process-bound-interrupt";
    const cleanupId = "effect:lifecycle:02-replay-safe-cleanup";

    yield* outbox.enqueue([
      interruptEffect({
        id: interruptId,
        threadId: ThreadId.make("thread:lifecycle-process-loss-interrupt"),
        ordinal: 1,
        availableAt,
      }),
      cleanupEffect({
        id: cleanupId,
        threadId: ThreadId.make("thread:lifecycle-process-loss-cleanup"),
        ordinal: 1,
        availableAt,
      }),
    ]);

    assert.equal(
      requireSome(
        yield* outbox.claimNext({
          workerId: "worker:lifecycle:process-bound",
          leaseDurationMs: 60_000,
        }),
        "expected the interrupt effect to be claimed",
      ).id,
      interruptId,
    );
    assert.equal(
      requireSome(
        yield* outbox.claimNext({
          workerId: "worker:lifecycle:replay-safe",
          leaseDurationMs: 60_000,
        }),
        "expected the cleanup effect to be claimed",
      ).id,
      cleanupId,
    );

    assert.deepEqual(yield* outbox.reconcileAfterProcessLoss, {
      requeued: 1,
      cancelled: 1,
    });

    const interrupted = requireSome(
      yield* outbox.get(interruptId),
      "expected the interrupt effect to remain queryable",
    );
    const cleanup = requireSome(
      yield* outbox.get(cleanupId),
      "expected the cleanup effect to remain queryable",
    );

    assert.equal(interrupted.status, "cancelled");
    assert.include(interrupted.lastError ?? "", "server process ended");
    assert.equal(cleanup.status, "pending");
    assert.include(cleanup.lastError ?? "", "Requeued after");
  }).pipe(Effect.provide(TestLayer)),
);
