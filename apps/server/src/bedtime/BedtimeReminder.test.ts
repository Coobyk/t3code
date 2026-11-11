import { assert, it } from "@effect/vitest";
import type { BedtimeReminderState } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as Scheduler from "../scheduling/Scheduler.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as BedtimeReminder from "./BedtimeReminder.ts";

const atLocal = (
  year: number,
  month: number,
  day: number,
  hour: number,
  minute: number,
): DateTime.Zoned =>
  DateTime.makeZonedUnsafe(
    { year, month, day, hour, minute, second: 0, millisecond: 0 },
    { timeZone: DateTime.zoneMakeLocal(), adjustForTimeZone: true },
  );

const iso = (value: DateTime.DateTime) => DateTime.formatIso(DateTime.toUtc(value));

const settings = (overrides: {
  enabled?: boolean;
  time?: string;
  intervalMinutes?: number;
  acknowledgedFor?: string | null;
}) => ({
  bedtimeReminderEnabled: overrides.enabled ?? true,
  bedtimeReminderTime: overrides.time ?? "23:00",
  bedtimeReminderNudgeIntervalMinutes: overrides.intervalMinutes ?? 15,
  bedtimeReminderAcknowledgedFor: overrides.acknowledgedFor ?? null,
});

it.effect("reports off when disabled", () =>
  Effect.sync(() => {
    const { state } = BedtimeReminder.bedtimeReminderStateAt(
      settings({ enabled: false }),
      atLocal(2026, 7, 1, 23, 30),
    );
    assert.deepStrictEqual(state, { kind: "off" });
  }),
);

it.effect("schedules the next bedtime before it fires", () =>
  Effect.sync(() => {
    const { state, dueAtMs } = BedtimeReminder.bedtimeReminderStateAt(
      settings({}),
      atLocal(2026, 7, 1, 22, 0),
    );
    const expected = atLocal(2026, 7, 1, 23, 0);
    assert.deepStrictEqual(state, {
      kind: "scheduled",
      nextFireAt: iso(expected),
      acknowledged: false,
    });
    assert.strictEqual(dueAtMs, DateTime.toEpochMillis(expected));
  }),
);

it.effect("reminds after bedtime and escalates nudgeIndex per interval", () =>
  Effect.sync(() => {
    const atTenPast = BedtimeReminder.bedtimeReminderStateAt(
      settings({}),
      atLocal(2026, 7, 1, 23, 10),
    );
    assert.deepStrictEqual(atTenPast.state, {
      kind: "reminding",
      nightStartedAt: iso(atLocal(2026, 7, 1, 23, 0)),
      nudgeIndex: 0,
      nextNudgeAt: iso(atLocal(2026, 7, 1, 23, 15)),
    });

    const atThirtyFivePast = BedtimeReminder.bedtimeReminderStateAt(
      settings({}),
      atLocal(2026, 7, 1, 23, 35),
    );
    assert.strictEqual(atThirtyFivePast.state.kind, "reminding");
    if (atThirtyFivePast.state.kind !== "reminding") return;
    assert.strictEqual(atThirtyFivePast.state.nudgeIndex, 2);
    assert.strictEqual(atThirtyFivePast.state.nextNudgeAt, iso(atLocal(2026, 7, 1, 23, 45)));
  }),
);

it.effect("stays quiet once tonight is acknowledged, then re-arms the next day", () =>
  Effect.sync(() => {
    const acknowledged = BedtimeReminder.bedtimeReminderStateAt(
      settings({ acknowledgedFor: iso(atLocal(2026, 7, 1, 23, 0)) }),
      atLocal(2026, 7, 1, 23, 30),
    );
    assert.deepStrictEqual(acknowledged.state, {
      kind: "scheduled",
      nextFireAt: iso(atLocal(2026, 7, 2, 23, 0)),
      acknowledged: true,
    });

    const nextNight = BedtimeReminder.bedtimeReminderStateAt(
      settings({ acknowledgedFor: iso(atLocal(2026, 7, 1, 23, 0)) }),
      atLocal(2026, 7, 2, 23, 5),
    );
    assert.strictEqual(nextNight.state.kind, "reminding");
  }),
);

it.effect("skipping the upcoming bedtime moves the next fire to the following day", () =>
  Effect.sync(() => {
    const { state } = BedtimeReminder.bedtimeReminderStateAt(
      settings({ acknowledgedFor: iso(atLocal(2026, 7, 1, 23, 0)) }),
      atLocal(2026, 7, 1, 22, 0),
    );
    assert.deepStrictEqual(state, {
      kind: "scheduled",
      nextFireAt: iso(atLocal(2026, 7, 2, 23, 0)),
      acknowledged: true,
    });
  }),
);

it.effect("stops nudging by the next morning", () =>
  Effect.sync(() => {
    const stillNight = BedtimeReminder.bedtimeReminderStateAt(
      settings({}),
      atLocal(2026, 7, 2, 8, 0),
    );
    assert.strictEqual(stillNight.state.kind, "reminding");

    const morning = BedtimeReminder.bedtimeReminderStateAt(
      settings({}),
      atLocal(2026, 7, 2, 10, 0),
    );
    assert.deepStrictEqual(morning.state, {
      kind: "scheduled",
      nextFireAt: iso(atLocal(2026, 7, 2, 23, 0)),
      acknowledged: false,
    });
  }),
);

it.effect("acknowledge quiets the active night and the next sweep re-arms it", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(DateTime.toEpochMillis(atLocal(2026, 7, 1, 23, 10)));
    const layer = BedtimeReminder.layer.pipe(
      Layer.provideMerge(Layer.mergeAll(Scheduler.layer, ServerSettings.layerTest(settings({})))),
    );
    yield* Effect.gen(function* () {
      const service = yield* BedtimeReminder.BedtimeReminder;
      const settingsService = yield* ServerSettings.ServerSettingsService;
      const states = yield* Queue.unbounded<BedtimeReminderState>();
      yield* Effect.forkChild(
        service.streamChanges.pipe(Stream.runForEach((state) => Queue.offer(states, state))),
      );

      // The initial snapshot can be "off" until the register-time sweep publishes.
      const takeKind = (kind: BedtimeReminderState["kind"]): Effect.Effect<BedtimeReminderState> =>
        Queue.take(states).pipe(
          Effect.filterOrElse(
            (state) => state.kind === kind,
            () => takeKind(kind),
          ),
        );

      const first = yield* takeKind("reminding");
      assert.strictEqual(first.kind, "reminding");

      const acknowledged = yield* service.acknowledge({});
      assert.strictEqual(acknowledged.kind, "scheduled");
      if (acknowledged.kind === "scheduled") {
        assert.strictEqual(acknowledged.acknowledged, true);
        assert.strictEqual(acknowledged.nextFireAt, iso(atLocal(2026, 7, 2, 23, 0)));
      }
      const persisted = yield* settingsService.getSettings;
      assert.strictEqual(persisted.bedtimeReminderAcknowledgedFor, iso(atLocal(2026, 7, 1, 23, 0)));

      const quieted = yield* takeKind("scheduled");
      assert.strictEqual(quieted.kind, "scheduled");

      // The next bedtime re-arms because each night is keyed by its occurrence.
      yield* TestClock.setTime(DateTime.toEpochMillis(atLocal(2026, 7, 2, 23, 10)));
      yield* TestClock.adjust("6 seconds");
      const rearmed = yield* takeKind("reminding");
      assert.strictEqual(rearmed.kind, "reminding");
      if (rearmed.kind === "reminding") {
        assert.strictEqual(rearmed.nightStartedAt, iso(atLocal(2026, 7, 2, 23, 0)));
      }
    }).pipe(Effect.provide(layer));
  }),
);
