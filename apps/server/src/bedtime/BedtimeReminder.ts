import {
  type BedtimeReminderAcknowledgeInput,
  type BedtimeReminderState,
  type ServerSettings,
  ServerSettingsError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import * as DueHint from "../scheduling/DueHint.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import { forkParked } from "../serverActivation.ts";
import * as Settings from "../serverSettings.ts";

export class BedtimeReminder extends Context.Service<
  BedtimeReminder,
  {
    readonly current: Effect.Effect<BedtimeReminderState>;
    readonly streamChanges: Stream.Stream<BedtimeReminderState>;
    readonly acknowledge: (
      input: BedtimeReminderAcknowledgeInput,
    ) => Effect.Effect<BedtimeReminderState, ServerSettingsError>;
  }
>()("t3/bedtime/BedtimeReminder") {}

type BedtimeSettings = Pick<
  ServerSettings,
  | "bedtimeReminderEnabled"
  | "bedtimeReminderTime"
  | "bedtimeReminderNudgeIntervalMinutes"
  | "bedtimeReminderAcknowledgedFor"
>;

// "Morning": nudges give up this long after bedtime even when unanswered.
const BEDTIME_NUDGE_WINDOW_MS = 10 * 60 * 60 * 1000;

function parseTimeOfDay(value: string): { hour: number; minute: number } | null {
  const match = /^([01]\d|2[0-3]):([0-5]\d)$/.exec(value.trim());
  if (!match) return null;
  return { hour: Number(match[1]), minute: Number(match[2]) };
}

const iso = (value: DateTime.DateTime): string => DateTime.formatIso(DateTime.toUtc(value));

const epochMillis = (value: string | null): number | null => {
  if (value === null) return null;
  const parsed = DateTime.make(value);
  return Option.isSome(parsed) ? DateTime.toEpochMillis(parsed.value) : null;
};

interface BedtimeReminderComputation {
  readonly state: BedtimeReminderState;
  /** Next instant the state can change; Infinity when nothing is scheduled. */
  readonly dueAtMs: number;
}

/**
 * Derives the reminder state at `now` (a local-zoned DateTime). Each "night" is keyed
 * by the bedtime occurrence it started at, so acknowledging stores the occurrence's
 * ISO instant and automatically re-arms the next one.
 */
export function bedtimeReminderStateAt(
  settings: BedtimeSettings,
  now: DateTime.DateTime,
): BedtimeReminderComputation {
  if (!settings.bedtimeReminderEnabled) {
    return { state: { kind: "off" }, dueAtMs: Infinity };
  }
  const time = parseTimeOfDay(settings.bedtimeReminderTime);
  if (time === null) {
    return { state: { kind: "off" }, dueAtMs: Infinity };
  }
  const nowMs = DateTime.toEpochMillis(now);
  const bedtimeToday = DateTime.setParts(now, {
    hour: time.hour,
    minute: time.minute,
    second: 0,
    millisecond: 0,
  });
  const pastToday = nowMs >= DateTime.toEpochMillis(bedtimeToday);
  const last = pastToday ? bedtimeToday : DateTime.add(bedtimeToday, { days: -1 });
  const next = pastToday ? DateTime.add(bedtimeToday, { days: 1 }) : bedtimeToday;
  const lastMs = DateTime.toEpochMillis(last);
  const nextMs = DateTime.toEpochMillis(next);
  const acknowledgedMs = epochMillis(settings.bedtimeReminderAcknowledgedFor);

  const windowEndMs = lastMs + BEDTIME_NUDGE_WINDOW_MS;
  if (acknowledgedMs !== lastMs && nowMs < windowEndMs) {
    const intervalMs = settings.bedtimeReminderNudgeIntervalMinutes * 60_000;
    const nudgeIndex = Math.floor((nowMs - lastMs) / intervalMs);
    const nextNudgeAtMs = lastMs + (nudgeIndex + 1) * intervalMs;
    return {
      state: {
        kind: "reminding",
        nightStartedAt: iso(last),
        nudgeIndex,
        nextNudgeAt: iso(DateTime.makeUnsafe(nextNudgeAtMs)),
      },
      dueAtMs: Math.min(nextNudgeAtMs, windowEndMs),
    };
  }

  // A skipped upcoming bedtime shifts the next fire to the following day.
  const nextFire = acknowledgedMs === nextMs ? DateTime.add(next, { days: 1 }) : next;
  return {
    state: {
      kind: "scheduled",
      nextFireAt: iso(nextFire),
      acknowledged: acknowledgedMs === lastMs || acknowledgedMs === nextMs,
    },
    dueAtMs: DateTime.toEpochMillis(nextFire),
  };
}

/** The occurrence an `acknowledge` applies to: the active night, else the next one. */
function acknowledgeTarget(settings: BedtimeSettings, now: DateTime.DateTime): DateTime.DateTime {
  const time = parseTimeOfDay(settings.bedtimeReminderTime) ?? { hour: 23, minute: 0 };
  const nowMs = DateTime.toEpochMillis(now);
  const bedtimeToday = DateTime.setParts(now, {
    hour: time.hour,
    minute: time.minute,
    second: 0,
    millisecond: 0,
  });
  const pastToday = nowMs >= DateTime.toEpochMillis(bedtimeToday);
  const last = pastToday ? bedtimeToday : DateTime.add(bedtimeToday, { days: -1 });
  const lastMs = DateTime.toEpochMillis(last);
  const reminding =
    epochMillis(settings.bedtimeReminderAcknowledgedFor) !== lastMs &&
    nowMs < lastMs + BEDTIME_NUDGE_WINDOW_MS;
  return reminding ? last : pastToday ? DateTime.add(bedtimeToday, { days: 1 }) : bedtimeToday;
}

const localNow = DateTime.withCurrentZoneLocal(DateTime.nowInCurrentZone);

const make = Effect.gen(function* () {
  const settingsService = yield* Settings.ServerSettingsService;
  const scheduler = yield* Scheduler.Scheduler;
  const stateRef = yield* Ref.make<BedtimeReminderState>({ kind: "off" });
  const changes = yield* Effect.acquireRelease(
    PubSub.unbounded<BedtimeReminderState>(),
    PubSub.shutdown,
  );
  const due = DueHint.make();

  const publish = Effect.fn("BedtimeReminder.publish")(function* (next: BedtimeReminderState) {
    const changed = yield* Ref.modify(stateRef, (previous) =>
      Equal.equals(previous, next) ? ([false, previous] as const) : ([true, next] as const),
    );
    if (changed) yield* PubSub.publish(changes, next);
  });

  const readSettings = settingsService.getSettings.pipe(
    Effect.orElseSucceed((): ServerSettings | null => null),
  );

  const sweep = Effect.fn("BedtimeReminder.sweep")(function* () {
    const now = yield* localNow;
    const nowMs = DateTime.toEpochMillis(now);
    if (yield* due.isIdle(nowMs)) return;
    const readVersion = yield* due.version;
    const settings = yield* readSettings;
    const { state, dueAtMs } =
      settings === null
        ? { state: { kind: "off" } as const, dueAtMs: Infinity }
        : bedtimeReminderStateAt(settings, now);
    yield* publish(state);
    yield* due.record(readVersion, dueAtMs, nowMs);
  });

  const acknowledge = Effect.fn("BedtimeReminder.acknowledge")(function* (
    input: BedtimeReminderAcknowledgeInput,
  ) {
    const action = input.action ?? "acknowledge";
    let acknowledgedFor: string | null = null;
    if (action !== "clear") {
      const settings = yield* settingsService.getSettings;
      const target = acknowledgeTarget(settings, yield* localNow);
      acknowledgedFor = iso(target);
    }
    yield* settingsService.updateSettings({ bedtimeReminderAcknowledgedFor: acknowledgedFor });
    yield* due.invalidate;
    yield* sweep();
    return yield* Ref.get(stateRef);
  });

  yield* forkParked(Stream.runForEach(settingsService.streamChanges, () => due.invalidate));
  yield* scheduler.register("bedtime-reminder", sweep());

  return {
    acknowledge,
    current: Ref.get(stateRef),
    get streamChanges() {
      return Stream.unwrap(
        Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(changes);
          const snapshot = yield* Ref.get(stateRef);
          return Stream.concat(Stream.make(snapshot), Stream.fromSubscription(subscription)).pipe(
            Stream.changes,
          );
        }),
      );
    },
  } satisfies BedtimeReminder["Service"];
});

export const layer = Layer.effect(BedtimeReminder, make);
