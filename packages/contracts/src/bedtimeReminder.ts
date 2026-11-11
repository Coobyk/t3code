import * as Schema from "effect/Schema";
import { TrimmedNonEmptyString } from "./baseSchemas.ts";

export const BedtimeReminderTimeOfDay = TrimmedNonEmptyString.check(
  Schema.isPattern(/^([01]\d|2[0-3]):[0-5]\d$/),
);
export type BedtimeReminderTimeOfDay = typeof BedtimeReminderTimeOfDay.Type;
export const DEFAULT_BEDTIME_REMINDER_TIME: BedtimeReminderTimeOfDay = "23:00";

const MIN_BEDTIME_NUDGE_INTERVAL_MINUTES = 1;
const MAX_BEDTIME_NUDGE_INTERVAL_MINUTES = 240;
export const BedtimeReminderNudgeIntervalMinutes = Schema.Int.check(
  Schema.isBetween({
    minimum: MIN_BEDTIME_NUDGE_INTERVAL_MINUTES,
    maximum: MAX_BEDTIME_NUDGE_INTERVAL_MINUTES,
  }),
);
export type BedtimeReminderNudgeIntervalMinutes = typeof BedtimeReminderNudgeIntervalMinutes.Type;
export const DEFAULT_BEDTIME_NUDGE_INTERVAL_MINUTES = 15;

/**
 * What the server reports on `bedtimeReminder.subscribe`. `scheduled` is quiet
 * until `nextFireAt` (`acknowledged` marks an answered or skipped night);
 * `reminding` means bedtime passed unanswered and `nudgeIndex` climbs per
 * nudge interval so clients can escalate copy.
 */
export const BedtimeReminderState = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("off") }),
  Schema.Struct({
    kind: Schema.Literal("scheduled"),
    nextFireAt: Schema.String,
    acknowledged: Schema.Boolean,
  }),
  Schema.Struct({
    kind: Schema.Literal("reminding"),
    nightStartedAt: Schema.String,
    nudgeIndex: Schema.Int,
    nextNudgeAt: Schema.String,
  }),
]);
export type BedtimeReminderState = typeof BedtimeReminderState.Type;

/** `acknowledge` quiets the active or upcoming night; `clear` undoes that. */
export const BedtimeReminderAcknowledgeInput = Schema.Struct({
  action: Schema.optionalKey(Schema.Literals(["acknowledge", "clear"])),
});
export type BedtimeReminderAcknowledgeInput = typeof BedtimeReminderAcknowledgeInput.Type;
