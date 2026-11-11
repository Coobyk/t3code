import { useAtomValue } from "@effect/atom-react";
import { useState } from "react";

import { primaryEnvironmentIdAtom } from "../../state/primaryEnvironment";
import { primaryBedtimeReminderAtom, serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { ScopedSwitch } from "./ScopedSwitch";
import { SettingsRow } from "./settingsLayout";
import { searchableSetting } from "./settingsSearch";
import { useScopedSettings, useUpdateScopedSettings } from "./useScopedSettings";

const NUDGE_INTERVAL_OPTIONS = [5, 10, 15, 20, 30, 45, 60] as const;

function BedtimeReminderActiveRow() {
  const reminder = useAtomValue(primaryBedtimeReminderAtom);
  const environmentId = useAtomValue(primaryEnvironmentIdAtom);
  const acknowledge = useAtomCommand(serverEnvironment.acknowledgeBedtimeReminder, {
    label: "bedtime reminder acknowledge",
  });
  const [busy, setBusy] = useState(false);
  if (environmentId === null || reminder?.kind !== "reminding") return null;
  return (
    <SettingsRow
      title="Bedtime reminder is active"
      description="Nudges keep firing until you call it a night."
      control={
        <Button
          variant="outline"
          size="sm"
          disabled={busy}
          onClick={async () => {
            setBusy(true);
            await acknowledge({ environmentId, input: {} });
            setBusy(false);
          }}
        >
          Disable for tonight
        </Button>
      }
    />
  );
}

export function BedtimeReminderSettings() {
  const enabled = useScopedSettings((settings) => settings.bedtimeReminderEnabled);
  const time = useScopedSettings((settings) => settings.bedtimeReminderTime);
  const intervalMinutes = useScopedSettings(
    (settings) => settings.bedtimeReminderNudgeIntervalMinutes,
  );
  const updateSettings = useUpdateScopedSettings();

  return (
    <>
      <SettingsRow
        serverScoped
        {...searchableSetting("bedtime-reminder")}
        description="Remind yourself to stop working and go to sleep."
        settingKeys={["bedtimeReminderEnabled"]}
        control={
          <ScopedSwitch
            settingKeys={["bedtimeReminderEnabled"]}
            checked={enabled}
            onCheckedChange={(checked) =>
              updateSettings({ bedtimeReminderEnabled: Boolean(checked) })
            }
            aria-label="Bedtime reminder"
          />
        }
      />
      {enabled ? (
        <>
          <BedtimeReminderActiveRow />
          <SettingsRow
            serverScoped
            {...searchableSetting("bedtime-reminder-time")}
            description="The reminder starts at this time every day, in the environment's timezone."
            settingKeys={["bedtimeReminderTime"]}
            control={
              <Input
                type="time"
                className="w-28"
                value={time}
                onChange={(event) => {
                  const value = event.target.value;
                  if (/^([01]\d|2[0-3]):[0-5]\d$/.test(value)) {
                    updateSettings({ bedtimeReminderTime: value });
                  }
                }}
                aria-label="Bedtime"
              />
            }
          />
          <SettingsRow
            serverScoped
            {...searchableSetting("bedtime-reminder-interval")}
            description="How often to nudge again if you keep working past bedtime."
            settingKeys={["bedtimeReminderNudgeIntervalMinutes"]}
            control={
              <Select
                value={String(intervalMinutes)}
                onValueChange={(value) =>
                  updateSettings({ bedtimeReminderNudgeIntervalMinutes: Number(value) })
                }
              >
                <SelectTrigger className="w-40" aria-label="Nudge interval">
                  <SelectValue />
                </SelectTrigger>
                <SelectPopup>
                  {NUDGE_INTERVAL_OPTIONS.map((minutes) => (
                    <SelectItem key={minutes} value={String(minutes)}>
                      Every {minutes} minutes
                    </SelectItem>
                  ))}
                </SelectPopup>
              </Select>
            }
          />
        </>
      ) : null}
    </>
  );
}
