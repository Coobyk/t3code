import { runAtomCommand } from "@t3tools/client-runtime/state/runtime";

import { appAtomRegistry } from "../rpc/atomRegistry";
import { primaryEnvironmentIdAtom } from "../state/primaryEnvironment";
import { primaryBedtimeReminderAtom, serverEnvironment } from "../state/server";
import { toastManager } from "./ui/toast";

const NUDGE_COPY = [
  {
    title: "It's past your bedtime",
    description: "Still working? Time to wrap up.",
  },
  {
    title: "Bedtime check-in",
    description: "You planned to be asleep by now. Still going?",
  },
  {
    title: "Still up?",
    description: "Your future self will thank you for stopping now.",
  },
  {
    title: "Go to bed",
    description: "Seriously, the code will still be here in the morning.",
  },
] as const;

function nudgeCopy(nudgeIndex: number) {
  return NUDGE_COPY[Math.min(nudgeIndex, NUDGE_COPY.length - 1)]!;
}

/**
 * Keeps one persistent toast in step with the server's reminder state: it escalates
 * copy on each nudge, returns if the toast was closed between nudges, and leaves
 * only when the night is acknowledged or morning ends the window.
 */
export function startBedtimeReminderToasts(): () => void {
  let toastId: ReturnType<typeof toastManager.add> | null = null;
  let lastNudgeIndex = -1;

  const closeToast = () => {
    if (toastId !== null) {
      toastManager.close(toastId);
      toastId = null;
    }
    lastNudgeIndex = -1;
  };

  const acknowledge = () => {
    const environmentId = appAtomRegistry.get(primaryEnvironmentIdAtom);
    if (environmentId === null) return;
    void runAtomCommand(appAtomRegistry, serverEnvironment.acknowledgeBedtimeReminder, {
      environmentId,
      input: {},
    });
  };

  return appAtomRegistry.subscribe(
    primaryBedtimeReminderAtom,
    (state) => {
      if (state?.kind !== "reminding") {
        closeToast();
        return;
      }
      const copy = nudgeCopy(state.nudgeIndex);
      const toast = {
        type: "warning" as const,
        title: copy.title,
        description: copy.description,
        timeout: 0,
        actionProps: {
          children: "I'm going to sleep",
          onClick: acknowledge,
        },
        data: {
          hideCopyButton: true,
          actionLayout: "stacked-end" as const,
          onClose: () => {
            toastId = null;
          },
          secondaryActionProps: {
            children: "Disable for tonight",
            onClick: acknowledge,
          },
          secondaryActionVariant: "ghost" as const,
        },
      };
      // The toast only re-appears for a fresh nudge, not on unrelated re-emissions.
      if (toastId !== null && state.nudgeIndex === lastNudgeIndex) {
        toastManager.update(toastId, toast);
        return;
      }
      lastNudgeIndex = state.nudgeIndex;
      if (toastId === null) {
        toastId = toastManager.add(toast);
      } else {
        toastManager.update(toastId, toast);
      }
    },
    { immediate: true },
  );
}
