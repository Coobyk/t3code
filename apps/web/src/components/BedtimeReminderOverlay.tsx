import { useAtomValue } from "@effect/atom-react";
import { useEffect, useReducer } from "react";
import { createPortal } from "react-dom";

import { readLocalApi } from "../localApi";
import { primaryBedtimeReminderAtom } from "../state/server";

const RICKROLL_URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";
const OVERLAY_ID = "t3-bedtime-overlay";

/**
 * The bedtime reminder is deliberately inescapable: once the environment reports
 * "reminding" it covers the whole app until morning. There is no close affordance —
 * "Dismiss" opens a rickroll and the overlay stays up. A watchdog remounts the node
 * if it is removed from the DOM.
 */
export function BedtimeReminderOverlay() {
  const reminder = useAtomValue(primaryBedtimeReminderAtom);
  const [mountKey, remount] = useReducer((count: number) => count + 1, 0);
  const reminding = reminder?.kind === "reminding";

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- keeps the overlay DOM node alive while reminding; re-adds it if it is removed outside React.
  useEffect(() => {
    if (!reminding) return;
    const watchdog = window.setInterval(() => {
      if (document.getElementById(OVERLAY_ID) === null) remount();
    }, 300);
    return () => window.clearInterval(watchdog);
  }, [reminding]);

  if (!reminding) return null;
  return createPortal(
    <div
      id={OVERLAY_ID}
      key={mountKey}
      role="alert"
      className="fixed inset-0 z-2147483647 flex flex-col items-center justify-center gap-10 bg-background text-center select-none"
    >
      <div className="space-y-4 px-8">
        <p className="text-6xl font-black tracking-tight text-foreground md:text-8xl">
          Hey, it's getting late
        </p>
        <p className="text-4xl font-bold text-muted-foreground md:text-6xl">go to sleep</p>
      </div>
      <button
        type="button"
        className="rounded-full border border-border px-8 py-4 text-xl text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
        onClick={() => {
          void readLocalApi()?.shell.openExternal(RICKROLL_URL);
        }}
      >
        Dismiss
      </button>
    </div>,
    document.body,
  );
}
