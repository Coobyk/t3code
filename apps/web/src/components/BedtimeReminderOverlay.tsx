import { useAtomValue } from "@effect/atom-react";
import { useEffect, useReducer, useRef, useState } from "react";
import { createPortal } from "react-dom";

import { readLocalApi } from "../localApi";
import { primaryBedtimeReminderAtom } from "../state/server";

const RICKROLL_URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ";
const OVERLAY_ID = "t3-bedtime-overlay";

const CARD_START_WIDTH = 340;
const CARD_START_HEIGHT = 132;
const CARD_MARGIN = 16;
// Exponential growth: the card doubles about every two minutes, so it takes
// roughly six minutes to eat the screen. A flat term keeps it visibly creeping
// from the very first second.
const GROWTH_PER_MS = Math.LN2 / (2 * 60 * 1000);
const GROWTH_FLAT_PX_PER_MS = 0.9 / 1000;
// Save the card's size this often so reloads and DOM tampering lose nothing.
const SIZE_PERSIST_MS = 1000;

type Stage = "prompt" | "card" | "engulfed";

function readStorage(key: string): string | null {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeStorage(key: string, value: string): void {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // Private mode and friends: the reminder still works, it just forgets.
  }
}

function rickroll(): void {
  void readLocalApi()?.shell.openExternal(RICKROLL_URL);
}

function readCardSize(key: string): { w: number; h: number } {
  const persisted = readStorage(key)?.split(",").map(Number);
  return {
    w: persisted?.[0] !== undefined && persisted[0] > 0 ? persisted[0] : CARD_START_WIDTH,
    h: persisted?.[1] !== undefined && persisted[1] > 0 ? persisted[1] : CARD_START_HEIGHT,
  };
}

/**
 * Deliberately inescapable: while the environment reports "reminding" a panel
 * offers Dismiss (rickroll, nothing closes) or OK, which shrinks it to a
 * draggable top-right card that grows until it fills the screen. A watchdog
 * remounts the node if it is removed from the DOM.
 */
export function BedtimeReminderOverlay() {
  const reminder = useAtomValue(primaryBedtimeReminderAtom);
  if (reminder?.kind !== "reminding") return null;
  return <NightReminder key={reminder.nightStartedAt} nightStartedAt={reminder.nightStartedAt} />;
}

function NightReminder({ nightStartedAt }: { nightStartedAt: string }) {
  const stageKey = `t3-bedtime:${nightStartedAt}`;
  const [mountKey, remount] = useReducer((count: number) => count + 1, 0);
  const storedStage = readStorage(stageKey);
  const [stage, setStage] = useState<Stage>(
    storedStage === "card" || storedStage === "engulfed" ? storedStage : "prompt",
  );

  const advanceTo = (next: Stage) => {
    writeStorage(stageKey, next);
    setStage(next);
  };

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- keeps the overlay DOM node alive while reminding; re-adds it if it is removed outside React.
  useEffect(() => {
    const watchdog = window.setInterval(() => {
      if (document.getElementById(OVERLAY_ID) === null) remount();
    }, 300);
    return () => window.clearInterval(watchdog);
  }, []);

  return createPortal(
    <div id={OVERLAY_ID} key={mountKey} className="contents select-none">
      {stage === "prompt" ? (
        <div
          role="alert"
          className="fixed inset-0 z-2147483647 flex items-center justify-center bg-black/60 p-6 backdrop-blur-sm lg:p-10"
        >
          <div className="flex size-full max-h-[60rem] max-w-[80rem] flex-col items-center justify-center gap-10 rounded-2xl border border-border bg-background text-center shadow-2xl/40">
            <div className="space-y-4 px-8">
              <p className="text-6xl font-black tracking-tight text-foreground md:text-8xl">
                Hey, it's getting late
              </p>
              <p className="text-4xl font-bold text-muted-foreground md:text-6xl">go to sleep</p>
            </div>
            <div className="flex items-center gap-4">
              <button
                type="button"
                className="rounded-full border border-border px-8 py-4 text-xl text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                onClick={rickroll}
              >
                Dismiss
              </button>
              <button
                type="button"
                className="rounded-full border border-primary bg-primary px-8 py-4 text-xl font-medium text-primary-foreground transition-colors hover:bg-primary/90"
                onClick={() => advanceTo("card")}
              >
                OK
              </button>
            </div>
          </div>
        </div>
      ) : null}
      {stage === "card" ? (
        <GrowingCard sizeKey={`${stageKey}:size`} onEngulfed={() => advanceTo("engulfed")} />
      ) : null}
      {stage === "engulfed" ? (
        <div
          role="alert"
          className="fixed inset-0 z-2147483647 flex flex-col items-center justify-center gap-10 bg-background text-center"
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
            onClick={rickroll}
          >
            Dismiss
          </button>
        </div>
      ) : null}
    </div>,
    document.body,
  );
}

/**
 * The acknowledged reminder: a small card parked top-right that the user can drag
 * anywhere but never close. It grows continuously — slowly at first, then faster
 * — and reports `onEngulfed` once it covers the whole viewport. Size is kept in
 * refs and written straight to the DOM so growth and dragging don't re-render.
 */
function GrowingCard({ sizeKey, onEngulfed }: { sizeKey: string; onEngulfed: () => void }) {
  const ref = useRef<HTMLDivElement>(null);
  const onEngulfedRef = useRef(onEngulfed);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- mirrors the latest callback into a ref so the rAF loop below never re-registers.
  useEffect(() => {
    onEngulfedRef.current = onEngulfed;
  });

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- drives the card's DOM node directly: rAF growth writes, drag listeners, and localStorage size persistence for the mounted element.
  useEffect(() => {
    const el = ref.current;
    if (el === null) return;

    const metrics = { ...readCardSize(sizeKey), cx: 0, cy: 0 };
    metrics.cx = window.innerWidth - metrics.w / 2 - CARD_MARGIN;
    metrics.cy = metrics.h / 2 + CARD_MARGIN;

    let engulfed = false;
    let dragging = false;
    let moved = false;
    let grabX = 0;
    let grabY = 0;
    let downX = 0;
    let downY = 0;
    let lastPersist = 0;

    const clamp = () => {
      const vw = window.innerWidth;
      const vh = window.innerHeight;
      metrics.cx =
        metrics.w >= vw
          ? vw / 2
          : Math.min(Math.max(metrics.cx, metrics.w / 2), vw - metrics.w / 2);
      metrics.cy =
        metrics.h >= vh
          ? vh / 2
          : Math.min(Math.max(metrics.cy, metrics.h / 2), vh - metrics.h / 2);
    };

    const paint = () => {
      el.style.width = `${metrics.w}px`;
      el.style.height = `${metrics.h}px`;
      el.style.transform = `translate(${metrics.cx - metrics.w / 2}px, ${metrics.cy - metrics.h / 2}px)`;
      el.style.fontSize = `${Math.max(14, metrics.w * 0.05)}px`;
    };

    let last = performance.now();
    let frame = requestAnimationFrame(function tick(now: number) {
      const dt = Math.min(now - last, 100);
      last = now;
      metrics.w += (metrics.w * GROWTH_PER_MS + GROWTH_FLAT_PX_PER_MS) * dt;
      metrics.h += (metrics.h * GROWTH_PER_MS + GROWTH_FLAT_PX_PER_MS) * dt;
      if (metrics.w >= window.innerWidth && metrics.h >= window.innerHeight) {
        engulfed = true;
        writeStorage(sizeKey, `${window.innerWidth},${window.innerHeight}`);
        onEngulfedRef.current();
        return;
      }
      clamp();
      paint();
      if (now - lastPersist >= SIZE_PERSIST_MS) {
        lastPersist = now;
        writeStorage(sizeKey, `${metrics.w},${metrics.h}`);
      }
      frame = requestAnimationFrame(tick);
    });
    paint();

    const onPointerDown = (event: PointerEvent) => {
      if (event.button !== 0 || engulfed) return;
      // Presses on the Dismiss button are clicks, not drags.
      if (event.target instanceof Element && event.target.closest("button") !== null) return;
      dragging = true;
      moved = false;
      downX = event.clientX;
      downY = event.clientY;
      grabX = event.clientX - (metrics.cx - metrics.w / 2);
      grabY = event.clientY - (metrics.cy - metrics.h / 2);
      el.setPointerCapture(event.pointerId);
      event.preventDefault();
    };
    const onPointerMove = (event: PointerEvent) => {
      if (!dragging) return;
      if (!moved && Math.hypot(event.clientX - downX, event.clientY - downY) < 5) return;
      moved = true;
      metrics.cx = event.clientX - grabX + metrics.w / 2;
      metrics.cy = event.clientY - grabY + metrics.h / 2;
      clamp();
      paint();
    };
    const onPointerUp = () => {
      dragging = false;
    };
    // A press that moved is a drag, not a click — don't rickroll on release.
    const onClickCapture = (event: MouseEvent) => {
      if (moved) {
        moved = false;
        event.preventDefault();
        event.stopPropagation();
      }
    };

    el.addEventListener("pointerdown", onPointerDown);
    el.addEventListener("pointermove", onPointerMove);
    el.addEventListener("pointerup", onPointerUp);
    el.addEventListener("pointercancel", onPointerUp);
    el.addEventListener("click", onClickCapture, true);
    return () => {
      cancelAnimationFrame(frame);
      el.removeEventListener("pointerdown", onPointerDown);
      el.removeEventListener("pointermove", onPointerMove);
      el.removeEventListener("pointerup", onPointerUp);
      el.removeEventListener("pointercancel", onPointerUp);
      el.removeEventListener("click", onClickCapture, true);
    };
  }, [sizeKey]);

  const { w: startW, h: startH } = readCardSize(sizeKey);

  return (
    <div
      ref={ref}
      role="alert"
      className="fixed top-0 left-0 z-2147483647 flex cursor-grab touch-none flex-col items-center justify-center overflow-hidden rounded-2xl border border-border bg-background text-center shadow-2xl/40 active:cursor-grabbing"
      style={{
        width: startW,
        height: startH,
        transform: `translate(${window.innerWidth - startW - CARD_MARGIN}px, ${CARD_MARGIN}px)`,
        fontSize: Math.max(14, startW * 0.05),
        gap: "0.6em",
      }}
    >
      <p
        className="leading-tight font-black tracking-tight text-foreground"
        style={{ padding: "0 0.8em", fontSize: "1.35em" }}
      >
        Hey, it's getting late
      </p>
      <p className="font-bold text-muted-foreground" style={{ fontSize: "0.95em" }}>
        go to sleep
      </p>
      <button
        type="button"
        className="rounded-full border border-border text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
        style={{ padding: "0.4em 1.2em", fontSize: "0.7em" }}
        onClick={rickroll}
      >
        Dismiss
      </button>
    </div>
  );
}
