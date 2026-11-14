# Mission: rewrite the 67 Code backend in Rust

You are working in `/data/workspace/67code` on branch `rust-gpui` (remote `git@github.com:Coobyk/67code.git`, SSH push works — verify with `ssh -T git@github.com`).

## Context

67 Code is a GUI for coding agents. A server wraps provider CLIs (Codex, Claude Code, Cursor, OpenCode, ACP, etc.) and serves clients over a WebSocket JSON-RPC protocol. Today the server is TypeScript (`apps/server`, Effect-based, event-sourced over SQLite). A native Rust+GPUI desktop client already exists at `native/app` — it works end-to-end against the TS server.

**Goal: replace the entire TypeScript backend with Rust.** The GPUI client must keep working against the new Rust server with ZERO client changes — the wire protocol is the contract. Eventually all client surfaces become native Rust too; the TS web/desktop/mobile apps become obsolete.

## Hard-won protocol facts (verified against the live TS server — treat as spec)

- Auth: `POST /api/auth/websocket-ticket/rpc` with `Authorization: Bearer <token>` → `{ "ticket": "<jwt>" }`.
- WS endpoint: `ws://host/ws?wsTicket=<ticket>&frameParts=1&orchestrationProtocol=2`. `orchestrationProtocol=2` is REQUIRED.
- Frames are JSON. `_tag` discriminators: `Request`, `Chunk`, `Exit`, `Ack`, `Interrupt`, `Ping`, `Pong`, `Defect`.
- `Request` shape: `{ "_tag":"Request", "id":N, "tag":"<rpc.method>", "payload":{...}, "headers":[["k","v"]...] }` — headers MUST be an array of `[key,value]` tuples, not an object.
- Streaming RPCs deliver `{"_tag":"Chunk","requestId":N,"value":{...}}` items, then a final `Exit`.
- **Windowed streams**: `subscribeShell`/`subscribeThread` are count-windowed (window ~8). The server stalls until the client sends `{"_tag":"Ack","requestId":N}` per Chunk. The server auto-acks its own first 8; clients MUST ack every received Chunk to keep the stream flowing.
- **Split frames**: outbound frames >~48KiB are split; every part except the last is prefixed with record separator `\u001e`. Clients accumulate until the un-prefixed final part.
- `Exit` value for `cause._tag=="Interrupt"` is EXPECTED when a subscription is deliberately interrupted — not an error.
- Enrichment shell snapshots carry `resolvedRepositoryIdentityRoots` and `threads:[]` — clients merge projects, must NOT wipe thread state.
- RPCs the client uses: `orchestration.subscribeShell`, `orchestration.subscribeThread`, `orchestration.dispatchCommand` (commands: `thread.create`, `message.dispatch`, `thread.stop`), `orchestration.getOlderSettledThreads`.

Study the real definitions in `packages/contracts/src/` (orchestration schemas, command payloads, projection/turn-item types) and the TS server impl in `apps/server/src/` — port behavior faithfully, don't guess shapes.

## Phases (commit AND push after each)

1. **Server skeleton** — new crate `native/server`: HTTP + WS, ticket auth (accept a static `CODE67_DEV_AUTH_TOKEN`-style env token for dev), protocol framing (windowed ACKs, frame-part splitting), `Ping`/`Pong`. The existing GPUI client (`C67_SERVER_URL`, `C67_TOKEN`) must connect.
2. **State + projections** — SQLite (rusqlite or sqlx) event-sourced store compatible with `statev2.sqlite` semantics (read the schema/migrations in apps/server). `subscribeShell` snapshot + enrichment, `subscribeThread` snapshot + live `turn-item.updated`/`node.updated` event stream, `getOlderSettledThreads`.
3. **Providers** — spawn provider CLIs; translate their output into persisted domain events → turn items → wire events. Start with the `devReplay` provider (free, replays a canned transcript — find its impl in the TS server) and ONE real provider (codex or acp, whichever is simplest).
4. **Dispatch** — `thread.create`, `message.dispatch`, `thread.stop` end-to-end; verify a live turn streams into the GPUI client.

## Rules

- Commit and push to `origin/rust-gpui` FREQUENTLY — every milestone and every working increment. Never squash-push over pushed work.
- Keep the GPUI client compatible — if you discover a protocol detail that differs, fix the server, not the client, unless the client is clearly wrong.
- Do NOT touch the TS code under `apps/`, `packages/` except reading it. The Rust rewrite lives under `native/`.
- Keep commits small and descriptive; `native:` prefix.
- Verify each milestone for real: run `native/server` + `native/app` on the box desktop (DISPLAY=:0 exists) and confirm the window renders real data.
- The box's `.t3`/home is workspace-local; nothing here touches the user's real data.
