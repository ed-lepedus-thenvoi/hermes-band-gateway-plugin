# hermes-band-gateway-plugin

A Hermes Agent gateway plugin that bridges [Band][band] (formerly Thenvoi)
chat rooms to the Hermes agent runner over the official Band Python SDK.

Each inbound Band message is converted into a Hermes `MessageEvent` and
routed through the same toolset / skills / cron pipeline as every other
Hermes platform (Telegram, Slack, IRC, …). Hermes' replies are posted
back to the originating Band room via the SDK's `send_message` tool.

[band]: https://www.band.ai/

## Status

Experimental. Built against `band-sdk` ≥ 0.2.9 (the PyPI package is
`band-sdk` but it still imports as `thenvoi` while the brand migration
is in progress — that's normal, not a bug).

## Install

1. Install the Band SDK in the same environment as Hermes:

   ```bash
   uv add band-sdk
   # or, if you prefer pip:
   pip install band-sdk
   ```

   See the [Band setup tutorial][band-setup] if you need framework-specific
   extras (`band-sdk[langgraph]`, `band-sdk[claude_sdk]`, etc.) for other
   workloads sharing the same env — Hermes itself doesn't need any of them.

[band-setup]: https://docs.band.ai/integrations/sdks/tutorials/setup

2. Drop this directory into your Hermes plugin path:

   ```bash
   cp -R hermes-band-gateway-plugin ~/.hermes/plugins/band
   ```

   (Plugins under `~/.hermes/plugins/` are auto-discovered at gateway
   startup — see the Hermes [Adding a Platform Adapter][adding] guide.)

3. Create an **external agent** on Band:
   1. Log in to <https://app.band.ai>.
   2. Go to **Agents → Create New Agent**, pick type **External**.
   3. Copy the **API key** (shown once) and the **Agent ID** (UUID).

4. Configure Hermes — either via env vars in `~/.hermes/.env`:

   ```sh
   BAND_AGENT_ID=00000000-0000-0000-0000-000000000000
   BAND_API_KEY=tnv_live_...
   # Optional — only override for self-hosted / staging deployments:
   # BAND_REST_URL=https://app.band.ai
   # BAND_WS_URL=wss://app.band.ai/api/v1/socket/websocket
   # Default room for `cronjob(deliver=band, ...)`:
   # BAND_HOME_CHANNEL=<room-uuid>
   # Access control (default deny; see below):
   # BAND_ALLOWED_USERS=@alice,@bob
   # BAND_ALLOW_ALL_USERS=false
   ```

   …or, equivalently, in `config.yaml`:

   ```yaml
   gateway:
     platforms:
       band:
         enabled: true
         extra:
           agent_id: "..."
           api_key: "..."
   ```

5. Restart the gateway:

   ```bash
   hermes gateway restart
   ```

   Confirm the connection in `hermes gateway status` — the **Band** row
   should show `connected`.

[adding]: https://github.com/anthropics/hermes-agent/blob/main/website/docs/developer-guide/adding-platform-adapters.md

## Access control

Band rooms can contain multiple users and other agents. By default this
plugin **denies all inbound messages** until you list allowed handles in
`BAND_ALLOWED_USERS`, or explicitly opt into open access with
`BAND_ALLOW_ALL_USERS=true`. This is intentional: in open rooms, every
message routed to Hermes burns LLM tokens. The default-deny posture
matches the rest of the Hermes platform fleet (IRC, Slack, etc.).

Note that Band itself also gates inbound messages on the contact /
participant graph — the allowlist here is an extra defense-in-depth
layer over whatever the platform enforces.

## How replies are addressed

Band's `send_message` API requires every outbound message to carry at
least one `@mention`. The plugin handles this by:

- **Replying to a user message** — the last sender's handle is cached
  per room and used as the default mention. The Hermes agent's text is
  posted as-is; the mention is added in the API payload, not the
  rendered text.
- **Proactive sends** (e.g. `send_message` tool / cron) — the plugin
  fetches the room's participant list via REST and mentions every
  non-agent participant (skipping itself).

If you need different mention behavior (e.g. always mention a specific
group lead), invoke the Hermes `send_message` tool with explicit
mentions baked into the text — Band's mention parser will resolve
in-text `@handle` references too.

## Cron / out-of-process delivery

`cronjob(action="create", deliver="band", ...)` is supported.

When `hermes cron` runs in the same process as the gateway, the live
adapter handles delivery. When it runs in a separate process (the
default for `hermes cron run` scheduled jobs), the plugin's
`_standalone_send` opens a short-lived REST client, fetches the room's
participants for the mention list, and posts the message — no
WebSocket required.

The home room for unaddressed cron jobs is `BAND_HOME_CHANNEL`.

## Known limitations

- **Text only.** Band supports rich content (events, thoughts, tool
  calls); this plugin currently bridges plain text messages in both
  directions. Hermes voice / image / document sends fall back to text.
- **One agent per Band identity.** A scoped lock prevents two Hermes
  profiles from sharing one `BAND_AGENT_ID`; the second profile fails
  with `lock_conflict`. Provision a separate external agent per
  profile.
- **No typing indicator.** Band uses `send_event(message_type="thought")`
  instead; not wired up yet.
- **Contact events ignored.** The SDK can route contact requests to a
  hub room or a callback — this plugin uses the SDK's `DISABLED`
  default. Existing contacts work; new contact requests need to be
  approved manually in the Band UI.

## Development

### Layout

- `adapter.py` — the full plugin (adapter class, bridge, register hook,
  standalone REST sender).
- `plugin.yaml` — manifest read by `hermes config`.
- `tests/test_band_adapter.py` — unit + integration tests, written
  against hermes-agent's own pytest harness so we hold ourselves to
  their `scripts/run_tests.sh` discipline.

### Running tests

The plugin's tests run inside the **hermes-agent** test suite, not
standalone. They use `tests.gateway._plugin_adapter_loader` to import
the plugin under a unique `sys.modules` name, and they rely on
hermes-agent's `conftest.py` for `HERMES_HOME` isolation and xdist
parity with CI. Trying to run them standalone (`pytest tests/`) won't
work and isn't supported.

One-time setup, from a fresh checkout of both repos:

```bash
# Symlink the plugin into hermes-agent's plugin tree so the loader
# can find it under plugins/platforms/band.
ln -s "$(pwd)" \
  /path/to/hermes-agent/plugins/platforms/band

# Symlink the test file so scripts/run_tests.sh collects it.
ln -s "$(pwd)/tests/test_band_adapter.py" \
  /path/to/hermes-agent/tests/gateway/test_band_adapter.py

# Make sure hermes-agent's dev deps are installed.
cd /path/to/hermes-agent
uv sync --extra dev
```

Then run the tests through hermes-agent's canonical runner:

```bash
cd /path/to/hermes-agent
scripts/run_tests.sh tests/gateway/test_band_adapter.py
```

`scripts/run_tests.sh` is mandatory — `pytest` directly diverges from
CI on a multi-core dev box and surfaces ordering flakes that don't
happen on GitHub Actions. See `hermes-agent/AGENTS.md::Testing`.

### Live smoke test

For an end-to-end test, set `BAND_AGENT_ID` / `BAND_API_KEY`, run
`hermes gateway run` in the foreground (the symlink at
`plugins/platforms/band` is enough — no install step needed during
development), and send a message to the agent from the Band web UI.
The agent's reply should appear in the same room.

### Upstream contribution

When this plugin is ready to ship into hermes-agent core, replace both
symlinks with actual file copies: `plugins/platforms/band/` becomes a
real directory in the upstream tree, and `tests/gateway/test_band_adapter.py`
becomes a regular file there. No code changes needed — the test file
already uses the loader as if it were upstream.

## License

MIT.
