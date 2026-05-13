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

## Running in a Lima VM (end-to-end smoke test)

Recipe for a clean, isolated Linux environment that talks to the Band
WebSocket and routes inference through a local LM Studio on the macOS
host. Verified twice in a row at ~5 minutes per VM (after the first
download). Each VM hosts one Band agent — provision a second VM to run
a second agent in parallel.

### Prerequisites (macOS host)

1. **Lima ≥ 1.0** — `brew install lima`.
2. **LM Studio** (or any OpenAI-compatible local server) listening on
   `0.0.0.0:1234`. Toggle **Developer → Serve on Local Network** in the
   LM Studio UI; the default `127.0.0.1` bind isn't reachable from the
   VM. Load a chat-capable model. Sanity check: from the host,
   `curl http://$(ipconfig getifaddr en0):1234/v1/models` returns a
   non-empty `data` array.
3. **Band external agent** — log in to <https://app.band.ai>, **Agents
   → Create New Agent → External**, copy the **agent ID** (UUID) and
   the **API key** (shown once).

### One-time host setup

```bash
git clone <this-repo> ~/projects/hermes-band-gateway-plugin
cd ~/projects/hermes-band-gateway-plugin
cp examples/lima.yaml /tmp/hermes-lima.yaml
# Edit /tmp/hermes-lima.yaml and replace the mount `location:` with the
# absolute path you just cloned to.
```

Create a gitignored env file with your Band creds (the file pattern
`.env.*` is already in `.gitignore`):

```bash
cat > .env.hermes <<'EOF'
BAND_AGENT_ID=<your-agent-uuid>
BAND_API_KEY=<your-band-api-key>
# Open access for the smoke test — tighten with BAND_ALLOWED_USERS later.
BAND_ALLOW_ALL_USERS=true

# LM Studio reached from inside the VM. host.lima.internal resolves to
# the macOS host; LM_API_KEY is a dummy LM Studio ignores.
LM_BASE_URL=http://host.lima.internal:1234/v1
LM_API_KEY=lm-studio
EOF
chmod 600 .env.hermes
```

### Boot the VM

```bash
limactl create --name=hermes /tmp/hermes-lima.yaml --tty=false
limactl start hermes
```

First boot downloads the Ubuntu 24.04 arm64 cloud image (~700 MB,
cached for subsequent VMs). Subsequent VMs from the same yaml take
under a minute.

### Bootstrap hermes-agent inside the VM

Run everything below as a single `limactl shell hermes -- bash -lc '…'`
block, or paste line-by-line in `limactl shell hermes`:

```bash
# 1. Install uv + git.
curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null
sudo apt-get update -qq && sudo apt-get install -qq -y git

# 2. Fresh clone of hermes-agent. We do NOT host-mount it — the host
#    .venv/ is macOS-only and breaks inside the VM.
git clone --depth 1 https://github.com/NousResearch/hermes-agent.git ~/hermes-agent

# 3. Pin to a known-good commit (optional but reproducible).
cd ~/hermes-agent
git fetch --depth 1 origin 942adf617910f50a39f41bd200d8083bf4cb2bed
git checkout 942adf617910f50a39f41bd200d8083bf4cb2bed

# 4. Install hermes-agent's dev deps + the Band SDK into a venv.
~/.local/bin/uv sync --extra dev
~/.local/bin/uv pip install band-sdk

# 5. Wire the plugin into hermes-agent's plugin tree via the host mount.
#    Replace the path if you mounted somewhere else.
ln -s /Users/Shared/_Projects/hermes-band-gateway-plugin \
      ~/hermes-agent/plugins/platforms/band

# 6. (Optional) symlink the test file so scripts/run_tests.sh sees it.
ln -s /Users/Shared/_Projects/hermes-band-gateway-plugin/tests/test_band_adapter.py \
      ~/hermes-agent/tests/gateway/test_band_adapter.py

# 7. Drop the env file you prepared into ~/.hermes/.env.
mkdir -p ~/.hermes
cp /Users/Shared/_Projects/hermes-band-gateway-plugin/.env.hermes ~/.hermes/.env
chmod 600 ~/.hermes/.env

# 8. Configure Hermes to use LM Studio as its inference provider.
cd ~/hermes-agent
~/.local/bin/uv run hermes auth add lmstudio --type api-key \
    --api-key lm-studio \
    --inference-url http://host.lima.internal:1234/v1
~/.local/bin/uv run hermes config set provider lmstudio
~/.local/bin/uv run hermes config set model <model-id-from-lmstudio>
#   (e.g.  unsloth/qwen3.6-35b-a3b  — pick whatever your LM Studio has loaded;
#    `curl http://host.lima.internal:1234/v1/models` lists them)
```

### Verify

```bash
# Optional smoke test of the plugin's unit tests, run via hermes-agent's
# canonical harness:
cd ~/hermes-agent
scripts/run_tests.sh tests/gateway/test_band_adapter.py
# expect: all tests passed
```

### Install the gateway as a systemd service

This is the form you want for any VM that should keep running across
reboots. The service starts automatically when the VM boots and gets
restarted by systemd if it crashes.

```bash
cd ~/hermes-agent
# One-time install. --system installs to /etc/systemd/system so it
# starts at boot without needing a logged-in user (Lima VMs have no
# interactive login session). --run-as-user keeps the actual gateway
# process running as your normal user account.
sudo ~/hermes-agent/.venv/bin/hermes gateway install \
  --system --run-as-user $USER

# Start it now.
sudo systemctl start hermes-gateway

# Confirm.
sudo systemctl status hermes-gateway --no-pager
# expect: Loaded: ... enabled; Active: active (running)
```

After `limactl start <vm>`, the gateway is back online ~5–15 seconds
later with no further input. Logs at `~/.hermes/logs/gateway.log` and
via `journalctl -u hermes-gateway`.

Then in the Band web UI, message your agent (e.g. `@ed01/testmes hi!`)
and a reply should appear within the LM Studio round-trip time.

> **Foreground mode for iteration.** If you're actively editing the
> plugin and want to see crash output directly, swap the service for a
> foreground run:
> ```bash
> sudo systemctl stop hermes-gateway
> cd ~/hermes-agent && ~/.local/bin/uv run hermes gateway run
> ```
> Re-start the service when done.

### Running multiple agents in parallel

Each Band agent identity needs its own Hermes instance (the plugin
holds a scoped lock on `BAND_AGENT_ID`). Once you have one VM working,
**clone it** rather than redoing the full bootstrap — the heavy steps
(`uv sync`, `uv pip install band-sdk`, plugin symlinks,
`hermes auth add lmstudio`, `hermes config set …`) are all in the disk
image already.

```bash
# Write the new agent's env (gitignored under .env.*):
cat > .env.hermie <<'EOF'
BAND_AGENT_ID=<new-agent-uuid>
BAND_API_KEY=<new-api-key>
BAND_ALLOW_ALL_USERS=true
LM_BASE_URL=http://host.lima.internal:1234/v1
LM_API_KEY=lm-studio
EOF
chmod 600 .env.hermie

# Clone (Lima requires the source to be stopped briefly):
limactl stop hermes
limactl clone hermes hermes2
limactl start hermes        # source is back up in ~5s
limactl start hermes2

# Swap creds before the cloned gateway service connects (otherwise it
# would use the source's BAND_AGENT_ID and trip the scoped identity
# lock). The cloned VM already has the systemd service installed and
# enabled, so we stop it first, swap, then restart:
limactl shell hermes2 -- bash -lc '
  sudo systemctl stop hermes-gateway
  cp /Users/Shared/_Projects/hermes-band-gateway-plugin/.env.hermie \
     ~/.hermes/.env
  chmod 600 ~/.hermes/.env
  sudo systemctl start hermes-gateway
'

# Confirm:
limactl shell hermes2 -- bash -lc 'grep "✓ band connected" ~/.hermes/logs/gateway.log | tail -1'
# expect: a fresh "✓ band connected" line
```

End-to-end this takes under a minute. All VMs share the host's LM
Studio, so total throughput is bounded by the model server, not by
the number of agents.

For the **first** VM (no source to clone from), use the full
from-scratch recipe above.

### Tear down

```bash
limactl stop hermes && limactl delete hermes
```

Removes the VM's disk image entirely; nothing leaks onto the host.

---

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
