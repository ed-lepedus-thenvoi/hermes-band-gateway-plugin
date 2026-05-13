"""
Band Platform Adapter for Hermes Agent.

A plugin-based gateway adapter that connects to the Band collaborative
platform (formerly Thenvoi) via the official Python SDK and bridges
inbound Band messages to the Hermes agent runner.

Architecture
------------

Band's SDK normally expects to *own* the LLM loop: you give it an adapter
(LangGraph, Anthropic, etc.) and it calls your adapter's ``on_message``
with each new platform event. Hermes wants the opposite — it has its
own agent runner and expects the platform adapter to surface raw inbound
text.

This plugin reconciles the two by registering a tiny pass-through
``_BridgeAdapter(SimpleAdapter)`` with the Band SDK. The bridge does not
run any LLM logic; instead, on each ``on_message`` callback it converts
the incoming Band ``PlatformMessage`` into a Hermes ``MessageEvent``
and hands it to ``BasePlatformAdapter.handle_message``. The Hermes
runner then composes a reply and calls ``BandAdapter.send``, which
constructs ``AgentTools`` from the captured runtime link and forwards
the reply back into the originating Band room.

Configuration (env vars take precedence over ``config.extra``)::

    BAND_AGENT_ID       — UUID of the external agent on the Band platform
    BAND_API_KEY        — API key shown once when the agent was created
    BAND_REST_URL       — REST endpoint (default: https://app.band.ai)
    BAND_WS_URL         — WebSocket endpoint
                          (default: wss://app.band.ai/api/v1/socket/websocket)
    BAND_HOME_CHANNEL   — default room UUID for cron / notification delivery
    BAND_ALLOWED_USERS  — comma-separated allowlist of sender handles
    BAND_ALLOW_ALL_USERS — "true" to skip allowlist (dev only)
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import logging
import os
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Hermes-side imports. Lazy at module top: the gateway only imports this
# plugin when the platform registry decides to instantiate it, so these
# are always available in practice.
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)
from gateway.config import Platform, PlatformConfig


# --- Defaults --------------------------------------------------------------

# Band is mid-rebrand from Thenvoi. The SDK ships ``app.thenvoi.com`` as its
# baked-in default; the user-facing brand is ``band.ai``. We default to the
# new brand here and fall back to legacy ``THENVOI_*`` env vars so users who
# already configured the SDK don't need to change anything.
_DEFAULT_REST_URL = "https://app.band.ai"
_DEFAULT_WS_URL = "wss://app.band.ai/api/v1/socket/websocket"


def _env(name: str, *fallbacks: str, default: str = "") -> str:
    for key in (name, *fallbacks):
        value = os.getenv(key)
        if value:
            return value.strip()
    return default


def _normalize_handle(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value if value.startswith("@") else f"@{value}"


def _truthy(value: Optional[str]) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes", "on")


# --- Band SDK import guard -------------------------------------------------

def _import_band_sdk():
    """Import the Band SDK, raising a helpful error if it's missing.

    Returns ``(Agent, SimpleAdapter, AgentTools)``.
    """
    try:
        from thenvoi import Agent, AgentTools  # type: ignore
        from thenvoi.core.simple_adapter import SimpleAdapter  # type: ignore
    except ImportError as exc:  # pragma: no cover - exercised at runtime only
        raise ImportError(
            "Band SDK not installed. Install with:\n"
            "  uv add band-sdk\n"
            "or:\n"
            "  pip install band-sdk\n"
            "(The PyPI package is 'band-sdk' but it still imports as "
            "'thenvoi' while the brand migration is in progress.)"
        ) from exc
    return Agent, SimpleAdapter, AgentTools


# --- Adapter ---------------------------------------------------------------

class BandAdapter(BasePlatformAdapter):
    """Hermes ↔ Band bridge."""

    def __init__(self, config: PlatformConfig, **_kwargs: Any):
        super().__init__(config=config, platform=Platform("band"))

        extra = getattr(config, "extra", {}) or {}

        self.agent_id = _env("BAND_AGENT_ID") or extra.get("agent_id", "")
        self.api_key = _env("BAND_API_KEY") or extra.get("api_key", "")
        self.rest_url = (
            _env("BAND_REST_URL", "THENVOI_REST_URL")
            or extra.get("rest_url")
            or _DEFAULT_REST_URL
        )
        self.ws_url = (
            _env("BAND_WS_URL", "THENVOI_WS_URL")
            or extra.get("ws_url")
            or _DEFAULT_WS_URL
        )

        # Allowlist: stored normalized with leading ``@`` for cheap compare.
        raw_allowed: Any = extra.get("allowed_users", [])
        if isinstance(raw_allowed, str):
            raw_allowed = [h.strip() for h in raw_allowed.split(",") if h.strip()]
        env_allowed = _env("BAND_ALLOWED_USERS")
        if env_allowed:
            raw_allowed = [h.strip() for h in env_allowed.split(",") if h.strip()]
        self._allowed_handles: set[str] = {
            _normalize_handle(h) or "" for h in raw_allowed if isinstance(h, str)
        }
        self._allowed_handles.discard("")
        self._allow_all = _truthy(_env("BAND_ALLOW_ALL_USERS")) or bool(
            extra.get("allow_all_users")
        )

        # Runtime state populated by ``connect()``.
        self._agent: Any = None  # thenvoi.Agent
        self._bridge: Any = None  # _BridgeAdapter instance
        self._run_task: Optional[asyncio.Task] = None
        self._started_event = asyncio.Event()

        # Per-room caches used by ``send()``. The Band ``send_message`` tool
        # requires at least one mention, so we track the most recent sender
        # per room and address replies to them by default.
        self._room_last_sender: Dict[str, Dict[str, str]] = {}
        self._room_participants: Dict[str, List[Dict[str, Any]]] = {}
        self._room_meta: Dict[str, Dict[str, Any]] = {}

    @property
    def name(self) -> str:
        return "Band"

    # --- Lifecycle ---------------------------------------------------------

    async def connect(self) -> bool:
        if not self.agent_id or not self.api_key:
            self._set_fatal_error(
                "config_missing",
                "BAND_AGENT_ID and BAND_API_KEY must be set",
                retryable=False,
            )
            return False

        try:
            Agent, SimpleAdapter, _AgentTools = _import_band_sdk()
        except ImportError as exc:
            self._set_fatal_error("sdk_missing", str(exc), retryable=False)
            return False

        # Hold the Band agent identity lock so two Hermes profiles can't
        # share one Band agent (the SDK enforces a single live WebSocket
        # per agent_id; without the lock the second connection would
        # silently displace the first).
        try:
            from gateway.status import acquire_scoped_lock
            if not acquire_scoped_lock("band", self.agent_id):
                self._set_fatal_error(
                    "lock_conflict",
                    "Band agent already in use by another profile",
                    retryable=False,
                )
                return False
            self._lock_key = self.agent_id
        except ImportError:
            self._lock_key = None

        self._bridge = _BridgeAdapter(SimpleAdapter, hermes=self)

        try:
            self._agent = Agent.create(
                adapter=self._bridge,
                agent_id=self.agent_id,
                api_key=self.api_key,
                ws_url=self.ws_url,
                rest_url=self.rest_url,
            )
        except Exception as exc:
            logger.error("Band: Agent.create failed: %s", exc)
            self._set_fatal_error("create_failed", str(exc), retryable=True)
            return False

        # Run the SDK's message loop in the background. ``agent.run()``
        # blocks until shutdown, so we cannot await it here.
        self._run_task = asyncio.create_task(
            self._run_agent_with_recovery(), name="band-agent-run"
        )

        # The SDK calls ``adapter.on_started`` once the WebSocket is up
        # and the agent profile has been fetched. We wait for that signal
        # (with a generous timeout) so ``hermes gateway status`` sees us
        # as connected only after the link is actually live.
        try:
            await asyncio.wait_for(self._started_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            logger.error("Band: timed out waiting for agent to start")
            await self.disconnect()
            self._set_fatal_error(
                "start_timeout",
                "Band agent did not start within 30s",
                retryable=True,
            )
            return False

        self._mark_connected()
        logger.info("Band: connected as agent %s via %s", self.agent_id, self.ws_url)
        return True

    async def _run_agent_with_recovery(self) -> None:
        """Run ``agent.run()`` and surface fatal errors to the gateway."""
        try:
            await self._agent.run()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Band: agent.run() crashed: %s", exc, exc_info=True)
            if self.is_connected:
                self._set_fatal_error(
                    "connection_lost",
                    f"Band agent run loop exited: {exc}",
                    retryable=True,
                )
                await self._notify_fatal_error()

    async def disconnect(self) -> None:
        if getattr(self, "_lock_key", None):
            try:
                from gateway.status import release_scoped_lock
                release_scoped_lock("band", self._lock_key)
            except Exception:
                pass
            self._lock_key = None

        self._mark_disconnected()

        if self._agent is not None:
            try:
                await self._agent.stop()
            except Exception:
                logger.debug("Band: agent.stop() raised", exc_info=True)

        if self._run_task and not self._run_task.done():
            self._run_task.cancel()
            try:
                await self._run_task
            except (asyncio.CancelledError, Exception):
                pass
        self._run_task = None
        self._agent = None
        self._bridge = None
        self._started_event.clear()
        self._room_last_sender.clear()
        self._room_participants.clear()

    # --- Inbound (called by the bridge) ------------------------------------

    def _is_authorized(self, handle: Optional[str]) -> bool:
        """Apply the local allowlist on top of whatever Band already gates."""
        if self._allow_all:
            return True
        if not self._allowed_handles:
            # Default deny when no allowlist configured. Matches the IRC
            # plugin's behavior; admins who want open access set
            # ``BAND_ALLOW_ALL_USERS=true`` explicitly.
            return False
        return _normalize_handle(handle) in self._allowed_handles

    def _record_inbound(self, room_id: str, msg: Any, participants: List[Dict[str, Any]]) -> None:
        """Cache room state so ``send()`` can address replies later."""
        if participants:
            self._room_participants[room_id] = list(participants)
        sender_id = getattr(msg, "sender_id", None)
        # Resolve handle from the participants list — ``msg.sender_name`` is
        # a display name and is not safe to use as a mention handle.
        sender_handle: Optional[str] = None
        if sender_id:
            for p in participants:
                pid = p.get("id") if isinstance(p, dict) else getattr(p, "id", None)
                if pid and str(pid) == str(sender_id):
                    raw = (
                        p.get("handle") if isinstance(p, dict) else getattr(p, "handle", None)
                    )
                    sender_handle = _normalize_handle(raw)
                    break
        if sender_id and sender_handle:
            self._room_last_sender[room_id] = {
                "id": str(sender_id),
                "handle": sender_handle,
            }
        self._room_meta.setdefault(room_id, {})["last_message_id"] = getattr(msg, "id", "")

    # --- Outbound ----------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not self._agent or not self.is_connected:
            return SendResult(success=False, error="Not connected to Band")

        try:
            _Agent, _SimpleAdapter, AgentTools = _import_band_sdk()
        except ImportError as exc:
            return SendResult(success=False, error=str(exc))

        link = getattr(self._agent.runtime, "link", None)
        rest = getattr(link, "rest", None)
        if rest is None:
            return SendResult(success=False, error="Band runtime has no REST client")

        participants = self._room_participants.get(chat_id) or []
        tools = AgentTools(chat_id, rest, participants=participants)

        mentions = self._resolve_default_mentions(chat_id, participants)
        if not mentions:
            return SendResult(
                success=False,
                error=(
                    "Band: cannot send to room — no known recipients. Send at "
                    "least one inbound message in this room first, or use the "
                    "send_message tool with explicit mentions."
                ),
            )

        try:
            response = await tools.send_message(content=content, mentions=mentions)
        except Exception as exc:
            logger.error("Band: send_message failed for room %s: %s", chat_id, exc)
            return SendResult(success=False, error=str(exc))

        message_id = ""
        if response is not None:
            message_id = str(getattr(response, "id", "") or "")
        return SendResult(success=True, message_id=message_id or str(int(time.time() * 1000)))

    def _resolve_default_mentions(
        self,
        chat_id: str,
        participants: List[Dict[str, Any]],
    ) -> List[str]:
        """Pick a mention for an outbound message in ``chat_id``.

        Strategy:
        1. The handle of the last sender in the room (reply addressing).
        2. All known non-agent participants (proactive nudge).
        Falls back to ``[]`` when neither is available — the caller surfaces
        that as a SendResult error rather than letting the SDK raise.
        """
        last = self._room_last_sender.get(chat_id)
        if last and last.get("handle"):
            return [last["handle"]]
        handles: List[str] = []
        for p in participants:
            handle = p.get("handle") if isinstance(p, dict) else getattr(p, "handle", None)
            ptype = (
                p.get("type") if isinstance(p, dict) else getattr(p, "type", None)
            )
            if handle and ptype != "Agent":
                handles.append(_normalize_handle(handle) or handle)
        return handles

    async def send_typing(self, chat_id: str, metadata: Any = None) -> None:
        # Band uses ``send_event`` thoughts rather than typing indicators.
        # Skipping is safe — Hermes treats typing as best-effort.
        return None

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        meta = self._room_meta.get(chat_id, {})
        return {
            "name": meta.get("name") or chat_id,
            "type": "group",  # Band rooms are always multi-participant
            "chat_id": chat_id,
        }


# --- The Band-facing bridge ------------------------------------------------

def _BridgeAdapter(simple_adapter_cls: type, hermes: BandAdapter):
    """Build a subclass of the SDK's ``SimpleAdapter`` bound to ``hermes``.

    The class is created lazily inside ``connect()`` so the plugin module
    is importable in environments where the Band SDK is missing (the
    plugin loader still needs to read ``register`` to surface the
    ``hermes config`` UI entries even when the SDK isn't installed yet).
    """

    class _Bridge(simple_adapter_cls):  # type: ignore[misc, valid-type]
        async def on_started(self, agent_name: str, agent_description: str) -> None:
            await super().on_started(agent_name, agent_description)
            hermes._started_event.set()

        async def on_message(
            self,
            msg: Any,
            tools: Any,
            history: Any,
            participants_msg: Optional[str],
            contacts_msg: Optional[str],
            *,
            is_session_bootstrap: bool,
            room_id: str,
        ) -> None:
            # Skip echoes of our own outbound messages — the SDK emits them
            # on the same event stream as user input, and feeding them back
            # would loop forever.
            if (
                getattr(msg, "sender_type", "") == "Agent"
                and str(getattr(msg, "sender_id", "")) == str(hermes.agent_id)
            ):
                return

            participants = list(getattr(tools, "participants", []) or [])
            hermes._record_inbound(room_id, msg, participants)

            resolved = hermes._room_last_sender.get(room_id, {})
            sender_handle = resolved.get("handle")
            if not hermes._is_authorized(sender_handle):
                logger.debug(
                    "Band: ignoring message from unauthorized sender %s in room %s",
                    sender_handle, room_id,
                )
                return

            text = getattr(msg, "content", "") or ""
            if not text.strip():
                return

            source = hermes.build_source(
                chat_id=room_id,
                chat_name=room_id,
                chat_type="group",
                user_id=str(getattr(msg, "sender_id", "")) or None,
                user_name=getattr(msg, "sender_name", None) or None,
                message_id=str(getattr(msg, "id", "")) or None,
            )

            created_at = getattr(msg, "created_at", None)
            if not isinstance(created_at, _dt.datetime):
                created_at = _dt.datetime.now()

            event = MessageEvent(
                text=text,
                message_type=MessageType.TEXT,
                source=source,
                message_id=str(getattr(msg, "id", "")) or str(int(time.time() * 1000)),
                timestamp=created_at,
            )
            await hermes.handle_message(event)

        async def on_cleanup(self, room_id: str) -> None:
            hermes._room_last_sender.pop(room_id, None)
            hermes._room_participants.pop(room_id, None)
            hermes._room_meta.pop(room_id, None)

    return _Bridge()


# --- Plugin entry points ---------------------------------------------------

def check_requirements() -> bool:
    """Cheap check used by ``hermes gateway setup`` / status."""
    try:
        import thenvoi  # noqa: F401
    except ImportError:
        return False
    return bool(os.getenv("BAND_AGENT_ID") and os.getenv("BAND_API_KEY"))


def validate_config(config: Any) -> bool:
    extra = getattr(config, "extra", {}) or {}
    agent_id = os.getenv("BAND_AGENT_ID") or extra.get("agent_id", "")
    api_key = os.getenv("BAND_API_KEY") or extra.get("api_key", "")
    return bool(agent_id and api_key)


def is_connected(config: Any) -> bool:
    return validate_config(config)


def _env_enablement() -> Optional[dict]:
    """Seed ``PlatformConfig.extra`` from BAND_* env vars during gateway load."""
    agent_id = os.getenv("BAND_AGENT_ID", "").strip()
    api_key = os.getenv("BAND_API_KEY", "").strip()
    if not (agent_id and api_key):
        return None
    seed: Dict[str, Any] = {"agent_id": agent_id, "api_key": api_key}
    rest = os.getenv("BAND_REST_URL", "").strip()
    ws = os.getenv("BAND_WS_URL", "").strip()
    if rest:
        seed["rest_url"] = rest
    if ws:
        seed["ws_url"] = ws
    home = os.getenv("BAND_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("BAND_HOME_CHANNEL_NAME", "Band Home"),
        }
    return seed


async def _standalone_send(
    pconfig: Any,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """REST-only send for cron / out-of-process delivery.

    The gateway's standalone-sender hook is invoked when ``hermes cron``
    runs in a separate process from the gateway, so we cannot rely on a
    live ``Agent.run()`` loop. Instead we open a short-lived REST client,
    fetch the room's participants (Band's send_message API requires at
    least one mention), and post the message.

    ``thread_id``, ``media_files``, and ``force_document`` are accepted for
    signature parity with the rest of the platform fleet; Band has no
    native thread or attachment primitive that matches Hermes' chunking,
    so they are ignored here.
    """
    extra = getattr(pconfig, "extra", {}) or {}
    agent_id = os.getenv("BAND_AGENT_ID") or extra.get("agent_id", "")
    api_key = os.getenv("BAND_API_KEY") or extra.get("api_key", "")
    rest_url = (
        os.getenv("BAND_REST_URL")
        or os.getenv("THENVOI_REST_URL")
        or extra.get("rest_url")
        or _DEFAULT_REST_URL
    )
    if not agent_id or not api_key:
        return {"error": "BAND_AGENT_ID and BAND_API_KEY must be configured"}
    if not chat_id:
        return {"error": "Band standalone send: chat_id is required"}

    try:
        from thenvoi.client.rest import (  # type: ignore
            AsyncRestClient,
            ChatMessageRequest,
            ChatMessageRequestMentionsItem,
            DEFAULT_REQUEST_OPTIONS,
        )
    except ImportError as exc:
        return {"error": f"Band SDK not installed: {exc}"}

    rest = AsyncRestClient(api_key=api_key, base_url=rest_url)

    try:
        # Fetch participants so we can satisfy the "≥1 mention" requirement.
        try:
            participants_resp = await rest.agent_api_participants.list_agent_chat_participants(
                chat_id=chat_id,
                request_options=DEFAULT_REQUEST_OPTIONS,
            )
            participants = list(getattr(participants_resp, "data", None) or [])
        except Exception as exc:
            return {"error": f"Band standalone send: could not list participants: {exc}"}

        mention_items: List[Dict[str, str]] = []
        for p in participants:
            def _field(name: str) -> Any:
                if isinstance(p, dict):
                    return p.get(name)
                return getattr(p, name, None)

            handle = _field("handle")
            pid = _field("id")
            # Fern models expose participant type as ``.type``; cached dicts
            # use the same key. ``participant_type`` is the input-model name.
            ptype = _field("type") or _field("participant_type")
            # Don't mention ourselves — Band rejects self-mentions.
            if pid and str(pid) == str(agent_id):
                continue
            if not handle or not pid:
                continue
            if ptype == "Agent":
                # Skip other agents on proactive sends to avoid agent-to-agent
                # ping-pong from a notification.
                continue
            mention_items.append({"id": str(pid), "handle": _normalize_handle(handle) or handle})

        if not mention_items:
            return {"error": "Band standalone send: no eligible recipients (room has no users)"}

        api_mentions = [
            ChatMessageRequestMentionsItem(id=m["id"], handle=m["handle"])
            for m in mention_items
        ]
        response = await rest.agent_api_messages.create_agent_chat_message(
            chat_id=chat_id,
            message=ChatMessageRequest(content=message, mentions=api_mentions),
            request_options=DEFAULT_REQUEST_OPTIONS,
        )
        msg_id = str(getattr(getattr(response, "data", None), "id", "") or "")
        return {"success": True, "message_id": msg_id or str(int(time.time() * 1000))}
    except Exception as exc:
        logger.debug("Band standalone send raised", exc_info=True)
        return {"error": f"Band standalone send failed: {exc}"}
    finally:
        close = getattr(rest, "aclose", None) or getattr(rest, "close", None)
        if close is not None:
            try:
                result = close()
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                pass


def register(ctx: Any) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="band",
        label="Band",
        adapter_factory=lambda cfg: BandAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["BAND_AGENT_ID", "BAND_API_KEY"],
        install_hint="uv add band-sdk  (or: pip install band-sdk)",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="BAND_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="BAND_ALLOWED_USERS",
        allow_all_env="BAND_ALLOW_ALL_USERS",
        # Band has no documented per-message size limit, but the platform
        # is built for chat-style turns. 4000 keeps replies snappy and
        # avoids dumping a wall of text into rooms with multiple agents.
        max_message_length=4000,
        emoji="🎼",
        pii_safe=False,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via Band — a multi-agent collaboration "
            "platform. Rooms can contain multiple users and other agents. "
            "Messages support standard markdown. Every reply you send is "
            "addressed to the participant who spoke last; if you need to "
            "address someone else, mention them explicitly in your text "
            "(e.g. '@alice can you confirm?'). Keep replies focused and "
            "avoid speaking on behalf of other agents in the room."
        ),
    )
