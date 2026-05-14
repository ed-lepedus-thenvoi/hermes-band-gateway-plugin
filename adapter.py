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
import re
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
        # Set by ``disconnect()`` so the reconnect loop knows to exit
        # cleanly rather than fight cancellation while rebuilding the
        # Agent. Without this, ``agent.run()`` returning cleanly during
        # an in-flight shutdown would re-trigger a reconnect attempt.
        self._shutdown_requested: bool = False

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

        # Install Bug A recovery on the freshly-created link. See
        # _install_mark_processed_recovery for full context — without
        # this the agent's per-agent pipeline silently stalls server-side
        # whenever Band returns 422 from mark_processed.
        try:
            self._install_mark_processed_recovery(self._agent.runtime.link)
        except Exception as exc:
            logger.warning(
                "Band: could not install mark_processed recovery wrapper: %s. "
                "Agent will run without it; restart may be needed if Bug A fires.",
                exc,
            )

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

    # Backoff schedule for reconnects after a dropped WS. 1s start, doubling
    # to a 60s ceiling so we don't hammer a Band cluster mid-deploy. Kept as
    # constants so tests can inspect / patch them.
    _RECONNECT_BACKOFF_START_S: float = 1.0
    _RECONNECT_BACKOFF_MAX_S: float = 60.0

    async def _run_agent_with_recovery(self) -> None:
        """Run ``agent.run()`` in a reconnect loop.

        The Phoenix Channels client (Band's underlying WS transport) gives
        up on a code-1000 close — and Band releases trigger exactly that.
        The SDK doesn't expose the ``reconnect_on_normal_close`` policy
        knob, so we wrap ``agent.run()`` ourselves: when it returns (clean
        or raised), rebuild the Agent and try again with exponential
        backoff, until ``disconnect()`` signals shutdown.
        """
        backoff = self._RECONNECT_BACKOFF_START_S
        while not self._shutdown_requested:
            try:
                await self._agent.run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Band: agent.run() raised: %s", exc)

            if self._shutdown_requested:
                return

            # send() should fail fast during the gap rather than calling
            # into a dead REST client. We re-mark connected only after a
            # successful rebuild below.
            self._mark_disconnected()
            logger.info(
                "Band: agent run loop exited; reconnecting in %.1fs", backoff,
            )
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                raise

            if self._shutdown_requested:
                return

            try:
                Agent, _SimpleAdapter, _AgentTools = _import_band_sdk()
                self._agent = Agent.create(
                    adapter=self._bridge,
                    agent_id=self.agent_id,
                    api_key=self.api_key,
                    ws_url=self.ws_url,
                    rest_url=self.rest_url,
                )
                # Re-install mark_processed recovery on the freshly-
                # rebuilt link. The wrapper closes over `link.rest`, so
                # it has to be re-installed after each rebuild.
                try:
                    self._install_mark_processed_recovery(self._agent.runtime.link)
                except Exception as install_exc:
                    logger.warning(
                        "Band: could not re-install mark_processed recovery "
                        "on rebuilt agent: %s", install_exc,
                    )
            except Exception as exc:
                logger.error(
                    "Band: failed to rebuild Agent: %s — retrying after backoff",
                    exc,
                )
                backoff = min(backoff * 2.0, self._RECONNECT_BACKOFF_MAX_S)
                continue

            self._mark_connected()
            backoff = min(backoff * 2.0, self._RECONNECT_BACKOFF_MAX_S)

    async def disconnect(self) -> None:
        # Signal the reconnect loop FIRST so it sees the flag before the
        # SDK's stop() / cancel propagate. Otherwise a clean return from
        # agent.run() during shutdown would trigger an unwanted reconnect
        # attempt before our cancel arrives.
        self._shutdown_requested = True

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

    def _strip_leading_self_mention(self, text: str) -> str:
        """Remove a leading ``@[[<own-agent-id>]]`` from inbound text.

        Band addresses messages to an agent with this wire-format prefix.
        Hermes' slash-command detector is ``text.startswith("/")``, so
        without stripping, ``/model`` / ``/sethome`` / ``/help`` etc.
        fall through to the LLM, which then hallucinates plausible
        answers instead of invoking the real handlers. Strip ONLY the
        leading occurrence (with adjacent whitespace) — mid-text
        self-mentions are left alone so anything the LLM might want to
        quote remains intact, and the is_self marker on tool output
        covers any lookup confusion.
        """
        if not text or not self.agent_id:
            return text
        pattern = re.compile(
            r"^\s*@\[\[" + re.escape(self.agent_id) + r"\]\]\s*",
            flags=re.IGNORECASE,
        )
        return pattern.sub("", text, count=1)

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

        # A fresh REST client per send avoids the cross-loop asyncio.Event
        # poisoning described on _new_rest_client. The agent's own
        # runtime.link.rest exists but reusing it would re-poison from
        # any prior tool call that ran on Hermes' worker thread loop.
        try:
            rest = self._new_rest_client()
        except ImportError as exc:
            return SendResult(success=False, error=f"Band SDK not importable: {exc}")

        participants = self._room_participants.get(chat_id) or []

        # Cold-cache recovery: right after a service restart the SDK's
        # ExecutionContext hasn't yet hydrated participants for this room,
        # so the bridge handed us an empty list when the inbound arrived.
        # Without participants we can't resolve a mention and send() would
        # fail with "no known recipients", dropping the first reply.
        # Pay one REST round-trip to fetch them ourselves so the first
        # send still goes out.
        if not participants:
            participants = await self._lazy_fetch_participants(chat_id, rest)
            if participants:
                self._room_participants[chat_id] = participants

        tools = AgentTools(chat_id, rest, participants=participants)

        # Mention resolution layers, in priority order:
        #   1. Handles the LLM @-mentioned (or echoed via @[[uuid]] wire
        #      format) in the reply body — these are the explicit
        #      addressees so Band actually notifies them.
        #   2. The last sender in the room, so the user/agent who asked
        #      always gets the reply even if the LLM forgot to name them.
        # Band's API requires ≥1 mention; combining both layers gives the
        # most reliable delivery for both 1:1 and agent-to-agent flows.
        text_mentions = self._extract_text_mentions(content, participants)
        default_mentions = self._resolve_default_mentions(chat_id, participants)
        mentions: List[str] = []
        for handle in (*text_mentions, *default_mentions):
            if handle and handle not in mentions:
                mentions.append(handle)

        # Strip self-mentions: Band rejects send_message with
        # 422 'cannot_mention_self' if the sending agent appears in the
        # mentions payload. The LLM tends to echo back ``@[[<our-uuid>]]``
        # or its own @handle when quoting the inbound message, so this
        # is a hot path. We dedupe by participant ID (the source of
        # truth) and rebuild the handle set without our own entry.
        self_handles: set = set()
        for p in participants:
            pid = p.get("id") if isinstance(p, dict) else getattr(p, "id", None)
            if pid and str(pid) == str(self.agent_id):
                raw = (
                    p.get("handle") if isinstance(p, dict)
                    else getattr(p, "handle", None)
                )
                normalized = _normalize_handle(raw)
                if normalized:
                    self_handles.add(normalized)
        if self_handles:
            mentions = [m for m in mentions if m not in self_handles]

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

    # Band wire format: ``@[[<participant-id>]]``. The platform sends this
    # in inbound message bodies and LLMs sometimes echo it back when quoting.
    _MENTION_WIRE_RE = re.compile(r"@\[\[([^\]]+)\]\]")
    # Plain ``@handle``. Handles permit ``/`` (agent suffix) and ``.`` / ``-``
    # / ``_`` — same character class the SDK uses.
    _MENTION_HANDLE_RE = re.compile(r"@([A-Za-z0-9_./-]+)")

    def _extract_text_mentions(
        self,
        content: str,
        participants: List[Dict[str, Any]],
    ) -> List[str]:
        """Resolve ``@[[id]]`` and ``@handle`` references in ``content``
        against the room's participant cache. Returns a deduped list of
        normalized handles. Unknown references are silently skipped — the
        API would reject them anyway.
        """
        if not content or not participants:
            return []

        # Index participants by both ID and normalized handle for O(1) lookup.
        by_id: Dict[str, Dict[str, Any]] = {}
        by_handle: Dict[str, Dict[str, Any]] = {}
        # ``shortform_counts`` lets us register UNAMBIGUOUS shortform aliases
        # (the post-slash portion of an owner/agent handle) without guessing
        # when two participants collide. LLMs naturally write ``@testmes``
        # rather than ``@ed01/testmes`` because Band's UI shows the short
        # name — without this aliasing, every cross-agent @mention drops
        # out of the outbound mentions payload.
        shortform_counts: Dict[str, int] = {}
        for p in participants:
            phandle = (
                p.get("handle") if isinstance(p, dict) else getattr(p, "handle", None)
            )
            if not phandle:
                continue
            normalized = _normalize_handle(phandle)
            if normalized and "/" in normalized:
                short = "@" + normalized.split("/", 1)[1]
                shortform_counts[short.lower()] = shortform_counts.get(short.lower(), 0) + 1

        for p in participants:
            pid = (p.get("id") if isinstance(p, dict) else getattr(p, "id", None))
            phandle = (
                p.get("handle") if isinstance(p, dict) else getattr(p, "handle", None)
            )
            if pid:
                by_id[str(pid)] = p
            if phandle:
                normalized = _normalize_handle(phandle)
                if normalized:
                    by_handle[normalized.lower()] = p
                    # Register the shortform alias only when (a) it's
                    # unambiguous across the participant list AND (b) the
                    # alias doesn't shadow an existing full-handle entry
                    # (e.g., a user ``@foo`` shouldn't be displaced by a
                    # shortform alias for ``alice/foo``).
                    if "/" in normalized:
                        short = "@" + normalized.split("/", 1)[1]
                        short_lower = short.lower()
                        if (
                            shortform_counts.get(short_lower, 0) == 1
                            and short_lower not in by_handle
                        ):
                            by_handle[short_lower] = p

        ordered_handles: List[str] = []
        seen: set = set()

        def _push(participant: Any) -> None:
            raw_handle = (
                participant.get("handle") if isinstance(participant, dict)
                else getattr(participant, "handle", None)
            )
            handle = _normalize_handle(raw_handle) or raw_handle
            if handle and handle not in seen:
                ordered_handles.append(handle)
                seen.add(handle)

        # Wire-format matches first (consume them so the @ char doesn't also
        # match the bare-handle regex on the inner ``[[...]]``).
        stripped = content
        for match in self._MENTION_WIRE_RE.finditer(content):
            target_id = match.group(1).strip()
            if target_id in by_id:
                _push(by_id[target_id])
        stripped = self._MENTION_WIRE_RE.sub(" ", content)

        for match in self._MENTION_HANDLE_RE.finditer(stripped):
            candidate = _normalize_handle(match.group(1))
            if not candidate:
                continue
            participant = by_handle.get(candidate.lower())
            if participant is not None:
                _push(participant)

        return ordered_handles

    def _install_mark_processed_recovery(self, link: Any) -> None:
        """Wrap ``link.mark_processed`` to recover from server-side 422s.

        Bug A in the field: Band's API returns
        ``status_code: 422, body: Validation failed`` on
        ``mark_agent_message_processed`` for reasons we can't see from
        the response. The SDK's ``mark_processed`` swallows the
        exception (just logs warning), so the SDK's local
        ``_processed_ids`` dedup cache records the message as done
        even though Band's server-side per-agent pipeline still has it
        in the unprocessed queue. Band then refuses to deliver any
        NEWER message to this agent — total stall, only restart
        unsticks it. We've seen this hit testmes ~4 times in a single
        session.

        This wrapper replaces ``link.mark_processed`` to call the
        underlying REST method directly so we can see the failure, and
        on failure falls through to ``mark_agent_message_failed`` —
        which Band considers a legitimate terminal state for the
        message. Either way the message leaves the unprocessed queue
        and the pipeline advances.

        We don't lie about success: an upstream "failed" mark is
        accurate (something genuinely went wrong, even if our agent's
        reply already delivered). The reason field cites the original
        error so any future support thread / SDK issue can correlate
        request_ids server-side.
        """
        try:
            from thenvoi.client.rest import DEFAULT_REQUEST_OPTIONS  # type: ignore
        except ImportError:
            DEFAULT_REQUEST_OPTIONS = {"max_retries": 3}

        rest = link.rest

        async def _mark_processed_with_recovery(
            room_id: str, message_id: str,
        ) -> None:
            # Python 3 deletes the exception-variable binding when the
            # ``except`` block exits, so hold the failure reason in an
            # ordinary local that we can reference below.
            primary_error: Optional[str] = None
            try:
                await rest.agent_api_messages.mark_agent_message_processed(
                    chat_id=room_id, id=message_id,
                    request_options=DEFAULT_REQUEST_OPTIONS,
                )
                return
            except Exception as primary_exc:
                primary_error = str(primary_exc)
                logger.warning(
                    "Band: mark_processed for message %s failed (%s); "
                    "falling back to mark_failed so the per-agent pipeline "
                    "doesn't stall server-side.",
                    message_id, primary_error,
                )
            # Fallback: mark as failed. Same terminal-state effect on
            # Band's queue without lying about success.
            try:
                await rest.agent_api_messages.mark_agent_message_failed(
                    chat_id=room_id, id=message_id,
                    error=(
                        f"mark_processed rejected by server; routed via "
                        f"mark_failed as recovery. original_error={primary_error}"
                    ),
                    request_options=DEFAULT_REQUEST_OPTIONS,
                )
            except Exception as fallback_exc:
                logger.error(
                    "Band: mark_failed fallback ALSO failed for message %s "
                    "(%s) — Band's per-agent pipeline may stall until the "
                    "next reconnect.",
                    message_id, fallback_exc,
                )

        link.mark_processed = _mark_processed_with_recovery

    def _new_rest_client(self) -> Any:
        """Construct a fresh ``AsyncRestClient`` for a single operation.

        Hermes' tool dispatcher runs ``is_async=True`` handlers in a worker
        thread with its own event loop (see ``model_tools._run_async``).
        The SDK's shared REST client's ``asyncio.Event`` locks bind lazily
        to whichever loop first acquires them, so a tool call from the
        worker loop poisons the shared client for subsequent main-loop
        ``send()`` calls (and vice versa) — producing
        "Event is bound to a different event loop" errors at runtime.

        Always returning a fresh client guarantees each operation's
        asyncio state is created on the calling loop and discarded with
        it. httpx's pool warmup is amortised in milliseconds; the extra
        cost is dominated by the REST call itself.
        """
        from thenvoi.client.rest import AsyncRestClient  # type: ignore
        return AsyncRestClient(api_key=self.api_key, base_url=self.rest_url)

    async def _lazy_fetch_participants(
        self,
        chat_id: str,
        rest: Any,
    ) -> List[Dict[str, Any]]:
        """One-shot REST call to populate the participants cache.

        Used by ``send()`` when the bridge handed us an empty participants
        list (typical right after a service restart, before the SDK has
        hydrated its per-room ExecutionContext). On failure we log and
        return an empty list — the caller surfaces a clean
        ``SendResult.error`` from there rather than crashing.
        """
        try:
            from thenvoi.client.rest import DEFAULT_REQUEST_OPTIONS  # type: ignore
        except ImportError:
            DEFAULT_REQUEST_OPTIONS = {"max_retries": 3}

        try:
            response = await rest.agent_api_participants.list_agent_chat_participants(
                chat_id=chat_id, request_options=DEFAULT_REQUEST_OPTIONS,
            )
        except Exception as exc:
            logger.warning(
                "Band: lazy participants fetch for room %s failed: %s",
                chat_id, exc,
            )
            return []
        data = getattr(response, "data", None) or []
        return [
            {
                "id": getattr(p, "id", None) or (p.get("id") if isinstance(p, dict) else None),
                "handle": getattr(p, "handle", None) or (p.get("handle") if isinstance(p, dict) else None),
                "name": getattr(p, "name", None) or (p.get("name") if isinstance(p, dict) else None),
                "type": getattr(p, "type", None) or (p.get("type") if isinstance(p, dict) else None),
            }
            for p in data
        ]

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

            text = hermes._strip_leading_self_mention(
                getattr(msg, "content", "") or ""
            )
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


# --- Platform tools exposed to Hermes' LLM ---------------------------------
#
# These let an agent enumerate peers, see who's in the current room, and
# invite / remove participants. Each tool wraps the same REST methods the
# Band SDK uses internally, but is routed through Hermes' tool dispatcher
# so the model can call them natively.
#
# Handler signature is the dispatcher's: ``handler(args: dict, **kwargs)``
# returning a JSON-encoded string. ``kwargs`` carries ``task_id`` /
# ``user_task`` and is currently ignored — chat scoping defaults to the
# adapter's most-recently-active room and can be overridden by the LLM
# passing ``chat_id``.

import json as _json


def _get_live_adapter() -> Optional["BandAdapter"]:
    """Return the live ``BandAdapter`` from the running gateway, or ``None``.

    Mirrors ``tools/send_message_tool._send_via_adapter``'s pattern: the
    gateway runner stashes itself as a weakref so out-of-process tools
    can fail cleanly when the adapter isn't reachable.
    """
    try:
        from gateway.run import _gateway_runner_ref  # type: ignore
    except Exception:
        return None
    runner = _gateway_runner_ref()
    if runner is None:
        return None
    try:
        return runner.adapters.get(Platform("band"))
    except Exception:
        return None


def _resolve_chat_id(adapter: "BandAdapter", chat_id_arg: str) -> Optional[str]:
    """Pick a chat_id for a tool call.

    Explicit > implicit. When the LLM doesn't pass one we use the adapter's
    most-recently-active room — fine for the common case where the model is
    operating on the room it's currently replying in.
    """
    if chat_id_arg:
        return str(chat_id_arg)
    if adapter._room_last_sender:
        # Insertion order in Python 3.7+ dicts is preserved; the most
        # recently recorded room is the one whose last_sender entry was
        # written most recently, which is what we want.
        return list(adapter._room_last_sender.keys())[-1]
    return None


def _serialize_participant(p: Any, *, self_agent_id: Optional[str] = None) -> Dict[str, Any]:
    """Convert a Fern participant model (or dict) to a stable JSON shape.

    When ``self_agent_id`` is supplied and matches the participant's ID,
    the returned dict gets ``is_self: True`` so the LLM can recognise its
    own entry without having to guess from a UUID. Non-self entries get
    no ``is_self`` key at all (cleaner than ``is_self: False`` and avoids
    misleading the LLM if the field is ever forgotten elsewhere).
    """
    def _f(name: str) -> Any:
        if isinstance(p, dict):
            return p.get(name)
        return getattr(p, name, None)

    pid = _f("id")
    normalized_handle = _normalize_handle(_f("handle")) or _f("handle")
    entry: Dict[str, Any] = {
        "id": pid,
        "handle": normalized_handle,
        "name": _f("name"),
        "type": _f("type") or _f("participant_type"),
        # Canonical full-form handle the LLM should use when @-mentioning
        # this participant in a reply. Same as ``handle`` today, but
        # surfaced under an explicit name so platform_hint can point the
        # LLM at it without ambiguity (Band's UI displays the short form
        # which doesn't always resolve to a unique participant).
        "mention_handle": normalized_handle,
    }
    if self_agent_id and pid is not None and str(pid) == str(self_agent_id):
        entry["is_self"] = True
    return entry


def _serialize_peer(p: Any, *, self_agent_id: Optional[str] = None) -> Dict[str, Any]:
    """Same shape as a participant; peers come from a different endpoint.

    Band's ``list_agent_peers`` already excludes the calling agent from
    its response, so ``is_self`` rarely fires here — we still thread the
    arg through so the serialiser stays symmetric and future endpoints
    that DO include self get the marker for free.
    """
    return _serialize_participant(p, self_agent_id=self_agent_id)


def _band_tool_error(message: str) -> str:
    return _json.dumps({"error": message})


async def band_get_participants_handler(args: dict, **_kw: Any) -> str:
    """List the participants in a Band chat room.

    Async so the registry dispatches us on Hermes' running loop — the SDK
    REST client holds asyncio.Event locks bound to that loop, so spinning
    up a fresh loop via ``asyncio.run()`` crashes with
    "Event is bound to a different event loop".
    """
    adapter = _get_live_adapter()
    if adapter is None:
        return _band_tool_error("Band gateway is not running in this process")

    chat_id = _resolve_chat_id(adapter, args.get("chat_id", "") if args else "")
    if not chat_id:
        return _band_tool_error(
            "No chat_id available — pass chat_id explicitly or send a message "
            "in the target room first so the adapter caches it"
        )

    try:
        from thenvoi.client.rest import DEFAULT_REQUEST_OPTIONS  # type: ignore
    except ImportError:
        DEFAULT_REQUEST_OPTIONS = {"max_retries": 3}  # safe default

    rest = adapter._new_rest_client()

    try:
        response = await rest.agent_api_participants.list_agent_chat_participants(
            chat_id=chat_id, request_options=DEFAULT_REQUEST_OPTIONS,
        )
    except Exception as exc:
        return _band_tool_error(f"list_agent_chat_participants failed: {exc}")
    data = getattr(response, "data", None) or []
    return _json.dumps({
        "chat_id": chat_id,
        "participants": [
            _serialize_participant(p, self_agent_id=adapter.agent_id)
            for p in data
        ],
    })


async def band_lookup_peers_handler(args: dict, **_kw: Any) -> str:
    """List Band peers (users + agents) that can be invited to a room."""
    adapter = _get_live_adapter()
    if adapter is None:
        return _band_tool_error("Band gateway is not running in this process")

    args = args or {}
    page = int(args.get("page", 1) or 1)
    page_size = int(args.get("page_size", 50) or 50)

    try:
        from thenvoi.client.rest import DEFAULT_REQUEST_OPTIONS  # type: ignore
    except ImportError:
        DEFAULT_REQUEST_OPTIONS = {"max_retries": 3}

    rest = adapter._new_rest_client()

    try:
        response = await rest.agent_api_peers.list_agent_peers(
            page=page,
            page_size=page_size,
            request_options=DEFAULT_REQUEST_OPTIONS,
        )
    except Exception as exc:
        return _band_tool_error(f"list_agent_peers failed: {exc}")
    data = getattr(response, "data", None) or []
    return _json.dumps({
        "peers": [
            _serialize_peer(p, self_agent_id=adapter.agent_id) for p in data
        ],
        "page": page,
        "page_size": page_size,
    })


async def band_add_participant_handler(args: dict, **_kw: Any) -> str:
    """Invite a peer (user or agent) into a Band chat room by ID."""
    adapter = _get_live_adapter()
    if adapter is None:
        return _band_tool_error("Band gateway is not running in this process")

    args = args or {}
    identifier = str(args.get("identifier") or "").strip()
    if not identifier:
        return _band_tool_error(
            "identifier is required — pass a peer ID from band_lookup_peers"
        )
    role = str(args.get("role") or "member").strip() or "member"

    chat_id = _resolve_chat_id(adapter, args.get("chat_id", "") or "")
    if not chat_id:
        return _band_tool_error(
            "No chat_id available — pass chat_id explicitly or send a message "
            "in the target room first"
        )

    try:
        from thenvoi.client.rest import (  # type: ignore
            DEFAULT_REQUEST_OPTIONS,
            ParticipantRequest,
        )
    except ImportError as exc:
        return _band_tool_error(f"Band SDK not importable: {exc}")

    rest = adapter._new_rest_client()

    try:
        response = await rest.agent_api_participants.add_agent_chat_participant(
            chat_id=chat_id,
            participant=ParticipantRequest(
                participant_id=identifier, role=role,
            ),
            request_options=DEFAULT_REQUEST_OPTIONS,
        )
    except Exception as exc:
        return _band_tool_error(f"add_agent_chat_participant failed: {exc}")
    added = getattr(response, "data", None)
    return _json.dumps({
        "chat_id": chat_id,
        "added": _serialize_participant(added) if added else {"id": identifier},
        "role": role,
    })


async def band_remove_participant_handler(args: dict, **_kw: Any) -> str:
    """Remove a peer from a Band chat room by ID."""
    adapter = _get_live_adapter()
    if adapter is None:
        return _band_tool_error("Band gateway is not running in this process")

    args = args or {}
    identifier = str(args.get("identifier") or "").strip()
    if not identifier:
        return _band_tool_error("identifier is required (the participant ID to remove)")

    chat_id = _resolve_chat_id(adapter, args.get("chat_id", "") or "")
    if not chat_id:
        return _band_tool_error(
            "No chat_id available — pass chat_id explicitly or send a message "
            "in the target room first"
        )

    try:
        from thenvoi.client.rest import DEFAULT_REQUEST_OPTIONS  # type: ignore
    except ImportError:
        DEFAULT_REQUEST_OPTIONS = {"max_retries": 3}

    rest = adapter._new_rest_client()

    try:
        await rest.agent_api_participants.remove_agent_chat_participant(
            chat_id=chat_id,
            participant_id=identifier,
            request_options=DEFAULT_REQUEST_OPTIONS,
        )
    except Exception as exc:
        return _band_tool_error(
            f"remove_agent_chat_participant failed: {exc}"
        )
    return _json.dumps({
        "chat_id": chat_id,
        "removed": {"id": identifier},
    })


_TOOLSET = "hermes-band"


_BAND_TOOL_SCHEMAS: List[Dict[str, Any]] = [
    {
        "name": "band_get_participants",
        "description": (
            "List the participants currently in a Band chat room (users and "
            "agents). Operates on the room you are currently replying in by "
            "default; pass chat_id to target a different room."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "chat_id": {
                    "type": "string",
                    "description": "Optional room UUID. Defaults to the current room.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "band_lookup_peers",
        "description": (
            "List Band peers (users and agents) you can invite to a room. "
            "Use this to find another agent's UUID before calling "
            "band_add_participant."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "page": {"type": "integer", "description": "Page number (1-indexed)."},
                "page_size": {
                    "type": "integer",
                    "description": "Items per page (default 50, max 100).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "band_add_participant",
        "description": (
            "Invite a peer (user or agent) into a Band chat room. Pass the "
            "peer's UUID as `identifier`; find IDs via band_lookup_peers. "
            "Defaults to the current room."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "identifier": {
                    "type": "string",
                    "description": "Peer UUID from band_lookup_peers.",
                },
                "role": {
                    "type": "string",
                    "enum": ["owner", "admin", "member"],
                    "description": "Role to assign in the room (default member).",
                },
                "chat_id": {
                    "type": "string",
                    "description": "Optional room UUID. Defaults to the current room.",
                },
            },
            "required": ["identifier"],
        },
    },
    {
        "name": "band_remove_participant",
        "description": (
            "Remove a participant from a Band chat room. Pass the peer's UUID "
            "as `identifier`. Defaults to the current room."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "identifier": {
                    "type": "string",
                    "description": "UUID of the participant to remove.",
                },
                "chat_id": {
                    "type": "string",
                    "description": "Optional room UUID. Defaults to the current room.",
                },
            },
            "required": ["identifier"],
        },
    },
]


_BAND_TOOL_HANDLERS: Dict[str, Any] = {
    "band_get_participants": band_get_participants_handler,
    "band_lookup_peers": band_lookup_peers_handler,
    "band_add_participant": band_add_participant_handler,
    "band_remove_participant": band_remove_participant_handler,
}


def _register_band_tools(ctx: Any) -> None:
    """Expose Band-native platform tools to the Hermes LLM."""
    register_tool = getattr(ctx, "register_tool", None)
    if register_tool is None:
        # Older ctx implementations (tests, stubs) may not provide tool
        # registration. Don't crash — platform registration still succeeds.
        return
    for schema in _BAND_TOOL_SCHEMAS:
        register_tool(
            name=schema["name"],
            toolset=_TOOLSET,
            schema=schema,
            handler=_BAND_TOOL_HANDLERS[schema["name"]],
            # All handlers are async coroutines so the registry dispatches
            # them on Hermes' running loop. Without is_async=True, the
            # registry would call them sync, return a coroutine object,
            # and the LLM would see a useless "<coroutine ...>" string.
            is_async=True,
            requires_env=["BAND_AGENT_ID", "BAND_API_KEY"],
        )


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
            "address someone else, mention them explicitly in your text. "
            "When @-mentioning anyone, copy the `mention_handle` field "
            "verbatim from band_get_participants (e.g. `@ed01/testmes`, "
            "not the short `@testmes` that Band's UI displays) — only "
            "the full owner/agent form addresses participants reliably "
            "across rooms. Keep replies focused and avoid speaking on "
            "behalf of other agents in the room.\n"
            "\n"
            "You can introduce other agents into the conversation: call "
            "band_lookup_peers to find them, then band_add_participant "
            "with their UUID. band_get_participants shows who's already "
            "in the room — the entry marked `\"is_self\": true` is you. "
            "Inbound messages addressed to you arrive with an "
            "`@[[<your-uuid>]]` prefix in the wire format — that's just "
            "routing, you don't need to look it up."
        ),
    )
    _register_band_tools(ctx)
