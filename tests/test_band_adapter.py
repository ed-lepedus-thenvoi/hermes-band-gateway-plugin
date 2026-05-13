"""Tests for the Band platform adapter plugin.

These tests exercise the plugin source that lives in
``plugins/platforms/band/`` (a symlink into the standalone
``hermes-band-gateway-plugin`` repo while the plugin is in
development). They follow the same conventions as
``tests/gateway/test_irc_adapter.py`` and load the adapter via
``load_plugin_adapter`` so xdist workers can't collide on the
``adapter`` module name.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import sys
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.gateway._plugin_adapter_loader import load_plugin_adapter

_band_mod = load_plugin_adapter("band")

BandAdapter = _band_mod.BandAdapter
_BridgeAdapter = _band_mod._BridgeAdapter
_normalize_handle = _band_mod._normalize_handle
_truthy = _band_mod._truthy
_env_enablement = _band_mod._env_enablement
validate_config = _band_mod.validate_config
check_requirements = _band_mod.check_requirements
is_connected = _band_mod.is_connected
register = _band_mod.register
_standalone_send = _band_mod._standalone_send


# Tool handlers — added incrementally via TDD. Bound lazily so individual
# tests can xfail cleanly while a tool is still RED.
def _tool(name: str):
    return getattr(_band_mod, name, None)


# ── Test fixtures ────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _clear_band_env(monkeypatch):
    """Strip BAND_* / THENVOI_* env vars so every test starts clean."""
    for key in (
        "BAND_AGENT_ID",
        "BAND_API_KEY",
        "BAND_REST_URL",
        "BAND_WS_URL",
        "BAND_HOME_CHANNEL",
        "BAND_HOME_CHANNEL_NAME",
        "BAND_ALLOWED_USERS",
        "BAND_ALLOW_ALL_USERS",
        "THENVOI_REST_URL",
        "THENVOI_WS_URL",
    ):
        monkeypatch.delenv(key, raising=False)


def _make_config(**extra: Any):
    from gateway.config import PlatformConfig
    return PlatformConfig(enabled=True, extra=extra or {})


# ── Pure helpers ─────────────────────────────────────────────────────────


class TestHelperFunctions:

    def test_normalize_handle_adds_at_sign(self):
        assert _normalize_handle("alice") == "@alice"

    def test_normalize_handle_preserves_existing_at_sign(self):
        assert _normalize_handle("@alice") == "@alice"

    def test_normalize_handle_returns_none_for_empty(self):
        assert _normalize_handle("") is None
        assert _normalize_handle(None) is None

    @pytest.mark.parametrize("value", ["1", "true", "TRUE", "Yes", "on"])
    def test_truthy_accepts_common_truthy_strings(self, value):
        assert _truthy(value) is True

    @pytest.mark.parametrize("value", ["", "0", "false", "no", "off", None, "maybe"])
    def test_truthy_rejects_everything_else(self, value):
        assert _truthy(value) is False


# ── _env_enablement ──────────────────────────────────────────────────────


class TestEnvEnablement:

    def test_returns_none_when_credentials_missing(self):
        assert _env_enablement() is None

    def test_returns_none_when_only_agent_id_set(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "agent-uuid")
        assert _env_enablement() is None

    def test_returns_seed_with_creds(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "agent-uuid")
        monkeypatch.setenv("BAND_API_KEY", "secret")
        seed = _env_enablement()
        assert seed is not None
        assert seed["agent_id"] == "agent-uuid"
        assert seed["api_key"] == "secret"

    def test_includes_url_overrides_when_set(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "agent-uuid")
        monkeypatch.setenv("BAND_API_KEY", "secret")
        monkeypatch.setenv("BAND_REST_URL", "https://staging.band.example/")
        monkeypatch.setenv("BAND_WS_URL", "wss://staging.band.example/socket")
        seed = _env_enablement()
        assert seed["rest_url"] == "https://staging.band.example/"
        assert seed["ws_url"] == "wss://staging.band.example/socket"

    def test_includes_home_channel_when_set(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "agent-uuid")
        monkeypatch.setenv("BAND_API_KEY", "secret")
        monkeypatch.setenv("BAND_HOME_CHANNEL", "room-uuid")
        monkeypatch.setenv("BAND_HOME_CHANNEL_NAME", "Ops")
        seed = _env_enablement()
        assert seed["home_channel"] == {"chat_id": "room-uuid", "name": "Ops"}

    def test_home_channel_name_defaults_to_band_home(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "a")
        monkeypatch.setenv("BAND_API_KEY", "b")
        monkeypatch.setenv("BAND_HOME_CHANNEL", "room-uuid")
        seed = _env_enablement()
        assert seed["home_channel"]["name"] == "Band Home"


# ── validate_config / is_connected ───────────────────────────────────────


class TestValidateConfig:

    def test_rejects_empty(self):
        assert validate_config(_make_config()) is False

    def test_accepts_env_creds(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "a")
        monkeypatch.setenv("BAND_API_KEY", "b")
        assert validate_config(_make_config()) is True

    def test_accepts_extra_creds(self):
        cfg = _make_config(agent_id="a", api_key="b")
        assert validate_config(cfg) is True

    def test_is_connected_mirrors_validate(self, monkeypatch):
        cfg = _make_config()
        assert is_connected(cfg) is False
        monkeypatch.setenv("BAND_AGENT_ID", "a")
        monkeypatch.setenv("BAND_API_KEY", "b")
        assert is_connected(cfg) is True


class TestCheckRequirements:

    def test_returns_false_without_env(self):
        # Without env creds, returns False regardless of whether the SDK is
        # installed in the test venv. This is the path we care about for
        # the ``hermes gateway setup`` "not configured yet" branch.
        assert check_requirements() is False

    def test_returns_false_when_sdk_missing(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "a")
        monkeypatch.setenv("BAND_API_KEY", "b")
        # Force the SDK import path to fail even if band-sdk is installed.
        real_thenvoi = sys.modules.pop("thenvoi", None)
        monkeypatch.setitem(sys.modules, "thenvoi", None)  # poisons import
        try:
            assert check_requirements() is False
        finally:
            if real_thenvoi is not None:
                sys.modules["thenvoi"] = real_thenvoi


# ── BandAdapter.__init__ ─────────────────────────────────────────────────


class TestBandAdapterInit:

    def test_reads_credentials_from_env(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "uuid-env")
        monkeypatch.setenv("BAND_API_KEY", "key-env")
        adapter = BandAdapter(_make_config())
        assert adapter.agent_id == "uuid-env"
        assert adapter.api_key == "key-env"

    def test_reads_credentials_from_extra(self):
        adapter = BandAdapter(_make_config(agent_id="uuid-cfg", api_key="key-cfg"))
        assert adapter.agent_id == "uuid-cfg"
        assert adapter.api_key == "key-cfg"

    def test_env_beats_extra(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "uuid-env")
        adapter = BandAdapter(_make_config(agent_id="uuid-cfg", api_key="k"))
        assert adapter.agent_id == "uuid-env"

    def test_defaults_when_url_unset(self):
        adapter = BandAdapter(_make_config(agent_id="a", api_key="b"))
        assert adapter.rest_url == "https://app.band.ai"
        assert adapter.ws_url == "wss://app.band.ai/api/v1/socket/websocket"

    def test_falls_back_to_legacy_thenvoi_env(self, monkeypatch):
        # Mid-rebrand: users already configured for the SDK have THENVOI_* set
        # and shouldn't have to duplicate it under BAND_*.
        monkeypatch.setenv("THENVOI_REST_URL", "https://legacy.band.example")
        monkeypatch.setenv("THENVOI_WS_URL", "wss://legacy.band.example/socket")
        adapter = BandAdapter(_make_config(agent_id="a", api_key="b"))
        assert adapter.rest_url == "https://legacy.band.example"
        assert adapter.ws_url == "wss://legacy.band.example/socket"

    def test_band_env_beats_legacy_thenvoi_env(self, monkeypatch):
        monkeypatch.setenv("BAND_REST_URL", "https://new.band.example")
        monkeypatch.setenv("THENVOI_REST_URL", "https://legacy.band.example")
        adapter = BandAdapter(_make_config(agent_id="a", api_key="b"))
        assert adapter.rest_url == "https://new.band.example"

    def test_allowlist_from_env_csv(self, monkeypatch):
        monkeypatch.setenv("BAND_ALLOWED_USERS", "@alice, bob, @carol")
        adapter = BandAdapter(_make_config(agent_id="a", api_key="b"))
        assert adapter._allowed_handles == {"@alice", "@bob", "@carol"}

    def test_allowlist_from_extra_list(self):
        adapter = BandAdapter(_make_config(
            agent_id="a", api_key="b",
            allowed_users=["alice", "@bob"],
        ))
        assert adapter._allowed_handles == {"@alice", "@bob"}

    def test_allow_all_flag(self, monkeypatch):
        monkeypatch.setenv("BAND_ALLOW_ALL_USERS", "true")
        adapter = BandAdapter(_make_config(agent_id="a", api_key="b"))
        assert adapter._allow_all is True


# ── _is_authorized ───────────────────────────────────────────────────────


class TestAuthorization:

    def _adapter(self, **kw):
        return BandAdapter(_make_config(agent_id="a", api_key="b", **kw))

    def test_default_deny_when_no_allowlist(self):
        assert self._adapter()._is_authorized("@alice") is False

    def test_allowlist_permits_listed_handle(self):
        assert self._adapter(allowed_users=["@alice"])._is_authorized("@alice") is True

    def test_allowlist_normalises_at_sign(self):
        # Handle stored with @ but caller passes without — should still match.
        adapter = self._adapter(allowed_users=["alice"])
        assert adapter._is_authorized("@alice") is True

    def test_allowlist_blocks_unlisted_handle(self):
        adapter = self._adapter(allowed_users=["@alice"])
        assert adapter._is_authorized("@bob") is False

    def test_allow_all_bypasses_allowlist(self):
        adapter = self._adapter(allow_all_users=True)
        assert adapter._is_authorized("@stranger") is True

    def test_allow_all_handles_none_handle(self):
        # When sender handle can't be resolved we should still apply the policy
        # rather than crashing.
        adapter = self._adapter(allow_all_users=True)
        assert adapter._is_authorized(None) is True


# ── _record_inbound / _resolve_default_mentions ──────────────────────────


def _msg(message_id="m1", sender_id="user-1", sender_name="Alice", content="hi"):
    """Build a fake PlatformMessage-shaped object."""
    return MagicMock(
        id=message_id,
        sender_id=sender_id,
        sender_name=sender_name,
        sender_type="User",
        content=content,
        created_at=_dt.datetime(2026, 5, 13, 12, 0, 0),
    )


class TestRecordInbound:

    def _adapter(self):
        return BandAdapter(_make_config(agent_id="agent-self", api_key="b"))

    def test_resolves_handle_from_participants(self):
        adapter = self._adapter()
        participants = [{"id": "user-1", "handle": "alice", "type": "User"}]
        adapter._record_inbound("room-1", _msg(sender_id="user-1"), participants)
        assert adapter._room_last_sender["room-1"] == {
            "id": "user-1",
            "handle": "@alice",
        }

    def test_skips_last_sender_when_handle_unknown(self):
        # Sender not present in the participant list — we have no handle, so
        # we must NOT fall back to msg.sender_name (which is a display name,
        # not a mention handle).
        adapter = self._adapter()
        adapter._record_inbound("room-1", _msg(sender_id="user-1"), participants=[])
        assert "room-1" not in adapter._room_last_sender

    def test_caches_participants_list(self):
        adapter = self._adapter()
        participants = [{"id": "user-1", "handle": "alice", "type": "User"}]
        adapter._record_inbound("room-1", _msg(), participants)
        assert adapter._room_participants["room-1"] == participants


class TestResolveDefaultMentions:

    def _adapter(self):
        return BandAdapter(_make_config(agent_id="agent-self", api_key="b"))

    def test_prefers_last_sender(self):
        adapter = self._adapter()
        adapter._room_last_sender["room-1"] = {"id": "u1", "handle": "@alice"}
        mentions = adapter._resolve_default_mentions("room-1", participants=[])
        assert mentions == ["@alice"]

    def test_falls_back_to_non_agent_participants(self):
        adapter = self._adapter()
        participants = [
            {"id": "u1", "handle": "alice", "type": "User"},
            {"id": "u2", "handle": "weatherbot", "type": "Agent"},
            {"id": "u3", "handle": "bob", "type": "User"},
        ]
        mentions = adapter._resolve_default_mentions("room-1", participants)
        assert mentions == ["@alice", "@bob"]  # agent filtered, others kept

    def test_returns_empty_when_no_recipients(self):
        adapter = self._adapter()
        # Only agents in the room → nothing to mention proactively.
        participants = [{"id": "u1", "handle": "bot", "type": "Agent"}]
        assert adapter._resolve_default_mentions("room-1", participants) == []


# ── _BridgeAdapter.on_message dispatch ───────────────────────────────────


@pytest.fixture
def bridge_factory(monkeypatch):
    """Build a real Hermes adapter + bridge with a stub SimpleAdapter base.

    We don't need the Band SDK installed to exercise the bridge — the
    adapter constructs the SDK's ``SimpleAdapter`` subclass lazily, and we
    can substitute a minimal stand-in. The stand-in mirrors the SDK's
    on_started signature so ``super().on_started()`` chains correctly.
    """

    class StubSimpleAdapter:
        async def on_started(self, agent_name: str, agent_description: str) -> None:
            self.agent_name = agent_name
            self.agent_description = agent_description

        async def on_message(self, *a, **kw) -> None:  # pragma: no cover - overridden
            ...

        async def on_cleanup(self, room_id: str) -> None:  # pragma: no cover
            ...

    def factory(*, agent_id="agent-self", allow_all=False, allowed=None):
        extra = {"agent_id": agent_id, "api_key": "key"}
        if allowed is not None:
            extra["allowed_users"] = list(allowed)
        if allow_all:
            extra["allow_all_users"] = True
        adapter = BandAdapter(_make_config(**extra))
        bridge = _BridgeAdapter(StubSimpleAdapter, hermes=adapter)
        return adapter, bridge

    return factory


class TestBridgeOnMessage:

    @pytest.mark.asyncio
    async def test_dispatches_authorized_message(self, bridge_factory):
        adapter, bridge = bridge_factory(allowed=["@alice"])
        adapter.handle_message = AsyncMock()

        tools = MagicMock()
        tools.participants = [{"id": "u1", "handle": "alice", "type": "User"}]

        await bridge.on_message(
            _msg(sender_id="u1"),
            tools,
            history=None,
            participants_msg=None,
            contacts_msg=None,
            is_session_bootstrap=True,
            room_id="room-1",
        )

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.call_args.args[0]
        assert event.text == "hi"
        assert event.source.chat_id == "room-1"

    @pytest.mark.asyncio
    async def test_skips_self_echo(self, bridge_factory):
        adapter, bridge = bridge_factory(allow_all=True)
        adapter.handle_message = AsyncMock()

        echo = MagicMock(
            id="m-echo",
            sender_id="agent-self",
            sender_name="MyAgent",
            sender_type="Agent",
            content="loop?",
            created_at=_dt.datetime(2026, 5, 13),
        )

        await bridge.on_message(
            echo,
            MagicMock(participants=[]),
            history=None,
            participants_msg=None,
            contacts_msg=None,
            is_session_bootstrap=False,
            room_id="room-1",
        )

        adapter.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_drops_unauthorized_sender(self, bridge_factory):
        adapter, bridge = bridge_factory(allowed=["@alice"])
        adapter.handle_message = AsyncMock()

        tools = MagicMock()
        tools.participants = [{"id": "u9", "handle": "stranger", "type": "User"}]

        await bridge.on_message(
            _msg(sender_id="u9", sender_name="Stranger"),
            tools,
            history=None,
            participants_msg=None,
            contacts_msg=None,
            is_session_bootstrap=False,
            room_id="room-1",
        )

        adapter.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_ignores_empty_content(self, bridge_factory):
        adapter, bridge = bridge_factory(allow_all=True)
        adapter.handle_message = AsyncMock()

        msg = _msg(content="   ")
        tools = MagicMock(participants=[{"id": "u1", "handle": "alice", "type": "User"}])

        await bridge.on_message(
            msg, tools,
            history=None, participants_msg=None, contacts_msg=None,
            is_session_bootstrap=False, room_id="room-1",
        )

        adapter.handle_message.assert_not_called()


# ── BandAdapter.send ─────────────────────────────────────────────────────


class TestBandAdapterSend:

    def _adapter(self):
        return BandAdapter(_make_config(agent_id="agent-self", api_key="b"))

    @pytest.mark.asyncio
    async def test_send_fails_when_not_connected(self):
        adapter = self._adapter()
        result = await adapter.send("room-1", "hello")
        assert result.success is False
        assert "Not connected" in result.error

    @pytest.mark.asyncio
    async def test_send_fails_without_resolvable_mentions(self, monkeypatch):
        adapter = self._adapter()
        adapter._mark_connected()
        adapter._agent = MagicMock()
        adapter._agent.runtime.link.rest = MagicMock()

        # No last_sender and no participants — the API would reject the
        # request, so we should fail early with a useful error.
        fake_tools_cls = MagicMock()
        monkeypatch.setattr(
            _band_mod, "_import_band_sdk",
            lambda: (MagicMock(), MagicMock(), fake_tools_cls),
        )

        result = await adapter.send("room-1", "hello")
        assert result.success is False
        assert "no known recipients" in result.error

    @pytest.mark.asyncio
    async def test_send_uses_last_sender_as_mention(self, monkeypatch):
        adapter = self._adapter()
        adapter._mark_connected()
        adapter._agent = MagicMock()
        adapter._agent.runtime.link.rest = MagicMock()
        adapter._room_last_sender["room-1"] = {"id": "u1", "handle": "@alice"}

        sent = MagicMock(id="msg-123")
        fake_tools_instance = MagicMock()
        fake_tools_instance.send_message = AsyncMock(return_value=sent)
        fake_tools_cls = MagicMock(return_value=fake_tools_instance)

        monkeypatch.setattr(
            _band_mod, "_import_band_sdk",
            lambda: (MagicMock(), MagicMock(), fake_tools_cls),
        )

        result = await adapter.send("room-1", "hi alice")
        assert result.success is True
        assert result.message_id == "msg-123"
        fake_tools_instance.send_message.assert_awaited_once_with(
            content="hi alice", mentions=["@alice"]
        )

    @pytest.mark.asyncio
    async def test_send_resolves_at_handle_mentions_from_text(self, monkeypatch):
        """When the LLM writes '@testtestmes please help' in the reply,
        testtestmes should end up in the structured mentions payload so
        Band actually notifies them — not just left as literal text."""
        adapter = self._adapter()
        adapter._mark_connected()
        adapter._agent = MagicMock()
        adapter._agent.runtime.link.rest = MagicMock()
        adapter._room_last_sender["room-1"] = {"id": "u-ed", "handle": "@ed"}
        adapter._room_participants["room-1"] = [
            {"id": "u-ed", "handle": "ed", "type": "User"},
            {"id": "a-tt", "handle": "ed01/testtestmes", "type": "Agent"},
        ]

        sent = MagicMock(id="msg-1")
        fake_tools_instance = MagicMock()
        fake_tools_instance.send_message = AsyncMock(return_value=sent)
        fake_tools_cls = MagicMock(return_value=fake_tools_instance)
        monkeypatch.setattr(
            _band_mod, "_import_band_sdk",
            lambda: (MagicMock(), MagicMock(), fake_tools_cls),
        )

        result = await adapter.send(
            "room-1", "@ed01/testtestmes please weigh in here.",
        )
        assert result.success is True
        mentions = fake_tools_instance.send_message.await_args.kwargs["mentions"]
        # Order doesn't matter; both the in-text mention AND the last sender
        # (the user who asked) should be addressable so they all get notified.
        assert set(mentions) == {"@ed01/testtestmes", "@ed"}

    @pytest.mark.asyncio
    async def test_send_resolves_band_wire_format_mentions(self, monkeypatch):
        """Band's wire format ``@[[uuid]]`` sometimes leaks into LLM replies
        because it appears in the inbound text we forward. The adapter
        should still resolve those to handles so the mention payload is
        clean."""
        adapter = self._adapter()
        adapter._mark_connected()
        adapter._agent = MagicMock()
        adapter._agent.runtime.link.rest = MagicMock()
        adapter._room_last_sender["room-1"] = {"id": "u-ed", "handle": "@ed"}
        adapter._room_participants["room-1"] = [
            {"id": "u-ed", "handle": "ed", "type": "User"},
            {"id": "a-tt", "handle": "ed01/testtestmes", "type": "Agent"},
        ]

        fake_tools_instance = MagicMock()
        fake_tools_instance.send_message = AsyncMock(return_value=MagicMock(id="m"))
        fake_tools_cls = MagicMock(return_value=fake_tools_instance)
        monkeypatch.setattr(
            _band_mod, "_import_band_sdk",
            lambda: (MagicMock(), MagicMock(), fake_tools_cls),
        )

        await adapter.send("room-1", "Hey @[[a-tt]], can you help?")
        mentions = fake_tools_instance.send_message.await_args.kwargs["mentions"]
        assert "@ed01/testtestmes" in mentions

    @pytest.mark.asyncio
    async def test_send_ignores_unknown_at_handles(self, monkeypatch):
        """An @handle that doesn't match any participant is silently
        skipped — we shouldn't inject a fake mention that the API will
        reject. The default last-sender mention should still get through."""
        adapter = self._adapter()
        adapter._mark_connected()
        adapter._agent = MagicMock()
        adapter._agent.runtime.link.rest = MagicMock()
        adapter._room_last_sender["room-1"] = {"id": "u-ed", "handle": "@ed"}
        adapter._room_participants["room-1"] = [
            {"id": "u-ed", "handle": "ed", "type": "User"},
        ]

        fake_tools_instance = MagicMock()
        fake_tools_instance.send_message = AsyncMock(return_value=MagicMock(id="m"))
        fake_tools_cls = MagicMock(return_value=fake_tools_instance)
        monkeypatch.setattr(
            _band_mod, "_import_band_sdk",
            lambda: (MagicMock(), MagicMock(), fake_tools_cls),
        )

        result = await adapter.send("room-1", "Sorry @ghost, I don't know who that is.")
        assert result.success is True
        mentions = fake_tools_instance.send_message.await_args.kwargs["mentions"]
        assert mentions == ["@ed"]  # ghost dropped, last sender retained

    @pytest.mark.asyncio
    async def test_send_surfaces_sdk_error_as_send_failure(self, monkeypatch):
        adapter = self._adapter()
        adapter._mark_connected()
        adapter._agent = MagicMock()
        adapter._agent.runtime.link.rest = MagicMock()
        adapter._room_last_sender["room-1"] = {"id": "u1", "handle": "@alice"}

        fake_tools_instance = MagicMock()
        fake_tools_instance.send_message = AsyncMock(
            side_effect=RuntimeError("rate limited")
        )
        fake_tools_cls = MagicMock(return_value=fake_tools_instance)

        monkeypatch.setattr(
            _band_mod, "_import_band_sdk",
            lambda: (MagicMock(), MagicMock(), fake_tools_cls),
        )

        result = await adapter.send("room-1", "hi")
        assert result.success is False
        assert "rate limited" in result.error


# ── BandAdapter.connect failure modes ────────────────────────────────────


class TestBandAdapterConnect:

    @pytest.mark.asyncio
    async def test_connect_fails_without_credentials(self):
        adapter = BandAdapter(_make_config())  # no creds
        assert await adapter.connect() is False
        assert adapter._fatal_error_code == "config_missing"
        assert adapter._fatal_error_retryable is False

    @pytest.mark.asyncio
    async def test_connect_fails_when_sdk_missing(self, monkeypatch):
        adapter = BandAdapter(_make_config(agent_id="a", api_key="b"))

        def boom():
            raise ImportError("band-sdk not installed")
        monkeypatch.setattr(_band_mod, "_import_band_sdk", boom)

        assert await adapter.connect() is False
        assert adapter._fatal_error_code == "sdk_missing"


# ── _run_agent_with_recovery: reconnect on WS drop ───────────────────────


class TestBandReconnectLoop:
    """The Band SDK gives up on a code-1000 close (Band release shutdowns
    trigger these), and there's no public knob to override that. Our
    plugin wraps ``agent.run()`` in a loop so a clean exit triggers a
    rebuild + reconnect with exponential backoff. ``disconnect()`` sets
    a shutdown flag that breaks the loop cleanly.
    """

    def _adapter_with_fakes(self, monkeypatch, run_behaviours):
        """Build a BandAdapter wired with fake SDK so we can drive
        ``agent.run()`` from the test. ``run_behaviours`` is a list of
        callables — each invocation pops the next behaviour and uses it
        as the body of ``agent.run()`` for that attempt.
        """
        adapter = BandAdapter(_make_config(agent_id="a", api_key="b"))
        adapter._bridge = MagicMock()
        adapter._mark_connected()

        run_call_log = []
        create_call_log = []

        def _make_run(behaviour):
            async def _run():
                run_call_log.append(behaviour)
                await behaviour(adapter)
            return _run

        def fake_create(**kwargs):
            create_call_log.append(kwargs)
            behaviour = run_behaviours.pop(0)
            agent = MagicMock()
            agent.run = _make_run(behaviour)
            agent.stop = AsyncMock()
            return agent

        fake_Agent = MagicMock()
        fake_Agent.create = fake_create
        monkeypatch.setattr(
            _band_mod, "_import_band_sdk",
            lambda: (fake_Agent, MagicMock(), MagicMock()),
        )

        # Bypass the real backoff sleep so tests don't actually wait.
        monkeypatch.setattr(
            _band_mod.asyncio, "sleep", AsyncMock(return_value=None),
        )

        # Pre-build the first agent so the loop has something to start with.
        adapter._agent = fake_create()

        return adapter, run_call_log, create_call_log

    @pytest.mark.asyncio
    async def test_reconnects_when_run_returns_cleanly(self, monkeypatch):
        """The headline case: Band closes the WS with code 1000, the SDK
        returns from agent.run() without raising, and we have to rebuild
        the Agent and reconnect."""
        async def clean_exit(adapter):
            return  # simulates Phoenix code-1000 close

        async def stay_and_then_stop(adapter):
            adapter._shutdown_requested = True

        adapter, runs, creates = self._adapter_with_fakes(
            monkeypatch, run_behaviours=[clean_exit, stay_and_then_stop],
        )

        await adapter._run_agent_with_recovery()

        assert len(runs) == 2, "should have run twice (initial + 1 reconnect)"
        assert len(creates) == 2, "should have rebuilt the Agent once"

    @pytest.mark.asyncio
    async def test_reconnects_when_run_raises(self, monkeypatch):
        """A raised exception is the other path the SDK can take — still
        recover."""
        async def boom(adapter):
            raise RuntimeError("WS error mid-call")

        async def stay_and_then_stop(adapter):
            adapter._shutdown_requested = True

        adapter, runs, creates = self._adapter_with_fakes(
            monkeypatch, run_behaviours=[boom, stay_and_then_stop],
        )

        await adapter._run_agent_with_recovery()

        assert len(runs) == 2
        assert len(creates) == 2

    @pytest.mark.asyncio
    async def test_marks_disconnected_during_gap_then_reconnected(self, monkeypatch):
        """Between attempts, send() should see ``is_connected`` False so
        it fails fast rather than hitting the dead REST client. After a
        successful rebuild, we should be marked connected again."""
        states_during_run = []

        async def first_exit(adapter):
            return

        async def observe_state_then_stop(adapter):
            # If the rebuild succeeded, we should be marked connected
            # again by the time run() is invoked the second time.
            states_during_run.append(adapter.is_connected)
            adapter._shutdown_requested = True

        adapter, _, _ = self._adapter_with_fakes(
            monkeypatch, run_behaviours=[first_exit, observe_state_then_stop],
        )

        await adapter._run_agent_with_recovery()

        assert states_during_run == [True], (
            "second run() should see adapter marked connected again"
        )

    @pytest.mark.asyncio
    async def test_does_not_reconnect_when_shutdown_requested(self, monkeypatch):
        """disconnect() sets _shutdown_requested mid-run. After agent.run()
        returns cleanly, the loop sees the flag and exits instead of
        rebuilding."""
        async def signal_shutdown_then_exit(adapter):
            # Models the realistic race: disconnect() flips the flag, then
            # SDK's agent.stop() makes agent.run() return cleanly.
            adapter._shutdown_requested = True

        # Only one behaviour registered — if the loop wrongly tries to
        # reconnect we'll pop() an empty list and the test will surface
        # the bug as an IndexError.
        adapter, runs, creates = self._adapter_with_fakes(
            monkeypatch, run_behaviours=[signal_shutdown_then_exit],
        )

        await adapter._run_agent_with_recovery()

        assert len(runs) == 1
        assert len(creates) == 1  # only the pre-built initial agent

    @pytest.mark.asyncio
    async def test_exponential_backoff_between_attempts(self, monkeypatch):
        """The sleep duration should grow exponentially across failures
        (1s → 2s → 4s …) so we don't hammer a recovering Band."""
        async def clean_exit(adapter):
            return

        async def stop(adapter):
            adapter._shutdown_requested = True

        sleeps = []

        async def capture_sleep(duration):
            sleeps.append(duration)

        adapter = BandAdapter(_make_config(agent_id="a", api_key="b"))
        adapter._bridge = MagicMock()
        adapter._mark_connected()

        behaviours = [clean_exit, clean_exit, clean_exit, stop]

        def fake_create(**kwargs):
            behaviour = behaviours.pop(0)
            agent = MagicMock()
            async def _run():
                await behaviour(adapter)
            agent.run = _run
            agent.stop = AsyncMock()
            return agent

        fake_Agent = MagicMock()
        fake_Agent.create = fake_create
        monkeypatch.setattr(
            _band_mod, "_import_band_sdk",
            lambda: (fake_Agent, MagicMock(), MagicMock()),
        )
        monkeypatch.setattr(_band_mod.asyncio, "sleep", capture_sleep)

        adapter._agent = fake_create()
        await adapter._run_agent_with_recovery()

        # 3 reconnect attempts → 3 sleeps. Grows exp until capped at 60.
        assert sleeps[0] == 1.0
        assert sleeps[1] == 2.0
        assert sleeps[2] == 4.0


# ── register() ───────────────────────────────────────────────────────────


class TestPluginRegistration:

    def test_register_calls_register_platform_with_band_name(self):
        ctx = MagicMock()
        register(ctx)
        ctx.register_platform.assert_called_once()
        kwargs = ctx.register_platform.call_args.kwargs
        assert kwargs["name"] == "band"
        assert kwargs["label"] == "Band"

    def test_register_wires_required_env(self):
        ctx = MagicMock()
        register(ctx)
        kwargs = ctx.register_platform.call_args.kwargs
        assert kwargs["required_env"] == ["BAND_AGENT_ID", "BAND_API_KEY"]

    def test_register_wires_lifecycle_hooks(self):
        ctx = MagicMock()
        register(ctx)
        kwargs = ctx.register_platform.call_args.kwargs
        # All four hooks are required for the integration points the README
        # promises: env-only auto-config, cron home channel, out-of-process
        # cron delivery, and live adapter construction.
        assert kwargs["env_enablement_fn"] is _env_enablement
        assert kwargs["cron_deliver_env_var"] == "BAND_HOME_CHANNEL"
        assert kwargs["standalone_sender_fn"] is _standalone_send
        assert callable(kwargs["adapter_factory"])

    def test_register_wires_allowlist_env_vars(self):
        ctx = MagicMock()
        register(ctx)
        kwargs = ctx.register_platform.call_args.kwargs
        assert kwargs["allowed_users_env"] == "BAND_ALLOWED_USERS"
        assert kwargs["allow_all_env"] == "BAND_ALLOW_ALL_USERS"


# ── _standalone_send (out-of-process cron delivery) ──────────────────────


class _FakeRestClient:
    """Captures REST calls so tests can assert on the request shape."""

    def __init__(self, *, list_participants_response=None, raise_on_list=None):
        self.list_participants_response = list_participants_response
        self.raise_on_list = raise_on_list
        self.create_message_calls: list[dict] = []
        self.closed = False

        # Build the nested attribute structure the production code uses:
        #   rest.agent_api_participants.list_agent_chat_participants(...)
        #   rest.agent_api_messages.create_agent_chat_message(...)
        async def _list(chat_id, request_options=None):
            if self.raise_on_list is not None:
                raise self.raise_on_list
            return self.list_participants_response

        async def _create(chat_id, message, request_options=None):
            self.create_message_calls.append(
                {"chat_id": chat_id, "message": message}
            )
            return MagicMock(data=MagicMock(id="msg-from-rest"))

        self.agent_api_participants = MagicMock()
        self.agent_api_participants.list_agent_chat_participants = AsyncMock(
            side_effect=_list
        )
        self.agent_api_messages = MagicMock()
        self.agent_api_messages.create_agent_chat_message = AsyncMock(
            side_effect=_create
        )

    async def aclose(self):
        self.closed = True


def _patch_rest(monkeypatch, fake_rest):
    """Replace ``thenvoi.client.rest`` symbols used by ``_standalone_send``."""
    fake_module = MagicMock(
        AsyncRestClient=MagicMock(return_value=fake_rest),
        ChatMessageRequest=lambda content, mentions: MagicMock(
            content=content, mentions=mentions
        ),
        ChatMessageRequestMentionsItem=lambda id, handle: {"id": id, "handle": handle},
        DEFAULT_REQUEST_OPTIONS={"max_retries": 3},
    )
    monkeypatch.setitem(sys.modules, "thenvoi.client.rest", fake_module)
    return fake_module


class TestStandaloneSend:

    @pytest.mark.asyncio
    async def test_returns_error_without_credentials(self):
        result = await _standalone_send(_make_config(), "room-1", "hi")
        assert "error" in result
        assert "BAND_AGENT_ID" in result["error"]

    @pytest.mark.asyncio
    async def test_returns_error_without_chat_id(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "a")
        monkeypatch.setenv("BAND_API_KEY", "b")
        result = await _standalone_send(_make_config(), "", "hi")
        assert "error" in result
        assert "chat_id is required" in result["error"]

    @pytest.mark.asyncio
    async def test_returns_error_when_list_participants_fails(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "a")
        monkeypatch.setenv("BAND_API_KEY", "b")

        fake_rest = _FakeRestClient(raise_on_list=RuntimeError("forbidden"))
        _patch_rest(monkeypatch, fake_rest)

        result = await _standalone_send(_make_config(), "room-1", "hi")
        assert "error" in result
        assert "could not list participants" in result["error"]

    @pytest.mark.asyncio
    async def test_returns_error_when_no_eligible_recipients(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "a")
        monkeypatch.setenv("BAND_API_KEY", "b")

        # Room only contains us + another agent — no humans to mention.
        only_agents = MagicMock(data=[
            MagicMock(id="a", handle="self", type="Agent"),
            MagicMock(id="other-bot", handle="bot", type="Agent"),
        ])
        fake_rest = _FakeRestClient(list_participants_response=only_agents)
        _patch_rest(monkeypatch, fake_rest)

        result = await _standalone_send(_make_config(), "room-1", "hi")
        assert "error" in result
        assert "no eligible recipients" in result["error"]

    @pytest.mark.asyncio
    async def test_happy_path_posts_message_with_user_mentions(self, monkeypatch):
        monkeypatch.setenv("BAND_AGENT_ID", "agent-self")
        monkeypatch.setenv("BAND_API_KEY", "k")

        participants = MagicMock(data=[
            MagicMock(id="agent-self", handle="self", type="Agent"),
            MagicMock(id="u-alice", handle="alice", type="User"),
            MagicMock(id="u-bob", handle="bob", type="User"),
            MagicMock(id="other-bot", handle="weatherbot", type="Agent"),
        ])
        fake_rest = _FakeRestClient(list_participants_response=participants)
        _patch_rest(monkeypatch, fake_rest)

        result = await _standalone_send(_make_config(), "room-1", "morning all")
        assert result.get("success") is True
        assert result["message_id"] == "msg-from-rest"

        # We mentioned the two users, and NOT ourselves or the other agent.
        assert len(fake_rest.create_message_calls) == 1
        msg = fake_rest.create_message_calls[0]["message"]
        mention_handles = {m["handle"] for m in msg.mentions}
        assert mention_handles == {"@alice", "@bob"}
        mention_ids = {m["id"] for m in msg.mentions}
        assert "agent-self" not in mention_ids
        assert "other-bot" not in mention_ids


# ── Platform tools (Band-native operations exposed to the Hermes LLM) ────
#
# These tools let an agent enumerate peers, see who's in the current room,
# and invite/remove participants. They wrap the same REST methods the SDK
# uses internally but route through Hermes' tool-call dispatcher.


import json
from types import SimpleNamespace


def _peer(id, handle, type="User", name=None):
    """Build a Fern-shaped peer/participant. SimpleNamespace avoids the
    MagicMock(name=…) trap (where ``name`` sets the mock's display label
    instead of the attribute).
    """
    return SimpleNamespace(id=id, handle=handle, name=name or handle, type=type)


@pytest.fixture
def live_band(monkeypatch):
    """Patch ``_get_live_adapter`` to return a fake BandAdapter with a
    captured REST client. Returns ``(adapter, rest)`` so the test can
    seed state and assert REST calls.

    Also stubs ``thenvoi.client.rest`` symbols the tool handlers import
    (``DEFAULT_REQUEST_OPTIONS``, ``ParticipantRequest``) so the host
    test venv doesn't need band-sdk installed.
    """
    rest = MagicMock()

    fake_adapter = BandAdapter(_make_config(agent_id="agent-self", api_key="b"))
    fake_adapter._mark_connected()
    fake_adapter._agent = MagicMock()
    fake_adapter._agent.runtime.link.rest = rest

    monkeypatch.setattr(_band_mod, "_get_live_adapter", lambda: fake_adapter)

    # Stub the SDK's REST client module so handler-time imports succeed.
    fake_rest_module = MagicMock(
        DEFAULT_REQUEST_OPTIONS={"max_retries": 3},
        ParticipantRequest=lambda participant_id, role: SimpleNamespace(
            participant_id=participant_id, role=role,
        ),
    )
    monkeypatch.setitem(sys.modules, "thenvoi.client.rest", fake_rest_module)
    return fake_adapter, rest


class TestBandGetParticipantsTool:

    @pytest.mark.asyncio
    async def test_returns_participants_from_current_room(self, live_band):
        adapter, rest = live_band
        adapter._room_last_sender["room-1"] = {"id": "u1", "handle": "@alice"}

        rest.agent_api_participants.list_agent_chat_participants = AsyncMock(
            return_value=SimpleNamespace(data=[
                _peer("agent-self", "self", "Agent", "Self"),
                _peer("u1", "alice", "User", "Alice"),
            ])
        )

        handler = _tool("band_get_participants_handler")
        assert handler is not None, "band_get_participants_handler not registered"

        result = json.loads(await handler({}))
        assert "error" not in result, result
        handles = {p["handle"] for p in result["participants"]}
        assert handles == {"@self", "@alice"}
        rest.agent_api_participants.list_agent_chat_participants.assert_awaited_once()
        kwargs = rest.agent_api_participants.list_agent_chat_participants.await_args.kwargs
        assert kwargs["chat_id"] == "room-1"

    @pytest.mark.asyncio
    async def test_returns_error_when_no_live_adapter(self, monkeypatch):
        monkeypatch.setattr(_band_mod, "_get_live_adapter", lambda: None)
        handler = _tool("band_get_participants_handler")
        result = json.loads(await handler({}))
        assert "error" in result
        assert "not running" in result["error"]

    @pytest.mark.asyncio
    async def test_returns_error_when_no_chat_id_available(self, live_band):
        # live_band fixture doesn't seed _room_last_sender, and no chat_id
        # is passed — handler must fail clean rather than crash or guess.
        handler = _tool("band_get_participants_handler")
        result = json.loads(await handler({}))
        assert "error" in result
        assert "chat_id" in result["error"]

    @pytest.mark.asyncio
    async def test_explicit_chat_id_overrides_cached_room(self, live_band):
        adapter, rest = live_band
        adapter._room_last_sender["room-from-cache"] = {"id": "u1", "handle": "@a"}

        rest.agent_api_participants.list_agent_chat_participants = AsyncMock(
            return_value=SimpleNamespace(data=[])
        )

        handler = _tool("band_get_participants_handler")
        json.loads(await handler({"chat_id": "room-explicit"}))

        kwargs = rest.agent_api_participants.list_agent_chat_participants.await_args.kwargs
        assert kwargs["chat_id"] == "room-explicit"


class TestBandLookupPeersTool:

    @pytest.mark.asyncio
    async def test_returns_peers_with_default_pagination(self, live_band):
        _adapter, rest = live_band
        rest.agent_api_peers.list_agent_peers = AsyncMock(
            return_value=SimpleNamespace(data=[
                _peer("u-bob", "bob", "User", "Bob"),
                _peer("a-weather", "weatherbot", "Agent", "Weather Bot"),
            ])
        )

        handler = _tool("band_lookup_peers_handler")
        assert handler is not None, "band_lookup_peers_handler not registered"

        result = json.loads(await handler({}))
        assert "error" not in result, result
        handles = {p["handle"] for p in result["peers"]}
        assert handles == {"@bob", "@weatherbot"}

        rest.agent_api_peers.list_agent_peers.assert_awaited_once()
        kwargs = rest.agent_api_peers.list_agent_peers.await_args.kwargs
        # Default page sizing is the SDK's responsibility — we just don't
        # constrain it by default.
        assert kwargs.get("page", 1) == 1

    @pytest.mark.asyncio
    async def test_passes_through_pagination_args(self, live_band):
        _adapter, rest = live_band
        rest.agent_api_peers.list_agent_peers = AsyncMock(
            return_value=SimpleNamespace(data=[])
        )

        handler = _tool("band_lookup_peers_handler")
        await handler({"page": 2, "page_size": 25})

        kwargs = rest.agent_api_peers.list_agent_peers.await_args.kwargs
        assert kwargs["page"] == 2
        assert kwargs["page_size"] == 25

    @pytest.mark.asyncio
    async def test_returns_error_when_no_live_adapter(self, monkeypatch):
        monkeypatch.setattr(_band_mod, "_get_live_adapter", lambda: None)
        handler = _tool("band_lookup_peers_handler")
        result = json.loads(await handler({}))
        assert "error" in result and "not running" in result["error"]


class TestBandAddParticipantTool:

    @pytest.mark.asyncio
    async def test_adds_participant_by_id_to_current_room(self, live_band):
        adapter, rest = live_band
        adapter._room_last_sender["room-1"] = {"id": "u1", "handle": "@alice"}

        rest.agent_api_participants.add_agent_chat_participant = AsyncMock(
            return_value=SimpleNamespace(data=_peer("a-helper", "helper", "Agent"))
        )

        handler = _tool("band_add_participant_handler")
        assert handler is not None, "band_add_participant_handler not registered"

        result = json.loads(await handler({"identifier": "a-helper"}))
        assert "error" not in result, result
        assert result["added"]["id"] == "a-helper"
        assert result["added"]["handle"] == "@helper"

        rest.agent_api_participants.add_agent_chat_participant.assert_awaited_once()
        kwargs = rest.agent_api_participants.add_agent_chat_participant.await_args.kwargs
        assert kwargs["chat_id"] == "room-1"
        # The participant payload should carry the participant_id and a role.
        participant = kwargs["participant"]
        assert participant.participant_id == "a-helper"
        assert participant.role == "member"

    @pytest.mark.asyncio
    async def test_role_argument_is_respected(self, live_band):
        adapter, rest = live_band
        adapter._room_last_sender["room-1"] = {"id": "u1", "handle": "@a"}
        rest.agent_api_participants.add_agent_chat_participant = AsyncMock(
            return_value=SimpleNamespace(data=_peer("a-helper", "helper", "Agent"))
        )

        handler = _tool("band_add_participant_handler")
        await handler({"identifier": "a-helper", "role": "admin"})

        kwargs = rest.agent_api_participants.add_agent_chat_participant.await_args.kwargs
        assert kwargs["participant"].role == "admin"

    @pytest.mark.asyncio
    async def test_requires_identifier(self, live_band):
        handler = _tool("band_add_participant_handler")
        result = json.loads(await handler({}))
        assert "error" in result
        assert "identifier" in result["error"]

    @pytest.mark.asyncio
    async def test_surfaces_rest_failures(self, live_band):
        adapter, rest = live_band
        adapter._room_last_sender["room-1"] = {"id": "u1", "handle": "@a"}
        rest.agent_api_participants.add_agent_chat_participant = AsyncMock(
            side_effect=RuntimeError("already a participant"),
        )

        handler = _tool("band_add_participant_handler")
        result = json.loads(await handler({"identifier": "a-helper"}))
        assert "error" in result
        assert "already a participant" in result["error"]


class TestBandRemoveParticipantTool:

    @pytest.mark.asyncio
    async def test_removes_participant_from_current_room(self, live_band):
        adapter, rest = live_band
        adapter._room_last_sender["room-1"] = {"id": "u1", "handle": "@a"}

        rest.agent_api_participants.remove_agent_chat_participant = AsyncMock(
            return_value=SimpleNamespace(data=None)
        )

        handler = _tool("band_remove_participant_handler")
        assert handler is not None, "band_remove_participant_handler not registered"

        result = json.loads(await handler({"identifier": "u-evicted"}))
        assert "error" not in result, result
        assert result["removed"]["id"] == "u-evicted"
        assert result["chat_id"] == "room-1"

        rest.agent_api_participants.remove_agent_chat_participant.assert_awaited_once()
        kwargs = rest.agent_api_participants.remove_agent_chat_participant.await_args.kwargs
        assert kwargs["chat_id"] == "room-1"
        assert kwargs["participant_id"] == "u-evicted"

    @pytest.mark.asyncio
    async def test_requires_identifier(self, live_band):
        handler = _tool("band_remove_participant_handler")
        result = json.loads(await handler({}))
        assert "error" in result and "identifier" in result["error"]

    @pytest.mark.asyncio
    async def test_surfaces_rest_failures(self, live_band):
        adapter, rest = live_band
        adapter._room_last_sender["room-1"] = {"id": "u1", "handle": "@a"}
        rest.agent_api_participants.remove_agent_chat_participant = AsyncMock(
            side_effect=RuntimeError("not a participant"),
        )

        handler = _tool("band_remove_participant_handler")
        result = json.loads(await handler({"identifier": "u-evicted"}))
        assert "error" in result and "not a participant" in result["error"]


class TestRegisterToolsWiring:
    """register() should call ctx.register_tool for each of the 4 tools."""

    def test_register_wires_all_four_tools(self):
        ctx = MagicMock()
        register(ctx)
        names = [
            call.kwargs.get("name") or call.args[0]
            for call in ctx.register_tool.call_args_list
        ]
        assert set(names) == {
            "band_get_participants",
            "band_lookup_peers",
            "band_add_participant",
            "band_remove_participant",
        }

    def test_tools_share_one_toolset(self):
        ctx = MagicMock()
        register(ctx)
        toolsets = {
            call.kwargs.get("toolset") for call in ctx.register_tool.call_args_list
        }
        assert toolsets == {"hermes-band"}

    def test_tools_declare_band_creds_as_required_env(self):
        ctx = MagicMock()
        register(ctx)
        for call in ctx.register_tool.call_args_list:
            assert call.kwargs.get("requires_env") == ["BAND_AGENT_ID", "BAND_API_KEY"]

    def test_tools_register_as_async(self):
        # All handlers are coroutines — without is_async=True the registry
        # would call them sync and the LLM would see a stringified coroutine.
        ctx = MagicMock()
        register(ctx)
        for call in ctx.register_tool.call_args_list:
            assert call.kwargs.get("is_async") is True

    def test_register_survives_ctx_without_register_tool(self):
        # Some test/stub ctxs don't have register_tool. Platform registration
        # must still succeed — _register_band_tools should be a no-op then.
        ctx = MagicMock(spec=["register_platform"])
        register(ctx)  # must not raise
        ctx.register_platform.assert_called_once()
