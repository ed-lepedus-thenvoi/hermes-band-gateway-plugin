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
