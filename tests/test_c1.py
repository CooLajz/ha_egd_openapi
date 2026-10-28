"""C1 contract tests for the unified endpoint verified against a live meter."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

pytest.importorskip("homeassistant")

from custom_components.ha_egd_openapi import coordinator as coordinator_module
from custom_components.ha_egd_openapi.api import EgdApiClient, EgdApiError
from custom_components.ha_egd_openapi.config_flow import (
    EgdConfigFlow,
    EgdOptionsFlowHandler,
    _build_user_schema,
)
from custom_components.ha_egd_openapi.const import (
    ATTR_LAST_API_SYNC_UTC,
    ATTR_LAST_VALID_EXPORT_TS,
    ATTR_LAST_VALID_IMPORT_TS,
    ATTR_NEXT_SYNC_ATTEMPT_UTC,
    C1_EXPORT_PROFILE,
    C1_IMPORT_PROFILE,
    CONF_ENABLE_EXPORT,
    CONF_EXPORT_PROFILE,
    CONF_IMPORT_PROFILE,
    DATA_URL,
    LEGACY_C1_PROFILES,
)
from custom_components.ha_egd_openapi.coordinator import EgdDataUpdateCoordinator

START = datetime(2026, 10, 27, tzinfo=timezone.utc)
END = START + timedelta(hours=23, minutes=45)


def _payload(profile=C1_IMPORT_PROFILE, records=None):
    return {
        "profile": profile,
        "units": "kWh",
        "total": len(records) if records is not None else 2,
        "data": records
        if records is not None
        else [
            {"timestamp": "2026-10-27T00:15:00Z", "value": 0.013, "status": "W"},
            {"timestamp": "2026-10-27T00:30:00Z", "value": 0.025, "status": "W"},
        ],
    }


class _PayloadClient(EgdApiClient):
    def __init__(self, payloads):
        super().__init__(session=None, client_id="test", client_secret="test")
        self.payloads = payloads
        self.requests = []

    async def _async_request_profile_data(self, **kwargs):
        self.requests.append(kwargs)
        return 200, self.payloads[kwargs["profile"]]


def _response(payload, status=200):
    response = SimpleNamespace(
        status=status,
        json=AsyncMock(return_value=payload),
        text=AsyncMock(return_value=""),
        headers={},
    )
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=False)
    return context


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "profile",
    [C1_IMPORT_PROFILE, C1_EXPORT_PROFILE, "ICQ2", "ICC1", *LEGACY_C1_PROFILES],
)
async def test_unified_request_routing_and_interval_boundaries(profile):
    session = SimpleNamespace(get=MagicMock(return_value=_response({})))
    client = EgdApiClient(session, "test", "test")
    client._access_token = "test-token"
    await client._async_request_profile_data(
        ean="test-ean",
        profile=profile,
        from_dt=START,
        to_dt=END,
        page_start=1,
        page_size=3000,
    )
    args, kwargs = session.get.call_args
    assert args == (DATA_URL,)
    params = kwargs["params"]
    assert "zdrojDat" not in params
    assert params["pageStart"] == "1" and params["pageSize"] == "3000"
    expected_profile = LEGACY_C1_PROFILES.get(profile, profile)
    assert params["profile"] == expected_profile
    if expected_profile in (C1_IMPORT_PROFILE, C1_EXPORT_PROFILE):
        assert params["from"] == "2026-10-26T23:59:59.999Z"
        assert params["to"] == "2026-10-27T23:59:59.999Z"
    else:
        assert params["from"] == "2026-10-27T00:00:00.000Z"
        assert params["to"] == "2026-10-28T00:00:00.000Z"


@pytest.mark.asyncio
async def test_c1_refreshes_expired_token():
    session = SimpleNamespace(
        get=MagicMock(side_effect=[_response({}, 401), _response(_payload())]),
        post=MagicMock(return_value=_response({"access_token": "renewed-test-token"})),
    )
    client = EgdApiClient(session, "test", "test")
    client._access_token = "expired-test-token"
    records = await client._async_get_profile_data_chunk(
        ean="test-ean", profile=C1_IMPORT_PROFILE, from_dt=START, to_dt=END
    )
    assert len(records) == 2
    assert session.get.call_count == 2
    assert all(call.args == (DATA_URL,) for call in session.get.call_args_list)
    assert (
        session.get.call_args.kwargs["headers"]["Authorization"]
        == "Bearer renewed-test-token"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("day, intervals", [(27, 96), (25, 100)])
async def test_c1_fetch_includes_every_quarter_with_exclusive_server_bounds(
    day, intervals
):
    """Reproduce the live API's bounds, including the DST transition day."""
    prague = ZoneInfo("Europe/Prague")
    local_start = datetime(2026, 10, day, tzinfo=prague)
    start = local_start.astimezone(timezone.utc)
    end = (local_start + timedelta(days=1)).astimezone(timezone.utc) - timedelta(
        minutes=15
    )
    points = [
        {
            "timestamp": (start + timedelta(minutes=15 * i)).isoformat(),
            "value": 0.25,
            "status": "W",
        }
        for i in range(intervals)
    ]

    def get_response(url, *, params, **kwargs):
        lower = datetime.fromisoformat(params["from"].replace("Z", "+00:00"))
        upper = datetime.fromisoformat(params["to"].replace("Z", "+00:00"))
        selected = [
            point
            for point in points
            if lower < datetime.fromisoformat(point["timestamp"]) < upper
        ]
        return _response(_payload(records=selected))

    session = SimpleNamespace(get=MagicMock(side_effect=get_response))
    client = EgdApiClient(session, "test", "test")
    client._access_token = "test-token"
    records = await client.async_get_profile_data(
        ean="test-ean",
        profile=C1_IMPORT_PROFILE,
        from_dt=start,
        to_dt=end,
    )
    assert len(records) == intervals
    assert records[0].timestamp == start
    assert records[-1].timestamp == end
    assert sum(record.value for record in records) == intervals * 0.25


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_c1_payload_produces_energy_without_shifting_timestamps(legacy):
    client = _PayloadClient({C1_IMPORT_PROFILE: _payload()})
    profile = next(iter(LEGACY_C1_PROFILES)) if legacy else C1_IMPORT_PROFILE
    records = await client.async_get_profile_data(
        ean="test-ean", profile=profile, from_dt=START, to_dt=END
    )
    assert [record.value for record in records] == [0.013, 0.025]
    assert [record.timestamp for record in records] == [
        START + timedelta(minutes=15),
        START + timedelta(minutes=30),
    ]
    assert client.requests[0]["profile"] == C1_IMPORT_PROFILE


@pytest.mark.asyncio
async def test_c1_deduplicates_records_without_summing_overlapping_pages():
    payload = _payload()
    payload["data"] += deepcopy(payload["data"])
    payload["total"] = 4
    client = _PayloadClient({C1_IMPORT_PROFILE: payload})
    records = await client.async_get_profile_data(
        ean="test-ean", profile=C1_IMPORT_PROFILE, from_dt=START, to_dt=END
    )
    assert len(records) == 2
    payload["data"][2]["value"] = 9
    with pytest.raises(EgdApiError, match="konfliktní"):
        await client.async_get_profile_data(
            ean="test-ean", profile=C1_IMPORT_PROFILE, from_dt=START, to_dt=END
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {},
        "invalid",
        {"MeasurementsValues": []},
        {"data": None},
        {"data": [], "total": None},
        _payload(records=[{"timestamp": "invalid", "value": 1, "status": "W"}]),
        _payload(
            records=[{"timestamp": "2026-10-27T00:15:00", "value": 1, "status": "W"}]
        ),
        _payload(
            records=[
                {"timestamp": "2026-10-27T00:15:00Z", "value": "NaN", "status": "W"}
            ]
        ),
        _payload(
            records=[
                {"timestamp": "2026-10-27T00:15:00Z", "value": None, "status": "W"}
            ]
        ),
    ],
)
async def test_c1_rejects_invalid_data_instead_of_reporting_no_data(payload):
    client = _PayloadClient({C1_IMPORT_PROFILE: payload})
    with pytest.raises(EgdApiError):
        await client.async_get_profile_data(
            ean="test-ean", profile=C1_IMPORT_PROFILE, from_dt=START, to_dt=END
        )


@pytest.mark.asyncio
async def test_c1_accepts_empty_energy_series():
    client = _PayloadClient(
        {C1_EXPORT_PROFILE: _payload(C1_EXPORT_PROFILE, records=[])}
    )
    assert (
        await client.async_get_profile_data(
            ean="test-ean", profile=C1_EXPORT_PROFILE, from_dt=START, to_dt=END
        )
        == []
    )


def _entry(
    import_profile=C1_IMPORT_PROFILE,
    export_profile=C1_EXPORT_PROFILE,
    enable_export=True,
):
    return SimpleNamespace(
        entry_id="test-entry",
        title="Test EG.D",
        options={},
        data={
            "ean": "test-ean",
            "name": "Test EG.D",
            "client_id": "test",
            "client_secret": "test",
            CONF_IMPORT_PROFILE: import_profile,
            CONF_EXPORT_PROFILE: export_profile,
            CONF_ENABLE_EXPORT: enable_export,
            "update_hour": 18,
            "update_minute": 40,
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("enable_export", [True, False])
async def test_c1_refresh_imports_kwh_once_and_respects_disabled_export(
    monkeypatch, enable_export
):
    coordinator = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    coordinator.hass = SimpleNamespace()
    coordinator.config_entry = _entry(enable_export=enable_export)
    coordinator._persisted = {}
    coordinator._store = SimpleNamespace(async_save=AsyncMock())
    coordinator.client = _PayloadClient(
        {C1_IMPORT_PROFILE: _payload(), C1_EXPORT_PROFILE: _payload(C1_EXPORT_PROFILE)}
    )
    monkeypatch.setattr(
        coordinator, "_hard_min_for_profile", lambda profile, latest: START
    )
    monkeypatch.setattr(coordinator, "_get_latest_available_utc", lambda: END)
    writer = AsyncMock()
    monkeypatch.setattr(coordinator_module, "async_import_external_statistics", writer)
    state = await coordinator._async_refresh_energy_state()
    assert state.total_import_kwh == 0.038
    assert state.total_export_kwh == (0.038 if enable_export else 0)
    assert state.sync_status == "ok"
    assert writer.await_count == (2 if enable_export else 1)
    for call in writer.await_args_list:
        assert call.kwargs["rows"] == [{"start": START, "state": 0.038, "sum": 0.038}]
    if not enable_export:
        assert all(
            request["profile"] == C1_IMPORT_PROFILE
            for request in coordinator.client.requests
        )
    writer.reset_mock()
    state = await coordinator._async_refresh_energy_state()
    assert state.total_import_kwh == 0.038
    writer.assert_not_awaited()


def test_c1_dst_day_contains_100_distinct_quarters_and_preserves_energy():
    coordinator = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    start = datetime(2026, 10, 24, 22, tzinfo=timezone.utc)
    from custom_components.ha_egd_openapi.api import IntervalRecord

    records = [
        IntervalRecord(start + timedelta(minutes=15 * i), 0.5, "W") for i in range(100)
    ]
    hours, metadata = coordinator._process_records_hourly(
        records=records, profile=C1_IMPORT_PROFILE
    )
    assert len(hours) == 25
    assert sum(hours.values()) == 50
    assert metadata["last_valid_ts"] == start + timedelta(minutes=15 * 99)


def test_c1_latest_slot_includes_last_quarter_without_today_boundary(monkeypatch):
    coordinator = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    coordinator.config_entry = _entry()
    monkeypatch.setattr(
        coordinator_module.dt_util,
        "now",
        lambda: datetime(2026, 10, 28, 20, tzinfo=ZoneInfo("Europe/Prague")),
    )
    assert coordinator._get_latest_available_utc() == datetime(
        2026, 10, 27, 22, 45, tzinfo=timezone.utc
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "previous_profile, selected_profile, reset",
    [
        (None, "ICQ2", False),
        (None, C1_IMPORT_PROFILE, True),
        ("ICQ2", C1_IMPORT_PROFILE, True),
        (C1_IMPORT_PROFILE, C1_IMPORT_PROFILE, False),
        (next(iter(LEGACY_C1_PROFILES)), next(iter(LEGACY_C1_PROFILES)), True),
    ],
)
async def test_profile_switch_and_beta1_migration_revalidate_without_deleting_history(
    previous_profile, selected_profile, reset
):
    coordinator = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    coordinator.config_entry = _entry(selected_profile)
    stored = {
        "import_hourly_deltas": {"2026-10-26T00:00:00Z": 2.5},
        "import_hourly_deltas_complete": True,
        "export_hourly_deltas_complete": True,
        "import_accessible_start": "2026-10-01T00:00:00Z",
        ATTR_LAST_VALID_IMPORT_TS: "2026-10-26T23:45:00Z",
        ATTR_LAST_VALID_EXPORT_TS: "2026-10-26T23:45:00Z",
        ATTR_LAST_API_SYNC_UTC: "2026-10-27T18:40:00Z",
        ATTR_NEXT_SYNC_ATTEMPT_UTC: "2099-10-28T18:40:00Z",
        CONF_EXPORT_PROFILE: C1_EXPORT_PROFILE,
    }
    if previous_profile is not None:
        stored[CONF_IMPORT_PROFILE] = previous_profile
    coordinator._store = SimpleNamespace(async_load=AsyncMock(return_value=stored))
    await coordinator.async_load()
    assert coordinator._persisted["import_hourly_deltas"] == {
        "2026-10-26T00:00:00Z": 2.5
    }
    assert ("import_hourly_deltas_complete" not in coordinator._persisted) == reset
    assert coordinator.should_refresh_on_startup() == reset


def _options_flow(entry):
    flow = EgdOptionsFlowHandler()
    flow.handler = entry.entry_id
    flow.hass = SimpleNamespace(
        config_entries=SimpleNamespace(async_get_known_entry=lambda entry_id: entry)
    )
    return flow


def test_config_schema_accepts_c1_and_upgrades_beta1_defaults():
    data = _build_user_schema()(_entry().data)
    assert data[CONF_IMPORT_PROFILE] == C1_IMPORT_PROFILE
    defaults = _entry(next(iter(LEGACY_C1_PROFILES))).data
    assert _build_user_schema(defaults)({})[CONF_IMPORT_PROFILE] == C1_IMPORT_PROFILE


@pytest.mark.asyncio
async def test_config_rejects_mixed_api_types_before_authentication():
    result = await EgdConfigFlow().async_step_user(
        _entry(C1_IMPORT_PROFILE, "ISQ2").data
    )
    assert result["errors"] == {"base": "mixed_measurement_types"}


@pytest.mark.asyncio
async def test_options_allow_switching_to_c1_and_disabling_missing_export():
    flow = _options_flow(_entry("ICQ2", "ISQ2"))
    result = await flow.async_step_init(
        {
            CONF_IMPORT_PROFILE: C1_IMPORT_PROFILE,
            CONF_EXPORT_PROFILE: "ISQ2",
            CONF_ENABLE_EXPORT: False,
            "update_hour": 18,
            "update_minute": 40,
            "revalidate_days": 31,
            "enable_diagnostics": True,
        }
    )
    assert result["data"][CONF_IMPORT_PROFILE] == C1_IMPORT_PROFILE
    assert result["data"][CONF_ENABLE_EXPORT] is False


@pytest.mark.asyncio
async def test_options_reject_mixed_profiles_and_keep_submitted_values():
    result = await _options_flow(_entry("ICQ2", "ISQ2")).async_step_init(
        {CONF_IMPORT_PROFILE: C1_IMPORT_PROFILE, CONF_EXPORT_PROFILE: "ISQ2"}
    )
    assert result["errors"] == {"base": "mixed_measurement_types"}
    assert result["data_schema"]({})[CONF_IMPORT_PROFILE] == C1_IMPORT_PROFILE
