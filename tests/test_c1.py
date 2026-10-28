"""C1 contract tests using the documented EG.D response, without live credentials."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

pytest.importorskip("homeassistant")

from custom_components.ha_egd_openapi import api, coordinator as coordinator_module
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
    C1_DATA_SOURCE,
    C1_DATA_URL,
    C1_EXPORT_PROFILE,
    C1_IMPORT_PROFILE,
    CONF_EXPORT_PROFILE,
    CONF_IMPORT_PROFILE,
    DATA_URL,
)
from custom_components.ha_egd_openapi.coordinator import EgdDataUpdateCoordinator

START = datetime(2026, 10, 27, tzinfo=timezone.utc)
END = START + timedelta(hours=23, minutes=30)


def _c1_payload(profile=C1_IMPORT_PROFILE, measurements=None):
    """The documented nested C1 structure with fictional meter identifiers."""
    return {
        "EAN": "859182400000000000",
        "MeasurementType": profile,
        "MeasurementsValues": [
            {
                "SerialNumber": "test-meter",
                "Measurements": measurements if measurements is not None else [
                    {"datetime": "2026-10-27T01:15:00+01:00", "value": "0.013", "status": "W"},
                    {"datetime": "2026-10-27T01:30:00+01:00", "value": "0.025", "status": "W"},
                ],
            }
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
@pytest.mark.parametrize("profile", [C1_IMPORT_PROFILE, C1_EXPORT_PROFILE, "ICQ2", "ICC1"])
async def test_request_routes_c1_without_changing_ab(profile):
    """Check the actual HTTP request parameters, not just a URL helper."""
    payload = _c1_payload(profile)
    session = SimpleNamespace(get=MagicMock(return_value=_response(payload)))
    client = EgdApiClient(session, "test", "test")
    client._access_token = "test-token"

    status, data = await client._async_request_profile_data(
        ean="859182400000000000", profile=profile,
        from_dt=START, to_dt=END, page_start=1, page_size=3000,
    )

    assert status == 200
    assert data == payload
    args, kwargs = session.get.call_args
    params = kwargs["params"]
    assert params["from"] == "2026-10-27T00:00:00.000Z"
    assert params["to"] == "2026-10-27T23:45:00.000Z"
    assert params["profile"] == profile
    if profile in (C1_IMPORT_PROFILE, C1_EXPORT_PROFILE):
        assert args == (C1_DATA_URL,)
        assert params["zdrojDat"] == C1_DATA_SOURCE
        assert "pageStart" not in params
        assert "pageSize" not in params
    else:
        assert args == (DATA_URL,)
        assert "zdrojDat" not in params
        assert params["pageStart"] == "1"
        assert params["pageSize"] == "3000"


@pytest.mark.asyncio
async def test_c1_refreshes_expired_token_on_the_same_endpoint():
    payload = _c1_payload()
    session = SimpleNamespace(
        get=MagicMock(side_effect=[_response({}, 401), _response(payload)]),
        post=MagicMock(return_value=_response({"access_token": "renewed-test-token"})),
    )
    client = EgdApiClient(session, "test", "test")
    client._access_token = "expired-test-token"

    records = await client._async_get_profile_data_chunk(
        ean="859182400000000000", profile=C1_IMPORT_PROFILE, from_dt=START, to_dt=END,
    )

    assert len(records) == 2
    assert session.get.call_count == 2
    assert all(call.args == (C1_DATA_URL,) for call in session.get.call_args_list)
    assert session.get.call_args.kwargs["headers"]["Authorization"] == "Bearer renewed-test-token"
    session.post.assert_called_once()


@pytest.mark.asyncio
async def test_documented_c1_response_produces_interval_energy():
    client = _PayloadClient({C1_IMPORT_PROFILE: _c1_payload()})
    records = await client.async_get_profile_data(
        ean="859182400000000000", profile=C1_IMPORT_PROFILE, from_dt=START, to_dt=END,
    )
    assert [record.value for record in records] == [0.013, 0.025]
    assert [record.timestamp for record in records] == [
        START + timedelta(minutes=15), START + timedelta(minutes=30),
    ]
    assert all(record.status == "W" for record in records)
    assert len(client.requests) == 1


def test_c1_distinguishes_repeated_local_hour_at_dst_change():
    payload = _c1_payload(measurements=[
        {"datetime": "2026-10-25T02:15:00+01:00", "value": "2", "status": "W"},
        {"datetime": "2026-10-25T02:15:00+02:00", "value": "1", "status": "W"},
    ])
    records = EgdApiClient._parse_c1_records(payload, profile=C1_IMPORT_PROFILE)
    assert [record.timestamp.isoformat() for record in records] == [
        "2026-10-25T00:15:00+00:00", "2026-10-25T01:15:00+00:00",
    ]
    assert [record.value for record in records] == [1, 2]


def test_c1_does_not_double_count_duplicate_meter_groups():
    payload = _c1_payload()
    payload["MeasurementsValues"].append(deepcopy(payload["MeasurementsValues"][0]))
    assert len(EgdApiClient._parse_c1_records(payload, profile=C1_IMPORT_PROFILE)) == 2

    payload["MeasurementsValues"][1]["Measurements"][0]["value"] = "9"
    with pytest.raises(EgdApiError, match="konfliktní"):
        EgdApiClient._parse_c1_records(payload, profile=C1_IMPORT_PROFILE)


@pytest.mark.parametrize("payload", [
    {}, [], {"total": 0, "data": []}, {"MeasurementsValues": None},
    {"MeasurementsValues": [{}]},
    _c1_payload(profile="0.0.2.1.1.1.12.0.0.0.0.0.0.0.0.3.72.0"),
    _c1_payload(measurements=[{"datetime": "2026-10-27T01:15:00", "value": "1", "status": "W"}]),
    _c1_payload(measurements=[{"datetime": "invalid", "value": "1", "status": "W"}]),
    _c1_payload(measurements=[{"datetime": "2026-10-27T01:15:00Z", "value": "NaN", "status": "W"}]),
    _c1_payload(measurements=[{"datetime": "2026-10-27T01:15:00Z", "value": None, "status": "W"}]),
])
def test_c1_rejects_unexpected_responses_instead_of_reporting_no_data(payload):
    with pytest.raises(EgdApiError):
        EgdApiClient._parse_c1_records(payload, profile=C1_IMPORT_PROFILE)


def test_c1_accepts_empty_results_and_missing_invalid_measurements():
    assert EgdApiClient._parse_c1_records(
        {"MeasurementsValues": []}, profile=C1_IMPORT_PROFILE,
    ) == []
    assert EgdApiClient._parse_c1_records(
        _c1_payload(measurements=[
            {"datetime": "2026-10-27T01:15:00+01:00", "value": None, "status": "F"},
        ]), profile=C1_IMPORT_PROFILE,
    ) == []


@pytest.mark.asyncio
async def test_c1_clips_inclusive_api_boundaries_between_chunks():
    client = _PayloadClient({C1_IMPORT_PROFILE: _c1_payload()})
    records = await client._async_get_profile_data_chunk(
        ean="859182400000000000", profile=C1_IMPORT_PROFILE,
        from_dt=START, to_dt=START + timedelta(minutes=15),
    )
    assert len(records) == 1
    assert records[0].value == 0.013


@pytest.mark.asyncio
async def test_ab_does_not_silently_swallow_c1_responses():
    client = _PayloadClient({"ICQ2": _c1_payload()})
    with pytest.raises(EgdApiError, match="zvolte profily C1"):
        await client._async_get_profile_data_chunk(
            ean="859182400000000000", profile="ICQ2", from_dt=START, to_dt=END,
        )


def _entry(import_profile=C1_IMPORT_PROFILE, export_profile=C1_EXPORT_PROFILE):
    return SimpleNamespace(
        entry_id="test-entry", title="Test EG.D", options={}, data={
            "ean": "859182400000000000", "name": "Test EG.D",
            "client_id": "test", "client_secret": "test",
            CONF_IMPORT_PROFILE: import_profile, CONF_EXPORT_PROFILE: export_profile,
            "update_hour": 18, "update_minute": 40,
        },
    )


@pytest.mark.asyncio
async def test_c1_refresh_imports_energy_without_power_conversion_or_double_counting(monkeypatch):
    """Run parsing, hourly aggregation and cumulative rows as one refresh path."""
    coordinator = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    coordinator.hass = SimpleNamespace()
    coordinator.config_entry = _entry()
    coordinator._persisted = {}
    coordinator._store = SimpleNamespace(async_save=AsyncMock())
    coordinator.client = _PayloadClient({
        C1_IMPORT_PROFILE: _c1_payload(),
        C1_EXPORT_PROFILE: _c1_payload(C1_EXPORT_PROFILE),
    })
    monkeypatch.setattr(coordinator, "_hard_min_for_profile", lambda profile, latest: START)
    monkeypatch.setattr(coordinator, "_get_latest_available_utc", lambda: END)
    writer = AsyncMock()
    monkeypatch.setattr(coordinator_module, "async_import_external_statistics", writer)

    state = await coordinator._async_refresh_energy_state()
    assert state.total_import_kwh == 0.038
    assert state.total_export_kwh == 0.038
    assert state.sync_status == "ok"
    assert writer.await_count == 2
    for call in writer.await_args_list:
        assert call.kwargs["source"] == "ha_egd_openapi"
        assert call.kwargs["rows"] == [{"start": START, "state": 0.038, "sum": 0.038}]

    writer.reset_mock()
    state = await coordinator._async_refresh_energy_state()
    assert state.total_import_kwh == 0.038
    assert state.total_export_kwh == 0.038
    writer.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("previous_profile, selected_profile, reset", [
    (None, "ICQ2", False),
    (None, C1_IMPORT_PROFILE, True),
    ("ICQ2", C1_IMPORT_PROFILE, True),
    (C1_IMPORT_PROFILE, C1_IMPORT_PROFILE, False),
])
async def test_profile_switch_revalidates_history_and_preserves_cached_energy(
    previous_profile, selected_profile, reset,
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

    assert coordinator._persisted["import_hourly_deltas"] == {"2026-10-26T00:00:00Z": 2.5}
    assert coordinator._persisted[CONF_IMPORT_PROFILE] == selected_profile
    assert ("import_hourly_deltas_complete" not in coordinator._persisted) == reset
    assert ("import_accessible_start" not in coordinator._persisted) == reset
    assert coordinator.should_refresh_on_startup() == reset


def test_config_schema_accepts_c1_profiles():
    data = _build_user_schema()(_entry().data)
    assert data[CONF_IMPORT_PROFILE] == C1_IMPORT_PROFILE
    assert data[CONF_EXPORT_PROFILE] == C1_EXPORT_PROFILE


def _options_flow(entry):
    flow = EgdOptionsFlowHandler()
    flow.handler = entry.entry_id
    flow.hass = SimpleNamespace(config_entries=SimpleNamespace(
        async_get_known_entry=lambda entry_id: entry,
    ))
    return flow


@pytest.mark.asyncio
async def test_config_rejects_mixed_api_types_before_authentication():
    flow = EgdConfigFlow()
    result = await flow.async_step_user(_entry(C1_IMPORT_PROFILE, "ISQ2").data)
    assert result["errors"] == {"base": "mixed_measurement_types"}


@pytest.mark.asyncio
async def test_options_allow_switching_existing_entry_to_c1():
    entry = _entry("ICQ2", "ISQ2")
    flow = _options_flow(entry)
    result = await flow.async_step_init({
        CONF_IMPORT_PROFILE: C1_IMPORT_PROFILE, CONF_EXPORT_PROFILE: C1_EXPORT_PROFILE,
        "update_hour": 18, "update_minute": 40, "revalidate_days": 31,
        "enable_diagnostics": True,
    })
    assert result["data"][CONF_IMPORT_PROFILE] == C1_IMPORT_PROFILE
    assert result["data"][CONF_EXPORT_PROFILE] == C1_EXPORT_PROFILE


@pytest.mark.asyncio
async def test_options_reject_mixed_profiles_and_keep_submitted_values():
    flow = _options_flow(_entry("ICQ2", "ISQ2"))
    result = await flow.async_step_init({
        CONF_IMPORT_PROFILE: C1_IMPORT_PROFILE, CONF_EXPORT_PROFILE: "ISQ2",
    })
    assert result["errors"] == {"base": "mixed_measurement_types"}
    assert result["data_schema"]({})[CONF_IMPORT_PROFILE] == C1_IMPORT_PROFILE
