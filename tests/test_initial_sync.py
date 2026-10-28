"""Regressions for the first C1 sync and bounded history discovery."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("homeassistant")

from custom_components.ha_egd_openapi import api, coordinator as coordinator_module
from custom_components.ha_egd_openapi.api import (
    EgdApiClient,
    EgdApiError,
    EgdValidationError,
)
from custom_components.ha_egd_openapi.const import (
    C1_IMPORT_PROFILE,
    C1_EXPORT_PROFILE,
    CONF_ENABLE_EXPORT,
    CONF_EXPORT_PROFILE,
    CONF_IMPORT_PROFILE,
    PROFILE_UNITS,
)
from custom_components.ha_egd_openapi.coordinator import EgdDataUpdateCoordinator

RAW_LIMIT = datetime(2023, 10, 28, 20, 15, tzinfo=timezone.utc)
SAFE_LIMIT = RAW_LIMIT + timedelta(days=1)
CONTRACT_START = datetime(2026, 5, 19, tzinfo=timezone.utc)
LATEST = datetime(2026, 10, 27, 22, 45, tzinfo=timezone.utc)
LIMIT_MESSAGE = 'Datum "from" nesmí být starší než 3 roky.'
PERMISSION_MESSAGE = "V požadovaném období nemáte oprávnění na data odběrného místa."


class _BoundedClient(EgdApiClient):
    """Emulate EG.D validation and a contract beginning inside the history cap."""

    def __init__(self):
        super().__init__(None, "test", "test")
        self.requests = []

    async def _async_request_profile_data(self, **kwargs):
        self.requests.append(kwargs)
        if kwargs["from_dt"] < RAW_LIMIT:
            return 400, {"error": "validation_error", "message": LIMIT_MESSAGE}
        if kwargs["from_dt"] < CONTRACT_START:
            return 400, {"error": "validation_error", "message": PERMISSION_MESSAGE}
        if kwargs["page_size"] == 1:
            # An access probe must not follow all 96 pages of this response.
            return 200, {
                "total": 96,
                "data": [
                    {
                        "timestamp": kwargs["from_dt"].isoformat(),
                        "value": 0.01,
                        "status": "W",
                    }
                ],
            }
        samples = [(CONTRACT_START, 1), (LATEST - timedelta(hours=1), 2)]
        points = [
            {"timestamp": timestamp.isoformat(), "value": value, "status": "W"}
            for timestamp, value in samples
            if kwargs["from_dt"] <= timestamp <= kwargs["to_dt"]
        ]
        return 200, {"total": len(points), "data": points}


@pytest.fixture
def fixed_history_limit(monkeypatch):
    monkeypatch.setattr(api, "_safe_three_year_cap", lambda: SAFE_LIMIT)
    monkeypatch.setattr(
        coordinator_module, "_safe_three_year_cap", lambda: SAFE_LIMIT, raising=False
    )


@pytest.mark.asyncio
async def test_probe_skips_period_entirely_before_history_limit(fixed_history_limit):
    client = _BoundedClient()
    assert not await client.async_probe_access(
        ean="test-ean",
        profile=C1_IMPORT_PROFILE,
        from_dt=RAW_LIMIT - timedelta(days=1),
        to_dt=RAW_LIMIT,
    )
    assert client.requests == []


@pytest.mark.asyncio
async def test_probe_clamps_overlapping_period_before_making_request(
    fixed_history_limit,
):
    client = _BoundedClient()
    # This range overlaps the cap but is before the contract: deny access,
    # while ensuring the request itself never violates the 3-year rule.
    assert not await client.async_probe_access(
        ean="test-ean",
        profile=C1_IMPORT_PROFILE,
        from_dt=RAW_LIMIT - timedelta(days=1),
        to_dt=SAFE_LIMIT + timedelta(hours=1),
    )
    assert len(client.requests) == 1
    assert client.requests[0]["from_dt"] == SAFE_LIMIT


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", [C1_IMPORT_PROFILE, "ICQ2"])
async def test_access_probe_fetches_one_page_only(fixed_history_limit, profile):
    client = _BoundedClient()
    assert await client.async_probe_access(
        ean="test-ean",
        profile=profile,
        from_dt=CONTRACT_START,
        to_dt=CONTRACT_START + timedelta(hours=23, minutes=45),
    )
    assert len(client.requests) == 1
    assert client.requests[0]["page_start"] == 1
    assert client.requests[0]["page_size"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message, expected",
    [
        (LIMIT_MESSAGE, False),
        (PERMISSION_MESSAGE, False),
        ('Datum "to" může být nejvýše ke včerejšímu dni.', None),
    ],
)
async def test_probe_only_swallows_unavailable_history_windows(
    fixed_history_limit, message, expected
):
    client = _BoundedClient()
    client._async_request_profile_data = AsyncMock(
        return_value=(
            400,
            {
                "error": "validation_error",
                "message": message,
            },
        )
    )
    kwargs = dict(
        ean="test-ean",
        profile=C1_IMPORT_PROFILE,
        from_dt=CONTRACT_START,
        to_dt=CONTRACT_START + timedelta(hours=1),
    )
    if expected is None:
        with pytest.raises(EgdValidationError):
            await client.async_probe_access(**kwargs)
    else:
        assert await client.async_probe_access(**kwargs) is expected


@pytest.mark.parametrize("profile", PROFILE_UNITS)
def test_discovery_and_fetch_share_current_history_limit(fixed_history_limit, profile):
    coordinator = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    expected = (
        datetime(2024, 7, 1, tzinfo=timezone.utc)
        if profile in {"ICQ2", "ISQ2"}
        else SAFE_LIMIT
    )
    assert coordinator._hard_min_for_profile(profile, LATEST) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", PROFILE_UNITS)
async def test_full_fetch_clamps_old_start_before_any_request(
    fixed_history_limit, profile
):
    client = _BoundedClient()
    client._async_request_profile_data = AsyncMock(
        return_value=(200, {"total": 0, "data": []})
    )
    assert (
        await client.async_get_profile_data(
            ean="test-ean",
            profile=profile,
            from_dt=RAW_LIMIT - timedelta(days=1),
            to_dt=SAFE_LIMIT + timedelta(minutes=15),
        )
        == []
    )
    assert client._async_request_profile_data.await_args.kwargs["from_dt"] == SAFE_LIMIT


@pytest.mark.asyncio
async def test_probe_propagates_server_failure(fixed_history_limit):
    client = _BoundedClient()
    client._async_request_profile_data = AsyncMock(return_value=(503, {}))
    with pytest.raises(EgdApiError):
        await client.async_probe_access(
            ean="test-ean",
            profile=C1_IMPORT_PROFILE,
            from_dt=CONTRACT_START,
            to_dt=LATEST,
        )


@pytest.mark.asyncio
async def test_probe_requires_timezone_aware_bounds():
    client = _BoundedClient()
    with pytest.raises(ValueError, match="timezone-aware"):
        await client.async_probe_access(
            ean="test-ean",
            profile=C1_IMPORT_PROFILE,
            from_dt=datetime(2026, 10, 27),
            to_dt=LATEST,
        )
    assert client.requests == []


@pytest.mark.asyncio
async def test_first_sync_from_empty_store_recovers_contract_and_imports_statistics(
    fixed_history_limit,
    monkeypatch,
):
    coordinator = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    coordinator.hass = SimpleNamespace()
    coordinator.config_entry = SimpleNamespace(
        entry_id="test",
        title="Test",
        options={},
        data={
            "ean": "test-ean",
            CONF_IMPORT_PROFILE: C1_IMPORT_PROFILE,
            CONF_EXPORT_PROFILE: C1_EXPORT_PROFILE,
            CONF_ENABLE_EXPORT: False,
            "update_hour": 18,
            "update_minute": 40,
        },
    )
    coordinator._persisted = {}
    coordinator._store = SimpleNamespace(async_save=AsyncMock())
    coordinator.client = _BoundedClient()
    monkeypatch.setattr(coordinator, "_get_latest_available_utc", lambda: LATEST)
    writer = AsyncMock()
    monkeypatch.setattr(coordinator_module, "async_import_external_statistics", writer)

    state = await coordinator._async_refresh_energy_state()

    assert state.sync_status == "ok"
    assert state.total_import_kwh == 3
    assert state.last_api_sync_utc is not None
    assert coordinator._persisted["import_accessible_start"] == "2026-05-19T00:00:00Z"
    assert coordinator._persisted["import_hourly_deltas_complete"] is True
    assert all(
        request["from_dt"] >= SAFE_LIMIT for request in coordinator.client.requests
    )
    assert len(coordinator.client.requests) <= 25
    writer.assert_awaited_once()
    assert writer.await_args.kwargs["rows"][-1]["sum"] == 3


@pytest.mark.parametrize(
    "reference, expected",
    [
        (datetime(2026, 10, 28, 20, 15, tzinfo=timezone.utc), SAFE_LIMIT),
        (
            datetime(2026, 10, 28, 20, 15, 0, 1, tzinfo=timezone.utc),
            datetime(2023, 10, 29, 20, 30, tzinfo=timezone.utc),
        ),
        (
            datetime(2026, 10, 28, 23, 59, 59, tzinfo=timezone.utc),
            datetime(2023, 10, 30, tzinfo=timezone.utc),
        ),
        (
            datetime(2024, 2, 29, 12, tzinfo=timezone.utc),
            datetime(2021, 3, 1, 12, tzinfo=timezone.utc),
        ),
    ],
)
def test_history_limit_keeps_safety_margin_and_handles_leap_day(reference, expected):
    assert api._safe_three_year_cap(reference) == expected
