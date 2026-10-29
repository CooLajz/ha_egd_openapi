"""A/B requests must include complete local days without crossing midnight."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

pytest.importorskip("homeassistant")

from custom_components.ha_egd_openapi import coordinator as coordinator_module
from custom_components.ha_egd_openapi.api import EgdApiClient
from custom_components.ha_egd_openapi.const import PROFILE_UNITS
from custom_components.ha_egd_openapi.coordinator import EgdDataUpdateCoordinator

PRAGUE = ZoneInfo("Europe/Prague")
UTC = timezone.utc
QUARTER = timedelta(minutes=15)


@pytest.mark.parametrize("profile", PROFILE_UNITS)
@pytest.mark.parametrize("days", [1, 31])
def test_revalidation_preserves_configured_days_and_c1_behavior(monkeypatch, profile, days):
    c = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    c.config_entry = SimpleNamespace(
        options={"revalidate_days": days}, data={"revalidate_days": 31}
    )
    latest = datetime(2026, 10, 28, 22, 30, tzinfo=UTC)
    monkeypatch.setattr(coordinator_module.dt_util, "as_local", lambda dt: dt.astimezone(PRAGUE))
    expected = (
        datetime(2026, 10, 28, tzinfo=UTC)
        if days == 1 else datetime(2026, 9, 28, tzinfo=UTC)
    )
    if profile not in {"DCQC", "DSQC"}:
        expected = expected.replace(tzinfo=PRAGUE).astimezone(UTC)
    assert c._get_revalidation_start(latest, profile) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("profiles", [("ICQ2", "ISQ2"), ("ICC1", "ISC1")])
@pytest.mark.parametrize("cache_complete", [True, False])
@pytest.mark.parametrize("day", ["2026-01-15", "2026-07-15", "2026-03-29", "2026-10-25"])
async def test_daily_refresh_loads_complete_day_with_one_day_setting(
    monkeypatch, profiles, cache_complete, day
):
    """Exercise real request formatting, parsing, merging and statistics rows."""
    start_local = datetime.fromisoformat(day).replace(tzinfo=PRAGUE)
    start = start_local.astimezone(UTC)
    next_midnight = (start_local + timedelta(days=1)).astimezone(UTC)
    end = (start_local + timedelta(days=2)).astimezone(UTC)
    now = start_local + timedelta(days=1, hours=18)
    slots = [start + i * QUARTER for i in range(int((end - start) / QUARTER))]
    first_day_count = int((next_midnight - start) / QUARTER)
    monkeypatch.setattr(coordinator_module.dt_util, "now", lambda: now)
    monkeypatch.setattr(coordinator_module.dt_util, "utcnow", lambda: now.astimezone(UTC))
    monkeypatch.setattr(coordinator_module.dt_util, "as_local", lambda dt: dt.astimezone(PRAGUE))

    requests = []

    def get(url, *, params, **kwargs):
        lower = datetime.fromisoformat(params["from"].replace("Z", "+00:00"))
        upper = datetime.fromisoformat(params["to"].replace("Z", "+00:00"))
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        # The live API accepts the last millisecond, but rejects today.
        assert upper == midnight.astimezone(UTC) - timedelta(milliseconds=1)
        requests.append((params["profile"], lower, upper))
        profile = params["profile"]
        batch = [
            {"timestamp": ts.isoformat(), "value": 4 if PROFILE_UNITS[profile] == "kW" else 1, "status": "IU012"}
            for ts in slots if lower < ts < upper
        ]
        response = SimpleNamespace(status=200, json=AsyncMock(return_value={
            "profile": profile, "units": PROFILE_UNITS[profile], "total": len(batch), "data": batch,
        }))
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=response)
        context.__aexit__ = AsyncMock(return_value=False)
        return context

    c = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    c.hass = SimpleNamespace()
    c.config_entry = SimpleNamespace(title="Test", options={}, data={
        "ean": "test", "import_profile": profiles[0], "export_profile": profiles[1],
        "enable_export": True, "revalidate_days": 1, "update_hour": 18, "update_minute": 0,
    })
    c._persisted = {}
    c._store = SimpleNamespace(async_save=AsyncMock())
    c.client = EgdApiClient(SimpleNamespace(get=get), "test", "test")
    c.client._access_token = "test"
    c._find_first_accessible_timestamp = AsyncMock(return_value=start)
    writer = AsyncMock()
    monkeypatch.setattr(coordinator_module, "async_import_external_statistics", writer)

    first = await c._async_refresh_energy_state()
    assert first.total_import_kwh == first_day_count
    assert first.total_export_kwh == first_day_count
    last_hour = (next_midnight - QUARTER).replace(minute=0)
    assert c._load_hourly_deltas(c._IMPORT_CACHE_KEY)[last_hour] == 4

    # Also cover stores whose completeness marker is absent but have valid data.
    c._persisted[c._IMPORT_CACHE_COMPLETE_KEY] = cache_complete
    c._persisted[c._EXPORT_CACHE_COMPLETE_KEY] = cache_complete
    writer.reset_mock()
    now += timedelta(days=1)
    second = await c._async_refresh_energy_state()
    assert second.total_import_kwh == len(slots)
    assert second.total_export_kwh == len(slots)
    for key in (c._IMPORT_CACHE_KEY, c._EXPORT_CACHE_KEY):
        cache = c._load_hourly_deltas(key)
        assert cache[last_hour] == 4
        assert sum(value for ts, value in cache.items() if start <= ts < next_midnight) == first_day_count
    assert writer.await_count == 2
    for call in writer.await_args_list:
        rows = call.kwargs["rows"]
        assert rows[0]["start"] == next_midnight
        assert rows[-1]["sum"] == len(slots)
    for profile, lower, upper in requests[-2:]:
        assert lower == next_midnight - timedelta(milliseconds=1)
        assert upper == end - timedelta(milliseconds=1)

    writer.reset_mock()
    repeated = await c._async_refresh_energy_state()
    assert repeated.total_import_kwh == second.total_import_kwh
    assert repeated.total_export_kwh == second.total_export_kwh
    writer.assert_not_awaited()
    assert len(requests) == 6


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", ["ICQ2", "ICC1", "ISQ2", "ISC1"])
async def test_ab_chunks_include_every_boundary_slot_once(profile):
    """Exclusive API bounds must not drop or duplicate slots between chunks."""
    start = datetime(2026, 9, 1, tzinfo=UTC)
    end = start + timedelta(days=32) - QUARTER
    slots = [start + i * QUARTER for i in range(32 * 96)]
    requests = []

    def get(url, *, params, **kwargs):
        lower = datetime.fromisoformat(params["from"].replace("Z", "+00:00"))
        upper = datetime.fromisoformat(params["to"].replace("Z", "+00:00"))
        requests.append((lower, upper))
        batch = [
            {"timestamp": ts.isoformat(), "value": 1, "status": "W"}
            for ts in slots if lower < ts < upper
        ]
        response = SimpleNamespace(status=200, json=AsyncMock(return_value={
            "profile": profile, "units": PROFILE_UNITS[profile],
            "total": len(batch), "data": batch,
        }))
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=response)
        context.__aexit__ = AsyncMock(return_value=False)
        return context

    client = EgdApiClient(SimpleNamespace(get=get), "test", "test")
    client._access_token = "test"
    records = await client.async_get_profile_data(
        ean="test", profile=profile, from_dt=start, to_dt=end
    )
    assert [record.timestamp for record in records] == slots
    assert len(requests) == 2
    assert requests[0][1] == requests[1][0]
