"""Regression for successful monthly API responses that omit available slots."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

pytest.importorskip("homeassistant")

from custom_components.ha_egd_openapi import coordinator as coordinator_module
from custom_components.ha_egd_openapi.api import EgdApiClient
from custom_components.ha_egd_openapi.const import PROFILE_UNITS
from custom_components.ha_egd_openapi.coordinator import EgdDataUpdateCoordinator

UTC = timezone.utc
PRAGUE = ZoneInfo("Europe/Prague")
QUARTER = timedelta(minutes=15)
START = datetime(2026, 10, 1, tzinfo=PRAGUE).astimezone(UTC)
LAST_DAY = datetime(2026, 10, 31, tzinfo=PRAGUE).astimezone(UTC)
END = datetime(2026, 11, 1, tzinfo=PRAGUE).astimezone(UTC) - QUARTER
SLOTS = [START + index * QUARTER for index in range(int((END - START) / QUARTER) + 1)]


class _TruncatedMonthlyClient(EgdApiClient):
    """Mirror the observed failure: HTTP 200 and total match a truncated list."""

    def __init__(self):
        super().__init__(None, "test", "test")
        self.requests = []

    async def _async_request_profile_data(self, **kwargs):
        self.requests.append(kwargs)
        profile = kwargs["profile"]
        available = [ts for ts in SLOTS if kwargs["from_dt"] <= ts <= kwargs["to_dt"]]
        # A monthly response hid the last 71 quarters despite its valid total.
        available = available[:2905]
        offset = kwargs["page_start"] - 1
        return 200, {
            "profile": profile,
            "units": PROFILE_UNITS[profile],
            "total": len(available),
            "data": [
                {
                    "timestamp": ts.isoformat(),
                    "value": 4 if PROFILE_UNITS[profile] == "kW" else 1,
                    "status": "W",
                }
                for ts in available[offset : offset + kwargs["page_size"]]
            ],
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", PROFILE_UNITS)
async def test_monthly_window_recovers_all_available_intervals(profile):
    client = _TruncatedMonthlyClient()

    records = await client.async_get_profile_data(
        ean="test-ean", profile=profile, from_dt=START, to_dt=END
    )

    assert [record.timestamp for record in records] == SLOTS
    coordinator = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    hourly, _ = coordinator._process_records_hourly(records=records, profile=profile)
    assert len(hourly) == 31 * 24 + 1  # Includes the autumn DST transition.
    assert sum(value for ts, value in hourly.items() if ts >= LAST_DAY) == 96


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "profiles", [("ICQ2", "ISQ2"), ("ICC1", "ISC1"), ("DCQC", "DSQC")]
)
async def test_revalidation_repairs_sparse_cache_without_resetting_history(
    monkeypatch, profiles
):
    c = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    c.hass = SimpleNamespace()
    c.config_entry = SimpleNamespace(
        entry_id="test",
        title="Test",
        options={},
        data={
            "ean": "test-ean",
            "import_profile": profiles[0],
            "export_profile": profiles[1],
            "enable_export": True,
            "revalidate_days": 31,
            "update_hour": 8,
            "update_minute": 33,
        },
    )
    c.client = _TruncatedMonthlyClient()
    c._store = SimpleNamespace(async_save=AsyncMock())
    partial_slots = SLOTS[:2905] + SLOTS[-4:]
    cached = {}
    for ts in partial_slots:
        hour = ts.replace(minute=0)
        cached[hour] = cached.get(hour, 0) + 1
    c._persisted = {
        "import_hourly_deltas": c._serialize_hourly_deltas(cached),
        "export_hourly_deltas": c._serialize_hourly_deltas(cached),
        "import_hourly_deltas_complete": True,
        "export_hourly_deltas_complete": True,
        "total_import_kwh": len(partial_slots),
        "total_export_kwh": len(partial_slots),
    }
    assert len([ts for ts in cached if ts >= LAST_DAY]) == 7
    monkeypatch.setattr(c, "_get_latest_available_utc", lambda: END)
    monkeypatch.setattr(c, "_get_revalidation_start", lambda latest, profile: START)
    writer = AsyncMock()
    monkeypatch.setattr(coordinator_module, "async_import_external_statistics", writer)

    state = await c._async_refresh_energy_state()

    assert state.total_import_kwh == len(SLOTS)
    assert state.total_export_kwh == len(SLOTS)
    assert state.sync_status == "ok"
    for key in (c._IMPORT_CACHE_KEY, c._EXPORT_CACHE_KEY):
        repaired = c._load_hourly_deltas(key)
        assert len([ts for ts in repaired if ts >= LAST_DAY]) == 24
        assert sum(value for ts, value in repaired.items() if ts >= LAST_DAY) == 96
        assert all(
            repaired[ts] == value for ts, value in cached.items() if ts < LAST_DAY
        )
    assert writer.await_count == 2
    for call in writer.await_args_list:
        rows = call.kwargs["rows"]
        assert rows[0]["start"] == LAST_DAY + timedelta(hours=5)
        assert rows[-1]["start"] == END.replace(minute=0)
        assert rows[-1]["sum"] == len(SLOTS)

    writer.reset_mock()
    await c._async_refresh_energy_state()
    writer.assert_not_awaited()
