"""Unit tests for EG.D coordinator logic."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("homeassistant")

from custom_components.ha_egd_openapi.api import IntervalRecord
from types import SimpleNamespace

from custom_components.ha_egd_openapi.const import (
    ATTR_LAST_ERROR,
    ATTR_SYNC_STATUS,
    CONF_ENABLE_DIAGNOSTICS,
    DIAGNOSTICS_EVENTS_KEY,
    MAX_DIAGNOSTIC_EVENTS,
)
from custom_components.ha_egd_openapi.coordinator import EgdDataUpdateCoordinator


class _ProbeClient:
    """Probe test double that allows access from a configured day."""

    def __init__(self, accessible_from: datetime) -> None:
        self.accessible_from = accessible_from
        self.probes: list[tuple[datetime, datetime]] = []

    async def async_probe_access(
        self,
        *,
        ean: str,
        profile: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> bool:
        self.probes.append((from_dt, to_dt))
        return from_dt >= self.accessible_from


def _build_coordinator() -> EgdDataUpdateCoordinator:
    """Create an uninitialized coordinator instance for pure method tests."""
    coordinator = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    coordinator._persisted = {}  # noqa: SLF001
    coordinator.config_entry = SimpleNamespace(
        options={},
        data={"update_hour": 16, "update_minute": 17},
    )
    return coordinator


def test_process_records_hourly_filters_invalid_statuses() -> None:
    """Only valid EG.D statuses should contribute to statistics."""
    coordinator = _build_coordinator()
    records = [
        IntervalRecord(
            timestamp=datetime(2026, 4, 10, 0, 0, tzinfo=timezone.utc),
            value=1.0,
            status="W",
        ),
        IntervalRecord(
            timestamp=datetime(2026, 4, 10, 0, 15, tzinfo=timezone.utc),
            value=2.0,
            status="INVALID",
        ),
        IntervalRecord(
            timestamp=datetime(2026, 4, 10, 0, 30, tzinfo=timezone.utc),
            value=3.0,
            status="IU012",
        ),
    ]

    hourly, meta = coordinator._process_records_hourly(records=records, profile="ICQ2")  # noqa: SLF001

    assert hourly == {datetime(2026, 4, 10, 0, 0, tzinfo=timezone.utc): 4.0}
    assert meta["last_valid_ts"] == datetime(2026, 4, 10, 0, 30, tzinfo=timezone.utc)
    assert meta["last_status"] == "IU012"


def test_process_records_hourly_converts_quarter_hour_profiles_to_kwh() -> None:
    """ICC1/ISC1 profiles should be converted from quarter-hour power values."""
    coordinator = _build_coordinator()
    records = [
        IntervalRecord(
            timestamp=datetime(2026, 4, 10, 1, 0, tzinfo=timezone.utc),
            value=400.0,
            status="IU012",
        ),
        IntervalRecord(
            timestamp=datetime(2026, 4, 10, 1, 15, tzinfo=timezone.utc),
            value=200.0,
            status="IU012",
        ),
    ]

    hourly, _meta = coordinator._process_records_hourly(records=records, profile="ICC1")  # noqa: SLF001

    assert hourly == {datetime(2026, 4, 10, 1, 0, tzinfo=timezone.utc): 150.0}


def test_waiting_for_latest_data_detects_missing_latest_day() -> None:
    """Diagnostic waiting state should reflect missing import or export data for the latest day."""
    latest_available = datetime(2026, 4, 10, 23, 45, tzinfo=timezone.utc)

    assert EgdDataUpdateCoordinator._is_waiting_for_latest_data(  # noqa: SLF001
        latest_available_utc=latest_available,
        last_valid_import_ts=None,
        last_valid_export_ts=datetime(2026, 4, 10, 23, 45, tzinfo=timezone.utc),
    )
    assert EgdDataUpdateCoordinator._is_waiting_for_latest_data(  # noqa: SLF001
        latest_available_utc=latest_available,
        last_valid_import_ts=datetime(2026, 4, 9, 23, 45, tzinfo=timezone.utc),
        last_valid_export_ts=datetime(2026, 4, 10, 23, 45, tzinfo=timezone.utc),
    )
    assert EgdDataUpdateCoordinator._is_waiting_for_latest_data(  # noqa: SLF001
        latest_available_utc=latest_available,
        last_valid_import_ts=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
        last_valid_export_ts=datetime(2026, 4, 9, 23, 45, tzinfo=timezone.utc),
    )
    assert not EgdDataUpdateCoordinator._is_waiting_for_latest_data(  # noqa: SLF001
        latest_available_utc=latest_available,
        last_valid_import_ts=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
        last_valid_export_ts=datetime(2026, 4, 10, 18, 0, tzinfo=timezone.utc),
    )


def test_store_error_state_updates_diagnostic_persisted_values() -> None:
    """A failed refresh should leave a clear diagnostic footprint in persisted state."""
    coordinator = _build_coordinator()

    coordinator._store_error_state("Boom")  # noqa: SLF001

    assert coordinator._persisted[ATTR_SYNC_STATUS] == "error"  # noqa: SLF001
    assert coordinator._persisted[ATTR_LAST_ERROR] == "Boom"  # noqa: SLF001
    assert coordinator._persisted["last_api_sync_utc"].endswith("Z")  # noqa: SLF001


def test_did_timestamp_advance_only_when_value_moves_forward() -> None:
    """Successful sync timestamp should advance only on real data progress."""
    previous = datetime(2026, 4, 10, 23, 45, tzinfo=timezone.utc)

    assert EgdDataUpdateCoordinator._did_timestamp_advance(  # noqa: SLF001
        current=datetime(2026, 4, 11, 23, 45, tzinfo=timezone.utc),
        previous=previous,
    )
    assert not EgdDataUpdateCoordinator._did_timestamp_advance(  # noqa: SLF001
        current=previous,
        previous=previous,
    )
    assert not EgdDataUpdateCoordinator._did_timestamp_advance(  # noqa: SLF001
        current=None,
        previous=previous,
    )


def test_diagnostic_events_are_stored_only_when_enabled() -> None:
    """Structured diagnostics should respect the user toggle."""
    coordinator = _build_coordinator()

    coordinator._record_diagnostic_event("info", "disabled")  # noqa: SLF001
    assert coordinator.get_diagnostic_events() == []

    coordinator.config_entry.options[CONF_ENABLE_DIAGNOSTICS] = True
    coordinator._record_diagnostic_event("info", "enabled", {"step": "refresh"})  # noqa: SLF001

    assert coordinator.get_diagnostic_events()[0]["message"] == "enabled"
    assert coordinator.get_diagnostic_events()[0]["details"] == {"step": "refresh"}


@pytest.mark.asyncio
async def test_determine_start_timestamp_skips_unauthorized_history() -> None:
    """Initial sync should begin at the first day accepted by EG.D."""
    coordinator = _build_coordinator()
    coordinator.client = _ProbeClient(
        accessible_from=datetime(2026, 4, 5, 0, 0, tzinfo=timezone.utc)
    )

    start = await coordinator._determine_start_timestamp(  # noqa: SLF001
        ean="859182400000000000",
        profile="ICQ2",
        latest_available_utc=datetime(2026, 4, 10, 23, 45, tzinfo=timezone.utc),
        cache_complete_key="cache_complete",
        last_valid_key="last_valid",
    )

    assert start == datetime(2026, 4, 5, 0, 0, tzinfo=timezone.utc)
    assert coordinator.client.probes[0] == (  # type: ignore[attr-defined]
        datetime(2024, 7, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2024, 7, 1, 23, 45, tzinfo=timezone.utc),
    )
    assert all(
        probe_to - probe_from <= timedelta(days=1)
        for probe_from, probe_to in coordinator.client.probes  # type: ignore[attr-defined]
    )


def test_diagnostic_events_buffer_is_bounded() -> None:
    """Diagnostics buffer should keep only the most recent events."""
    coordinator = _build_coordinator()
    coordinator.config_entry.options[CONF_ENABLE_DIAGNOSTICS] = True

    for idx in range(MAX_DIAGNOSTIC_EVENTS + 5):
        coordinator._record_diagnostic_event("debug", f"event-{idx}")  # noqa: SLF001

    events = coordinator._persisted[DIAGNOSTICS_EVENTS_KEY]  # noqa: SLF001
    assert len(events) == MAX_DIAGNOSTIC_EVENTS
    assert events[0]["message"] == "event-5"
    assert events[-1]["message"] == f"event-{MAX_DIAGNOSTIC_EVENTS + 4}"


def test_next_sync_attempt_prefers_watchdog_when_waiting_for_data() -> None:
    """Waiting for data should expose the next hourly retry, not tomorrow's daily run."""
    coordinator = _build_coordinator()
    now = datetime(2026, 4, 12, 18, 0, tzinfo=timezone.utc)

    next_attempt, reason = coordinator._get_next_sync_attempt(  # noqa: SLF001
        now_utc=now,
        sync_status="waiting_for_data",
    )

    assert next_attempt == datetime(2026, 4, 12, 19, 0, tzinfo=timezone.utc)
    assert reason == "watchdog_retry"
