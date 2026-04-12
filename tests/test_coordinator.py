"""Unit tests for EG.D coordinator logic."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("homeassistant")

from custom_components.ha_egd_openapi.api import IntervalRecord
from custom_components.ha_egd_openapi.const import ATTR_LAST_ERROR, ATTR_SYNC_STATUS
from custom_components.ha_egd_openapi.coordinator import EgdDataUpdateCoordinator


def _build_coordinator() -> EgdDataUpdateCoordinator:
    """Create an uninitialized coordinator instance for pure method tests."""
    coordinator = EgdDataUpdateCoordinator.__new__(EgdDataUpdateCoordinator)
    coordinator._persisted = {}  # noqa: SLF001
    return coordinator


def test_process_records_hourly_filters_invalid_statuses() -> None:
    """Only valid EG.D statuses should contribute to statistics."""
    coordinator = _build_coordinator()
    records = [
        IntervalRecord(
            timestamp=datetime(2026, 4, 10, 0, 0, tzinfo=timezone.utc),
            value=1.0,
            status="IU012",
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
    """Diagnostic waiting state should reflect missing import data for the latest day."""
    latest_available = datetime(2026, 4, 10, 23, 45, tzinfo=timezone.utc)

    assert EgdDataUpdateCoordinator._is_waiting_for_latest_data(  # noqa: SLF001
        latest_available_utc=latest_available,
        last_valid_import_ts=None,
    )
    assert EgdDataUpdateCoordinator._is_waiting_for_latest_data(  # noqa: SLF001
        latest_available_utc=latest_available,
        last_valid_import_ts=datetime(2026, 4, 9, 23, 45, tzinfo=timezone.utc),
    )
    assert not EgdDataUpdateCoordinator._is_waiting_for_latest_data(  # noqa: SLF001
        latest_available_utc=latest_available,
        last_valid_import_ts=datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc),
    )


def test_store_error_state_updates_diagnostic_persisted_values() -> None:
    """A failed refresh should leave a clear diagnostic footprint in persisted state."""
    coordinator = _build_coordinator()

    coordinator._store_error_state("Boom")  # noqa: SLF001

    assert coordinator._persisted[ATTR_SYNC_STATUS] == "error"  # noqa: SLF001
    assert coordinator._persisted[ATTR_LAST_ERROR] == "Boom"  # noqa: SLF001
    assert coordinator._persisted["last_api_sync_utc"].endswith("Z")  # noqa: SLF001
