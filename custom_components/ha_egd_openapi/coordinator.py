"""Coordinator for EG.D OpenAPI."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .api import EgdApiClient, EgdApiError, EgdAuthError, IntervalRecord
from .const import (
    ATTR_LAST_API_SYNC_UTC,
    ATTR_LAST_ERROR,
    ATTR_LAST_EXPORT_STATUS,
    ATTR_LAST_IMPORT_STATUS,
    ATTR_SYNC_STATUS,
    ATTR_LAST_UPDATE_UTC,
    ATTR_LAST_VALID_EXPORT_TS,
    ATTR_LAST_VALID_IMPORT_TS,
    CONF_EAN,
    CONF_EXPORT_PROFILE,
    CONF_IMPORT_PROFILE,
    CONF_REVALIDATE_DAYS,
    DOMAIN,
    DEFAULT_REVALIDATE_DAYS,
    STORE_KEY,
    STORE_VERSION,
)
from .statistics import async_import_external_statistics

_LOGGER = logging.getLogger(__name__)

PROFILE_MIN_DATES: dict[str, date] = {
    "ICQ2": date(2024, 7, 1),
    "ISQ2": date(2024, 7, 1),
}

# Návod EG.D doporučuje zapisovat jen standardně platné A/B hodnoty.
ALLOWED_STATUSES = {"IU012"}


def _three_years_ago_safe(dt: datetime) -> datetime:
    """Return a conservative timestamp within EG.D rolling 3-year limit."""
    try:
        capped = dt.replace(year=dt.year - 3)
    except ValueError:
        capped = dt.replace(month=2, day=28, year=dt.year - 3)
    return capped + timedelta(days=1)


@dataclass(slots=True)
class EnergyState:
    """Runtime state exposed to entities."""

    total_import_kwh: float
    total_export_kwh: float
    last_valid_import_timestamp: str | None
    last_valid_export_timestamp: str | None
    last_import_status: str | None
    last_export_status: str | None
    last_api_sync_utc: str | None
    last_update_utc: str | None
    sync_status: str
    last_error: str | None


class EgdDataUpdateCoordinator(DataUpdateCoordinator[EnergyState]):
    """Coordinates fetching, statistics import and cumulative totals."""

    _IMPORT_CACHE_KEY = "import_hourly_deltas"
    _EXPORT_CACHE_KEY = "export_hourly_deltas"
    _IMPORT_CACHE_COMPLETE_KEY = "import_hourly_deltas_complete"
    _EXPORT_CACHE_COMPLETE_KEY = "export_hourly_deltas_complete"

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, client: EgdApiClient) -> None:
        self.hass = hass
        self.config_entry = entry
        self.client = client
        self._store = Store(hass, STORE_VERSION, f"{STORE_KEY}_{entry.entry_id}")
        self._persisted: dict[str, Any] = {}

        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(hours=24),
        )

    async def async_load(self) -> None:
        """Load persisted state."""
        self._persisted = await self._store.async_load() or {}

    def should_retry_refresh(self) -> bool:
        """Return whether latest expected import data are still missing.

        This is used by a lightweight watchdog to recover from missed daily
        schedule callbacks or from cases where EG.D publishes the previous day
        later than the configured refresh time.
        """
        latest_available_day = self._get_latest_available_utc().date()
        last_valid_import = self._parse_dt(self._persisted.get(ATTR_LAST_VALID_IMPORT_TS))
        return last_valid_import is None or last_valid_import.date() < latest_available_day

    async def _async_update_data(self) -> EnergyState:
        """Fetch data from API and update cumulative totals."""
        try:
            return await self._async_refresh_energy_state()
        except EgdAuthError as err:
            self._store_error_state(str(err))
            await self._store.async_save(self._persisted)
            raise ConfigEntryAuthFailed(str(err)) from err
        except EgdApiError as err:
            self._store_error_state(str(err))
            await self._store.async_save(self._persisted)
            if self.data is not None:
                _LOGGER.warning("EG.D refresh failed, keeping last known data: %s", err)
                return replace(
                    self.data,
                    last_api_sync_utc=self._persisted.get(ATTR_LAST_API_SYNC_UTC),
                    sync_status="error",
                    last_error=str(err),
                )
            raise UpdateFailed(str(err)) from err

    async def _async_refresh_energy_state(self) -> EnergyState:
        """Refresh import/export state from EG.D."""
        ean = str(self.config_entry.data[CONF_EAN]).strip()

        import_profile = self.config_entry.options.get(
            CONF_IMPORT_PROFILE,
            self.config_entry.data[CONF_IMPORT_PROFILE],
        )
        export_profile = self.config_entry.options.get(
            CONF_EXPORT_PROFILE,
            self.config_entry.data[CONF_EXPORT_PROFILE],
        )

        now_utc = dt_util.utcnow().astimezone(timezone.utc)
        latest_available_utc = self._get_latest_available_utc()

        import_statistic_id = f"{DOMAIN}:meter_{ean}_import"
        export_statistic_id = f"{DOMAIN}:meter_{ean}_export"

        import_from = await self._determine_start_timestamp(
            profile=import_profile,
            latest_available_utc=latest_available_utc,
            cache_complete_key=self._IMPORT_CACHE_COMPLETE_KEY,
            last_valid_key=ATTR_LAST_VALID_IMPORT_TS,
        )
        export_from = await self._determine_start_timestamp(
            profile=export_profile,
            latest_available_utc=latest_available_utc,
            cache_complete_key=self._EXPORT_CACHE_COMPLETE_KEY,
            last_valid_key=ATTR_LAST_VALID_EXPORT_TS,
        )

        import_records: list[IntervalRecord] = []
        export_records: list[IntervalRecord] = []

        if import_from and import_from <= latest_available_utc:
            import_records = await self.client.async_get_profile_data(
                ean=ean,
                profile=import_profile,
                from_dt=import_from,
                to_dt=latest_available_utc,
            )

        if export_from and export_from <= latest_available_utc:
            export_records = await self.client.async_get_profile_data(
                ean=ean,
                profile=export_profile,
                from_dt=export_from,
                to_dt=latest_available_utc,
            )

        import_hourly, import_meta = self._process_records_hourly(
            records=import_records,
            profile=import_profile,
        )
        export_hourly, export_meta = self._process_records_hourly(
            records=export_records,
            profile=export_profile,
        )

        total_import, import_stats = self._merge_statistics(
            cache_key=self._IMPORT_CACHE_KEY,
            cache_complete_key=self._IMPORT_CACHE_COMPLETE_KEY,
            persisted_total_key="total_import_kwh",
            fetched_from=import_from,
            latest_available_utc=latest_available_utc,
            profile=import_profile,
            hourly_deltas=import_hourly,
        )
        total_export, export_stats = self._merge_statistics(
            cache_key=self._EXPORT_CACHE_KEY,
            cache_complete_key=self._EXPORT_CACHE_COMPLETE_KEY,
            persisted_total_key="total_export_kwh",
            fetched_from=export_from,
            latest_available_utc=latest_available_utc,
            profile=export_profile,
            hourly_deltas=export_hourly,
        )

        if import_stats:
            await async_import_external_statistics(
                self.hass,
                statistic_id=import_statistic_id,
                name=f"{self.config_entry.title} Odběr",
                source=DOMAIN,
                rows=import_stats,
            )
            _LOGGER.debug(
                "Imported %s hourly import rows for EAN %s starting from %s",
                len(import_stats),
                ean,
                import_from.isoformat() if import_from else None,
            )
        else:
            _LOGGER.debug(
                "No new import statistics for EAN %s (from=%s to=%s, last_valid=%s)",
                ean,
                import_from.isoformat() if import_from else None,
                latest_available_utc.isoformat(),
                self._persisted.get(ATTR_LAST_VALID_IMPORT_TS),
            )

        if export_stats:
            await async_import_external_statistics(
                self.hass,
                statistic_id=export_statistic_id,
                name=f"{self.config_entry.title} Dodávka",
                source=DOMAIN,
                rows=export_stats,
            )
            _LOGGER.debug(
                "Imported %s hourly export rows for EAN %s starting from %s",
                len(export_stats),
                ean,
                export_from.isoformat() if export_from else None,
            )
        else:
            _LOGGER.debug(
                "No new export statistics for EAN %s (from=%s to=%s, last_valid=%s)",
                ean,
                export_from.isoformat() if export_from else None,
                latest_available_utc.isoformat(),
                self._persisted.get(ATTR_LAST_VALID_EXPORT_TS),
            )

        # Důležité:
        # last_valid_* se posouvá jen pokud opravdu přišla nová validní data.
        sync_status = (
            "waiting_for_data"
            if self._is_waiting_for_latest_data(
                latest_available_utc=latest_available_utc,
                last_valid_import_ts=import_meta["last_valid_ts"],
            )
            else "ok"
        )
        should_advance_successful_sync = sync_status == "ok" and (
            bool(import_stats)
            or bool(export_stats)
            or self._did_timestamp_advance(
                current=import_meta["last_valid_ts"],
                previous=self._parse_dt(self._persisted.get(ATTR_LAST_VALID_IMPORT_TS)),
            )
            or self._did_timestamp_advance(
                current=export_meta["last_valid_ts"],
                previous=self._parse_dt(self._persisted.get(ATTR_LAST_VALID_EXPORT_TS)),
            )
        )
        last_successful_sync_utc = (
            self._iso(now_utc)
            if should_advance_successful_sync
            else self._persisted.get(ATTR_LAST_API_SYNC_UTC)
        )
        state = EnergyState(
            total_import_kwh=round(total_import, 3),
            total_export_kwh=round(total_export, 3),
            last_valid_import_timestamp=(
                self._iso(import_meta["last_valid_ts"])
                or self._persisted.get(ATTR_LAST_VALID_IMPORT_TS)
            ),
            last_valid_export_timestamp=(
                self._iso(export_meta["last_valid_ts"])
                or self._persisted.get(ATTR_LAST_VALID_EXPORT_TS)
            ),
            last_import_status=import_meta["last_status"] or self._persisted.get(ATTR_LAST_IMPORT_STATUS),
            last_export_status=export_meta["last_status"] or self._persisted.get(ATTR_LAST_EXPORT_STATUS),
            last_api_sync_utc=last_successful_sync_utc,
            last_update_utc=self._iso(now_utc),
            sync_status=sync_status,
            last_error=None,
        )

        self._persisted.update(
            {
                "total_import_kwh": state.total_import_kwh,
                "total_export_kwh": state.total_export_kwh,
                ATTR_LAST_VALID_IMPORT_TS: state.last_valid_import_timestamp,
                ATTR_LAST_VALID_EXPORT_TS: state.last_valid_export_timestamp,
                ATTR_LAST_IMPORT_STATUS: state.last_import_status,
                ATTR_LAST_EXPORT_STATUS: state.last_export_status,
                ATTR_LAST_API_SYNC_UTC: state.last_api_sync_utc,
                ATTR_LAST_UPDATE_UTC: state.last_update_utc,
                ATTR_SYNC_STATUS: state.sync_status,
                ATTR_LAST_ERROR: state.last_error,
            }
        )

        await self._store.async_save(self._persisted)
        return state

    def _process_records_hourly(
        self,
        *,
        records: list[IntervalRecord],
        profile: str,
    ) -> tuple[dict[datetime, float], dict[str, Any]]:
        """Aggregate quarter-hour records to hourly delta rows."""
        last_status: str | None = None
        newest_valid_ts: datetime | None = None

        hourly: dict[datetime, float] = defaultdict(float)

        for record in records:
            last_status = record.status

            if record.status not in ALLOWED_STATUSES:
                continue

            value_kwh = self._record_to_kwh(record.value, profile)
            hour_start = record.timestamp.replace(minute=0, second=0, microsecond=0)
            hourly[hour_start] += value_kwh
            newest_valid_ts = record.timestamp

        return (
            {hour_start: round(value, 6) for hour_start, value in hourly.items()},
            {"last_valid_ts": newest_valid_ts, "last_status": last_status},
        )

    async def _determine_start_timestamp(
        self,
        *,
        profile: str,
        latest_available_utc: datetime,
        cache_complete_key: str,
        last_valid_key: str,
    ) -> datetime | None:
        """Determine where next fetch should start.

        Behavior:
        - first sync (or migration without local hourly cache): fetch full history
        - subsequent daily runs: revalidate a rolling window from the configured day offset
        """
        hard_min = self._hard_min_for_profile(profile, latest_available_utc)
        cache_complete = bool(self._persisted.get(cache_complete_key))
        if not cache_complete:
            if self._parse_dt(self._persisted.get(last_valid_key)) is None:
                return hard_min
            return max(self._get_revalidation_start(latest_available_utc), hard_min)

        revalidation_start = self._get_revalidation_start(latest_available_utc)
        return max(revalidation_start, hard_min)

    def _merge_statistics(
        self,
        *,
        cache_key: str,
        cache_complete_key: str,
        persisted_total_key: str,
        fetched_from: datetime | None,
        latest_available_utc: datetime,
        profile: str,
        hourly_deltas: dict[datetime, float],
    ) -> tuple[float, list[dict[str, Any]]]:
        """Merge fetched hourly deltas into local cache and build changed rows."""
        existing = self._load_hourly_deltas(cache_key)
        merged = dict(existing)
        merged.update(hourly_deltas)

        latest_hour = latest_available_utc.replace(minute=0, second=0, microsecond=0)
        window_start = (
            fetched_from or self._hard_min_for_profile(profile, latest_available_utc)
        ).replace(minute=0, second=0, microsecond=0)

        old_sums = self._build_cumulative_sum_map(existing, window_start, latest_hour)
        new_sums = self._build_cumulative_sum_map(merged, window_start, latest_hour)

        rows = [
            {
                "start": hour_start,
                "state": sum_value,
                "sum": sum_value,
            }
            for hour_start, sum_value in new_sums.items()
            if not self._numbers_equal(old_sums.get(hour_start), sum_value)
        ]

        self._persisted[cache_key] = self._serialize_hourly_deltas(merged)

        hard_min = self._hard_min_for_profile(profile, latest_available_utc)
        cache_complete = bool(self._persisted.get(cache_complete_key))
        if fetched_from is not None and fetched_from <= hard_min:
            self._persisted[cache_complete_key] = True
            cache_complete = True

        if cache_complete:
            total = round(sum(merged.values()), 6)
        else:
            total = float(self._persisted.get(persisted_total_key, 0.0))
        return total, rows

    def _store_error_state(self, error_message: str) -> None:
        """Persist the last refresh error for diagnostic entities."""
        now_utc = self._iso(dt_util.utcnow().astimezone(timezone.utc))
        self._persisted[ATTR_LAST_ERROR] = error_message
        self._persisted[ATTR_SYNC_STATUS] = "error"
        self._persisted[ATTR_LAST_API_SYNC_UTC] = now_utc

    @staticmethod
    def _is_waiting_for_latest_data(
        *,
        latest_available_utc: datetime,
        last_valid_import_ts: datetime | None,
    ) -> bool:
        """Return whether the latest expected import day is still missing."""
        return (
            last_valid_import_ts is None
            or last_valid_import_ts.date() < latest_available_utc.date()
        )

    @staticmethod
    def _did_timestamp_advance(
        *,
        current: datetime | None,
        previous: datetime | None,
    ) -> bool:
        """Return whether a valid data timestamp moved forward."""
        if current is None:
            return False
        if previous is None:
            return True
        return current > previous

    def _get_revalidation_start(self, latest_available_utc: datetime) -> datetime:
        """Return start of rolling revalidation window in UTC."""
        revalidate_days = max(
            1,
            int(
                self.config_entry.options.get(
                    CONF_REVALIDATE_DAYS,
                    self.config_entry.data.get(CONF_REVALIDATE_DAYS, DEFAULT_REVALIDATE_DAYS),
                )
            ),
        )
        start_date = latest_available_utc.date() - timedelta(days=revalidate_days - 1)
        return datetime.combine(start_date, time(0, 0), tzinfo=timezone.utc)

    def _load_hourly_deltas(self, cache_key: str) -> dict[datetime, float]:
        """Load cached hourly deltas from persistent storage."""
        raw = self._persisted.get(cache_key, {})
        if not isinstance(raw, dict):
            return {}

        deltas: dict[datetime, float] = {}
        for timestamp, value in raw.items():
            parsed = self._parse_dt(timestamp)
            if parsed is None:
                continue
            deltas[parsed] = round(float(value), 6)
        return deltas

    def _serialize_hourly_deltas(self, deltas: dict[datetime, float]) -> dict[str, float]:
        """Serialize cached hourly deltas for Home Assistant storage."""
        return {
            self._iso(timestamp): round(value, 6)
            for timestamp, value in sorted(deltas.items())
            if self._iso(timestamp) is not None
        }

    def _build_cumulative_sum_map(
        self,
        deltas: dict[datetime, float],
        window_start: datetime,
        latest_hour: datetime,
    ) -> dict[datetime, float]:
        """Build cumulative sums for a specific hourly window."""
        cumulative = round(
            sum(value for timestamp, value in deltas.items() if timestamp < window_start),
            6,
        )
        sums: dict[datetime, float] = {}

        for hour_start in sorted(
            timestamp
            for timestamp in deltas
            if window_start <= timestamp <= latest_hour
        ):
            cumulative = round(cumulative + deltas[hour_start], 6)
            sums[hour_start] = cumulative

        return sums

    @staticmethod
    def _numbers_equal(left: float | None, right: float | None, tolerance: float = 1e-6) -> bool:
        """Compare floats safely for recorder reimports."""
        if left is None or right is None:
            return left is right
        return abs(left - right) <= tolerance

    def _hard_min_for_profile(self, profile: str, latest: datetime) -> datetime:
        """Return earliest allowed start for a profile."""
        hard_min = _three_years_ago_safe(latest)

        profile_min = PROFILE_MIN_DATES.get(profile)
        if profile_min is not None:
            profile_min_dt = datetime.combine(profile_min, time(0, 0), tzinfo=timezone.utc)
            hard_min = max(hard_min, profile_min_dt)

        return min(hard_min, latest)

    def _get_latest_available_utc(self) -> datetime:
        """Return latest allowed EG.D quarter-hour timestamp.

        EG.D allows querying only up to yesterday and the last slot is 23:45.
        """
        now_local = dt_util.now()
        latest_local = datetime.combine(
            now_local.date() - timedelta(days=1),
            time(23, 45),
            tzinfo=now_local.tzinfo,
        )
        return latest_local.astimezone(timezone.utc)

    @staticmethod
    def _record_to_kwh(value: float, profile: str) -> float:
        """Convert API value to kWh."""
        if profile in {"ICC1", "ISC1"}:
            return value / 4
        return value

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        """Parse stored ISO datetime."""
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    @staticmethod
    def _iso(value: datetime | None) -> str | None:
        """Format datetime as UTC ISO string."""
        if value is None:
            return None
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
