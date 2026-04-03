"""Coordinator for EG.D OpenAPI."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
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
    ATTR_LAST_EXPORT_STATUS,
    ATTR_LAST_IMPORT_STATUS,
    ATTR_LAST_UPDATE_UTC,
    ATTR_LAST_VALID_EXPORT_TS,
    ATTR_LAST_VALID_IMPORT_TS,
    CONF_EAN,
    CONF_EXPORT_PROFILE,
    CONF_IMPORT_PROFILE,
    DOMAIN,
    STORE_KEY,
    STORE_VERSION,
)
from .statistics import async_import_external_statistics

_LOGGER = logging.getLogger(__name__)

QUARTER_HOUR = timedelta(minutes=15)

PROFILE_MIN_DATES: dict[str, date] = {
    "ICQ2": date(2024, 7, 1),
    "ISQ2": date(2024, 7, 1),
}

# Chování sladěné s EG.D webem
ALLOWED_STATUSES = {"IU012", "IU020", "IU022"}


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


class EgdDataUpdateCoordinator(DataUpdateCoordinator[EnergyState]):
    """Coordinates fetching, statistics import and cumulative totals."""

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

    async def _async_update_data(self) -> EnergyState:
        """Fetch data from API and update cumulative totals."""
        try:
            return await self._async_refresh_energy_state()
        except EgdAuthError as err:
            raise ConfigEntryAuthFailed(str(err)) from err
        except EgdApiError as err:
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

        import_last = self._parse_dt(self._persisted.get(ATTR_LAST_VALID_IMPORT_TS))
        export_last = self._parse_dt(self._persisted.get(ATTR_LAST_VALID_EXPORT_TS))

        total_import = float(self._persisted.get("total_import_kwh", 0.0))
        total_export = float(self._persisted.get("total_export_kwh", 0.0))

        import_from = await self._determine_start_timestamp(
            profile=import_profile,
            last_valid=import_last,
            latest_available_utc=latest_available_utc,
            persisted_key="earliest_import_utc",
        )
        export_from = await self._determine_start_timestamp(
            profile=export_profile,
            last_valid=export_last,
            latest_available_utc=latest_available_utc,
            persisted_key="earliest_export_utc",
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

        total_import, import_meta, import_stats = self._process_records_hourly(
            current_total=total_import,
            records=import_records,
            profile=import_profile,
        )
        total_export, export_meta, export_stats = self._process_records_hourly(
            current_total=total_export,
            records=export_records,
            profile=export_profile,
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
            last_api_sync_utc=self._iso(now_utc),
            last_update_utc=self._iso(now_utc),
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
            }
        )

        await self._store.async_save(self._persisted)
        return state

    def _process_records_hourly(
        self,
        *,
        current_total: float,
        records: list[IntervalRecord],
        profile: str,
    ) -> tuple[float, dict[str, Any], list[dict[str, Any]]]:
        """Aggregate quarter-hour records to hourly cumulative rows."""
        total = current_total
        last_status: str | None = None
        newest_valid_ts: datetime | None = None
        rows: list[dict[str, Any]] = []

        hourly: dict[datetime, float] = defaultdict(float)

        for record in records:
            last_status = record.status

            if record.status not in ALLOWED_STATUSES:
                continue

            value_kwh = self._record_to_kwh(record.value, profile)
            hour_start = record.timestamp.replace(minute=0, second=0, microsecond=0)
            hourly[hour_start] += value_kwh
            newest_valid_ts = record.timestamp

        for hour_start in sorted(hourly):
            total += hourly[hour_start]
            total_rounded = round(total, 6)
            rows.append(
                {
                    "start": hour_start,
                    "state": total_rounded,
                    "sum": total_rounded,
                }
            )

        return total, {"last_valid_ts": newest_valid_ts, "last_status": last_status}, rows

    async def _determine_start_timestamp(
        self,
        *,
        profile: str,
        last_valid: datetime | None,
        latest_available_utc: datetime,
        persisted_key: str,
    ) -> datetime | None:
        """Determine where next fetch should start.

        Behavior:
        - if we already know the last valid imported quarter-hour, continue from +15 min
        - if not, use stored earliest timestamp if present
        - otherwise start from profile hard minimum
        """
        hard_min = self._hard_min_for_profile(profile, latest_available_utc)

        if last_valid is not None:
            next_start = last_valid + QUARTER_HOUR
            return max(next_start, hard_min)

        persisted = self._parse_dt(self._persisted.get(persisted_key))
        if persisted is not None:
            return max(persisted, hard_min)

        return hard_min

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
