"""EG.D OpenAPI integration."""

from __future__ import annotations

from datetime import datetime, timedelta
import logging

import voluptuous as vol

from homeassistant.components.recorder import get_instance
from homeassistant.helpers import config_validation as cv
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall, callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import aiohttp_client, event
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from .api import EgdApiClient, EgdApiError, EgdAuthError
from .const import (
    CONF_CLIENT_ID,
    CONF_CLIENT_SECRET,
    CONF_EAN,
    CONF_UPDATE_HOUR,
    CONF_UPDATE_MINUTE,
    DOMAIN,
    PLATFORMS,
    STORE_KEY,
    STORE_VERSION,
)
from .coordinator import EgdDataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)

SERVICE_REMOVE_STATISTICS = "egd_remove_statistics_entity"
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

SERVICE_SCHEMA_REMOVE_STATISTICS = vol.Schema(
    {
        vol.Optional("entry_id"): str,
        vol.Optional("ean"): str,
    }
)


def _build_statistic_ids_for_ean(ean: str) -> list[str]:
    """Build statistic IDs for a single EAN."""
    clean_ean = str(ean).strip()
    return [
        f"{DOMAIN}:meter_{clean_ean}_import",
        f"{DOMAIN}:meter_{clean_ean}_export",
    ]


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up EG.D integration domain."""
    hass.data.setdefault(DOMAIN, {})

    async def _handle_remove_statistics(call: ServiceCall) -> None:
        """Remove external statistics and matching integration store."""
        entry_id: str | None = call.data.get("entry_id")
        ean: str | None = call.data.get("ean")

        statistic_ids: list[str] = []
        matched_entries: list[ConfigEntry] = []

        for entry in hass.config_entries.async_entries(DOMAIN):
            entry_ean = str(entry.data.get(CONF_EAN, "")).strip()
            if not entry_ean:
                continue

            if entry_id and entry.entry_id != entry_id:
                continue
            if ean and entry_ean != str(ean).strip():
                continue

            matched_entries.append(entry)
            statistic_ids.extend(_build_statistic_ids_for_ean(entry_ean))

        # Allow cleanup by explicit EAN even if no config entry currently exists
        if not statistic_ids and ean:
            statistic_ids.extend(_build_statistic_ids_for_ean(str(ean).strip()))

        if not statistic_ids:
            _LOGGER.warning(
                "No EG.D statistics matched for removal (entry_id=%s, ean=%s)",
                entry_id,
                ean,
            )
            return

        _LOGGER.warning("Removing EG.D statistics: %s", statistic_ids)
        get_instance(hass).async_clear_statistics(statistic_ids)

        # Remove persisted checkpoints so next refresh rebuilds history.
        for entry in matched_entries:
            store = Store[dict](hass, STORE_VERSION, f"{STORE_KEY}_{entry.entry_id}")
            await store.async_remove()
            _LOGGER.warning(
                "Removed EG.D store for entry_id=%s ean=%s",
                entry.entry_id,
                entry.data.get(CONF_EAN),
            )

            # Also clear coordinator in-memory cache if currently loaded.
            coordinator = hass.data.get(DOMAIN, {}).get(entry.entry_id)
            if coordinator is not None:
                coordinator._persisted = {}  # noqa: SLF001

    if not hass.services.has_service(DOMAIN, SERVICE_REMOVE_STATISTICS):
        hass.services.async_register(
            DOMAIN,
            SERVICE_REMOVE_STATISTICS,
            _handle_remove_statistics,
            schema=SERVICE_SCHEMA_REMOVE_STATISTICS,
        )

    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up EG.D from a config entry."""
    session = aiohttp_client.async_get_clientsession(hass)
    client = EgdApiClient(
        session=session,
        client_id=entry.data[CONF_CLIENT_ID],
        client_secret=entry.data[CONF_CLIENT_SECRET],
    )
    coordinator = EgdDataUpdateCoordinator(hass, entry, client)
    await coordinator.async_load()

    if coordinator.should_refresh_on_startup():
        try:
            await coordinator.async_config_entry_first_refresh()
        except EgdAuthError as err:
            raise ConfigEntryNotReady(f"Authentication not ready yet: {err}") from err
        except EgdApiError as err:
            raise ConfigEntryNotReady(str(err)) from err

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    async def _handle_time(now: datetime) -> None:
        _LOGGER.debug("Scheduled EG.D refresh at %s", now)
        await coordinator.async_request_refresh()

    async def _handle_watchdog(now: datetime) -> None:
        """Recover from missed refreshes or delayed EG.D publication."""
        scheduled_hour = entry.options.get(CONF_UPDATE_HOUR, entry.data[CONF_UPDATE_HOUR])
        scheduled_minute = entry.options.get(CONF_UPDATE_MINUTE, entry.data[CONF_UPDATE_MINUTE])
        now_local = dt_util.as_local(now)

        # Do not retry before the user-configured daily sync time.
        if (now_local.hour, now_local.minute) < (scheduled_hour, scheduled_minute):
            return

        if not coordinator.should_retry_refresh():
            return

        _LOGGER.debug(
            "EG.D watchdog requesting catch-up refresh at %s because latest import data are still missing",
            now_local,
        )
        await coordinator.async_request_refresh()

    @callback
    def _schedule_daily_refresh() -> None:
        daily_unsub = event.async_track_time_change(
            hass,
            _handle_time,
            hour=entry.options.get(CONF_UPDATE_HOUR, entry.data[CONF_UPDATE_HOUR]),
            minute=entry.options.get(CONF_UPDATE_MINUTE, entry.data[CONF_UPDATE_MINUTE]),
            second=0,
        )
        watchdog_unsub = event.async_track_time_interval(
            hass,
            _handle_watchdog,
            timedelta(hours=1),
        )
        entry.async_on_unload(daily_unsub)
        entry.async_on_unload(watchdog_unsub)

    _schedule_daily_refresh()

    async def _reload_entry(hass_: HomeAssistant, updated_entry: ConfigEntry) -> None:
        await hass_.config_entries.async_reload(updated_entry.entry_id)

    entry.async_on_unload(entry.add_update_listener(_reload_entry))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return unload_ok
