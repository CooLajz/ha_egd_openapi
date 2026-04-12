"""Diagnostics support for EG.D OpenAPI."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import CONF_CLIENT_ID, CONF_CLIENT_SECRET, CONF_EAN, DOMAIN
from .coordinator import EgdDataUpdateCoordinator

TO_REDACT = {
    CONF_CLIENT_ID,
    CONF_CLIENT_SECRET,
    CONF_EAN,
    "access_token",
    "Authorization",
}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: ConfigEntry,
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator: EgdDataUpdateCoordinator | None = hass.data.get(DOMAIN, {}).get(entry.entry_id)

    payload: dict[str, Any] = {
        "entry": {
            "entry_id": entry.entry_id,
            "title": entry.title,
            "data": dict(entry.data),
            "options": dict(entry.options),
        },
        "runtime": {
            "loaded": coordinator is not None,
            "state": asdict(coordinator.data) if coordinator and coordinator.data else None,
            "persisted": dict(coordinator._persisted) if coordinator else None,  # noqa: SLF001
            "diagnostic_events": coordinator.get_diagnostic_events() if coordinator else [],
        },
    }
    return async_redact_data(payload, TO_REDACT)
