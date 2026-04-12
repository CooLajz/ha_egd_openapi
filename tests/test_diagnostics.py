"""Unit tests for EG.D diagnostics export."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

pytest.importorskip("homeassistant")

from custom_components.ha_egd_openapi.const import (
    CONF_CLIENT_ID,
    CONF_CLIENT_SECRET,
    CONF_EAN,
    CONF_ENABLE_DIAGNOSTICS,
    DIAGNOSTICS_EVENTS_KEY,
    DOMAIN,
)
from custom_components.ha_egd_openapi.diagnostics import async_get_config_entry_diagnostics
from custom_components.ha_egd_openapi.coordinator import EnergyState


def _build_state() -> EnergyState:
    return EnergyState(
        total_import_kwh=123.456,
        total_export_kwh=78.9,
        last_valid_import_timestamp="2026-04-11T23:45:00Z",
        last_valid_export_timestamp="2026-04-11T23:45:00Z",
        last_import_status="IU012",
        last_export_status="IU012",
        last_api_sync_utc="2026-04-12T06:00:00Z",
        last_update_utc="2026-04-12T06:00:00Z",
        sync_status="ok",
        last_error=None,
        last_check_started_utc="2026-04-12T05:59:58Z",
        last_check_finished_utc="2026-04-12T06:00:00Z",
        next_sync_attempt_utc="2026-04-12T16:17:00Z",
        next_sync_reason="scheduled_daily",
    )


@pytest.mark.asyncio
async def test_config_entry_diagnostics_redacts_secrets() -> None:
    """Downloaded diagnostics should not expose credentials or the full EAN."""
    entry = SimpleNamespace(
        entry_id="entry-1",
        title="My EG.D",
        data={
            CONF_CLIENT_ID: "client-id",
            CONF_CLIENT_SECRET: "secret",
            CONF_EAN: "859182400000000000",
        },
        options={CONF_ENABLE_DIAGNOSTICS: True},
    )
    coordinator = SimpleNamespace(
        data=replace(_build_state(), last_error="none"),
        _persisted={
            DIAGNOSTICS_EVENTS_KEY: [
                {
                    "ts": "2026-04-12T06:00:00Z",
                    "level": "info",
                    "message": "refresh_completed",
                    "details": {"ean_suffix": "0000"},
                }
            ]
        },
        get_diagnostic_events=lambda: [
            {
                "ts": "2026-04-12T06:00:00Z",
                "level": "info",
                "message": "refresh_completed",
                "details": {"ean_suffix": "0000"},
            }
        ],
    )
    hass = SimpleNamespace(data={DOMAIN: {entry.entry_id: coordinator}})

    diagnostics = await async_get_config_entry_diagnostics(hass, entry)

    assert diagnostics["entry"]["data"][CONF_CLIENT_ID] == "**REDACTED**"
    assert diagnostics["entry"]["data"][CONF_CLIENT_SECRET] == "**REDACTED**"
    assert diagnostics["entry"]["data"][CONF_EAN] == "**REDACTED**"
    assert diagnostics["runtime"]["diagnostic_events"][0]["details"]["ean_suffix"] == "0000"
