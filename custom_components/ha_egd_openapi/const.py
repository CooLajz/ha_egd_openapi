"""Constants for the EG.D OpenAPI integration."""

from __future__ import annotations

DOMAIN = "ha_egd_openapi"
PLATFORMS = ["sensor"]

CONF_CLIENT_ID = "client_id"
CONF_CLIENT_SECRET = "client_secret"
CONF_EAN = "ean"
CONF_UPDATE_HOUR = "update_hour"
CONF_UPDATE_MINUTE = "update_minute"
CONF_REVALIDATE_DAYS = "revalidate_days"
CONF_IMPORT_PROFILE = "import_profile"
CONF_EXPORT_PROFILE = "export_profile"
CONF_ENABLE_EXPORT = "enable_export"
CONF_NAME = "name"
CONF_ENABLE_DIAGNOSTICS = "enable_diagnostics"

DEFAULT_NAME = "EG.D Smart Meter"
DEFAULT_UPDATE_HOUR = 18
DEFAULT_UPDATE_MINUTE = 40
DEFAULT_REVALIDATE_DAYS = 31
DEFAULT_IMPORT_PROFILE = "ICQ2"
DEFAULT_EXPORT_PROFILE = "ISQ2"
DEFAULT_ENABLE_DIAGNOSTICS = False

OAUTH_URL = "https://idm.distribuce24.cz/oauth/token"
DATA_URL = "https://data.distribuce24.cz/rest/spotreby"
# C1 interval energies served by the unified /rest/spotreby endpoint.
C1_IMPORT_PROFILE = "DCQC"
C1_EXPORT_PROFILE = "DSQC"
C1_PROFILES = frozenset({C1_IMPORT_PROFILE, C1_EXPORT_PROFILE})
PROFILE_UNITS = {
    "ICQ2": "kWh",
    "ICC1": "kW",
    "ISQ2": "kWh",
    "ISC1": "kW",
    C1_IMPORT_PROFILE: "kWh",
    C1_EXPORT_PROFILE: "kWh",
}
LEGACY_C1_PROFILES = {
    "0.0.2.4.1.2.12.0.0.0.0.0.0.0.0.3.72.0": C1_IMPORT_PROFILE,
    "0.0.2.4.19.2.12.0.0.0.0.0.0.0.0.3.72.0": C1_EXPORT_PROFILE,
}
STATUS_VALID = "IU012"

STORE_VERSION = 1
STORE_KEY = f"{DOMAIN}_store"
DIAGNOSTICS_EVENTS_KEY = "diagnostics_events"
MAX_DIAGNOSTIC_EVENTS = 500

ATTR_LAST_VALID_IMPORT_TS = "last_valid_import_timestamp"
ATTR_LAST_VALID_EXPORT_TS = "last_valid_export_timestamp"
ATTR_LAST_IMPORT_STATUS = "last_import_status"
ATTR_LAST_EXPORT_STATUS = "last_export_status"
ATTR_LAST_UPDATE_UTC = "last_update_utc"
ATTR_LAST_API_SYNC_UTC = "last_api_sync_utc"
ATTR_LAST_ERROR = "last_error"
ATTR_SYNC_STATUS = "sync_status"
ATTR_LAST_CHECK_STARTED_UTC = "last_check_started_utc"
ATTR_LAST_CHECK_FINISHED_UTC = "last_check_finished_utc"
ATTR_NEXT_SYNC_ATTEMPT_UTC = "next_sync_attempt_utc"
ATTR_NEXT_SYNC_REASON = "next_sync_reason"
ATTR_LAST_MANUAL_REFRESH_UTC = "last_manual_refresh_utc"
ATTR_LAST_MANUAL_REFRESH_RESULT = "last_manual_refresh_result"
ATTR_EAN = "ean"


def normalize_profile(profile: str) -> str:
    """Upgrade numeric C1 selections from beta.1 without touching A/B profiles."""
    return LEGACY_C1_PROFILES.get(profile, profile)
