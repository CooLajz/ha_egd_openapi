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
CONF_NAME = "name"

DEFAULT_NAME = "EG.D Smart Meter"
DEFAULT_UPDATE_HOUR = 16
DEFAULT_UPDATE_MINUTE = 17
DEFAULT_REVALIDATE_DAYS = 31
DEFAULT_IMPORT_PROFILE = "ICQ2"
DEFAULT_EXPORT_PROFILE = "ISQ2"

OAUTH_URL = "https://idm.distribuce24.cz/oauth/token"
DATA_URL = "https://data.distribuce24.cz/rest/spotreby"
STATUS_VALID = "IU012"

STORE_VERSION = 1
STORE_KEY = f"{DOMAIN}_store"

ATTR_LAST_VALID_IMPORT_TS = "last_valid_import_timestamp"
ATTR_LAST_VALID_EXPORT_TS = "last_valid_export_timestamp"
ATTR_LAST_IMPORT_STATUS = "last_import_status"
ATTR_LAST_EXPORT_STATUS = "last_export_status"
ATTR_LAST_UPDATE_UTC = "last_update_utc"
ATTR_LAST_API_SYNC_UTC = "last_api_sync_utc"
ATTR_EAN = "ean"
