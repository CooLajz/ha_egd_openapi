"""API client for EG.D OpenAPI."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
from typing import Any

import aiohttp
from aiohttp import ClientError

from .const import DATA_URL, OAUTH_URL

_LOGGER = logging.getLogger(__name__)


AUTHORIZATION_ERROR_FRAGMENT = "nemáte oprávnění na data odběrného místa"
VALIDATION_ERROR_FRAGMENT = "validation_error"


def _safe_three_year_cap(reference: datetime | None = None) -> datetime:
    """Return a conservative lower bound accepted by EG.D.

    EG.D enforces a rolling 3-year limit, but the exact comparison appears to
    happen on the server side against its current date/time. We therefore keep
    a one-day safety margin to avoid boundary failures around timezone / leap
    year differences.
    """
    ref = (reference or datetime.now(timezone.utc)).astimezone(timezone.utc)
    try:
        capped = ref.replace(year=ref.year - 3)
    except ValueError:
        capped = ref.replace(month=2, day=28, year=ref.year - 3)
    return capped + timedelta(days=1)


class EgdApiError(Exception):
    """Base API error."""


class EgdAuthError(EgdApiError):
    """Authentication error."""


class EgdValidationError(EgdApiError):
    """Validation error returned by EG.D."""

    def __init__(self, message: str, payload: Any | None = None) -> None:
        super().__init__(message)
        self.payload = payload

    @property
    def is_authorization_window_error(self) -> bool:
        payload_str = str(self.payload).lower()
        msg_str = str(self).lower()
        return AUTHORIZATION_ERROR_FRAGMENT in payload_str or AUTHORIZATION_ERROR_FRAGMENT in msg_str


@dataclass(slots=True)
class IntervalRecord:
    """One interval record from EG.D."""

    timestamp: datetime
    value: float
    status: str


class EgdApiClient:
    """Simple async client for EG.D OpenAPI."""

    def __init__(self, session: aiohttp.ClientSession, client_id: str, client_secret: str) -> None:
        self._session = session
        self._client_id = client_id
        self._client_secret = client_secret
        self._access_token: str | None = None

    async def async_get_token(self) -> str:
        """Get bearer token."""
        payload = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "scope": "namerena_data_openapi",
        }
        try:
            async with self._session.post(OAUTH_URL, json=payload, timeout=30) as response:
                data = await response.json(content_type=None)
        except (TimeoutError, ClientError, aiohttp.ContentTypeError) as err:
            raise EgdApiError(f"Token request failed: {err}") from err

        if response.status in (401, 403):
            raise EgdAuthError(f"Authentication failed: HTTP {response.status}")
        if response.status >= 400:
            raise EgdApiError(f"Token request failed: HTTP {response.status} {data}")

        token = data.get("access_token")
        if not token:
            raise EgdApiError("Token response does not contain access_token")

        self._access_token = token
        return token

    async def _async_request_profile_data(
        self,
        *,
        ean: str,
        profile: str,
        from_dt: datetime,
        to_dt: datetime,
        page_start: int,
        page_size: int,
    ) -> tuple[int, Any]:
        """Request one page of profile data and refresh token once on auth failure."""
        params = {
            "ean": ean,
            "profile": profile,
            "from": from_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "to": to_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "pageStart": str(page_start),
            "pageSize": str(page_size),
        }

        for attempt in range(2):
            token = self._access_token or await self.async_get_token()
            headers = {"Authorization": f"Bearer {token}"}
            try:
                async with self._session.get(
                    DATA_URL, params=params, headers=headers, timeout=60
                ) as response:
                    data = await response.json(content_type=None)
            except (TimeoutError, ClientError, aiohttp.ContentTypeError) as err:
                raise EgdApiError(f"Data request failed: {err}") from err

            if response.status in (401, 403):
                self._access_token = None
                if attempt == 0:
                    continue
                raise EgdAuthError(f"Authentication failed: HTTP {response.status}")

            return response.status, data

        raise EgdAuthError("Authentication failed after token refresh")

    async def async_probe_access(
        self,
        *,
        ean: str,
        profile: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> bool:
        """Return whether the given period is accessible for the EAN/profile.

        Uses a very small request to determine whether the server accepts the
        requested time window.
        """
        try:
            await self._async_get_profile_data_chunk(
                ean=ean,
                profile=profile,
                from_dt=from_dt,
                to_dt=to_dt,
                page_size=1,
            )
        except EgdValidationError as err:
            if err.is_authorization_window_error:
                _LOGGER.debug(
                    "Probe denied for %s/%s in %s -> %s: %s",
                    ean,
                    profile,
                    from_dt.isoformat(),
                    to_dt.isoformat(),
                    err,
                )
                return False
            raise
        return True

    async def async_get_profile_data(
        self,
        *,
        ean: str,
        profile: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> list[IntervalRecord]:
        """Fetch all pages of profile data.

        EG.D rejects a single request if the requested period is longer than one year,
        so longer ranges are split into smaller chunks automatically.
        """
        if from_dt.tzinfo is None or to_dt.tzinfo is None:
            raise ValueError("from_dt and to_dt must be timezone-aware")
        effective_from = max(from_dt.astimezone(timezone.utc), _safe_three_year_cap())
        effective_to = to_dt.astimezone(timezone.utc)
        if effective_from > effective_to:
            _LOGGER.debug(
                "Skipping fetch for %s/%s because effective_from > effective_to after 3-year clamp",
                ean,
                profile,
            )
            return []

        all_records: list[IntervalRecord] = []
        chunk_start = effective_from
        final_to = effective_to
        max_chunk = timedelta(days=364, hours=23, minutes=45)

        while chunk_start <= final_to:
            chunk_end = min(chunk_start + max_chunk, final_to)
            chunk_records = await self._async_get_profile_data_chunk(
                ean=ean,
                profile=profile,
                from_dt=chunk_start,
                to_dt=chunk_end,
            )
            all_records.extend(chunk_records)
            chunk_start = chunk_end + timedelta(minutes=15)

        all_records.sort(key=lambda rec: rec.timestamp)
        return all_records

    async def _async_get_profile_data_chunk(
        self,
        *,
        ean: str,
        profile: str,
        from_dt: datetime,
        to_dt: datetime,
        page_size: int = 3000,
    ) -> list[IntervalRecord]:
        """Fetch one API chunk including paging."""
        # In practice EG.D pagination behaves as 1-based even though the PDF
        # example shows PageStart=0. Using 0 can return an empty first page
        # without an explicit API error.
        page_start = 1
        records: list[IntervalRecord] = []

        while True:
            status, data = await self._async_request_profile_data(
                ean=ean,
                profile=profile,
                from_dt=from_dt,
                to_dt=to_dt,
                page_start=page_start,
                page_size=page_size,
            )

            if status >= 400:
                if status == 400:
                    raise EgdValidationError(
                        f"Data request failed: HTTP {status} {data}", payload=data
                    )
                raise EgdApiError(f"Data request failed: HTTP {status} {data}")
            if not isinstance(data, list) or not data:
                return records

            payload = data[0]
            batch = payload.get("data", [])
            total = int(payload.get("total", len(batch)))

            for item in batch:
                ts = datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))
                records.append(
                    IntervalRecord(
                        timestamp=ts,
                        value=float(item.get("value", 0.0)),
                        status=str(item.get("status", "")),
                    )
                )

            if len(batch) < page_size or len(records) >= total:
                break
            page_start += len(batch)

        return records
