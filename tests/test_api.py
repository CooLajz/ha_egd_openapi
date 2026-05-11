"""Unit tests for EG.D API client helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("homeassistant")

from custom_components.ha_egd_openapi import api
from custom_components.ha_egd_openapi.api import EgdApiClient, IntervalRecord


class _RecordingClient(EgdApiClient):
    """API client test double recording requested chunks."""

    def __init__(self) -> None:
        self.chunks: list[tuple[datetime, datetime, int]] = []

    async def _async_get_profile_data_chunk(
        self,
        *,
        ean: str,
        profile: str,
        from_dt: datetime,
        to_dt: datetime,
        page_size: int = api.DEFAULT_PAGE_SIZE,
    ) -> list[IntervalRecord]:
        self.chunks.append((from_dt, to_dt, page_size))
        return []


class _ResponseShapeClient(EgdApiClient):
    """API client test double returning one raw response payload."""

    def __init__(self, payload) -> None:
        self.payload = payload

    async def _async_request_profile_data(
        self,
        *,
        ean: str,
        profile: str,
        from_dt: datetime,
        to_dt: datetime,
        page_start: int,
        page_size: int,
    ):
        return 200, self.payload


@pytest.mark.asyncio
async def test_profile_data_fetch_splits_initial_history_into_page_sized_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Initial history fetch should avoid large ranges that depend on paging."""
    from_dt = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    to_dt = datetime(2026, 4, 15, 23, 45, tzinfo=timezone.utc)
    client = _RecordingClient()
    monkeypatch.setattr(api, "_safe_three_year_cap", lambda: from_dt - timedelta(days=1))

    await client.async_get_profile_data(
        ean="859182400000000000",
        profile="ICQ2",
        from_dt=from_dt,
        to_dt=to_dt,
    )

    assert len(client.chunks) == 4
    assert client.chunks[0] == (
        datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 1, 31, 23, 45, tzinfo=timezone.utc),
        api.DEFAULT_PAGE_SIZE,
    )
    assert client.chunks[-1] == (
        datetime(2026, 4, 4, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 4, 15, 23, 45, tzinfo=timezone.utc),
        api.DEFAULT_PAGE_SIZE,
    )
    assert all(
        chunk_to - chunk_from <= api.MAX_PROFILE_CHUNK
        for chunk_from, chunk_to, _page_size in client.chunks
    )


@pytest.mark.asyncio
async def test_profile_data_chunk_accepts_object_payload() -> None:
    """EG.D may return the profile payload as an object instead of a list."""
    client = _ResponseShapeClient(
        {
            "ean/eic": "859182400000000000",
            "profile": "ICQ2",
            "units": "kWh",
            "total": 1,
            "data": [
                {
                    "timestamp": "2026-05-08T22:15:00Z",
                    "value": 1.25,
                    "status": "W",
                }
            ],
        }
    )

    records = await client._async_get_profile_data_chunk(  # noqa: SLF001
        ean="859182400000000000",
        profile="ICQ2",
        from_dt=datetime(2026, 5, 8, 0, 0, tzinfo=timezone.utc),
        to_dt=datetime(2026, 5, 8, 23, 45, tzinfo=timezone.utc),
    )

    assert records == [
        IntervalRecord(
            timestamp=datetime(2026, 5, 8, 22, 15, tzinfo=timezone.utc),
            value=1.25,
            status="W",
        )
    ]
