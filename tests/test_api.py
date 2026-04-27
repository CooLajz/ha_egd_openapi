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
