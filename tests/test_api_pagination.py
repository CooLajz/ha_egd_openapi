"""Download integrity regressions shared by A/B and C1 profiles."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("homeassistant")

from custom_components.ha_egd_openapi.api import EgdApiClient, EgdApiError
from custom_components.ha_egd_openapi.const import PROFILE_UNITS

START = datetime(2026, 10, 27, tzinfo=timezone.utc)


def _point(index):
    return {
        "timestamp": (START + timedelta(minutes=15 * index)).isoformat(),
        "value": index + 1,
        "status": "W",
    }


class _PagedClient(EgdApiClient):
    def __init__(self, pages):
        super().__init__(None, "test", "test")
        self.pages = iter(pages)
        self.requests = []

    async def _async_request_profile_data(self, **kwargs):
        self.requests.append(kwargs)
        return 200, next(self.pages)


async def _fetch(client, profile="DCQC"):
    return await client._async_get_profile_data_chunk(
        ean="test-ean",
        profile=profile,
        from_dt=START,
        to_dt=START + timedelta(hours=1),
        page_size=2,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("profile,units", PROFILE_UNITS.items())
async def test_short_pages_follow_total_until_all_intervals_are_loaded(profile, units):
    client = _PagedClient(
        [
            {
                "profile": profile,
                "units": units.upper(),
                "total": "3",
                "data": [_point(index)],
            }
            for index in range(3)
        ]
    )
    records = await _fetch(client, profile)
    assert [record.value for record in records] == [1, 2, 3]
    assert [request["page_start"] for request in client.requests] == [1, 2, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", PROFILE_UNITS)
async def test_repeated_page_fails_instead_of_importing_duplicate_energy(profile):
    # The same instant with another offset must also be detected as a duplicate.
    repeated = {**_point(1), "timestamp": "2026-10-27T01:15:00+01:00"}
    client = _PagedClient(
        [
            {"total": 3, "data": [_point(0), _point(1)]},
            {"total": 3, "data": [repeated]},
        ]
    )
    with pytest.raises(EgdApiError, match="duplicitní"):
        await _fetch(client, profile)
    assert len(client.requests) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "last_page",
    [[], {"total": 3, "data": []}, {"data": []}],
)
async def test_missing_page_cannot_silently_truncate_history(last_page):
    client = _PagedClient([{"total": 3, "data": [_point(0), _point(1)]}, last_page])
    with pytest.raises(EgdApiError, match="neúplná"):
        await _fetch(client)


@pytest.mark.asyncio
async def test_total_remains_binding_when_later_page_omits_it():
    client = _PagedClient(
        [
            {"total": 3, "data": [_point(0)]},
            {"data": [_point(1)]},
            {"data": [_point(2)]},
        ]
    )
    assert len(await _fetch(client)) == 3


@pytest.mark.asyncio
async def test_legacy_payload_without_metadata_or_total_still_paginates():
    client = _PagedClient([{"data": [_point(0), _point(1)]}, {"data": [_point(2)]}])
    assert len(await _fetch(client)) == 3
    assert [request["page_start"] for request in client.requests] == [1, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pages",
    [
        [{"total": -1, "data": []}],
        [{"total": None, "data": []}],
        [{"total": True, "data": [_point(0)]}],
        [{"total": 1.5, "data": [_point(0)]}],
        [{"total": "invalid", "data": []}],
        [{"total": 1, "data": [_point(0), _point(1)]}],
        [
            {"total": 3, "data": [_point(0), _point(1)]},
            {"total": 2, "data": [_point(2)]},
        ],
        [[{"data": []}, {"data": []}]],
    ],
)
async def test_inconsistent_responses_fail_instead_of_returning_partial_records(pages):
    with pytest.raises(EgdApiError):
        await _fetch(_PagedClient(pages))


@pytest.mark.asyncio
@pytest.mark.parametrize("profile,units", PROFILE_UNITS.items())
@pytest.mark.parametrize("incorrect_field", ["profile", "units"])
async def test_wrong_profile_or_units_cannot_change_energy_totals(
    profile, units, incorrect_field
):
    payload = {
        "profile": profile,
        "units": units,
        "total": 1,
        "data": [_point(0)],
    }
    payload[incorrect_field] = "other" if incorrect_field == "profile" else "MWh"
    with pytest.raises(EgdApiError):
        await _fetch(_PagedClient([payload]), profile)
