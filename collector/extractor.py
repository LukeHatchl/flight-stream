"""Live aircraft state-vector sources, behind a swappable interface.

OpenSkySource is the implementation used today. A future adsb.lol (or other)
source only needs to implement StateSource.fetch_states() to drop in.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod

import requests
from opensky_api import StateVector, TokenManager

logger = logging.getLogger(__name__)

# Subset of StateVector fields we care about for the bronze layer (spec section 4/5).
STATE_FIELDS = (
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "geo_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "squawk",
    "position_source",
)


class RetryableSourceError(Exception):
    """A transient source failure (429 / 5xx) the caller should retry."""


class StateSource(ABC):
    """A live aircraft state-vector feed for a fixed bounding box."""

    @abstractmethod
    def fetch_states(self) -> list[dict]:
        """Return one snapshot of raw state-vector dicts."""


class OpenSkySource(StateSource):
    """Pulls state vectors from the OpenSky Network REST API.

    Calls the REST endpoint directly (rather than OpenSkyApi.get_states())
    so HTTP status codes reach the caller — the official client swallows
    them and returns None on any non-200 response, which would make it
    impossible to tell a rate limit or server error apart from "no data".
    """

    API_URL = "https://opensky-network.org/api/states/all"

    def __init__(self, bbox: tuple[float, float, float, float], credentials_path: str | None = None):
        """bbox is (lamin, lomin, lamax, lomax) in WGS84 decimal degrees."""
        self._bbox = bbox
        self._token_manager = TokenManager.from_json_file(credentials_path) if credentials_path else None
        self._session = requests.Session()

    def fetch_states(self) -> list[dict]:
        lamin, lomin, lamax, lomax = self._bbox
        params = {"lamin": lamin, "lomin": lomin, "lamax": lamax, "lomax": lomax}
        headers = self._token_manager.auth_headers() if self._token_manager else {}

        response = self._session.get(self.API_URL, params=params, headers=headers, timeout=15)

        if response.status_code == 429 or response.status_code >= 500:
            raise RetryableSourceError(f"OpenSky returned {response.status_code}: {response.reason}")
        response.raise_for_status()

        raw_states = response.json().get("states") or []
        vectors = [StateVector(row) for row in raw_states]
        return [{field: getattr(vector, field, None) for field in STATE_FIELDS} for vector in vectors]

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "OpenSkySource":
        return self

    def __exit__(self, *args) -> None:
        self.close()
