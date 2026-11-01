import logging
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

PUT_WIND_PATH = "/sailingserver/api/v1/wind/putWind"


class SapError(Exception):
    """A request that may succeed when retried (network, auth, server errors)."""


class SapRejectedError(Exception):
    """A request SAP Sailing refused as malformed; retrying it won't help."""


class SapClient:
    def __init__(
        self,
        sap_url: str,
        wind_source_type: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        bearer_token: Optional[str] = None,
        timeout_seconds: float = 30,
    ):
        self.sap_url = sap_url
        self.put_wind_url = sap_url + PUT_WIND_PATH
        self.wind_source_type = wind_source_type
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        if bearer_token:
            self.session.headers["Authorization"] = f"Bearer {bearer_token}"
        elif user and password:
            self.session.auth = (user, password)

    def put_wind(
        self,
        wind_source_id: str,
        fixes: List[Dict[str, Any]],
        races: Optional[List[Dict[str, str]]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Send wind fixes to the given races (None: every tracked race the login
        may update). Returns SAP's per-race answers; an empty list means no
        tracked race matched.
        """
        body: Dict[str, Any] = {
            "windData": fixes,
            "windSourceType": self.wind_source_type,
            "windSourceId": wind_source_id,
        }
        if races is not None:
            body["regattaNamesAndRaceNames"] = races

        try:
            response = self.session.put(self.put_wind_url, json=body, timeout=self.timeout_seconds)
        except requests.RequestException as e:
            raise SapError(f"putWind request failed: {e}") from e

        if response.status_code == 400:
            raise SapRejectedError(f"putWind rejected the data (400): {response.text[:500]}")
        if not response.ok:
            raise SapError(f"putWind returned {response.status_code}: {response.text[:500]}")

        try:
            answers = response.json()
        except ValueError:
            return []
        return answers if isinstance(answers, list) else []

