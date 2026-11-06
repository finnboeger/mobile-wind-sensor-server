"""
Superuser access to PocketBase, for the uplink's own collections.
"""

import logging
import threading
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

class PocketbaseStore:
    """Superuser access to PocketBase records; logs in again when the token is refused."""

    def __init__(self, pocketbase_url: str, email: str, password: str, timeout_seconds: float = 30):
        self.base_url = pocketbase_url
        self.email = email
        self.password = password
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self._token: Optional[str] = None
        self._lock = threading.Lock()

    def get_collection(self, name: str) -> Optional[Dict[str, Any]]:
        response = self._request("GET", f"/api/collections/{name}", allow_404=True)
        return None if response is None else response.json()

    def save_collection(self, name: Optional[str], data: Dict[str, Any]) -> None:
        """Update the named collection, or create one when name is None."""
        if name is None:
            self._request("POST", "/api/collections", json=data)
        else:
            self._request("PATCH", f"/api/collections/{name}", json=data)

    def get(self, collection: str, record_id: str) -> Optional[Dict[str, Any]]:
        response = self._request("GET", f"/api/collections/{collection}/records/{record_id}", allow_404=True)
        return None if response is None else response.json()

    def list(self, collection: str, filter_query: str = "", sort: str = "", per_page: int = 200) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"perPage": per_page, "skipTotal": 1}
        if filter_query:
            params["filter"] = filter_query
        if sort:
            params["sort"] = sort
        response = self._request("GET", f"/api/collections/{collection}/records", params=params)
        return response.json().get("items", [])

    def create(self, collection: str, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", f"/api/collections/{collection}/records", json=data).json()

    def update(self, collection: str, record_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("PATCH", f"/api/collections/{collection}/records/{record_id}", json=data).json()

    def upsert(self, collection: str, record_id: str, data: Dict[str, Any]) -> None:
        """Update the record with this id, creating it the first time."""
        if self._request("PATCH", f"/api/collections/{collection}/records/{record_id}", json=data, allow_404=True) is None:
            self.create(collection, {"id": record_id, **data})

    def _request(self, method: str, path: str, allow_404: bool = False, **kwargs) -> Optional[requests.Response]:
        for attempt in range(2):
            with self._lock:
                if self._token is None:
                    self._login()
                headers = {"Authorization": self._token}
            response = self.session.request(
                method, self.base_url + path, headers=headers, timeout=self.timeout_seconds, **kwargs
            )
            # PocketBase answers 401/403 for an expired token on superuser-only collections
            if response.status_code in (401, 403) and attempt == 0:
                with self._lock:
                    self._token = None
                continue
            if response.status_code == 404 and allow_404:
                return None
            response.raise_for_status()
            return response
        return None  # unreachable: the second attempt returns or raises

    def _login(self) -> None:
        response = self.session.post(
            f"{self.base_url}/api/collections/_superusers/auth-with-password",
            json={"identity": self.email, "password": self.password},
            timeout=self.timeout_seconds,
        )
        if response.status_code == 400:
            raise RuntimeError("PocketBase rejected the login (check POCKETBASE_ADMIN_EMAIL / POCKETBASE_ADMIN_PASSWORD)")
        response.raise_for_status()
        self._token = response.json()["token"]
        logger.info("Logged in to PocketBase")
