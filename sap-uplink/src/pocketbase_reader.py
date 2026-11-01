import logging
from typing import Any, Dict, Iterator, List

import requests

logger = logging.getLogger(__name__)

MEASUREMENTS_COLLECTION = "measurements"
FIELDS = "id,source_id,ts,created_at,gps_lat,gps_lng,true_wind_dir_deg,true_wind_speed_mps"


class PocketbaseReader:
    """Reads measurements through PocketBase's public list API."""

    def __init__(self, pocketbase_url: str, timeout_seconds: float = 30):
        self.records_url = f"{pocketbase_url}/api/collections/{MEASUREMENTS_COLLECTION}/records"
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def list_new(self, source_id: str, after: str, limit: int = 200) -> List[Dict[str, Any]]:
        """
        Records of one source stored after `after` (the `created_at` autodate), in
        the order they arrived. Measurements that reach PocketBase late, with an
        old `ts`, still show up here.
        """
        return self._list(f'source_id = "{source_id}" && created_at > "{after}"', "created_at", limit, page=1)

    def iter_range(
        self, source_id: str, after: str, until: str, page_size: int = 200
    ) -> Iterator[List[Dict[str, Any]]]:
        """Yield pages of records with after < ts <= until, oldest first."""
        filter_query = f'source_id = "{source_id}" && ts > "{after}" && ts <= "{until}"'
        page = 1
        while True:
            items = self._list(filter_query, "ts", page_size, page)
            if items:
                yield items
            if len(items) < page_size:
                return
            page += 1

    def _list(self, filter_query: str, sort: str, limit: int, page: int) -> List[Dict[str, Any]]:
        response = self.session.get(
            self.records_url,
            params={
                "filter": filter_query,
                "sort": sort,
                "perPage": limit,
                "page": page,
                "skipTotal": 1,
                "fields": FIELDS,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json().get("items", [])
