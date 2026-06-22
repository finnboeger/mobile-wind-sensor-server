import logging
from typing import Optional

from pocketbase import PocketBase
logger = logging.getLogger(__name__)

class PocketbaseWriter:
    def __init__(self, pocketbase_url: str, pocketbase_email: str, pocketbase_password: str):
        self.pocketbase_url = pocketbase_url
        self.pocketbase_email = pocketbase_email
        self.pocketbase_password = pocketbase_password
        self.pb: Optional[PocketBase] = None

        try:
            self.pb = PocketBase(self.pocketbase_url)
            self.pb.admins.auth_with_password(self.pocketbase_email, self.pocketbase_password)
            logger.info("✓ Authenticated with PocketBase")
        except Exception as e:
            logger.error(f"Failed to authenticate with PocketBase: {e}")
            raise


    def write_measurement(self, measurement: dict) -> None:
        """Write or update measurement in PocketBase"""
        try:
            if self.pb is None:
                raise ValueError("PocketBase client is not initialized")
            source_id = measurement.get("source_id")
            ts = measurement.get("ts")

            # Try to find existing record
            filter_query = f'source_id = "{source_id}" && ts = "{ts}"'
            records = self.pb.collection("measurements").get_list(
                query_params={"filter": filter_query, "limit": 1}
            )

            if records.items:
                # Update existing record
                record_id = records.items[0].id
                self.pb.collection("measurements").update(record_id, measurement)
            else:
                # Create new record
                self.pb.collection("measurements").create(measurement)

        except Exception as e:
            logger.error(f"Failed to write measurement to PocketBase: {e}")
            raise
