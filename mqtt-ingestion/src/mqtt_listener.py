"""
MQTT Listener for Wind Dashboard
Subscribes to MQTT topics and writes validated measurements to PocketBase
"""

import json
import logging
from typing import Optional
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import requests

logger = logging.getLogger(__name__)


class MqttListener:
    def __init__(self, pocketbase_url: str, pocketbase_token: str, mqtt_config):
        self.pocketbase_url = pocketbase_url
        self.pocketbase_token = pocketbase_token
        self.mqtt_config = mqtt_config
        self.client: Optional[mqtt.Client] = None

        self.stats = {"messages_received": 0, "messages_processed": 0, "messages_failed": 0}

    def connect(self) -> None:
        """Connect to MQTT broker"""
        self.client = mqtt.Client(CallbackAPIVersion.VERSION2)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

        if self.mqtt_config.mqtt_user and self.mqtt_config.mqtt_password:
            self.client.username_pw_set(self.mqtt_config.mqtt_user, self.mqtt_config.mqtt_password)

        logger.info(
            f"Connecting to MQTT broker: {self.mqtt_config.mqtt_host}:{self.mqtt_config.mqtt_port}"
        )
        logger.info(f"Topics: {', '.join(self.mqtt_config.mqtt_topics.keys())}")

        self.client.connect(self.mqtt_config.mqtt_host, self.mqtt_config.mqtt_port, keepalive=60)
        self.client.loop_start()

    def disconnect(self) -> None:
        """Disconnect from MQTT broker"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("Disconnected from MQTT broker")

    def _on_connect(self, client, userdata, connect_flags, reason_code, properties):
        """MQTT connect callback"""
        if reason_code.is_failure:
            logger.error(f"Failed to connect to MQTT broker, reason code: {reason_code}")
        else:
            logger.info("✓ Connected to MQTT broker")
            # Subscribe to all topics
            for topic in self.mqtt_config.mqtt_topics:
                client.subscribe(topic)
                logger.info(f"✓ Subscribed to: {topic}")

    def _on_disconnect(self, client, userdata, disconnect_flags, auth_data, reason_code):
        """MQTT disconnect callback"""
        if reason_code.is_failure:
            logger.warning(f"Unexpected disconnection from MQTT broker: {reason_code}")
        else:
            logger.info("Disconnected from MQTT broker")

    def _on_message(self, client, userdata, msg):
        """MQTT message callback"""
        try:
            self.handle_message(msg.topic, msg.payload)
        except Exception as e:
            logger.error(f"Error handling MQTT message: {e}")

    def handle_message(self, topic: str, payload: bytes) -> None:
        """Process incoming MQTT message"""
        self.stats["messages_received"] += 1

        try:
            # Get source_id from topic mapping
            source_id = self.mqtt_config.mqtt_topics.get(topic)
            if not source_id:
                logger.warning(f"Received message on unmapped topic: {topic}")
                return

            # Parse JSON payload
            json_str = payload.decode("utf-8")
            data = json.loads(json_str)

            # Ensure source_id is set
            data["source_id"] = source_id

            # Write to PocketBase
            self.write_measurement(data)
            self.stats["messages_processed"] += 1

            if self.mqtt_config.log_level == "debug":
                logger.debug(f"✓ Processed measurement from {source_id}")

        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON from {topic}: {e}")
            self.stats["messages_failed"] += 1
        except Exception as e:
            logger.error(f"Failed to process MQTT message: {e}")
            self.stats["messages_failed"] += 1

    def write_measurement(self, measurement: dict) -> None:
        """Write or update measurement in PocketBase"""
        try:
            headers = {
                "Authorization": self.pocketbase_token,
                "Content-Type": "application/json",
            }

            # Check if record exists
            source_id = measurement.get("source_id")
            ts = measurement.get("ts")

            url = f"{self.pocketbase_url}/api/collections/measurements/records"
            
            # Try to find existing record
            filter_query = f'(source_id = "{source_id}" && ts = "{ts}")'
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    params={"filter": filter_query, "limit": 1},
                    timeout=5,
                )
                response.raise_for_status()

                items = response.json().get("items", [])
                if items:
                    # Update existing record
                    record_id = items[0]["id"]
                    response = requests.patch(
                        f"{url}/{record_id}",
                        headers=headers,
                        json=measurement,
                        timeout=5,
                    )
                else:
                    # Create new record
                    response = requests.post(
                        url, headers=headers, json=measurement, timeout=5
                    )

                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.error(f"PocketBase write error: {e}")
                raise

        except Exception as e:
            logger.error(f"Failed to write measurement to PocketBase: {e}")
            raise

    def get_stats(self) -> dict:
        """Get listener statistics"""
        return self.stats.copy()

    def is_connected(self) -> bool:
        """Check if connected to MQTT broker"""
        return self.client.is_connected() if self.client else False
