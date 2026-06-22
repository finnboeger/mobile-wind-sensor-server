"""
MQTT Listener for Wind Dashboard
Subscribes to MQTT topics and writes validated measurements to PocketBase
"""

import json
import logging
from datetime import datetime, timezone
from ssl import PROTOCOL_TLS
from typing import Optional
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from pocketbase import PocketBase
from queue import Queue

logger = logging.getLogger(__name__)


class MqttListener:
    def __init__(self, mqtt_config):
        self.pb: Optional[PocketBase] = None
        self.mqtt_config = mqtt_config
        self.client: Optional[mqtt.Client] = None
        self.outputQueue = Queue()

        self.stats = {"messages_received": 0, "messages_processed": 0, "messages_failed": 0}

    def connect(self) -> None:
        """Connect to MQTT broker"""
        
        self.client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id="server_listener",
            userdata=None,
            protocol=mqtt.MQTTv5,
        )
        self.client.tls_set(tls_version=PROTOCOL_TLS)
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
            # Parse JSON payload
            json_str = payload.decode("utf-8")
            payload_data = json.loads(json_str)
            data = self._normalize_measurement(topic, payload_data)

            # Write to PocketBase
            self.outputQueue.put(data)
            self.stats["messages_processed"] += 1

            if self.mqtt_config.log_level == "debug":
                logger.debug(f"✓ Processed measurement from {data['source_id']}")

        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON from {topic}: {e}")
            self.stats["messages_failed"] += 1
        except Exception as e:
            logger.error(f"Failed to process MQTT message: {e}")
            self.stats["messages_failed"] += 1

    def _normalize_measurement(self, topic: str, payload_data: dict) -> dict:
        """Normalize nested MQTT payload to PocketBase measurement schema."""
        source_id = payload_data.get("source_id") or self.mqtt_config.mqtt_topics.get(topic)
        if not source_id:
            raise ValueError(f"Missing source_id in payload and no topic mapping for '{topic}'")

        timestamp = payload_data.get("timestamp")
        if timestamp is None:
            raise ValueError("Missing required field 'timestamp'")

        gps = payload_data.get("gps") or {}
        true_wind = payload_data.get("true") or {}
        apparent_wind = payload_data.get("apparent") or {}

        ts_iso = datetime.fromtimestamp(float(timestamp), tz=timezone.utc).isoformat()

        # Support both nested and flat payloads while preserving numeric zeros.
        true_wind_dir_deg = true_wind.get("direction")
        if true_wind_dir_deg is None:
            true_wind_dir_deg = payload_data.get("true_wind_dir_deg")

        true_wind_speed_mps = true_wind.get("speed")
        if true_wind_speed_mps is None:
            true_wind_speed_mps = payload_data.get("true_wind_speed_mps")

        if true_wind_dir_deg is None:
            raise ValueError("Missing required field 'true.direction' (or 'true_wind_dir_deg')")
        if true_wind_speed_mps is None:
            raise ValueError("Missing required field 'true.speed' (or 'true_wind_speed_mps')")

        return {
            "source_id": source_id,
            "ts": ts_iso,
            "gps_lat": gps.get("lat"),
            "gps_lng": gps.get("lon"),
            "sensor_heading_deg": gps.get("heading"),
            "sensor_speed_mps": gps.get("speed"),
            "true_wind_dir_deg": float(true_wind_dir_deg),
            "true_wind_speed_mps": float(true_wind_speed_mps),
            "apparent_wind_dir_deg": apparent_wind.get("direction"),
            "apparent_wind_speed_mps": apparent_wind.get("speed"),
        }

    def get_stats(self) -> dict:
        """Get listener statistics"""
        return self.stats.copy()

    def is_connected(self) -> bool:
        """Check if connected to MQTT broker"""
        return self.client.is_connected() if self.client else False
