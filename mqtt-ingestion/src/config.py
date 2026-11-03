"""
Environment configuration for MQTT Ingestion Service

This service only runs in Docker mode.
All settings are read from environment variables.

PocketBase authentication uses email/password instead of tokens.
"""

import os
from typing import Dict


class MqttConfig:
    def __init__(self):
        # COMPOSE_PROFILES=mosquitto runs the stack's own broker; it then is the
        # default: plain MQTT inside the docker network, login "ingestion"
        profiles = [p.strip() for p in os.getenv("COMPOSE_PROFILES", "").split(",")]
        builtin_broker = "mosquitto" in profiles

        # Empty values count as unset, so docker-compose can pass them through
        self.mqtt_host = os.getenv("MQTT_HOST") or ("mosquitto" if builtin_broker else None)
        if not self.mqtt_host:
            raise ValueError("Missing MQTT_HOST environment variable")

        self.mqtt_port = int(os.getenv("MQTT_PORT") or "1883")
        self.mqtt_user = os.getenv("MQTT_USER") or ("ingestion" if builtin_broker else None)
        self.mqtt_password = os.getenv("MQTT_PASSWORD")
        # TLS is on by default, except for the stack's own broker
        use_tls = (os.getenv("MQTT_USE_TLS") or ("false" if builtin_broker else "true")).strip().lower()
        if use_tls not in ("1", "true", "yes", "0", "false", "no"):
            raise ValueError(f"MQTT_USE_TLS must be true or false, got {use_tls!r}")
        self.mqtt_use_tls = use_tls in ("1", "true", "yes")

        # Parse MQTT topics
        mqtt_topics_str = os.getenv("MQTT_TOPICS")
        if not mqtt_topics_str:
            raise ValueError(
                "Missing MQTT_TOPICS environment variable (format: topic1=source1,topic2=source2)"
            )

        self.mqtt_topics: Dict[str, str] = {}
        for mapping in mqtt_topics_str.split(","):
            parts = mapping.split("=")
            if len(parts) == 2:
                topic = parts[0].strip()
                source_id = parts[1].strip()
                if topic and source_id:
                    self.mqtt_topics[topic] = source_id

        if not self.mqtt_topics:
            raise ValueError("No valid MQTT topic mappings found")

        self.pocketbase_url = os.getenv("POCKETBASE_URL")
        if not self.pocketbase_url:
            raise ValueError("Missing POCKETBASE_URL environment variable")

        self.pocketbase_url = self.pocketbase_url.rstrip("/")

        self.pocketbase_admin_email = os.getenv("POCKETBASE_ADMIN_EMAIL")
        if not self.pocketbase_admin_email:
            raise ValueError("Missing POCKETBASE_ADMIN_EMAIL environment variable")

        self.pocketbase_admin_password = os.getenv("POCKETBASE_ADMIN_PASSWORD")
        if not self.pocketbase_admin_password:
            raise ValueError("Missing POCKETBASE_ADMIN_PASSWORD environment variable")

        self.log_level = os.getenv("LOG_LEVEL", "info").lower()


def load_config() -> MqttConfig:
    """Load MQTT configuration from environment variables"""
    return MqttConfig()
