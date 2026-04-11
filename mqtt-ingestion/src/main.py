#!/usr/bin/env python3
"""
MQTT Ingestion Service

Docker-only service that:
- Subscribes to MQTT topics
- Validates 1Hz measurement payloads
- Writes to PocketBase measurements collection
- Provides statistics via polling

Configuration via environment variables:
- MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASSWORD
- MQTT_TOPICS=topic1=source1,topic2=source2
- POCKETBASE_URL, POCKETBASE_ADMIN_TOKEN
- LOG_LEVEL (debug, info, warn, error)
"""

import logging
import signal
import sys
import time
from src.config import load_config
from src.mqtt_listener import MqttListener

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

mqtt_listener: MqttListener = None


def shutdown_handler(signum, frame):
    """Handle SIGINT and SIGTERM"""
    logger.info("\nShutting down...")
    if mqtt_listener:
        mqtt_listener.disconnect()
    sys.exit(0)


def main():
    """Main entry point"""
    global mqtt_listener

    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()
        logger.info(f"Configuration loaded: {config.mqtt_host}:{config.mqtt_port}")

        # Set log level
        if config.log_level == "debug":
            logging.getLogger().setLevel(logging.DEBUG)
        elif config.log_level == "error":
            logging.getLogger().setLevel(logging.ERROR)
        elif config.log_level == "warn":
            logging.getLogger().setLevel(logging.WARNING)

        # Initialize MQTT listener
        logger.info("Initializing MQTT listener...")
        mqtt_listener = MqttListener(
            config.pocketbase_url, config.pocketbase_admin_token, config
        )

        # Connect to MQTT
        mqtt_listener.connect()

        # Setup signal handlers
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        logger.info("✓ MQTT Ingestion Service started successfully")

        # Log statistics every 30 seconds
        while True:
            time.sleep(30)
            stats = mqtt_listener.get_stats()
            logger.info(
                f"[STATS] Received: {stats['messages_received']}, "
                f"Processed: {stats['messages_processed']}, "
                f"Failed: {stats['messages_failed']}"
            )

    except Exception as e:
        logger.error(f"Failed to start MQTT Ingestion Service: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
