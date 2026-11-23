import logging

import orjson

from confluent_kafka import Producer

from src.config import Settings
from src.events import EventBase

logger = logging.getLogger(__name__)

class KafkaPublisher:

    def __init__(self, settings: Settings, client_id: str):
        self.settings = settings

        config = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 10,
        }

        self.producer = Producer(config)

        logger.info(
            "PRODUCER CONNECTED | client_id=%s | broker=%s",
            client_id,
            settings.kafka_bootstrap_servers,
        )

    def publish(
            self,
            topic: str,
            key: str,
            event: EventBase,
    ) -> None:

        value = orjson.dumps(
            event.model_dump(mode="json")
        )

        try:

            self.producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value,
                callback=self._delivery_report
            )

        except BufferError:

            logger.warning(
                "Producer queue full. Waiting for available capacity."
            )

            self.producer.poll(1.0)

            self.producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value,
                callback=self._delivery_report,
            )

        self.producer.poll(0)


    @staticmethod
    def _delivery_report(err, message) -> None:

        if err is not None:
            logger.error(
                "DELIVERY FAILED | error=%s",
                err,
            )

            return

        key = message.key()

        if key is not None:
            key = key.decode("utf-8")

        logger.info(
            "DELIVERED | topic=%s | partition=%s | offset=%s | key=%s",
            message.topic(),
            message.partition(),
            message.offset(),
            key,
        )


    def close(self) -> None:

        remaining = self.producer.flush(10)

        if remaining:
            logger.warning(
                "Messages still waiting for delivery | count=%s",
                remaining,
            )

        else:
            logger.info("PRODUCER CLOSED | all queued messages flushed")
