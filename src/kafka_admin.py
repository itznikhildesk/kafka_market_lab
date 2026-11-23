
import logging

from confluent_kafka import (
    KafkaError,
    KafkaException,
)

from confluent_kafka.admin import (
    AdminClient,
    NewTopic,
)

from src.config import Settings
from src.logging_config import setup_logging


logger = logging.getLogger(__name__)


class KafkaTopicManager:

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

        self.admin = AdminClient({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": f"{settings.kafka_client_id}-admin",
        })

    def create_topic(self) -> None:

        try:
            metadata = self.admin.list_topics(timeout=10)

            existing_topics = set(metadata.topics)

            logger.info(
                "EXISTING TOPICS | count=%d | topics=%s",
                len(existing_topics),
                sorted(existing_topics),
            )

            for topic_name in self.settings.all_topics:
                if topic_name in existing_topics:
                    logger.warning(
                        "TOPIC ALREADY EXISTS | topic=%s",
                        topic_name,
                    )

            new_topics = [
                NewTopic(
                    topic=topic_name,
                    num_partitions=self.settings.kafka_partitions,
                    replication_factor=self.settings.kafka_replication_factor,
                )
                for topic_name in self.settings.all_topics
                if topic_name not in existing_topics
            ]

            if not new_topics:
                logger.info("All configured topics already exist")
                return

            logger.info(
                "CREATING TOPICS | count=%d",
                len(new_topics),
            )

            futures = self.admin.create_topics(
                new_topics,
                request_timeout=15,
            )

            for topic_name, future in futures.items():

                try:
                    future.result()

                    logger.info(
                        "TOPIC CREATED | topic=%s",
                        topic_name,
                    )

                except KafkaException as exc:

                    error = exc.args[0] if exc.args else None

                    if (
                        error is not None
                        and error.code() == KafkaError.TOPIC_ALREADY_EXISTS
                    ):
                        logger.warning(
                            "TOPIC ALREADY EXISTS | topic=%s",
                            topic_name,
                        )

                    else:
                        logger.exception(
                            "TOPIC CREATION FAILED | topic=%s",
                            topic_name,
                        )
                        raise

        finally:
            try:
                self.show_topics()
            except KafkaException:
                logger.exception("FAILED TO INSPECT KAFKA TOPICS")

    def show_topics(self) -> None:

        metadata = self.admin.list_topics(timeout=10)

        for topic_name in self.settings.all_topics:

            topic_metadata = metadata.topics.get(topic_name)

            if topic_metadata is None:
                logger.error(
                    "TOPIC NOT FOUND | topic=%s",
                    topic_name,
                )
                continue

            partitions = topic_metadata.partitions

            logger.info(
                "TOPIC | name=%s | partitions=%d",
                topic_name,
                len(partitions),
            )

            if len(partitions) != self.settings.kafka_partitions:
                logger.warning(
                    "PARTITION COUNT MISMATCH | topic=%s | expected=%d | actual=%d",
                    topic_name,
                    self.settings.kafka_partitions,
                    len(partitions),
                )

            for partition_id, partition in sorted(partitions.items()):

                logger.info(
                    "PARTITION | topic=%s | id=%d | leader=%s | replicas=%s",
                    topic_name,
                    partition_id,
                    partition.leader,
                    partition.replicas,
                )

if __name__ == "__main__":

    settings = Settings()
    setup_logging(settings.log_level)
    logger.info("Starting Kafka Topic Manager")
    manager = KafkaTopicManager(settings)
    manager.create_topic()
    logger.info("Kafka Topic Manager completed")