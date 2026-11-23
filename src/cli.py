
import argparse
import logging

from src.config import get_settings
from src.logging_config import setup_logging
from src.kafka_admin import KafkaTopicManager
from src.producers import (
    PriceProducer,
    MarketEventsProducer,
)
from src.consumer import MarketConsumer


logger = logging.getLogger(__name__)


def main() -> None:

    settings = get_settings()

    setup_logging(settings.log_level)

    parser = argparse.ArgumentParser(
        description="Kafka Market Streaming Learning Lab"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
    )

    # Create topics

    subparsers.add_parser("create-topics")

    # Producer 1

    subparsers.add_parser("producer-prices")

    # Producer 2

    subparsers.add_parser("producer-events")

    # Consumer

    consumer_parser = subparsers.add_parser("consume")

    consumer_parser.add_argument(
        "--worker-id",
        default="worker-1",
    )

    consumer_parser.add_argument(
        "--group-id",
        default=settings.kafka_group_id,
    )

    consumer_parser.add_argument(
        "--topics",
        nargs="+",
        default=None,
    )

    args = parser.parse_args()

    if args.command == "create-topics":

        manager = KafkaTopicManager(settings)

        manager.create_topic()

    elif args.command == "producer-prices":

        producer = PriceProducer(settings)

        try:
            producer.run()

        except KeyboardInterrupt:
            logger.info("PRICE PRODUCER INTERRUPTED")

        finally:
            producer.close()

    elif args.command == "producer-events":

        producer = MarketEventsProducer(settings)

        try:
            producer.run()

        except KeyboardInterrupt:
            logger.info("MARKET EVENTS PRODUCER INTERRUPTED")

        finally:
            producer.close()

    elif args.command == "consume":

        topics = args.topics or settings.all_topics

        consumer = MarketConsumer(
            settings=settings,
            worker_id=args.worker_id,
            group_id=args.group_id,
        )

        consumer.run(topics)
