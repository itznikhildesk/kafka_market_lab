
import logging

import orjson

from confluent_kafka import Consumer

from src.config import Settings
from src.events import (
    PriceEvent,
    TradeEvent,
    MarketMetricEvent,
)
from src.calculations import (
    percentage_change,
    trade_notional,
    market_breadth,
)

logger = logging.getLogger(__name__)

class MarketConsumer:

    def __init__(
            self,
            settings: Settings,
            worker_id: str,
            group_id: str) -> None:

        self.settings = settings
        self.worker_id = worker_id
        self.group_id = group_id

        config = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": f"consumer-{worker_id}",
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }

        self.consumer = Consumer(config)

        self.previous_prices: dict[str, float] = {}

        self.trade_totals: dict[str, dict] = {}

        self.last_assignment: set[tuple[str, int]] = set()

        logger.info(
            "CONSUMER INITIALIZED | worker=%s | group=%s",
            worker_id,
            group_id,
        )


    def run(self, topics: list[str]) -> None:

        self.consumer.subscribe(topics)

        logger.info(
            "SUBSCRIBED | worker=%s | topics=%s",
            self.worker_id,
            topics,
        )

        try:

            while True:

                message = self.consumer.poll(1.0)

                self._log_assignment_changes()

                if message is None:
                    continue

                if message.error():
                    logger.warning(
                        "CONSUMER MESSAGE ERROR | %s",
                        message.error(),
                    )

                    continue

                try:

                    self._process_message(message)

                    # Commit only after successful processing.

                    self.consumer.commit(
                        message=message,
                        asynchronous=False,
                    )

                    logger.debug(
                        "OFFSET COMMITTED | topic=%s | partition=%s | offset=%s",
                        message.topic(),
                        message.partition(),
                        message.offset() + 1,
                    )

                except Exception:

                    logger.exception(
                        "PROCESSING FAILED | topic=%s | partition=%s | offset=%s",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                    )

                    raise

        except KeyboardInterrupt:

            logger.info(
                "CONSUMER STOPPING | worker=%s",
                self.worker_id,
            )

        finally:

            self.consumer.close()

            logger.info(
                "CONSUMER CLOSED | worker=%s",
                self.worker_id,
            )


    def _log_assignment_changes(self) -> None:

        assignment = self.consumer.assignment()

        current = {
            (partition.topic, partition.partition)
            for partition in assignment
        }

        if current != self.last_assignment:
            logger.info(
                "PARTITION ASSIGNMENT | worker=%s | partitions=%s",
                self.worker_id,
                sorted(current),
            )

            self.last_assignment = current


    def _process_message(self, message) -> None:
        topic = message.topic()

        payload = orjson.loads(message.value())

        key = message.key()

        if key is not None:
            key = key.decode("utf-8")

        context = (
            f"worker={self.worker_id} "
            f"partition={message.partition()} "
            f"offset={message.offset()} "
            f"key={key}"
        )

        if topic == self.settings.topic_prices:

            event = PriceEvent.model_validate(payload)

            previous = self.previous_prices.get(event.symbol)

            change = percentage_change(
                previous,
                event.price,
            )

            self.previous_prices[event.symbol] = event.price

            change_text = (
                "FIRST EVENT"
                if change is None
                else f"{change:+.3f}%"
            )

            logger.info(
                "PRICE | %s | symbol=%s | price=%.2f | change=%s | volume=%s",
                context,
                event.symbol,
                event.price,
                change_text,
                event.day_volume,
            )

        elif topic == self.settings.topic_trades:

            event = TradeEvent.model_validate(payload)

            notional = trade_notional(
                event.price,
                event.quantity,
            )

            stats = self.trade_totals.setdefault(
                event.symbol,
                {
                    "quantity": 0,
                    "turnover": 0.0,
                    "trades": 0,
                },
            )

            stats["quantity"] += event.quantity

            stats["turnover"] += notional

            stats["trades"] += 1

            logger.info(
                "TRADE | %s | symbol=%s | side=%s | price=%.2f "
                "| quantity=%s | notional=%.2f | running_turnover=%.2f",
                context,
                event.symbol,
                event.side,
                event.price,
                event.quantity,
                notional,
                stats["turnover"],
            )

            # MARKET METRICS EVENTS

        elif topic == self.settings.topic_metrics:

            event = MarketMetricEvent.model_validate(payload)

            breadth = market_breadth(
                event.advances,
                event.declines,
            )

            logger.info(
                "MARKET METRIC | %s | index=%s | value=%.2f "
                "| advances=%s | declines=%s | breadth=%.2f%%",
                context,
                event.index,
                event.index_value,
                event.advances,
                event.declines,
                breadth,
            )

        else:

            logger.warning(
                "UNKNOWN TOPIC | %s",
                topic,
            )

