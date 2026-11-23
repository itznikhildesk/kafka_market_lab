import logging
import random
import time

from src.config import Settings
from src.events import (
    PriceEvent,
    TradeEvent,
    MarketMetricEvent,
)
from src.producer_base import KafkaPublisher
from src.logging_config import setup_logging

logger = logging.getLogger(__name__)


class PriceProducer:

    def __init__(self, settings: Settings):

        self.settings = settings

        self.publisher = KafkaPublisher(
            settings=settings,
            client_id="price-producer",
        )

        self.prices = {
            "RELIANCE": 1450.0,
            "TCS": 3200.0,
            "INFY": 1650.0,
            "HDFC_BANK": 1750.0,
        }

        logger.info("PRICE PRODUCER INITIALIZED")

    def run(self) -> None:

        logger.info(
            "PRICE PRODUCER STARTED | topic=%s",
            self.settings.topic_prices
        )

        while True:

            for symbol, previous_price in list(self.prices.items()):

                movement = random.uniform(-2.0, 2.0)

                current_price = round(
                    max(1.0, previous_price + movement),
                    2,
                )

                self.prices[symbol] = current_price

                event = PriceEvent(
                    symbol=symbol,
                    price=current_price,
                    day_volume=random.randint(1000, 100000)
                )

                self.publisher.publish(
                    topic=self.settings.topic_prices,
                    key=symbol,
                    event=event,
                )

                time.sleep(self.settings.price_interval_seconds)

    def close(self) -> None:

        self.publisher.close()


class MarketEventsProducer:

    def __init__(self, settings: Settings):

        self.settings = settings

        self.publisher = KafkaPublisher(
            settings,
            client_id="market-events-producer",
        )

        self.symbols = [
            "RELIANCE",
            "TCS",
            "INFY",
            "HDFC_BANK",
        ]

        self.prices = {
            "RELIANCE": 1450.0,
            "TCS": 3200.0,
            "INFY": 1650.0,
            "HDFC_BANK": 1750.0,
        }

        logger.info("MARKET EVENTS PRODUCER INITIALIZED")

    def run(self) -> None:

        logger.info(
            "MARKET EVENTS PRODUCER STARTED | topics=%s,%s",
            self.settings.topic_trades,
            self.settings.topic_metrics,
        )

        next_metrics_time = time.monotonic()

        while True:

            symbol = random.choice(self.symbols)

            price = round(
                self.prices[symbol] + random.uniform(-3.0, 3.0),
                2,
            )

            side = random.choice(["BUY", "SELL"])

            quantity = random.choice([1, 5, 10, 20, 50, 100])

            trade = TradeEvent(
                symbol=symbol,
                side=side,
                price=price,
                quantity=quantity,
            )

            self.publisher.publish(
                topic=self.settings.topic_trades,
                key=symbol,
                event=trade,
            )

            now = time.monotonic()

            if now >= next_metrics_time:

                advances = random.randint(20, 40)
                declines = 50 - advances

                metric = MarketMetricEvent(
                    index="NIFTY50",
                    index_value=round(
                        24500 + random.uniform(-40, 40),
                        2,
                    ),
                    advances=advances,
                    declines=declines,
                )

                self.publisher.publish(
                    topic=self.settings.topic_metrics,
                    key="NIFTY50",
                    event=metric,
                )

                next_metrics_time = (
                    now + self.settings.metrics_interval_seconds
                )

            time.sleep(self.settings.trade_interval_seconds)

    def close(self) -> None:

        self.publisher.close()

if __name__ == "__main__":
    settings = Settings()
    setup_logging(settings.log_level)
    producer = MarketEventsProducer(settings)
    producer.run()
