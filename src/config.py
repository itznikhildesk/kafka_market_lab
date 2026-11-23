from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Kafka connection
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_client_id: str = "market-lab"
    kafka_group_id: str = "market-lab-workers"

    # Topic configuration
    kafka_partitions: int = Field(default=3, ge=1)
    kafka_replication_factor: int = Field(default=1, ge=1)

    topic_prices: str = "market.prices"
    topic_trades: str = "market.trades"
    topic_metrics: str = "market.metrics"

    # Producer configuration
    price_interval_seconds: float = 1.0
    trade_interval_seconds: float = 0.8
    metrics_interval_seconds: float = 3.0

    # Logging
    log_level: str = "INFO"

    @property
    def all_topics(self) -> list[str]:
        return [
            self.topic_prices,
            self.topic_trades,
            self.topic_metrics,
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()


if __name__ == "__main__":
    settings = get_settings()

    print(settings.all_topics)

    for k, v in settings.__dict__.items():
        print(k, v)
