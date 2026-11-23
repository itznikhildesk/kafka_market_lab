from datetime import datetime, timezone
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

class EventBase(BaseModel):

    model_config: ConfigDict = ConfigDict(
        frozen=True,
        extra="forbid",
        validate_assignment=True,
        str_strip_whitespace=True,
    )

    event_id: UUID = Field(default_factory=uuid4)

    event_time: datetime = Field(
        default_factory=lambda : datetime.now(timezone.utc)
    )


class PriceEvent(EventBase):

    event_typed: Literal["price"] = "price"

    symbol: str

    price: float = Field(gt=0)

    day_volume: int = Field(ge=0)

    source: str = "simulator"


class TradeEvent(EventBase):

    event_type: Literal["trade"] = "trade"

    trade_id: UUID = Field(default_factory=uuid4)

    symbol: str

    side: Literal["BUY", "SELL"]

    price: float = Field(gt=0)

    quantity: int = Field(gt=0)


class MarketMetricEvent(EventBase):

    event_type: Literal["market_metric"] = "market_metric"

    index: str

    index_value: float = Field(gt=0)

    advances: int = Field(ge=0)

    declines: int = Field(ge=0)


