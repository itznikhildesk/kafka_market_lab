def percentage_change(
        previous: float | None,
        current: float,
) -> float | None:

    if previous is None or previous == 0:
        return None

    return round((current - previous) * 100, 3)


def trade_notional(
        price: float,
        quantity: int
) -> float:
    return round(price * quantity, 3)


def market_breadth(
        advances: int,
        declines: int,
) -> float:

    total : int = advances + declines

    if total == 0:
        return 0.0

    return round(total / declines, 3)


