import logging

from colorlog import ColoredFormatter

def setup_logging(level: str = "INFO") -> None:

    handler = logging.StreamHandler()

    formatter = ColoredFormatter(
        "%(log_color)s%(asctime)s | %(levelname)-8s | "
        "%(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_purple",
        },
    )

    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    root.handlers.clear()
    root.addHandler(handler)
