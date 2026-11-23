import logging

from src.logging_config import setup_logging


def main() -> None:

    setup_logging("DEBUG")

    logger = logging.getLogger(__name__)

    logger.info("Application starting")
    logger.debug("Loading Kafka settings")
    logger.warning("Testing warning message")
    logger.error("Testing error message")
    logger.critical("Testing critical message")

    print("Application finished")


if __name__ == "__main__":
    main()