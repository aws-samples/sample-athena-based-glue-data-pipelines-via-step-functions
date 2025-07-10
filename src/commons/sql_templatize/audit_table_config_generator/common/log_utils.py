import logging
import sys


def setup_logger(
    set_root: bool = True,
    log_format: str = "%(asctime)s %(levelname)s %(module)s.%(funcName)s() ::: %(message)s",
) -> logging.Logger:
    logger = logging.getLogger()

    while logger.handlers:
        logger.handlers.pop()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.info("Logger setup")
    if set_root:
        logging.root = logger

    return logger
