import logging
import os
from logging.handlers import RotatingFileHandler


LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging(config) -> None:
    root_logger = logging.getLogger()
    if getattr(root_logger, "_ufc_logging_configured", False):
        return

    log_dir = config.LOG_DIR
    log_file = config.LOG_FILE
    log_level_name = str(config.LOG_LEVEL).upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter(LOG_FORMAT)
    file_handler = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger._ufc_logging_configured = True

