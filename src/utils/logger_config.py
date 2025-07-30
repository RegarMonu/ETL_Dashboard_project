import logging
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime

class AppLogger:
    def __init__(self, logger_name="app_logger"):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)

        log_dir = os.path.join("artifacts", "logs")
        os.makedirs(log_dir, exist_ok=True)

        log_filename = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")

        # Avoid adding handlers multiple times
        if not self.logger.handlers:
            # File handler
            file_handler = TimedRotatingFileHandler(
                filename=log_filename,
                when="midnight",
                interval=1,
                backupCount=7
            )
            file_handler.suffix = "%Y-%m-%d"
            file_handler.setLevel(logging.DEBUG)

            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)

            # Formatter
            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)

            # Add handlers
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

    def get_logger(self):
        return self.logger
