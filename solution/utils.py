import os
import logging

DEFAULT_HOST = 'Lynnsie'
DEFAULT_FILE_PATH = '../datasets/input-file-10000.txt'
DEFAULT_INIT_DATETIME = '2019-08-12 22:00:04.351000'
DEFAULT_END_DATETIME = '2019-08-13 21:59:58.341000'
STANDARD_FORMAT_VALUES_IN_FILE = 3
EMPTY_CONNECTION = "*EMPTY*"
STANDARD_LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'


def timestamp_ms_to_sec(timestamp_ms) -> int:
    return timestamp_ms / 1000


def create_logger(path: str, logs_file_name: str, logger_name: str = 'second_part') -> logging.Logger:

    # Configure root logger to write to a file
    log_file_path = os.path.join(path, logs_file_name)
    logging.basicConfig(
        filename=log_file_path,
        level=logging.DEBUG,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format=STANDARD_LOG_FORMAT
    )
    logger = logging.getLogger(logger_name)
    # Create a StreamHandler to output log messages to the console (terminal)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)  # Set the level for the console output
    console_handler.setFormatter(logging.Formatter(STANDARD_LOG_FORMAT))

    # Add the console handler to the logger
    logger.addHandler(console_handler)
    return logger