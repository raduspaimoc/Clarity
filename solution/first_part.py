import os
import datetime

from utils import timestamp_ms_to_sec, create_logger
from utils import (
    STANDARD_FORMAT_VALUES_IN_FILE,
    EMPTY_CONNECTION,
    DEFAULT_FILE_PATH,
    DEFAULT_HOST,
    DEFAULT_INIT_DATETIME,
    DEFAULT_END_DATETIME
)

DEFAULT_LOGS_DIR = 'logs'
DEFAULT_LOGS_FILE_NAME = 'first_part.log'

def filter_connection(host: str,
                     init_datetime: datetime.datetime,
                     end_datetime: datetime.datetime,
                     file_line: list[str]) -> str:
    # Parse file line
    timestamp, source_host, destination_host = file_line
    timestamp = timestamp_ms_to_sec(int(timestamp))
    connection_time = datetime.datetime.fromtimestamp(timestamp)

    if init_datetime <= connection_time <= end_datetime:
        if source_host == host:
            return destination_host
        elif destination_host == host:
            return source_host
    return EMPTY_CONNECTION


def read_log_file(file_path: str,
                  host: str,
                  init_datetime: datetime.datetime,
                  end_datetime: datetime.datetime) -> list:
    global logger
    line_number = 0
    connected_hosts = []
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) != STANDARD_FORMAT_VALUES_IN_FILE:
                logger.info(f"Line {line_number} doesn't follow file format")
            else:
                status = filter_connection(host=host,
                                  init_datetime=init_datetime,
                                  end_datetime=end_datetime,
                                  file_line=parts)
                if status != EMPTY_CONNECTION:
                    connected_hosts.append(status)
            line_number += 1
    return connected_hosts


def print_hosts_list(host: str, hosts: list):
    global logger
    logger.info(f"For host={host} found {len(connected_hosts)} between {init_datetime} and {end_datetime}. "
                f"Hosts connected:\n")
    for host in hosts:
        logger.info(f"{host}")


if __name__ == '__main__':

    script_dir = os.path.dirname(os.path.abspath(__file__))

    file_path = os.environ.get("FILE_PATH", DEFAULT_FILE_PATH)
    init_datetime = datetime.datetime.strptime(
        os.environ.get("INIT_DATETIME", DEFAULT_INIT_DATETIME), "%Y-%m-%d %H:%M:%S.%f") # In file: ('2019-08-12 22:00:04.351000')
    end_datetime = datetime.datetime.strptime(
        os.environ.get("END_DATETIME", DEFAULT_END_DATETIME), "%Y-%m-%d %H:%M:%S.%f")  # In file: ('2019-08-13 21:59:58.341000')
    host = os.environ.get("HOST", DEFAULT_HOST)

    logs_path = os.path.join(script_dir, "..\\" + DEFAULT_LOGS_DIR)
    os.makedirs(logs_path, exist_ok=True)
    logger = create_logger(path=logs_path, logs_file_name=DEFAULT_LOGS_FILE_NAME, logger_name='second_part')

    connected_hosts = read_log_file(file_path=file_path, host=host,
                                    init_datetime=init_datetime, end_datetime=end_datetime)
    logger.info(f"Connections log file: {file_path} read succesfully.")
    print_hosts_list(host, connected_hosts)



