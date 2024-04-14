import os
import time
import sched
import logging
import datetime
import threading
import configparser
from collections import defaultdict
from utils import (timestamp_ms_to_sec,
                   create_logger,
                   STANDARD_FORMAT_VALUES_IN_FILE,
                   STANDARD_LOG_FORMAT)

# Initialize the ConfigParser object
config = configparser.ConfigParser()

# Load the configuration file
config.read('../config/second_part_config.conf')

# Access settings from the configuration file
datasets_dir = config.get('Paths', 'datasets_directory')
logs_dir = config.get('Paths', 'logs_directory')
file_name = config.get('Paths', 'file_name')
host = config.get('Server', 'host_name')
logs_file_name = 'second_part.log'
logger = logging.getLogger()


SCHEDULER_SECONDS_DELAY = 3600
DEFAULT_HOST = 'Lynnsie'

# Global variables (use a sliding window approach)
connections_by_hour = defaultdict(list)
incoming_connections = defaultdict(lambda: defaultdict(set))  # Nested defaultdict of sets
outgoing_connections = defaultdict(lambda: defaultdict(set))  # Nested defaultdict of sets
connection_counts = defaultdict(lambda: defaultdict(int))  # Nested defaultdict for connection counts


def parse_log_line(line) -> tuple:
    """Parse a single log line and extract connection information."""
    parts = line.strip().split()
    if len(parts) == STANDARD_FORMAT_VALUES_IN_FILE:
        timestamp, source_host, destination_host = parts
        timestamp = timestamp_ms_to_sec(int(timestamp))
        connection_time = datetime.datetime.fromtimestamp(timestamp)
        return connection_time, source_host, destination_host
    return None


def update_hourly_stats(connection: tuple):
    """Update hourly statistics based on a new connection."""
    if connection:
        connection_time, source_host, destination_host = connection
        current_time = datetime.datetime.now()
        start_time = current_time - datetime.timedelta(hours=1)
        if start_time <= connection_time <= current_time:
            hour_key = connection_time.strftime('%Y-%m-%d %H')

            # Update connections by hour
            connections_by_hour[hour_key].append((connection_time, source_host, destination_host))

            # Update incoming and outgoing connections
            incoming_connections[hour_key][destination_host].add(source_host)
            outgoing_connections[hour_key][source_host].add(destination_host)

            # Update connection counts
            connection_counts[hour_key][source_host] += 1
            connection_counts[hour_key][destination_host] += 1


def clear_previous_hour_date():
    """Clear data for the specified previous hour."""
    current_key = datetime.datetime.now().strftime('%Y-%m-%d %H')
    keys_to_delete = []
    for hour_key in list(connections_by_hour.keys()):
        if hour_key < current_key:
            keys_to_delete.append(hour_key)

    for hour_key in keys_to_delete:
        if hour_key in connections_by_hour:
            del connections_by_hour[hour_key]
        if hour_key in incoming_connections:
            del incoming_connections[hour_key]
        if hour_key in outgoing_connections:
            del outgoing_connections[hour_key]
        if hour_key in connection_counts:
            del connection_counts[hour_key]


def process_log_file(file_path: str):
    """Process log file continuously, updating statistics for new connections."""
    with open(file_path, 'r') as file:
        for line in file:
            connection = parse_log_line(line)
            if connection:
                update_hourly_stats(connection)

    # Monitor file for new data and update statistics in real-time
    with open(file_path, 'r') as file:
        file.seek(0, os.SEEK_END)
        while True:
            line = file.readline()
            if line:
                connection = parse_log_line(line)
                if connection:
                    clear_previous_hour_date()
                    update_hourly_stats(connection)
            else:
                time.sleep(1)


def print_summary(start_time: datetime.datetime, end_time: datetime.datetime,
                  configurable_host: str, last_hour_counts: defaultdict):
    # Print the hourly summary
    most_connections_host = max(last_hour_counts, key=last_hour_counts.get)
    start_hour_key = start_time.strftime('%Y-%m-%d %H')
    logger.info(f"Hourly Summary ({start_time} - {end_time}):")
    logger.info(
        f"Hostnames connected to '{configurable_host}': {incoming_connections[start_hour_key][configurable_host]}")
    logger.info(
        f"Hostnames that received connections from '{configurable_host}': {outgoing_connections[start_hour_key][configurable_host]}")
    logger.info(
        f"Hostname with most connections: {most_connections_host} ({last_hour_counts[most_connections_host]} connections)")


def update_hourly_summaries(scheduler, configurable_host=host):
    global logger
    """Update hourly summaries based on the last hour's data."""
    current_time = datetime.datetime.now()
    start_time = current_time - datetime.timedelta(hours=1)
    end_time = current_time
    start_hour_key = start_time.strftime('%Y-%m-%d %H')

    # Aggregate connection counts within the last hour
    last_hour_counts = defaultdict(int)
    for hour_key in list(connection_counts.keys()):
        if hour_key >= start_hour_key:
            for host, count in connection_counts[hour_key].items():
                last_hour_counts[host] += count

    # Determine the hostname with the most connections within the last hour
    if last_hour_counts:
        print_summary(start_time=start_time,
                      end_time=end_time,
                      configurable_host=configurable_host,
                      last_hour_counts=last_hour_counts)
    else:
        logger.info("No connections recorded yet.")

    # Schedule the next hourly summary
    scheduler.enter(SCHEDULER_SECONDS_DELAY, 1, update_hourly_summaries, (scheduler,))


if __name__ == '__main__':

    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "..\\" + datasets_dir, file_name)
    logs_path = os.path.join(script_dir, "..\\" + logs_dir)
    os.makedirs(logs_path, exist_ok=True)
    logger = create_logger(path=logs_path, logs_file_name=logs_file_name, logger_name='second_part')

    # Start a separate thread for processing the log file continuously
    log_processing_thread = threading.Thread(target=process_log_file, args=(file_path,))
    log_processing_thread.daemon = True  # Allow the thread to exit when the main program exits
    log_processing_thread.start()

    # Initialize summary scheduler
    scheduler = sched.scheduler(time.time, time.sleep)

    # Schedule the first hourly summary
    scheduler.enter(0, 1, update_hourly_summaries, (scheduler,))

    # Run the scheduler indefinitely
    scheduler.run()
