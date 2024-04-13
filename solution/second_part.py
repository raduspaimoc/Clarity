import os
import time
import sched
import datetime
import threading

from collections import defaultdict
from utils import timestamp_ms_to_sec
from utils import STANDARD_FORMAT_VALUES_IN_FILE, EMPTY_CONNECTION

SCHEDULER_SECONDS_DELAY = 30

# Global variables
connections_by_hour = defaultdict(list)
incoming_connections = defaultdict(set)
outgoing_connections = defaultdict(set)
connection_counts = defaultdict(int)


# Function to parse a single log line
def parse_log_line(line) -> tuple:
    parts = line.strip().split()
    if len(parts) == STANDARD_FORMAT_VALUES_IN_FILE:
        timestamp, source_host, destination_host = parts
        timestamp = timestamp_ms_to_sec(int(timestamp))
        connection_time = datetime.datetime.fromtimestamp(timestamp)
        return connection_time, source_host, destination_host
    return None


def add_connection_info(connection: tuple):
    connection_time, source_host, destination_host = connection
    hour_key = connection_time.strftime('%Y-%m-%d %H')

    # Update connections by hour
    connections_by_hour[hour_key].append((connection_time, source_host, destination_host))

    # Update incoming and outgoing connections
    incoming_connections[destination_host].add(source_host)
    outgoing_connections[source_host].add(destination_host)

    # Update connection counts
    connection_counts[source_host] += 1


def process_log_file(file_path: str):

    with open(file_path, 'r') as file:
        for line in file:
            connection = parse_log_line(line)
            if connection:
                add_connection_info(connection)

    # Move to the end of the file and waits for new data
    with open(file_path, 'r') as file:
        file.seek(0, os.SEEK_END)
        while True:
            line = file.readline()
            if line:
                connection = parse_log_line(line)
                if connection:
                    add_connection_info(connection)
            else:
                time.sleep(1)


# Function to generate hourly summaries
def generate_hourly_summary(configurable_host='Lynnsie', delay: int = SCHEDULER_SECONDS_DELAY):
    current_hour = datetime.datetime.now().strftime('%Y-%m-%d %H')
    prev_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H')

    # Summary for the last hour
    #if prev_hour in connections_by_hour:
    hour_connections = connections_by_hour[prev_hour]

    connected_to_configurable_host = [dest for _, _, dest in hour_connections if _ == configurable_host]

    # Get hostnames that received connections from a configurable host
    received_from_configurable_host = [source for _, source, _ in hour_connections if _ == configurable_host]

    # Get the hostname that generated the most connections
    if connection_counts:
        most_connections_host = max(connection_counts, key=connection_counts.get)

        print(f"Hourly Summary ({prev_hour} - {current_hour}):")
        print(f"Hostnames connected to '{configurable_host}': {connected_to_configurable_host}")
        print(f"Hostnames that received connections from '{configurable_host}': {received_from_configurable_host}")
        print(
            f"Hostname with most connections: {most_connections_host} ({connection_counts[most_connections_host]} connections)")
    else:
        print("No connections recorded yet.")

    # Schedule the next hourly summary
    scheduler.enter(20, 1, generate_hourly_summary)


if __name__ == '__main__':
    file_path = '../datasets/input-file-100002.txt'
    host = 'Lynnsie'

    # Start a separate thread for processing the log file continuously
    log_processing_thread = threading.Thread(target=process_log_file, args=(file_path,))
    log_processing_thread.daemon = True  # Allow the thread to exit when the main program exits
    log_processing_thread.start()

    # Initialize summary scheduler
    scheduler = sched.scheduler(time.time, time.sleep)

    # # Start processing the log file continuously
    # process_log_file(file_path)

    # Schedule the first hourly summary
    scheduler.enter(0, 1, generate_hourly_summary, (scheduler,))

    # Run the scheduler indefinitely
    scheduler.run()
