import os
import time
import sched
import datetime
import threading
from collections import defaultdict
from utils import timestamp_ms_to_sec, STANDARD_FORMAT_VALUES_IN_FILE

SCHEDULER_SECONDS_DELAY = 30

# Global variables
connections_by_hour = defaultdict(list)
incoming_connections = defaultdict(set)
outgoing_connections = defaultdict(set)
connection_counts = defaultdict(int)


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
            incoming_connections[destination_host].add(source_host)
            outgoing_connections[source_host].add(destination_host)

            # Update connection counts
            connection_counts[source_host] += 1
            connection_counts[destination_host] += 1


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
                    update_hourly_stats(connection)
            else:
                time.sleep(1)


def update_hourly_summaries(scheduler, configurable_host='Lynnsie'):
    """Update hourly summaries based on the last hour's data."""
    current_time = datetime.datetime.now()
    start_time = current_time - datetime.timedelta(hours=1)
    end_time = current_time

    # Filter connections for the last hour
    connected_to_configurable_host = []
    received_from_configurable_host = []
    for hour_key, connections in connections_by_hour.items():
        for connection in connections:
            conn_time, source, dest = connection
            if start_time <= conn_time <= end_time:
                if dest == configurable_host:
                    connected_to_configurable_host.append(source)
                if source == configurable_host:
                    received_from_configurable_host.append(source)


    # recent_hour_connections = [
    #     (conn_time, source, dest) for hour_key, connections in connections_by_hour.items()
    #     for conn_time, source, dest in connections if start_time <= conn_time <= end_time
    # ]

    # # Extract relevant information from connections
    # connected_to_configurable_host = [dest for _, _, dest in recent_hour_connections if _ == configurable_host]
    # received_from_configurable_host = [source for _, source, _ in recent_hour_connections if _ == configurable_host]

    # Determine the hostname with the most connections
    if connection_counts:
        most_connections_host = max(connection_counts, key=connection_counts.get)

        print(f"Hourly Summary ({start_time} - {end_time}):")
        print(f"Hostnames connected to '{configurable_host}': {connected_to_configurable_host}")
        print(f"Hostnames that received connections from '{configurable_host}': {received_from_configurable_host}")
        print(f"Hostname with most connections: {most_connections_host} ({connection_counts[most_connections_host]} connections)")
    else:
        print("No connections recorded yet.")

    # Schedule the next hourly summary
    scheduler.enter(SCHEDULER_SECONDS_DELAY, 1, update_hourly_summaries, (scheduler,))


if __name__ == '__main__':
    file_path = '../datasets/input-file-100002.txt'

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