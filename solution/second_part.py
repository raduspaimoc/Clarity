import os
import time
import sched
import datetime
import threading
from collections import defaultdict
from utils import timestamp_ms_to_sec, STANDARD_FORMAT_VALUES_IN_FILE

SCHEDULER_SECONDS_DELAY = 30

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
            if len(connections_by_hour[hour_key]) > 3600:  # Limit to one hour of data (assuming 1 connection per second)
                connections_by_hour[hour_key].pop(0)

            # Update incoming and outgoing connections
            incoming_connections[hour_key][destination_host].add(source_host)
            outgoing_connections[hour_key][source_host].add(destination_host)

            # Update connection counts
            connection_counts[hour_key][source_host] += 1
            connection_counts[hour_key][destination_host] += 1


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
    start_hour_key = start_time.strftime('%Y-%m-%d %H')

    # Filter connections for the last hour
    connected_to_configurable_host = []
    received_from_configurable_host = []
    # Aggregate statistics within the last hour (sliding window)
    for hour_key in list(connections_by_hour.keys()):
        if hour_key >= start_hour_key:
            for conn_time, source, dest in connections_by_hour[hour_key]:
                if start_time <= conn_time <= end_time:
                    if dest == configurable_host:
                        connected_to_configurable_host.append(source)
                    if source == configurable_host:
                        received_from_configurable_host.append(dest)

    # Determine the hostname with the most connections within the last hour
    if connection_counts:
        last_hour_counts = connection_counts[start_hour_key]
        most_connections_host = max(last_hour_counts, key=last_hour_counts.get)

        print(f"Hourly Summary ({start_time} - {end_time}):")
        print(f"Hostnames connected to '{configurable_host}': {connected_to_configurable_host}")
        print(f"Hostnames that received connections from '{configurable_host}': {received_from_configurable_host}")
        print(
            f"Hostname with most connections: {most_connections_host} ({last_hour_counts[most_connections_host]} connections)")
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