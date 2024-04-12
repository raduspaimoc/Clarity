import os
import time
import sched
import datetime
from collections import defaultdict
from utils import timestamp_ms_to_sec
from utils import STANDARD_FORMAT_VALUES_IN_FILE, EMPTY_CONNECTION

# Global variables
connections_by_hour = defaultdict(list)
incoming_connections = defaultdict(set)
outgoing_connections = defaultdict(set)
connection_counts = defaultdict(int)


# Function to parse a single log line
def parse_log_line(line):
    parts = line.strip().split()
    if len(parts) == STANDARD_FORMAT_VALUES_IN_FILE:
        timestamp, source_host, destination_host = parts
        timestamp = timestamp_ms_to_sec(int(timestamp))
        connection_time = datetime.datetime.fromtimestamp(timestamp)
        return connection_time, source_host, destination_host
    return None


def process_log_file(file_path):
    with open(file_path, 'r') as file:
        file.seek(0, os.SEEK_END)  # Move to the end of the file
        while True:
            line = file.readline()
            if line:
                connection = parse_log_line(line)
                if connection:
                    connection_time, source_host, destination_host = connection
                    hour_key = connection_time.strftime('%Y-%m-%d %H')

                    # Update connections by hour
                    connections_by_hour[hour_key].append((connection_time, source_host, destination_host))

                    # Update incoming and outgoing connections
                    incoming_connections[destination_host].add(source_host)
                    outgoing_connections[source_host].add(destination_host)

                    # Update connection counts
                    connection_counts[source_host] += 1
            else:
                time.sleep(1)


# Function to generate hourly summaries
def generate_hourly_summary(configurable_host='Lynnsie'):
    current_hour = datetime.datetime.now().strftime('%Y-%m-%d %H')
    prev_hour = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime('%Y-%m-%d %H')

    # Summary for the last hour
    #if prev_hour in connections_by_hour:
    hour_connections = connections_by_hour[prev_hour]

    connected_to_configurable_host = [dest for _, _, dest in hour_connections if _ == configurable_host]

    # Get hostnames that received connections from a configurable host
    received_from_configurable_host = [source for _, source, _ in hour_connections if _ == configurable_host]

    # Get the hostname that generated the most connections
    most_connections_host = max(connection_counts, key=connection_counts.get)

    print(f"Hourly Summary ({prev_hour} - {current_hour}):")
    print(f"Hostnames connected to '{configurable_host}': {connected_to_configurable_host}")
    print(f"Hostnames that received connections from '{configurable_host}': {received_from_configurable_host}")
    print(
        f"Hostname with most connections: {most_connections_host} ({connection_counts[most_connections_host]} connections)")

    # Schedule the next hourly summary
    scheduler.enter(30, 1, generate_hourly_summary)

if __name__ == '__main__':
    file_path = '../datasets/input-file-100002.txt'
    scheduler = sched.scheduler(time.time, time.sleep)
    host = 'Lynnsie'

    # Start processing the log file continuously
    process_log_file(file_path)

    # Schedule the first hourly summary
    scheduler.enter(0, 1, generate_hourly_summary)

    # Run the scheduler indefinitely
    scheduler.run()