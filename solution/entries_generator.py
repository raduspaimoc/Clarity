import time
import random
import datetime
from utils import timestamp_ms_to_sec
from utils import STANDARD_FORMAT_VALUES_IN_FILE


# Function to read existing entries from a file
def read_existing_entries(file_path: str) -> set:
    unique_hostnames = set()
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) == STANDARD_FORMAT_VALUES_IN_FILE:
                parts = line.strip().split()
                timestamp, source_host, destination_host = parts
                unique_hostnames.add(source_host)
                unique_hostnames.add(destination_host)
    return unique_hostnames


def get_random_hostname(hostnames: set) -> set:
    random_hostname = random.choice(list(hostnames))
    return random_hostname


def generate_current_timestamp() -> int:
    current_time = datetime.datetime.now()  # Current datetime object
    timestamp_ms = int(current_time.timestamp() * 1000)  # Convert to timestamp with milliseconds
    return timestamp_ms


def add_random_entries(file_path: str, num_entries_to_add: int, destination_host: str = ''):
    # Read existing entries from the file
    hosts_set = read_existing_entries(file_path)

    # Generate and append new random entries
    with open(file_path, 'a') as file:
        for index in range(num_entries_to_add):
            current_timestamp = generate_current_timestamp()
            source_host = get_random_hostname(hosts_set)
            if destination_host == '':
                destination_host = get_random_hostname(hosts_set - {source_host})
            entry = f"{current_timestamp} {source_host} {destination_host}\n"
            file.write(entry)


if __name__ == '__main__':
    file_path = '../datasets/input-file-100002.txt'
    num_entries_to_add = 5  # Number of random entries to add
    destination_host = 'Lynnsie'

    while True:
        add_random_entries(file_path, num_entries_to_add, destination_host=destination_host)
        print(f"Added {num_entries_to_add} random entries to {file_path}.")
        time.sleep(15)
