import datetime

STANDARD_FORMAT_VALUES_IN_FILE = 3


def timestamp_ms_to_sec(timestamp_ms) -> int:
    return timestamp_ms / 1000


def read_log_file(file_path: str) -> list:
    line_number = 0
    connections_info = []
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) != STANDARD_FORMAT_VALUES_IN_FILE:
                print(f"Line {line_number} doesn't follow file format")
            else:
                timestamp, source_host, destination_host = parts
                timestamp = timestamp_ms_to_sec(int(timestamp))
                connection_time = datetime.datetime.fromtimestamp(timestamp)
                connections_info.append((
                    connection_time,
                    source_host,
                    destination_host
                ))
            line_number += 1
    return connections_info


def search_connected_host(init_datetime: datetime.datetime,
                          end_datetime: datetime.datetime,
                          host: str,
                          connections_info: list) -> list:

    connections_during_time_range = []
    for connection in connections_info:
        connection_time = connection[0]
        source_host = connection[1]
        destination_host = connection[2]

        if init_datetime <= connection_time <= end_datetime:
            if source_host == host:
                connections_during_time_range.append(destination_host)
            if destination_host == host:
                connections_during_time_range.append(source_host)
    return connections_during_time_range


def print_hosts_list(host: str, hosts: list):
    print(f"For host={host} found {len(connected_hosts)} between {init_datetime} and {end_datetime}. "
          f"Hosts connected:\n")
    for host in hosts:
        print(f"{host}")




if __name__ == '__main__':

    file_path = '../datasets/input-file-10000.txt'
    init_datetime = datetime.datetime(year=2019, month=8, day=12) # In file: ('2019-08-12 22:00:04.351000')
    end_datetime = datetime.datetime(year=2019, month=8, day=14)  # In file: ('2019-08-13 21:59:58.341000')
    host = 'Lynnsie'


    connections_info = read_log_file(file_path=file_path)
    print(f"Connections log file: {file_path} read succesfully.")
    connected_hosts = search_connected_host(init_datetime, end_datetime, host, connections_info)
    print_hosts_list(host, connected_hosts)


