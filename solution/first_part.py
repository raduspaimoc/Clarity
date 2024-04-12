import datetime

STANDARD_FORMAT_VALUES_IN_FILE = 3
EMPTY_CONNECTION = "*EMPTY*"


def timestamp_ms_to_sec(timestamp_ms) -> int:
    return timestamp_ms / 1000


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
    line_number = 0
    connected_hosts = []
    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split()
            if len(parts) != STANDARD_FORMAT_VALUES_IN_FILE:
                print(f"Line {line_number} doesn't follow file format")
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
    print(f"For host={host} found {len(connected_hosts)} between {init_datetime} and {end_datetime}. "
          f"Hosts connected:\n")
    for host in hosts:
        print(f"{host}")




if __name__ == '__main__':

    file_path = '../datasets/input-file-10000.txt'
    init_datetime = datetime.datetime(year=2019, month=8, day=12) # In file: ('2019-08-12 22:00:04.351000')
    end_datetime = datetime.datetime(year=2019, month=8, day=14)  # In file: ('2019-08-13 21:59:58.341000')
    host = 'Jameice'

    connected_hosts = read_log_file(file_path=file_path, host=host,
                                    init_datetime=init_datetime, end_datetime=end_datetime)
    print(f"Connections log file: {file_path} read succesfully.")
    print_hosts_list(host, connected_hosts)


