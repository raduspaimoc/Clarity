import unittest
import datetime
from solution.first_part import read_log_file


class TestReadLogFile(unittest.TestCase):
    def test_read_log_file_most_values(self):
        file_path = '../datasets/input-file-10000.txt'
        init_datetime = datetime.datetime(year=2019, month=8, day=12, hour=22, minute=0, second=4, microsecond=351000)
        end_datetime = datetime.datetime(year=2019, month=8, day=14, hour=21, minute=59, second=58, microsecond=341000)
        host = 'Lynnsie'

        connected_hosts = read_log_file(file_path, host, init_datetime, end_datetime)

        # Assert that the size of connected_hosts is 23 when host is 'Lynnsie'
        self.assertEqual(len(connected_hosts), 24)

    def test_read_log_file_most_values_reduced(self):
        file_path = '../datasets/input-file-10000.txt'
        init_datetime = datetime.datetime(year=2019, month=8, day=12, hour=22, minute=0, second=4, microsecond=351000)
        end_datetime = datetime.datetime(year=2019, month=8, day=13, hour=12, minute=59, second=58, microsecond=341000)
        host = 'Lynnsie'

        connected_hosts = read_log_file(file_path, host, init_datetime, end_datetime)

        # Assert that the size of connected_hosts is 23 when host is 'Lynnsie'
        self.assertEqual(len(connected_hosts), 14)

    def test_read_log_file_second_most_values(self):
        file_path = '../datasets/input-file-10000.txt'
        init_datetime = datetime.datetime(year=2019, month=8, day=12, hour=22, minute=0, second=4, microsecond=351000)
        end_datetime = datetime.datetime(year=2019, month=8, day=14, hour=22, minute=59, second=58, microsecond=341000)
        host = 'Tilda'

        connected_hosts = read_log_file(file_path, host, init_datetime, end_datetime)

        # Assert that the size of connected_hosts is 20 when host is 'Lynnsie'
        self.assertEqual(len(connected_hosts), 20)

    def test_read_log_file_least_values(self):
        file_path = '../datasets/input-file-10000.txt'
        init_datetime = datetime.datetime(year=2019, month=8, day=12, hour=22, minute=0, second=4, microsecond=351000)
        end_datetime = datetime.datetime(year=2019, month=8, day=14, hour=22, minute=59, second=58, microsecond=341000)
        host = 'Sharon'

        connected_hosts = read_log_file(file_path, host, init_datetime, end_datetime)

        # Assert that the size of connected_hosts is 20 when host is 'Lynnsie'
        self.assertEqual(len(connected_hosts), 2)


if __name__ == '__main__':
    unittest.main()
