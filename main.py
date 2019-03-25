#!/usr/bin/env python3
import sys
from enum import Enum

from numpy import percentile


class EventType(Enum):
    StartRequest = 1
    BackendConnect = 2
    BackendRequest = 3
    BackendOk = 4
    BackendError = 5
    StartMerge = 6
    StartSendResult = 7
    FinishRequest = 8


class LogParser:
    def __init__(self, path: str, percentile: int):
        self.failed = set()
        self.timestamps = {}
        self.path = path
        self.percentile = percentile

    def parse_line(self, line: str):
        parts = line.strip().split("\t")
        timestamp = int(parts[0])
        request_id = int(parts[1])
        event_type = parts[2]
        connect_id = int(parts[3]) if len(parts) > 3 else None
        if EventType[event_type] == EventType.BackendConnect:
            self.failed.add((request_id, connect_id,))
        elif EventType[event_type] == EventType.BackendOk:
            self.failed.discard((request_id, connect_id,))
        elif EventType[event_type] == EventType.StartRequest:
            self.timestamps[request_id] = timestamp
        elif EventType[event_type] == EventType.FinishRequest:
            self.timestamps[request_id] = timestamp - self.timestamps[request_id]

    def parse(self):
        with open(self.path, "r") as file:
            for line in file:
                self.parse_line(line)

    @property
    def results(self):
        diff_timestamps = [value for value in self.timestamps.values()]
        percentile_95 = percentile(diff_timestamps, self.percentile) / 1e6
        number_failed = len(set(e[0] for e in self.failed))
        return [number_failed, percentile_95]

    @property
    def result_message(self):
        message = ("Number failed responses:\t{}\n"
                   "95th percentile:        \t{}").format(*self.results)
        return message

    def write_results(self, path: str):
        with open(path, "w") as file:
            file.write(self.result_message)

    def print_results(self):
        print(self.result_message)


def main():
    filename = "input.txt"
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    parser = LogParser(filename, 95)
    parser.parse()
    parser.print_results()
    parser.write_results("output.txt")


if __name__ == '__main__':
    main()
