#!/usr/bin/env python3
from datetime import datetime
from enum import Enum

import numpy as np

LOG_FILENAME = "003.in"

failed = set()
timestamps = {}


class EventType(Enum):
    StartRequest = 1
    BackendConnect = 2
    BackendRequest = 3
    BackendOk = 4
    BackendError = 5
    StartMerge = 6
    StartSendResult = 7
    FinishRequest = 8


def parse_line(line: str):
    parts = line.strip().split("\t")
    timestamp = int(parts[0])
    request_id = int(parts[1])
    event_type = parts[2]
    if EventType[event_type] == EventType.BackendError and request_id not in failed:
        failed.add(request_id)
    elif EventType[event_type] == EventType.StartRequest:
        timestamps[request_id] = timestamp
    elif EventType[event_type] == EventType.FinishRequest:
        timestamps[request_id] = timestamp - timestamps[request_id]


def main():
    with open(LOG_FILENAME, "r") as file:
        for line in file:
            parse_line(line)
    diff_timestamps = [value for value in timestamps.values()]
    percentile_95 = percentile(diff_timestamps, 95) / 1e6
    print(f"Number failed responses: {len(failed)}")
    print(f"95th percentile: {percentile_95}")


if __name__ == '__main__':
    main()
