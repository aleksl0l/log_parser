#!/usr/bin/env python3
from datetime import datetime
from enum import Enum

import numpy as np

LOG_FILENAME = "003.in"

failed = set()
times = {}


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
        times[request_id] = get_datetime(timestamp)
    elif EventType[event_type] == EventType.FinishRequest:
        times[request_id] = (get_datetime(timestamp) - times[request_id]).total_seconds()


def get_datetime(timestamp: int) -> datetime:
    return datetime.fromtimestamp(int(timestamp) / 1e6)


def main():
    with open(LOG_FILENAME, "r") as file:
        for line in file:
            parse_line(line)
    print(len(failed), np.percentile(list(value for value in times.values()), 95))


if __name__ == '__main__':
    main()
