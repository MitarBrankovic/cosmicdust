#!/usr/bin/env python3
"""What gzip and zstd make of the log files a node's Docker wrote.

    compress.py <json-file log> <seconds>

Cuts the log into pieces of that many seconds, by the time of each line - a
file of the agent in the same run holds as much - and compresses every piece
on its own: with gzip -6 and with zstd -3 and -19, and the text of the lines
alone, without the JSON the runtime wraps each of them in, with zstd -3.

Prints a name and a number of bytes per line: the log and the text of its
lines as they are ("log", "messages"), and what each way of compressing left
of them ("gzip-6", "zstd-3", "zstd-19", "messages-zstd-3"); then "lines" and
"pieces".
"""

import json
import subprocess
import sys
from calendar import timegm
from collections import defaultdict
from time import strptime

CODECS = {
    "gzip-6": ["gzip", "-6", "-c"],
    "zstd-3": ["zstd", "-3", "-c", "-q"],
    "zstd-19": ["zstd", "-19", "-c", "-q"],
}


def compressed(data, codec):
    return len(subprocess.run(CODECS[codec], input=data, capture_output=True, check=True).stdout)


def second(stamp, seen={}):
    """The unix time of an RFC 3339 time in UTC, to the second."""
    key = stamp[:19]
    if key not in seen:
        seen[key] = timegm(strptime(key, "%Y-%m-%dT%H:%M:%S"))
    return seen[key]


def pieces(path, seconds):
    """The lines of the log and their text, by the piece their time falls in."""
    logs, messages = defaultdict(list), defaultdict(list)
    lines = 0
    with open(path, "rb") as f:
        for line in f:
            record = json.loads(line)
            piece = second(record["time"]) // seconds
            logs[piece].append(line)
            messages[piece].append(record["log"].encode())
            lines += 1
    return [b"".join(logs[k]) for k in sorted(logs)], [b"".join(messages[k]) for k in sorted(messages)], lines


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    logs, messages, lines = pieces(sys.argv[1], int(sys.argv[2]))
    if not lines:
        sys.exit(f"{sys.argv[1]} has no lines")
    print("log", sum(len(p) for p in logs))
    print("messages", sum(len(p) for p in messages))
    for codec in CODECS:
        print(codec, sum(compressed(p, codec) for p in logs))
    print("messages-zstd-3", sum(compressed(p, "zstd-3") for p in messages))
    print("lines", lines)
    print("pieces", len(logs))


if __name__ == "__main__":
    main()
