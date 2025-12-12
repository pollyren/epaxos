#!/usr/bin/env python3
import os
import glob
import matplotlib.pyplot as plt

# Directory that contains server-0, server-1, ...
LOG_ROOT = "./skew99-wan/2025-12-11-16-31-12/2025-12-11-16-31-16/out"
LOG_GLOB = os.path.join(LOG_ROOT, "server-*", "server-*-stdout-*.log")

def parse_all_logs(pattern: str):
    """
    Parse all logs matching the glob pattern.

    Expected line format:
        operationName, time, dependencyCount, depGraph.size(), key, fastPathTaken

    We keep only operationName == "write".
    Returns a list of dicts with keys: time, depCount, depSize.
    """
    records = []

    for path in glob.glob(pattern):
        with open(path) as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue

                parts = [p.strip() for p in line.split(",")]
                # Need at least 6 fields
                if len(parts) < 6:
                    continue

                op = parts[0]
                if op.lower() != "write":
                    # Skip reads/other ops
                    continue

                try:
                    time_val = int(parts[1])
                    dep_count = int(parts[3])
                    dep_size = int(parts[4])
                    # key = int(parts[4])          # not used here
                    # fast_path = int(parts[5])    # Jeffery:TBD
                except ValueError:
                    # Skip malformed lines
                    continue

                records.append(
                    {
                        "time": time_val,
                        "depCount": dep_count,
                        "depSize": dep_size,
                    }
                )

    return records

def main():
    records = parse_all_logs(LOG_GLOB)
    if not records:
        print("No records found. Check LOG_ROOT/LOG_GLOB and log format.")
        return

    # Sort by time
    records.sort(key=lambda r: r["time"])

    # Normalize time to start at 0 (purely for readability)
    t0 = records[0]["time"]
    times = [r["time"] - t0 for r in records]
    dep_counts = [r["depCount"] for r in records]
    dep_sizes = [r["depSize"] for r in records]

    print(f"Parsed {len(records)} write records from logs.")
    print(f"time range (raw): {records[0]['time']} .. {records[-1]['time']}")

    plt.figure(figsize=(10, 5))
    plt.plot(times, dep_counts, label="dependencyCount", alpha=0.7)
    plt.plot(times, dep_sizes, label="depGraph.size()", alpha=0.7)
    plt.xlabel("Time (normalized: time - min(time))")
    plt.ylabel("Value")
    plt.title("dependencyCount and depGraph.size() over time (all servers, writes only)")
    plt.legend()
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()
