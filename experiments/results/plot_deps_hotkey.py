#!/usr/bin/env python3
import csv
from collections import Counter
import matplotlib.pyplot as plt

CSV_FILE = "skew99-20client-server0.csv"  # change to your filename

def load_sorted_rows(path):

    rows = []
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)  # skip header if present
        for row in reader:
            if not row or len(row) < 7:
                continue

            opname = row[0].strip()
            try:
                time_val = int(row[1].strip())
                instance = int(row[2].strip())
                dep_size = int(row[3].strip())
                exec_len = int(row[4].strip())
                key = int(row[5].strip())
                fast_path = int(row[6].strip())
            except ValueError:
                # skip malformed line
                continue

            rows.append(
                {
                    "opname": opname,
                    "time": time_val,
                    "instance": instance,
                    "depGraphSize": dep_size,
                    "executionPathLength": exec_len,
                    "key": key,
                    "fastPath": fast_path,
                }
            )

    # sort globally by time
    rows.sort(key=lambda r: r["time"])
    return rows

def main():
    rows = load_sorted_rows(CSV_FILE)
    if not rows:
        print("No valid rows found. Check CSV_FILE and format.")
        return

    # Consider only writes when choosing the hot key
    write_rows = [r for r in rows if r["opname"].lower() == "write"]
    if not write_rows:
        print("No write operations found in the CSV.")
        return

    counts = Counter(r["key"] for r in write_rows)
    hot_key, hot_count = counts.most_common(1)[0]
    print(f"Hot key = {hot_key}, occurrences = {hot_count}")

    # Filter to rows for the hot key and keep them sorted by time
    hot_rows = [r for r in write_rows if r["key"] == hot_key]
    hot_rows.sort(key=lambda r: r["time"])

    if not hot_rows:
        print("No rows for hot key (this should not happen).")
        return

    # Build time series for plotting
    t0 = hot_rows[0]["time"]
    times = [r["time"] - t0 for r in hot_rows]  # relative time (same units as real-time)
    dep_sizes = [r["depGraphSize"] for r in hot_rows]
    exec_lens = [r["executionPathLength"] for r in hot_rows]

    # Plot depGraphSize and executionPathLength vs time
    plt.figure(figsize=(10, 5))
    plt.plot(times, dep_sizes, label="depGraphSize", marker="o", linestyle="-", alpha=0.7)
    plt.plot(times, exec_lens, label="executionPathLength", marker="x", linestyle="-", alpha=0.7)

    plt.xlabel("Time (real-time - min(real-time))")
    plt.ylabel("Value")
    plt.title(f"Hot key {hot_key}: depGraphSize and executionPathLength over time")
    plt.legend()
    plt.tight_layout()
    plt.savefig("dependency-hotkey.png")


if __name__ == "__main__":
    main()
