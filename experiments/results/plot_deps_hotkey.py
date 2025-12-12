#!/usr/bin/env python3
import csv
from collections import Counter, defaultdict
import matplotlib.pyplot as plt

# Input CSV file
CSV_FILE = "skew99-20client-server0.csv"  # change if needed

def load_sorted_rows(path):
    """
    Expected CSV columns:
        opname,real-time,instance,depGraphSize,executionPathLength,key,fastPath

    Returns:
        rows: list[dict] sorted by time
    """
    rows = []
    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)  # skip header
        if header is None:
            raise RuntimeError("Empty CSV file")

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

    rows.sort(key=lambda r: r["time"])
    return rows

def main():
    rows = load_sorted_rows(CSV_FILE)
    if not rows:
        print("No valid rows found.")
        return

    # Only consider writes for hotness
    write_rows = [r for r in rows if r["opname"].lower() == "write"]
    if not write_rows:
        print("No write operations found.")
        return

    # Find 5 hottest keys
    counts = Counter(r["key"] for r in write_rows)
    hot_keys = [k for k, _ in counts.most_common(5)]
    print("Hot keys (top 5):", hot_keys)

    # Global time normalization (so all keys share same time base)
    t0 = rows[0]["time"]

    # Build per-key time series for writes
    exec_by_key = defaultdict(lambda: {"time": [], "value": []})
    dep_by_key = defaultdict(lambda: {"time": [], "value": []})

    for r in write_rows:
        key = r["key"]
        if key not in hot_keys:
            continue
        t_rel = r["time"] - t0
        exec_by_key[key]["time"].append(t_rel)
        exec_by_key[key]["value"].append(r["executionPathLength"])
        dep_by_key[key]["time"].append(t_rel)
        dep_by_key[key]["value"].append(r["depGraphSize"])

    # Figure 1: executionPathLength vs time for 5 hottest keys
    plt.figure(figsize=(10, 5))
    for key in hot_keys:
        series = exec_by_key.get(key)
        if not series or not series["time"]:
            continue
        plt.plot(
            series["time"],
            series["value"],
            marker="o",
            linestyle="-",
            label=f"key={key}",
        )
    plt.xlabel("Real-time")
    plt.ylabel("executionPathLength")
    plt.title("executionPathLength over time for 5 hottest keys (writes only)")
    plt.legend()
    plt.tight_layout()
    plt.savefig("hotkey_exec_path_length_over_time.png")

    # Figure 2: depGraphSize vs time for 5 hottest keys
    plt.figure(figsize=(10, 5))
    for key in hot_keys:
        series = dep_by_key.get(key)
        if not series or not series["time"]:
            continue
        plt.plot(
            series["time"],
            series["value"],
            marker="x",
            linestyle="-",
            label=f"key={key}",
        )
    plt.xlabel("Real-time")
    plt.ylabel("depGraphSize")
    plt.title("depGraphSize over time for 5 hottest keys (writes only)")
    plt.legend()
    plt.tight_layout()

    plt.savefig("hotkey_exec_dep_over_time.png")

if __name__ == "__main__":
    main()
