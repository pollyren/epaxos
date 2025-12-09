#!/usr/bin/env python3
import matplotlib.pyplot as plt
from collections import defaultdict

LOG_FILE = "writes.log"

def parse_log(path):
    """
    Parse lines of the form:
      write, replicaInstance_id, dependencyCount, depGraph.size(), key, ...
    Returns:
      all_x:            list of request indices (0..N-1) over all writes
      all_dep_counts:   list of dependencyCount values
      all_dep_sizes:    list of depGraph.size() values
      per_key:          dict key -> {"x": [...], "dep_counts": [...], "dep_sizes": [...]}
    """
    all_x = []
    all_dep_counts = []
    all_dep_sizes = []
    per_key = defaultdict(lambda: {"x": [], "dep_counts": [], "dep_sizes": []})

    request_index = 0
    with open(path) as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line.startswith("write"):
                continue

            parts = [p.strip() for p in line.split(",")]
            # We only need first 5 elements: write, replica, depCount, depSize, key
            if len(parts) < 5:
                continue

            try:
                replica = int(parts[1])          # not used, but validates format
                dep_count = int(parts[2])
                dep_size = int(parts[3])
                key = int(parts[4])
            except ValueError:
                # Skip malformed lines (e.g. header text after numbers)
                continue

            all_x.append(request_index)
            all_dep_counts.append(dep_count)
            all_dep_sizes.append(dep_size)

            per_key[key]["x"].append(request_index)
            per_key[key]["dep_counts"].append(dep_count)
            per_key[key]["dep_sizes"].append(dep_size)

            request_index += 1

    return all_x, all_dep_counts, all_dep_sizes, per_key

def main():
    all_x, all_dep_counts, all_dep_sizes, per_key = parse_log(LOG_FILE)

    if not all_x:
        print("No valid 'write' lines found in the log.")
        return

    # -------- Graph 1: all requests --------
    plt.figure(figsize=(10, 5))
    plt.plot(all_x, all_dep_counts, label="dependencyCount", alpha=0.7)
    plt.plot(all_x, all_dep_sizes, label="depGraph.size()", alpha=0.7)
    plt.xlabel("Request (log order)")
    plt.ylabel("Value")
    plt.title("All keys: dependencyCount and depGraph.size() over requests")
    plt.legend()
    plt.tight_layout()

    # -------- Find hot key (key with most requests) --------
    hot_key = None
    hot_len = 0
    for key, data in per_key.items():
        n = len(data["x"])
        if n > hot_len:
            hot_key = key
            hot_len = n

    if hot_key is None:
        print("Could not determine a hot key.")
    else:
        data = per_key[hot_key]
        print(f"Hot key: {hot_key} with {hot_len} requests")

        # -------- Graph 2: hot key only --------
        plt.figure(figsize=(10, 5))
        plt.plot(
            data["x"],
            data["dep_counts"],
            label=f"dependencyCount (key={hot_key})",
            marker="o",
            linestyle="-",
        )
        plt.plot(
            data["x"],
            data["dep_sizes"],
            label=f"depGraph.size() (key={hot_key})",
            marker="x",
            linestyle="-",
        )
        plt.xlabel("Request (log order)")
        plt.ylabel("Value")
        plt.title(f"Hot key {hot_key}: dependencyCount and depGraph.size() over requests")
        plt.legend()
        plt.tight_layout()

    # Show both figures
    plt.show()

if __name__ == "__main__":
    main()
