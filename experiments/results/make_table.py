#!/usr/bin/env python3
import os
import csv

# Map directory -> skew label you want in the table
SKEW_DIRS = {
    "skew00-wan-100keys-consolidated": 0,
    "skew50-wan-100keys-consolidated": 50,
    "skew99-wan-100keys-consolidated": 99, 
}

# Base directory where those skew directories live
BASE_DIR = "."

def compute_percentage_for_csv(path):
    """
    Reads a CSV file and computes:
        (# rows with last column == '1') / (total rows) * 100
    Returns the percentage as a float, or None if the file is empty / no valid rows.
    """
    total = 0
    ones = 0
    with open(path, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            total += 1
            last = row[-1].strip()
            if last == "1":
                ones += 1

    if total == 0:
        return None
    return (ones / total) * 100.0

def main():
    # table[clients][skew] = percentage
    table = {}
    client_counts = set()
    skew_values = []

    for dirname, skew in SKEW_DIRS.items():
        skew_values.append(skew)
        dir_path = os.path.join(BASE_DIR, dirname)
        if not os.path.isdir(dir_path):
            print(f"Warning: directory not found: {dir_path}")
            continue

        for fname in os.listdir(dir_path):
            if not fname.endswith(".csv"):
                continue

            # Filename is like "5.csv" -> clients = 5
            name_without_ext = os.path.splitext(fname)[0]
            try:
                clients = int(name_without_ext)
            except ValueError:
                # Skip files that aren't simple "<int>.csv"
                continue

            client_counts.add(clients)

            csv_path = os.path.join(dir_path, fname)
            pct = compute_percentage_for_csv(csv_path)
            if pct is None:
                continue

            table.setdefault(clients, {})[skew] = pct

    if not table:
        print("No data found; check directory names and CSV contents.")
        return

    # Sort skew values and client counts for a stable table layout
    skew_values = sorted(set(skew_values))
    client_counts = sorted(client_counts)

    # Print as CSV: header row then one row per client count
    # Header: "clients, skew0, skew50, skew90"
    header = ["clients"] + [f"skew{skew}" for skew in skew_values]
    print(",".join(header))

    for clients in client_counts:
        row = [str(clients)]
        for skew in skew_values:
            pct = table.get(clients, {}).get(skew)
            if pct is None:
                row.append("")
            else:
                # Format to, say, 2 decimal places
                row.append(f"{pct:.2f}")
        print(",".join(row))

if __name__ == "__main__":
    main()
