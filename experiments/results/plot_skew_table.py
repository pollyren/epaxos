#!/usr/bin/env python3
import csv
import matplotlib.pyplot as plt

TABLE_FILE = "skew_clients_table.csv"  # produced by make_table.py

def read_table(path):
    clients = []
    skew_series = {}

    with open(path, newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            raise RuntimeError("Empty table file")

        # header[0] should be "clients"; others are skew columns
        skew_cols = header[1:]

        # init series
        for col in skew_cols:
            skew_series[col] = []

        for row in reader:
            if not row:
                continue
            try:
                c = int(row[0])
            except ValueError:
                # skip malformed row
                continue
            clients.append(c)

            for i, col in enumerate(skew_cols, start=1):
                val_str = row[i].strip() if i < len(row) else ""
                if val_str == "":
                    skew_series[col].append(None)
                else:
                    try:
                        skew_series[col].append(float(val_str))
                    except ValueError:
                        skew_series[col].append(None)

    return clients, skew_series

def main():
    clients, skew_series = read_table(TABLE_FILE)

    if not clients:
        print("No client data found.")
        return

    plt.figure(figsize=(8, 5))

    for col_name, values in skew_series.items():
        # Filter out None entries, if any
        xs = []
        ys = []
        for c, v in zip(clients, values):
            if v is None:
                continue
            xs.append(c)
            ys.append(v)

        if not xs:
            continue

        # label like "skew0" -> "skew 0"
        label = col_name.replace("skew", "skew ")
        plt.plot(xs, ys, marker="o", label=label)

    plt.xlabel("Number of clients")
    plt.ylabel("Percentage fast path (%)")
    plt.title("Percentage of request going to fast path vs. number of clients")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()
