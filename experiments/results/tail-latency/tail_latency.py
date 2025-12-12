import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

regions = ("California", "Ireland", "Oregon", "Japan", "Virginia")

p99_lat = {
    'Multi-Paxos': (88.66478466000001, 108.96086975, 173.00932554, 245.10250821000002, 207.05603908),
    'EPaxos': (182.48833858, 200.11479518, 195.53380235, 322.69384995, 263.12991139)
}

x = np.arange(len(regions))  # the label locations
width = 0.35  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(layout='constrained')

for attribute, measurement in p99_lat.items():
    offset = width * multiplier
    rects = ax.bar(x + offset, measurement, width, label=attribute)
    ax.bar_label(rects, padding=3, fontsize=8, label_type='edge', fmt="%.2f")
    multiplier += 1

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Tail (p99) Latency (ms)')
# ax.set_title('Penguin attributes by species')
ax.set_xticks(x + width, regions)
ax.legend(loc='upper left', ncols=3)
ax.set_ylim(0, 350)

plt.savefig("tail_latency.png")