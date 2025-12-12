import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

regions = ("California", "Ireland", "Oregon", "Japan", "Virginia")
p50_lat = {
    'Multi-Paxos': (87.91332200000001, 206.214968, 172.2628965, 108.1710405, 244.198506),
    'EPaxos': (88.11477099999999, 130.149485, 94.137929, 98.108678, 160.1798265)
}

p99_lat = {
    'Multi-Paxos': (88.51606531999998, 206.6268203, 172.98347655999999, 108.91612906, 244.4535844),
    'EPaxos': (88.604152, 130.82291064, 94.73021699999998, 98.62398312, 160.73217352)
}

x = np.arange(len(regions))  # the label locations
width = 0.35  # the width of the bars
multiplier = 0

fig, ax = plt.subplots(layout='constrained')

for attribute, measurement in p50_lat.items():
    offset = width * multiplier
    rects = ax.bar(x + offset, measurement, width, label=attribute)
    ax.bar_label(rects, padding=3, fontsize=8, label_type='edge', fmt="%.2f")
    multiplier += 1

# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Median Latency (ms)')
# ax.set_title('Penguin attributes by species')
ax.set_xticks(x + width, regions)
ax.legend(loc='upper left', ncols=3)
ax.set_ylim(0, 270)

plt.savefig("commit_latency_wan.png")