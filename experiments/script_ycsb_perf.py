import matplotlib.pyplot as plt
import sys
import numpy as np
import pandas as pd
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,AutoMinorLocator)
from matplotlib.ticker import FuncFormatter
import matplotlib
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

plt.rcParams['axes.linewidth'] = 2
plt.rcParams['axes.labelsize'] = 12
plt.rc('xtick', labelsize=12) 
plt.rc('ytick', labelsize=12) 

leanstore_file_1 = "../logs/250M_hotread.log"
leanstore_file_2 = "../logs/250M_hotreadseg.log"

def get_results(path):
    statistic = []
    with open(path, "r") as f:
        for line in f.readlines():
            if (line.startswith("[Epoch] ")):
                statistic.append(line.strip("[Epoch] ").strip("\n").split(","))

    if (statistic):
        statistic = np.array(statistic)
        statistic = pd.DataFrame(statistic, columns=['thread', 'last_done', 'cur_done', 'seg_time', 'time'], dtype=np.float)
    
    print(statistic)
    """
    Calculate throughput : (epoch_ops/epoch_time/1024/1024)
    """
    time_epoch = 1
    max_n_rows = 0
    for i in range(20):
        th = statistic[statistic["thread"]==i].shape
        if (th[0] > max_n_rows):
            max_n_rows = th[0]

    print("max runtime = ", max_n_rows,)
    

    results_padding = np.zeros(max_n_rows)
    
    for i in range(20):
        th = statistic[statistic["thread"]==i]
        cur_done = th['cur_done'].values
        last_done = th['last_done'].values
        eopch = last_done
        cur_length = cur_done.shape[0]
        print(cur_length, i)
        padding = np.pad(eopch, (0, max_n_rows-cur_length), 'constant', constant_values=(0,0))
        results_padding += padding
    through_x = results_padding[:max_n_rows]/time_epoch/1024/1024
    time_x = np.array([i for i in np.arange(time_epoch, (max_n_rows + 1)*time_epoch, time_epoch)])
    print (results_padding)
    print (through_x)
    print (time_x)
    return time_x, through_x

time_1, through_1 = get_results(leanstore_file_1)
time_2, through_2 = get_results(leanstore_file_2)

import matplotlib.pyplot as plt
import sys
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,AutoMinorLocator)
from matplotlib.ticker import FuncFormatter
import matplotlib
# plt.rcParams["font.family"] = "arial"
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

gloyend = None
# Main
dashes=[(2,2), (4,1), (2,0), (2,0), (3, 3), (2, 0), (2,2), (4,1), (2,0), (2,0), (3, 3), (2, 0)]
markers = ['x', '|', '.', 'D', 'd', '', 'x', '|', '.', 'D', 'd', '']
colors = ['grey', '#FF7F0E', '#2077B4', '#D62728', '#0A640C', '#343434', 'grey', '#FF7F0E', '#2077B4', '#D62728', '#0A640C', '#343434']

label = ['Leanstore_4th_YCSB_ORG', 'Using_Segment', 'ARTR_4th_YCSB']

fig, ax = plt.subplots(figsize=(4.8, 3.2), constrained_layout=True)

time_strip = 80
gap = 1


nthroughput1 = []

cnt = 0
subsum = 0
for num in through_1[:time_strip]:
    subsum += num
    cnt += 1
    if (cnt == gap):
        nthroughput1.append(subsum/gap)
        subsum = 0
        cnt = 0
    
nthroughput2 = [] 

for num in through_2[:time_strip]:
    subsum += num
    cnt += 1
    if (cnt == gap):
        nthroughput2.append(subsum/gap)
        subsum = 0
        cnt = 0
ntime = [i * gap for i in range(1, len(nthroughput2) + 1)]


start = 0 
end = len(ntime)
print(len(ntime), len(nthroughput1), len(nthroughput2))
ax.plot(ntime[start:end], np.array(nthroughput1[start:end]), color=colors[3], marker=markers[2], dashes=dashes[2], label = label[0], alpha=0.8, fillstyle='none', markersize=18)
ax.plot(ntime[start:end], np.array(nthroughput2[start:end]), color=colors[4], marker=markers[2], dashes=dashes[2], label = label[1], alpha=0.8, fillstyle='none', markersize=18)

# ax.legend(loc="upper right")
ax.set_title("leanstore original vs with seg cold start for OOM and 1 thread")
ax.legend(loc='upper center', ncol=3, borderaxespad=-3, frameon=False, prop={'size': 7})

ax.grid(which='major', linestyle='--', zorder=1)
ax.grid(which='minor', linestyle='--', zorder=1, linewidth=0.3)
# ax.xaxis.grid(False, which='both')
# ax.yaxis.grid(False, which='both')
ax.set_ylim(bottom=0)
ax.set_xlabel('Time (secs)', fontsize=12)
ax.set_ylabel('Throughput (Mops/s)', fontsize=12)

fig.savefig("../logs/YCSB_LEANSTORE_RND_ORG_VS_SEG_250M_OOM_hotread.png", bbox_inches='tight', pad_inches=0)

