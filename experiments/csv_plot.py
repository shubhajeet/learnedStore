import pandas as pd
import matplotlib.pyplot as plt
infile = "../profile_data/250M_readseg_dt.csv"
outfile = "../profile_data/250M_readseg_Miss_rate_1_threads_OOM.png"
# Read CSV file
df = pd.read_csv(infile)

# Plotting the data
x = 't'
y = ['dt_misses_counter']
label = ['buffeframe miss count']
for i in range(len(y)):
    plt.plot(df[x], df[y[i]], label=label[i])

# Add labels and title
plt.xlabel('time in seconds')
plt.ylabel('time in seconds vs parameters')
plt.title('Parameters vs time for 250M OOM dataset read')
plt.legend()

plt.savefig(outfile, bbox_inches='tight', pad_inches=0)