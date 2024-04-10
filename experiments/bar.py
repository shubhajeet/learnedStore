import matplotlib.pyplot as plt
import numpy as np

def bar_(df):
    fig, ax = plt.subplots()
    x = np.arange(len(df.index.tolist()))
    print(x)
    ax.bar(x,df["improvement"],width=0.6)
    ax.set_xlabel("Dataset")
    ax.set_ylabel("Improvement (%)")
    # ax.set_title("Improvement of LeanStore when using Learned Index to jump to leaf node")
    print(df.index.tolist())
    # ax.set_xticklabels(df.columns.tolist(), rotation=45)
    ax.set_xticks(x,df.index.tolist())
    return fig, ax
