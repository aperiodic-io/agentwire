"""Plot the measured full-wire compression ratios from results/bench.csv."""
from __future__ import annotations

import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def main():
    root=Path(__file__).resolve().parent
    with (root/"results/bench.csv").open(newline="") as f:
        data=list(csv.DictReader(f))
    workloads=["tool-json","mixed-agent","logs-diffs","high-entropy","short-chat"]
    names={"tool-json":"Tool JSON","mixed-agent":"Agent transcript",
           "logs-diffs":"Logs / diffs","high-entropy":"Random base64","short-chat":"Short chat"}
    variants=[("zstd3/one","Individual Zstd"),
              ("zstd3+dict/batch-64k","Batched + dict (fast synthetic arrivals, 25 ms wait)"),
              ("zstd3+dict/batch-64k@1s","Batched + dict (1 s gaps, 25 ms wait)"),
              ("zstd3+dict/stream","AWC1 stream + dict (no wait)")]
    data_by={(row["workload"],row["variant"]):float(row["ratio_x"]) for row in data}
    y=np.arange(len(workloads))
    width=0.8/len(variants)
    fig,ax=plt.subplots(figsize=(10.8,6.5))
    for k,(key,label) in enumerate(variants):
        vals=[data_by[(name,key)] for name in workloads]
        bars=ax.barh(y+(k-(len(variants)-1)/2)*width,vals,height=width*.9,label=label)
        for bar,value in zip(bars,vals):
            ax.text(bar.get_width()+0.08,bar.get_y()+bar.get_height()/2,
                    f"{value:.2f}×",va="center",ha="left",fontsize=9)
    ax.set_yticks(y,[names[n] for n in workloads])
    ax.invert_yaxis()
    ax.set_xlim(0,9.25)
    ax.axvline(1,linestyle="--",linewidth=.9,alpha=.6)
    ax.set_xlabel("Compression ratio (raw message bytes / full framed wire bytes)")
    ax.set_title("AgentWire: batching vs. per-connection Zstd stream",pad=15)
    ax.grid(axis="x",alpha=.17)
    ax.set_axisbelow(True)
    ax.legend(loc="lower right",frameon=False)
    fig.text(.5,.005,"240 synthetic messages per workload. Frame headers included; 8 KiB dictionary distribution excluded.",
             ha="center",fontsize=9)
    fig.tight_layout(rect=(0,.04,1,1))
    target=root/"results/compression-ratios.png"
    fig.savefig(target,dpi=170)
    print(target)


if __name__=="__main__":main()
