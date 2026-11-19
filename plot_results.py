"""Plot the measured full-wire compression ratios from results/bench.csv."""
from __future__ import annotations

import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def research(root):
    """Plots for results/research/ (written by research/bench_*.py)."""
    import json
    res=root/"results/research"
    t=json.loads((res/"transport.json").read_text())
    keys=[("awf1/one","AWF1 per message (baseline)"),("awf1/one+dict","AWF1 + dict"),
          ("awc1/w20+dict","AWC1 + dict"),("brotli5-stream/w22+dict","Brotli-5 stream + dict"),
          ("awd1/w22+dict","AWD1 delta + dict"),("ref/offline-zstd19-long","Offline zstd-19 --long")]
    corpora=list(t["corpora"])
    by={c:{r["variant"]:r["ratio"] for r in t["corpora"][c]["rows"]} for c in corpora}
    y=np.arange(len(corpora)); width=0.8/len(keys)
    fig,ax=plt.subplots(figsize=(11,6.5))
    for k,(key,label) in enumerate(keys):
        vals=[by[c].get(key,0) for c in corpora]
        bars=ax.barh(y+(k-(len(keys)-1)/2)*width,vals,height=width*.9,label=label)
        for bar,v in zip(bars,vals):
            ax.text(bar.get_width()*1.02,bar.get_y()+bar.get_height()/2,f"{v:.1f}×",va="center",fontsize=7)
    ax.set_xscale("log"); ax.set_yticks(y,corpora); ax.invert_yaxis()
    ax.set_xlabel("Compression ratio, log scale (payload bytes / wire bytes, framing included)")
    ax.set_title("Live transport on held-out public-trajectory corpora",pad=12)
    ax.grid(axis="x",alpha=.2); ax.set_axisbelow(True); ax.legend(loc="lower right",frameon=False,fontsize=8)
    fig.tight_layout(); fig.savefig(res/"transport-ratios.png",dpi=150); print(res/"transport-ratios.png")
    s=json.loads((res/"storage.json").read_text())
    fig,ax=plt.subplots(figsize=(9,6))
    for c,v in s["corpora"].items():
        pts=[(r["random_fetch_us"],r["ratio"],r["variant"]) for r in v["random_access"]]
        ax.plot([p[0] for p in pts],[p[1] for p in pts],"o",label=c,alpha=.8)
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("Mean time to fetch one random message (µs, log)"); ax.set_ylabel("Ratio (log)")
    ax.set_title("Archive formats: ratio vs random-access cost"); ax.grid(alpha=.2); ax.legend(frameon=False,fontsize=8)
    fig.tight_layout(); fig.savefig(res/"archive-random-access.png",dpi=150); print(res/"archive-random-access.png")


def main():
    root=Path(__file__).resolve().parent
    import sys
    if "--research" in sys.argv:
        return research(root)
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
