"""
PSU Campus Crime Analysis — Statistical Analysis Pipeline
=========================================================
Covers all professor suggestions:
  1. Data reduction / preprocessing
  2. Time-series analysis
  3. Heatmap & trend visualisation
  4. Frequent pattern mining
  5. Clustering analysis (K-Means)
  6. Prediction (moving average + linear regression)
  7. Hotspot / safety analysis
  8. Bias-of-fear reduction
  9. Evaluation metrics (silhouette, MAE, R2)
  10. CSV / PNG export

Usage:
    python src/analysis/analysis.py
    python src/analysis/analysis.py --campus Abington
    python src/analysis/analysis.py --campus "University Park" --output ./my_reports
    python src/analysis/analysis.py --no-plots
"""

import argparse, os, sys, warnings
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

warnings.filterwarnings("ignore")

NAVY   = "#1b2d45"
BLUE   = "#2980b9"
COLORS = ["#1b2d45","#2980b9","#27ae60","#e67e22","#9b59b6",
          "#e74c3c","#1abc9c","#f39c12","#3498db","#8e44ad",
          "#16a085","#c0392b","#2471a3","#148f77","#d35400"]
TB_COLORS = {"Morning":"#3498db","Afternoon":"#2ecc71",
             "Evening":"#e67e22","Late Night":"#9b59b6"}

plt.rcParams.update({
    "figure.facecolor":"white","axes.facecolor":"#f8fafc",
    "axes.edgecolor":"#d8e4ef","grid.color":"#e8f0f8",
    "font.family":"sans-serif","axes.titlesize":12,
    "axes.titleweight":"bold","axes.titlecolor":NAVY,
    "axes.labelcolor":NAVY,"xtick.color":"#5a7a9a","ytick.color":"#5a7a9a"})

DAYS_ORDER = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
TB_ORDER   = ["Morning","Afternoon","Evening","Late Night"]

# ─── 1. LOAD ──────────────────────────────────────────────────────────────────
def load_data(csv_path="data/processed/incidents.csv", campus=None):
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}\nRun preprocessing first.")
    df = pd.read_csv(csv_path, low_memory=False)
    df["date"]       = pd.to_datetime(df["date"], errors="coerce")
    df["hour"]       = pd.to_numeric(df["hour"], errors="coerce").fillna(0).astype(int)
    df["month"]      = pd.to_numeric(df["month"], errors="coerce").fillna(1).astype(int)
    df["is_weekend"] = df["is_weekend"].astype(str).str.lower().isin(["true","1"])
    df["day_of_week"]= pd.Categorical(df["day_of_week"], categories=DAYS_ORDER, ordered=True)
    df["time_bucket"]= pd.Categorical(df["time_bucket"], categories=TB_ORDER,   ordered=True)
    if campus:
        df = df[df["final_campus"].str.lower() == campus.lower()]
        if df.empty:
            print(f"[!] Campus '{campus}' not found.")
            sys.exit(1)
    print(f"[+] Loaded {len(df):,} incidents | {df['final_campus'].nunique()} campus(es)")
    return df

# ─── 2. SUMMARY ───────────────────────────────────────────────────────────────
def print_summary(df):
    total = len(df)
    print("\n" + "="*62)
    print("  PSU CAMPUS CRIME — SUMMARY REPORT")
    print("="*62)
    print(f"  Total incidents  : {total:,}")
    print(f"  Campuses         : {df['final_campus'].nunique()}")
    d_min = df['date'].min(); d_max = df['date'].max()
    print(f"  Date range       : {d_min.date() if pd.notna(d_min) else 'N/A'} → {d_max.date() if pd.notna(d_max) else 'N/A'}")
    print(f"  Weekend share    : {df['is_weekend'].sum():,} ({df['is_weekend'].mean()*100:.1f}%)")
    print()
    print("  Top 5 Campuses:")
    for c, n in df["final_campus"].value_counts().head(5).items():
        print(f"    {c:<30} {n:>5,}")
    print()
    print("  Time Buckets:")
    for tb in TB_ORDER:
        n = (df["time_bucket"]==tb).sum()
        print(f"    {tb:<12} {n:>5,}  ({n/total*100:.1f}%)")
    print("="*62 + "\n")

def _save(fig, out_dir, fname):
    path = Path(out_dir)/fname
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  [saved] {path}")

# ─── 3. CHARTS ────────────────────────────────────────────────────────────────
def plot_campus_bar(df, out, top_n=12):
    counts = df["final_campus"].value_counts().head(top_n)
    fig, ax = plt.subplots(figsize=(10, max(4, len(counts)*0.52)))
    bars = ax.barh(counts.index[::-1], counts.values[::-1],
                   color=COLORS[:len(counts)][::-1], edgecolor="white", height=0.7)
    for bar in bars:
        ax.text(bar.get_width()+counts.max()*0.01, bar.get_y()+bar.get_height()/2,
                f"{int(bar.get_width()):,}", va="center", fontsize=9, color=NAVY)
    ax.set_title(f"Incidents by Campus (Top {top_n})", pad=10)
    ax.set_xlabel("Number of Incidents")
    ax.spines[["top","right","left"]].set_visible(False)
    plt.tight_layout(); _save(fig, out, "01_campus_bar.png")

def plot_time_donut(df, out):
    tb = df["time_bucket"].value_counts().reindex(TB_ORDER).fillna(0)
    total = tb.sum()
    fig, ax = plt.subplots(figsize=(7,6))
    wedges, texts, ats = ax.pie(
        tb.values, labels=tb.index,
        autopct="%1.1f%%", colors=[TB_COLORS[k] for k in tb.index],
        startangle=90, pctdistance=0.76,
        wedgeprops={"linewidth":3,"edgecolor":"white"})
    for t in texts:  t.set_color(NAVY); t.set_fontsize(11)
    for t in ats:    t.set_color("white"); t.set_fontsize(10); t.set_fontweight("bold")
    ax.add_patch(plt.Circle((0,0),0.55,color="white"))
    ax.text(0,0,f"{int(total):,}\nincidents",ha="center",va="center",
            fontsize=13,color=NAVY,fontweight="bold")
    ax.set_title("Incidents by Time of Day", pad=14)
    plt.tight_layout(); _save(fig, out, "02_time_donut.png")

def plot_day_of_week(df, out):
    dc = df["day_of_week"].value_counts().reindex(DAYS_ORDER).fillna(0)
    cols = [NAVY if d not in("Saturday","Sunday") else "#e67e22" for d in DAYS_ORDER]
    fig, ax = plt.subplots(figsize=(9,4))
    ax.bar([d[:3] for d in DAYS_ORDER], dc.values, color=cols, edgecolor="white")
    ax.set_title("Incidents by Day of Week"); ax.set_ylabel("Incidents")
    ax.spines[["top","right"]].set_visible(False)
    ax.legend(handles=[mpatches.Patch(color=NAVY,label="Weekday"),
                        mpatches.Patch(color="#e67e22",label="Weekend")], frameon=False)
    plt.tight_layout(); _save(fig, out, "03_day_of_week.png")


def plot_incidents_per_year(df, out):
    if 'year' not in df.columns or df['year'].isna().all():
        return
    yc = df.groupby('year').size()
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(yc.index.astype(str), yc.values, color=COLORS[:len(yc)],
           edgecolor='white', linewidth=0.5)
    for bar in ax.patches:
        ax.text(bar.get_x()+bar.get_width()/2, bar.get_height()+yc.max()*0.01,
                f'{int(bar.get_height()):,}', ha='center', fontsize=9, color=NAVY)
    ax.set_title('Incidents per Year'); ax.set_ylabel('Incidents')
    ax.spines[['top','right']].set_visible(False)
    plt.tight_layout()
    _save(fig, out, '03b_incidents_per_year.png')

def plot_hourly_line(df, out):
    hc = df.groupby("hour").size().reindex(range(24), fill_value=0)
    fig, ax = plt.subplots(figsize=(11,4))
    ax.fill_between(hc.index, hc.values, alpha=0.10, color=NAVY)
    ax.plot(hc.index, hc.values, color=NAVY, linewidth=2.5,
            marker="o", markersize=5, markerfacecolor=BLUE,
            markeredgecolor="white", markeredgewidth=1.5)
    for s,e,c in [(6,12,"#3498db"),(12,18,"#2ecc71"),(18,22,"#e67e22"),(0,6,"#9b59b6")]:
        ax.axvspan(s,e,alpha=0.04,color=c)
    ax.set_xticks(range(0,24,2))
    ax.set_xticklabels([f"{h:02d}:00" for h in range(0,24,2)], rotation=45, ha="right")
    ax.set_title("Incidents by Hour of Day"); ax.set_ylabel("Incidents")
    ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout(); _save(fig, out, "04_hourly_line.png")

def plot_heatmap(df, out):
    pivot = (df.groupby(["day_of_week","hour"]).size()
               .unstack(fill_value=0).reindex(DAYS_ORDER)
               .reindex(columns=range(24), fill_value=0))
    fig, ax = plt.subplots(figsize=(14,5))
    try:
        import seaborn as sns
        sns.heatmap(pivot, ax=ax, cmap=sns.light_palette(NAVY, as_cmap=True),
                    linewidths=0.3, linecolor="white",
                    cbar_kws={"label":"Incident count","shrink":0.7},
                    annot=pivot.values.astype(int), fmt="d",
                    annot_kws={"size":7})
    except ImportError:
        im = ax.imshow(pivot.values, cmap="Blues", aspect="auto")
        fig.colorbar(im, ax=ax, shrink=0.7)
        ax.set_yticks(range(7)); ax.set_yticklabels([d[:3] for d in DAYS_ORDER])
        ax.set_xticks(range(24)); ax.set_xticklabels(range(24), fontsize=7)
    ax.set_title("Incident Heatmap — Day × Hour", pad=10)
    plt.tight_layout(); _save(fig, out, "05_heatmap.png")

def plot_incident_types(df, out, top_n=15):
    nc = df["nature_of_incident"].value_counts().head(top_n)
    labels = [t[:60]+("…" if len(t)>60 else "") for t in nc.index]
    fig, ax = plt.subplots(figsize=(12, max(5, top_n*0.52)))
    ax.barh(labels[::-1], nc.values[::-1],
            color=COLORS[:top_n][::-1], edgecolor="white", height=0.7)
    for bar in ax.patches:
        ax.text(bar.get_width()+nc.max()*0.005, bar.get_y()+bar.get_height()/2,
                f"{int(bar.get_width()):,}", va="center", fontsize=8, color=NAVY)
    ax.set_title(f"Top {top_n} Incident Types", pad=10); ax.set_xlabel("Count")
    ax.spines[["top","right","left"]].set_visible(False)
    plt.tight_layout(); _save(fig, out, "06_incident_types.png")

def plot_stacked_by_time(df, out, top_n=5):
    top5   = df["nature_of_incident"].value_counts().head(top_n).index.tolist()
    subset = df[df["nature_of_incident"].isin(top5)]
    pivot  = (subset.groupby(["time_bucket","nature_of_incident"])
                    .size().unstack(fill_value=0).reindex(TB_ORDER))
    pivot.columns = [c[:35]+("…" if len(c)>35 else "") for c in pivot.columns]
    fig, ax = plt.subplots(figsize=(10,5))
    pivot.plot(kind="bar", ax=ax, color=COLORS[:top_n],
               edgecolor="white", stacked=True, width=0.65)
    ax.set_title(f"Top {top_n} Incident Types by Time Bucket")
    ax.set_xticklabels(pivot.index, rotation=0); ax.set_ylabel("Incidents")
    ax.legend(fontsize=8, frameon=False, bbox_to_anchor=(1.35,1))
    ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout(); _save(fig, out, "07_stacked_by_time.png")

def plot_weekday_vs_weekend(df, out):
    wk=df[~df["is_weekend"]]; we=df[df["is_weekend"]]
    fig, axes = plt.subplots(1,2,figsize=(12,4))
    for ax, sub, lbl, col in [(axes[0],wk,"Weekday",BLUE),(axes[1],we,"Weekend","#e67e22")]:
        hc = sub.groupby("hour").size().reindex(range(24), fill_value=0)
        ax.fill_between(hc.index, hc.values, alpha=0.12, color=col)
        ax.plot(hc.index, hc.values, color=col, linewidth=2)
        ax.set_title(f"{lbl} — Hourly Pattern")
        ax.set_xticks(range(0,24,4)); ax.set_xlabel("Hour"); ax.set_ylabel("Incidents")
        ax.spines[["top","right"]].set_visible(False)
    plt.suptitle("Weekday vs Weekend Patterns", fontsize=13, fontweight="bold", color=NAVY)
    plt.tight_layout(); _save(fig, out, "08_weekday_vs_weekend.png")

def plot_top_locations(df, out, top_n=15):
    lc = df["location"].value_counts().head(top_n)
    labels = [l[:55]+("…" if len(l)>55 else "") for l in lc.index]
    fig, ax = plt.subplots(figsize=(11, max(4, top_n*0.48)))
    ax.barh(labels[::-1], lc.values[::-1], color=BLUE, edgecolor="white", height=0.68)
    for bar in ax.patches:
        ax.text(bar.get_width()+lc.max()*0.005, bar.get_y()+bar.get_height()/2,
                f"{int(bar.get_width()):,}", va="center", fontsize=8, color=NAVY)
    ax.set_title(f"Top {top_n} Incident Locations"); ax.set_xlabel("Count")
    ax.spines[["top","right","left"]].set_visible(False)
    plt.tight_layout(); _save(fig, out, "09_top_locations.png")

# ─── 4. FREQUENT PATTERNS ─────────────────────────────────────────────────────
def frequent_patterns(df):
    d2 = df.copy()
    d2["day_type"]     = d2["is_weekend"].map({True:"Weekend",False:"Weekday"})
    d2["nature_short"] = d2["nature_of_incident"].str.slice(0,55)
    return (d2.groupby(["time_bucket","day_type","nature_short"])
              .size().reset_index(name="count")
              .sort_values("count",ascending=False).head(20))

def plot_patterns(df, out):
    fp = frequent_patterns(df)
    labels = [f"{r['time_bucket']} | {r['day_type']}\n{r['nature_short'][:40]}"
              for _,r in fp.head(12).iterrows()]
    fig, ax = plt.subplots(figsize=(12,6))
    ax.barh(labels[::-1], fp["count"].values[:12][::-1],
            color=COLORS[:12][::-1], edgecolor="white", height=0.7)
    ax.set_title("Top Frequent Patterns (Time × Day-Type × Incident)", pad=10)
    ax.set_xlabel("Frequency"); ax.spines[["top","right","left"]].set_visible(False)
    plt.tight_layout(); _save(fig, out, "10_frequent_patterns.png")

# ─── 5. CLUSTERING ────────────────────────────────────────────────────────────
def run_clustering(df, out, k=4):
    from sklearn.preprocessing import LabelEncoder
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    feats = df[["hour","is_weekend"]].copy()
    feats["tb_code"] = LabelEncoder().fit_transform(df["time_bucket"].astype(str))
    X = feats.values.astype(float)
    if len(X) < k:
        print(f"  [!] Too few samples ({len(X)}) for k={k} clusters — skipping clustering.")
        return {}
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = km.fit_predict(X)
    score  = silhouette_score(X, labels)
    df2    = df.copy(); df2["cluster"] = labels
    fig, axes = plt.subplots(1,2,figsize=(12,5))
    c4 = ["#2980b9","#27ae60","#e67e22","#9b59b6"]
    jitter = np.random.uniform(-0.3,0.3,len(df2))
    axes[0].scatter(df2["hour"]+jitter, df2["cluster"],
                    c=[c4[c] for c in df2["cluster"]],
                    alpha=0.45, s=25, edgecolors="white", linewidths=0.3)
    axes[0].set_title("Cluster Assignment by Hour")
    axes[0].set_xlabel("Hour"); axes[0].set_ylabel("Cluster")
    axes[0].spines[["top","right"]].set_visible(False)
    cs = pd.Series(labels).value_counts().sort_index()
    axes[1].bar([f"Cluster {i}" for i in cs.index], cs.values,
                color=c4[:k], edgecolor="white")
    axes[1].set_title(f"Cluster Sizes  (Silhouette={score:.3f})")
    axes[1].set_ylabel("Incidents"); axes[1].spines[["top","right"]].set_visible(False)
    plt.suptitle(f"K-Means Clustering (k={k})", fontsize=13, fontweight="bold", color=NAVY)
    plt.tight_layout(); _save(fig, out, "11_clustering.png")
    return {"silhouette_score":round(score,4),"k":k,"cluster_sizes":cs.to_dict()}

# ─── 6. PREDICTION ────────────────────────────────────────────────────────────
def run_prediction(df, out):
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_absolute_error, r2_score
    daily = df.groupby("date").size().reset_index(name="count")
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.dropna().sort_values("date")
    if len(daily) < 5:
        print("  [!] Not enough daily data — skipping prediction.")
        return {}
    daily["ma3"] = daily["count"].rolling(3,min_periods=1).mean()
    daily["ma7"] = daily["count"].rolling(7,min_periods=1).mean()
    daily["t"]   = (daily["date"]-daily["date"].min()).dt.days
    X = daily["t"].values.reshape(-1,1); y = daily["count"].values
    lr = LinearRegression().fit(X, y)
    y_p = lr.predict(X)
    mae = mean_absolute_error(y, y_p)
    r2  = r2_score(y, y_p)
    fig, ax = plt.subplots(figsize=(12,5))
    ax.bar(daily["date"], daily["count"], color=BLUE, alpha=0.35, label="Daily", width=1)
    ax.plot(daily["date"], daily["ma3"], color=NAVY, linewidth=2, label="3-day MA")
    ax.plot(daily["date"], daily["ma7"], color="#e67e22", linewidth=2, linestyle="--", label="7-day MA")
    ax.plot(daily["date"], y_p, color="#e74c3c", linewidth=1.5, linestyle=":", label=f"Linear (R²={r2:.2f})")
    future_t = np.arange(daily["t"].max()+1, daily["t"].max()+8).reshape(-1,1)
    future_y = lr.predict(future_t)
    future_dates = pd.date_range(daily["date"].max()+pd.Timedelta(days=1), periods=7)
    ax.fill_between(future_dates, np.maximum(future_y-mae,0), future_y+mae,
                    alpha=0.15, color="#e74c3c", label="7-day forecast ±MAE")
    ax.plot(future_dates, future_y, color="#e74c3c", linewidth=2,
            linestyle="--", marker="o", markersize=4)
    ax.set_title("Daily Incident Trend + Forecast"); ax.set_xlabel("Date"); ax.set_ylabel("Incidents")
    ax.legend(frameon=False, fontsize=9); ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout(); _save(fig, out, "12_prediction.png")
    return {"mae":round(mae,3),"r2":round(r2,3),"slope_per_day":round(float(lr.coef_[0]),4)}

# ─── 7. HOTSPOT ───────────────────────────────────────────────────────────────
def hotspot_analysis(df, out):
    lc = df["location"].value_counts().reset_index()
    lc.columns = ["location","count"]
    fig, axes = plt.subplots(1,2,figsize=(14,5))
    top10 = lc.head(10)
    axes[0].barh(top10["location"].str[:40][::-1], top10["count"][::-1],
                 color=COLORS[:10][::-1], edgecolor="white", height=0.7)
    axes[0].set_title("Top 10 Hotspot Locations")
    axes[0].set_xlabel("Incidents"); axes[0].spines[["top","right","left"]].set_visible(False)
    hourly = df.groupby("hour").size()
    safe_h = hourly.sort_values().head(6)
    axes[1].bar(safe_h.index.astype(str), safe_h.values, color="#27ae60", edgecolor="white")
    axes[1].set_title("6 Safest Hours (Fewest Incidents)")
    axes[1].set_xlabel("Hour"); axes[1].set_ylabel("Incidents")
    axes[1].spines[["top","right"]].set_visible(False)
    plt.suptitle("Hotspot & Safety Analysis", fontsize=13, fontweight="bold", color=NAVY)
    plt.tight_layout(); _save(fig, out, "13_hotspot.png")
    return lc

# ─── 8. BIAS OF FEAR ──────────────────────────────────────────────────────────
def bias_of_fear(df, out):
    vkws = ["assault","robbery","theft","burglary","weapon","rape","sexual","dui","fight"]
    df2  = df.copy()
    df2["nature_str"] = df2["nature_of_incident"].fillna('').astype(str).str.lower()
    df2["is_violent"] = df2["nature_str"].apply(
        lambda x: any(kw in x for kw in vkws))
    counts = df2["is_violent"].value_counts()
    total  = len(df2)
    fig, axes = plt.subplots(1,2,figsize=(12,5))
    axes[0].pie([counts.get(False,0),counts.get(True,0)],
                labels=["Non-violent / Admin","Potentially violent"],
                colors=["#27ae60","#e74c3c"], autopct="%1.1f%%",
                startangle=90, wedgeprops={"linewidth":3,"edgecolor":"white"})
    axes[0].set_title("Incident Severity Breakdown")
    top_real = df["nature_of_incident"].value_counts().head(10)
    labels   = [t[:48]+("…" if len(t)>48 else "") for t in top_real.index]
    axes[1].barh(labels[::-1], top_real.values[::-1],
                 color=COLORS[:10][::-1], edgecolor="white", height=0.7)
    axes[1].set_title("What Actually Happens Most")
    axes[1].set_xlabel("Incidents"); axes[1].spines[["top","right","left"]].set_visible(False)
    plt.suptitle("Reducing Bias of Fear — Reality Check",
                 fontsize=13, fontweight="bold", color=NAVY)
    plt.tight_layout(); _save(fig, out, "14_bias_of_fear.png")
    pct = counts.get(True,0)/total*100 if total else 0
    print(f"  Violent/serious: {counts.get(True,0):,} ({pct:.1f}%) | Non-violent: {counts.get(False,0):,} ({100-pct:.1f}%)")

# ─── 9. EXPORTS ───────────────────────────────────────────────────────────────
def export_summary_csv(df, out_dir):
    s = df.groupby("final_campus").agg(
        total=("nature_of_incident","count"),
        unique_locations=("location","nunique"),
        weekend_incidents=("is_weekend","sum"),
        pct_weekend=("is_weekend", lambda x: round(x.mean()*100,1)),
        top_type=("nature_of_incident", lambda x: x.value_counts().index[0]),
    ).reset_index().sort_values("total",ascending=False)
    p = Path(out_dir)/"campus_summary.csv"
    s.to_csv(p, index=False); print(f"  [saved] {p}")

def export_full_csv(df, out_dir):
    p = Path(out_dir)/"cleaned_incidents.csv"
    df.to_csv(p, index=False); print(f"  [saved] {p}")

def export_patterns_csv(df, out_dir):
    fp = frequent_patterns(df)
    p  = Path(out_dir)/"frequent_patterns.csv"
    fp.to_csv(p, index=False); print(f"  [saved] {p}")

def export_hotspot_csv(df, out_dir):
    lc = df["location"].value_counts().reset_index()
    lc.columns = ["location","count"]
    p  = Path(out_dir)/"hotspot_locations.csv"
    lc.to_csv(p, index=False); print(f"  [saved] {p}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="PSU Campus Crime Analysis")
    p.add_argument("--csv",      default="data/processed/incidents.csv")
    p.add_argument("--campus",   default=None)
    p.add_argument("--output",   default="outputs")
    p.add_argument("--no-plots", action="store_true")
    return p.parse_args()

def main():
    args = parse_args()
    plots   = Path(args.output)/"plots"
    reports = Path(args.output)/"reports"
    plots.mkdir(parents=True, exist_ok=True)
    reports.mkdir(parents=True, exist_ok=True)

    df = load_data(args.csv, args.campus)
    print_summary(df)

    print("\n[+] Exporting CSVs …")
    export_summary_csv(df, str(reports))
    export_full_csv(df, str(reports))
    export_patterns_csv(df, str(reports))
    export_hotspot_csv(df, str(reports))

    fp = frequent_patterns(df)
    print("\n[+] Top Frequent Patterns:")
    print(fp.head(10).to_string(index=False))

    if not args.no_plots:
        print("\n[+] Generating charts …")
        plot_campus_bar(df, str(plots))
        plot_time_donut(df, str(plots))
        plot_day_of_week(df, str(plots))
        plot_incidents_per_year(df, str(plots))
        plot_hourly_line(df, str(plots))
        plot_heatmap(df, str(plots))
        plot_incident_types(df, str(plots))
        plot_stacked_by_time(df, str(plots))
        plot_weekday_vs_weekend(df, str(plots))
        plot_top_locations(df, str(plots))
        plot_patterns(df, str(plots))
        hotspot_analysis(df, str(plots))
        bias_of_fear(df, str(plots))

        print("\n[+] Clustering …")
        try:
            m = run_clustering(df, str(plots))
            if m:
                print(f"  Silhouette: {m.get('silhouette_score', 'N/A')}")
            else:
                print("  [!] Too few samples — clustering skipped")
        except ImportError:
            print("  [!] scikit-learn not installed — skipping clustering")
        except Exception as e:
            print(f"  [!] Clustering error: {e}")

        print("[+] Prediction …")
        try:
            m = run_prediction(df, str(plots))
            if m: print(f"  MAE={m['mae']}  R²={m['r2']}")
            else: print("  [!] Not enough daily data — prediction skipped")
        except ImportError:
            print("  [!] scikit-learn not installed — skipping prediction")
        except Exception as e:
            print(f"  [!] Prediction error: {e}")

    print(f"\n[✓] Done — outputs in: {args.output}/")

if __name__ == "__main__":
    main()
