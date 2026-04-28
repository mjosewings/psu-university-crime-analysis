"""
PSU Campus Crime Analysis — Full Tkinter Dashboard
====================================================
Loads real data from data/processed/incidents.csv.
Matches screenshot design with all professor suggestions implemented.

Run:
    python dashboard.py
    python dashboard.py --data ../../data/processed/incidents.csv

Deps: pip install matplotlib numpy pandas seaborn scikit-learn
"""

import os, sys, csv, argparse, warnings, re
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.patches as mpatches
import numpy as np
warnings.filterwarnings("ignore")

# ── Palette ──────────────────────────────────────────────────────
NAVY  = "#1b2d45"
NAVY2 = "#152338"
WHITE = "#ffffff"
BG    = "#f2f5f8"
CARD  = "#ffffff"
BORDER= "#d9e4ef"
MUTED = "#6b7f96"
DARK  = "#1b2d45"
CHART = ["#1b2d45","#2980b9","#27ae60","#e67e22","#9b59b6",
         "#e74c3c","#1abc9c","#f39c12","#3498db","#8e44ad",
         "#16a085","#c0392b","#2471a3","#148f77","#d35400"]
TC    = {"Morning":"#3498db","Afternoon":"#2ecc71",
         "Evening":"#e67e22","Late Night":"#9b59b6"}
DAYS  = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

MONTH_MAP = {"January":1,"February":2,"March":3,"April":4,"May":5,"June":6,
             "July":7,"August":8,"September":9,"October":10,"November":11,"December":12}


def _default_data():
    here = os.path.dirname(os.path.abspath(__file__))
    for rel in ["../../data/processed/incidents.csv",
                "../data/processed/incidents.csv",
                "data/processed/incidents.csv"]:
        p = os.path.normpath(os.path.join(here, rel))
        if os.path.exists(p):
            return p
    return None


def _bucket(h):
    try: h = int(float(h))
    except: return "Unknown"
    if  6<=h<12: return "Morning"
    if 12<=h<18: return "Afternoon"
    if 18<=h<22: return "Evening"
    return "Late Night"


def load_data(path=None):
    path = path or _default_data()
    if not path or not os.path.exists(path):
        print(f"[!] Data not found. Run preprocess.py first or pass --data <path>")
        sys.exit(1)
    print(f"[+] Loading: {path}")
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            r["is_weekend"] = str(r.get("is_weekend","False")).lower() in ("true","1","yes")
            if not r.get("time_bucket"):
                r["time_bucket"] = _bucket(r.get("hour",""))
            rows.append(r)
    print(f"[+] Loaded {len(rows):,} records")
    return rows


def card_frame(parent, **kw):
    return tk.Frame(parent, bg=CARD,
                    highlightbackground=BORDER, highlightthickness=1, **kw)


class App(tk.Tk):
    def __init__(self, data_path=None):
        super().__init__()
        self.title("Penn State Campus Crime Analysis")
        self.geometry("1380x920")
        self.minsize(1100, 720)
        self.configure(bg=BG)

        self.all_data = load_data(data_path)
        self.fdata    = list(self.all_data)
        self._tab     = "overview"

        self._build_topbar()
        self._build_navbar()
        self._build_scroll_host()
        self._build_header()
        self._build_filter_bar()
        self._build_metric_row()
        self._build_insight_row()
        self._build_tab_bar()
        self._panel_host = tk.Frame(self._main, bg=BG)
        self._panel_host.pack(fill="both", expand=True, padx=20, pady=(0,16))
        self._build_footer()

        self._apply_filters()

    # ─────────────────────────────────────────────────────────────
    # SCROLL HOST
    # ─────────────────────────────────────────────────────────────
    def _build_scroll_host(self):
        outer = tk.Frame(self, bg=BG)
        outer.pack(fill="both", expand=True)
        self._cv = tk.Canvas(outer, bg=BG, highlightthickness=0)
        sb = ttk.Scrollbar(outer, orient="vertical", command=self._cv.yview)
        self._cv.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self._cv.pack(side="left", fill="both", expand=True)
        self._main = tk.Frame(self._cv, bg=BG)
        self._win  = self._cv.create_window((0,0), window=self._main, anchor="nw")
        self._main.bind("<Configure>",
            lambda e: self._cv.configure(scrollregion=self._cv.bbox("all")))
        self._cv.bind("<Configure>",
            lambda e: self._cv.itemconfig(self._win, width=e.width))
        self._cv.bind_all("<MouseWheel>",
            lambda e: self._cv.yview_scroll(-1 if e.delta>0 else 1,"units"))

    # ─────────────────────────────────────────────────────────────
    # TOP BAR
    # ─────────────────────────────────────────────────────────────
    def _build_topbar(self):
        bar = tk.Frame(self, bg=NAVY, height=68)
        bar.pack(fill="x"); bar.pack_propagate(False)

        sc = tk.Canvas(bar, width=44, height=50, bg=NAVY, highlightthickness=0)
        sc.pack(side="left", padx=(22,10), pady=9)
        sc.create_polygon(22,2,5,10,5,26,22,46,39,26,39,10, fill=WHITE, outline="")
        sc.create_polygon(22,8,11,14,11,26,22,38,33,26,33,14, fill=NAVY, outline="")
        sc.create_text(22,24, text="C", font=("Helvetica",14,"bold"), fill=WHITE)

        tf = tk.Frame(bar, bg=NAVY); tf.pack(side="left", pady=10)
        tk.Label(tf, text="PennState", font=("Helvetica",20,"bold"),
                 fg=WHITE, bg=NAVY).pack(anchor="w")
        tk.Label(tf, text="Campus Crime Analysis", font=("Helvetica",10),
                 fg="#90aec8", bg=NAVY).pack(anchor="w")

        tk.Button(bar, text="REPORT A CONCERN",
                  font=("Helvetica",9,"bold"), fg=WHITE, bg=NAVY,
                  activebackground=WHITE, activeforeground=NAVY,
                  relief="solid", bd=1, padx=14, pady=6, cursor="hand2"
                  ).pack(side="right", padx=22, pady=18)

    def _build_navbar(self):
        nav = tk.Frame(self, bg=NAVY2, height=44)
        nav.pack(fill="x"); nav.pack_propagate(False)
        for i, lbl in enumerate(["Crime Dashboard","Campus Safety",
                                   "Annual Report","Resources"]):
            tk.Label(nav, text=lbl,
                     font=("Helvetica",11,"bold" if i==0 else "normal"),
                     fg=WHITE if i==0 else "#90aec8",
                     bg=NAVY2, padx=22, pady=10, cursor="hand2").pack(side="left")
        tk.Button(nav, text="REPORT A CONCERN",
                  font=("Helvetica",9,"bold"), fg=WHITE, bg=NAVY2,
                  activebackground=WHITE, activeforeground=NAVY2,
                  relief="solid", bd=1, padx=12, pady=5, cursor="hand2"
                  ).pack(side="right", padx=18, pady=8)

    def _build_header(self):
        f = tk.Frame(self._main, bg=BG, padx=24, pady=14); f.pack(fill="x")
        tk.Label(f, text="Campus crime analysis",
                 font=("Helvetica",22,"bold"), fg=DARK, bg=BG).pack(anchor="w")
        self._lbl_sub = tk.Label(f, text="", font=("Helvetica",11), fg=MUTED, bg=BG)
        self._lbl_sub.pack(anchor="w")

    # ─────────────────────────────────────────────────────────────
    # FILTER BAR
    # ─────────────────────────────────────────────────────────────
    def _build_filter_bar(self):
        outer = card_frame(self._main); outer.pack(fill="x", padx=20, pady=(0,12))
        row = tk.Frame(outer, bg=CARD, padx=16, pady=14); row.pack(fill="x")
        tk.Label(row, text="FILTER BY", font=("Helvetica",9,"bold"),
                 fg=MUTED, bg=CARD).grid(row=0, column=0, sticky="w", padx=(0,14))

        self._vc = tk.StringVar(value="All campuses")
        self._vt = tk.StringVar(value="All times")
        self._vd = tk.StringVar(value="All days")
        self._vm = tk.StringVar(value="All months")
        self._vy = tk.StringVar(value="All years")

        campuses = sorted(set(r.get("campus","") for r in self.all_data if r.get("campus")))
        years    = sorted(set(str(r.get("year","")) for r in self.all_data
                             if r.get("year","").strip().isdigit()), reverse=True)

        specs = [
            (1, self._vc, ["All campuses"] + campuses),
            (2, self._vt, ["All times","Morning","Afternoon","Evening","Late Night"]),
            (3, self._vd, ["All days","Weekday","Weekend"]),
            (4, self._vm, ["All months"] + list(MONTH_MAP.keys())),
            (5, self._vy, ["All years"] + years),
        ]
        for col, var, opts in specs:
            cb = ttk.Combobox(row, textvariable=var, values=opts,
                              state="readonly", width=15, font=("Helvetica",10))
            cb.grid(row=0, column=col, padx=5)
            cb.bind("<<ComboboxSelected>>", lambda _: self._apply_filters())

        tk.Button(row, text="Add Incident",
                  font=("Helvetica",9,"bold"), fg=WHITE, bg="#e67e22",
                  activebackground="#ca6f1e", relief="flat", padx=10, pady=5,
                  cursor="hand2", command=lambda: self._switch("add")
                  ).grid(row=0, column=6, padx=8)
        tk.Button(row, text="Export CSV",
                  font=("Helvetica",9,"bold"), fg=WHITE, bg="#27ae60",
                  activebackground="#1e8449", relief="flat", padx=10, pady=5,
                  cursor="hand2", command=self._export_csv
                  ).grid(row=0, column=7, padx=4)
        tk.Button(row, text="Export Charts",
                  font=("Helvetica",9,"bold"), fg=WHITE, bg=NAVY,
                  activebackground="#2980b9", relief="flat", padx=10, pady=5,
                  cursor="hand2", command=self._export_charts
                  ).grid(row=0, column=8, padx=4)

    # ─────────────────────────────────────────────────────────────
    # METRIC CARDS
    # ─────────────────────────────────────────────────────────────
    def _build_metric_row(self):
        row = tk.Frame(self._main, bg=BG); row.pack(fill="x", padx=20, pady=(0,10))
        row.columnconfigure((0,1,2,3), weight=1)
        self._m = {}
        specs = [("total","TOTAL INCIDENTS","#1b2d45"),
                 ("camps","CAMPUSES","#2980b9"),
                 ("peak","PEAK TIME","#27ae60"),
                 ("wknd","WEEKEND SHARE","#e67e22")]
        for i,(k,lbl,acc) in enumerate(specs):
            c = card_frame(row); c.grid(row=0,column=i,sticky="nsew",padx=5,pady=2)
            tk.Frame(c, bg=acc, height=4).pack(fill="x")
            tk.Label(c, text=lbl, font=("Helvetica",9,"bold"),
                     fg=MUTED, bg=CARD, anchor="w", padx=18, pady=(12,2)).pack(fill="x")
            v = tk.Label(c, text="—", font=("Helvetica",30,"bold"),
                         fg=DARK, bg=CARD, anchor="w", padx=18); v.pack(fill="x")
            s = tk.Label(c, text=" ", font=("Helvetica",10), fg="#8aa5bf",
                         bg=CARD, anchor="w", padx=18, pady=(0,14)); s.pack(fill="x")
            self._m[k] = (v, s)

    # ─────────────────────────────────────────────────────────────
    # INSIGHT BANNERS
    # ─────────────────────────────────────────────────────────────
    def _build_insight_row(self):
        row = tk.Frame(self._main, bg=BG); row.pack(fill="x", padx=20, pady=(0,10))
        row.columnconfigure((0,1,2), weight=1)
        self._ins = {}
        for i,(k,lbl) in enumerate([("ic","TOP CAMPUS"),("il","TOP LOCATION"),
                                     ("it","MOST COMMON INCIDENT")]):
            c = tk.Frame(row, bg=NAVY, highlightbackground="#2d4a6a", highlightthickness=1)
            c.grid(row=0, column=i, sticky="nsew", padx=5, pady=2)
            tk.Label(c, text=lbl, font=("Helvetica",8,"bold"), fg="#90aec8",
                     bg=NAVY, anchor="w", padx=18, pady=(10,2)).pack(fill="x")
            v = tk.Label(c, text="—", font=("Helvetica",15,"bold"),
                         fg=WHITE, bg=NAVY, anchor="w", padx=18, pady=(0,12))
            v.pack(fill="x"); self._ins[k] = v

    # ─────────────────────────────────────────────────────────────
    # TAB BAR
    # ─────────────────────────────────────────────────────────────
    def _build_tab_bar(self):
        outer = card_frame(self._main); outer.pack(fill="x", padx=20, pady=(0,10))
        inner = tk.Frame(outer, bg=CARD); inner.pack(fill="x")
        self._tbtn = {}
        tabs = [("overview","Overview"),("time","Time Patterns"),
                ("locations","Locations"),("types","Incident Types"),
                ("patterns","Patterns"),("predict","Prediction"),
                ("add","Add Record")]
        for k, lbl in tabs:
            b = tk.Button(inner, text=lbl, font=("Helvetica",11),
                          fg=MUTED, bg=CARD, activebackground=CARD,
                          relief="flat", bd=0, padx=18, pady=12,
                          cursor="hand2", command=lambda k=k: self._switch(k))
            b.pack(side="left"); self._tbtn[k] = b

    def _build_footer(self):
        f = tk.Frame(self._main, bg=NAVY2, pady=12); f.pack(fill="x")
        tk.Label(f, text="Penn State University — Campus Crime Log Analysis Tool  |  "
                         "Data sourced from PSU Police Services  |  "
                         "psu-university-crime-analysis project",
                 font=("Helvetica",9), fg="#7a9fbe", bg=NAVY2).pack()

    # ─────────────────────────────────────────────────────────────
    # FILTERING
    # ─────────────────────────────────────────────────────────────
    def _apply_filters(self):
        c=self._vc.get(); t=self._vt.get(); d=self._vd.get()
        m=self._vm.get(); y=self._vy.get()
        mo_num = str(MONTH_MAP.get(m, ""))

        self.fdata = [
            r for r in self.all_data
            if (c=="All campuses" or r.get("campus","")==c)
            and (t=="All times"   or r.get("time_bucket","")==t)
            and (d=="All days"
                 or (d=="Weekday" and not r["is_weekend"])
                 or (d=="Weekend" and r["is_weekend"]))
            and (m=="All months"  or str(r.get("month",""))==mo_num)
            and (y=="All years"   or str(r.get("year",""))==y)
        ]
        self._refresh_kpis()
        self._switch(self._tab)

    def _refresh_kpis(self):
        D=self.fdata; N=len(D)
        camps = len({r.get("campus","") for r in D})
        self._lbl_sub.configure(
            text=f"Showing {N:,} incidents across {camps} campuses")
        self._m["total"][0].configure(text=f"{N:,}")
        self._m["total"][1].configure(text="total incidents recorded")
        self._m["camps"][0].configure(text=str(camps))
        self._m["camps"][1].configure(text="campuses represented")

        tb = {}
        for r in D: tb[r.get("time_bucket","")]=tb.get(r.get("time_bucket",""),0)+1
        if tb:
            pk = max(tb, key=tb.get)
            self._m["peak"][0].configure(text=pk, font=("Helvetica",17,"bold"))
            self._m["peak"][1].configure(text=f"{tb[pk]:,} incidents")

        wk = sum(1 for r in D if r["is_weekend"])
        pct = round(wk/N*100) if N else 0
        self._m["wknd"][0].configure(text=f"{pct}%")
        self._m["wknd"][1].configure(text=f"{wk:,} weekend incidents")

        cc = {}
        for r in D: cc[r.get("campus","")]=cc.get(r.get("campus",""),0)+1
        tc = max(cc, key=cc.get) if cc else "—"
        self._ins["ic"].configure(text=f"{tc}  —  {cc.get(tc,0):,} incidents")

        lc = {}
        for r in D: lc[r.get("location","")]=lc.get(r.get("location",""),0)+1
        tl = max(lc, key=lc.get) if lc else "—"
        s = tl[:42]+("…" if len(tl)>42 else "")
        self._ins["il"].configure(text=f"{s}  ({lc.get(tl,0)})")

        nc = {}
        for r in D: nc[r.get("nature_of_incident","")]=nc.get(r.get("nature_of_incident",""),0)+1
        tn = max(nc, key=nc.get) if nc else "—"
        self._ins["it"].configure(text=str(tn)[:60]+("…" if len(str(tn))>60 else ""))

    # ─────────────────────────────────────────────────────────────
    # TAB SWITCH
    # ─────────────────────────────────────────────────────────────
    def _switch(self, key):
        self._tab = key
        for k,b in self._tbtn.items():
            b.configure(fg=DARK if k==key else MUTED,
                        font=("Helvetica",11,"bold" if k==key else "normal"))
        for w in self._panel_host.winfo_children(): w.destroy()
        {"overview":self._panel_overview,
         "time":    self._panel_time,
         "locations":self._panel_locations,
         "types":  self._panel_types,
         "patterns":self._panel_patterns,
         "predict": self._panel_predict,
         "add":    self._panel_add}[key](self._panel_host)

    def _embed(self, fig, parent):
        cv = FigureCanvasTkAgg(fig, master=parent)
        cv.draw()
        w = cv.get_tk_widget()
        w.configure(bg=CARD, highlightthickness=0)
        w.pack(fill="both", expand=True)
        return cv

    def _card_title(self, parent, text):
        tk.Label(parent, text=text, font=("Helvetica",9,"bold"),
                 fg=DARK, bg=CARD, anchor="w", padx=14, pady=10).pack(fill="x")
        tk.Frame(parent, bg=BORDER, height=1).pack(fill="x")

    # ═════════════════════════════════════════════════════════════
    # OVERVIEW PANEL
    # ═════════════════════════════════════════════════════════════
    def _panel_overview(self, host):
        D = self.fdata
        r1 = tk.Frame(host, bg=BG); r1.pack(fill="both", expand=True)
        r1.columnconfigure(0,weight=5); r1.columnconfigure(1,weight=3); r1.columnconfigure(2,weight=3)

        # ── Campus bar ────────────────────────────────────────────
        c1 = card_frame(r1); c1.grid(row=0,column=0,sticky="nsew",padx=(0,6),pady=4)
        self._card_title(c1, "INCIDENTS BY CAMPUS (TOP 15)")
        cc = {}
        for r in D: cc[r.get("campus","")]=cc.get(r.get("campus",""),0)+1
        top = sorted(cc.items(), key=lambda x:-x[1])[:15]
        labs=[t[0] for t in top][::-1]; vals=[t[1] for t in top][::-1]
        fig1=Figure(figsize=(5.6,max(3.2,len(labs)*0.42)),facecolor=CARD)
        fig1.subplots_adjust(left=0.28,right=0.93,top=0.97,bottom=0.04)
        ax=fig1.add_subplot(111)
        ax.barh(labs,vals,color=CHART[:len(labs)][::-1],edgecolor="white",linewidth=0.4,height=0.66)
        mv=max(vals) if vals else 1
        for bar in ax.patches:
            ax.text(bar.get_width()+mv*0.01,bar.get_y()+bar.get_height()/2,
                    f"{int(bar.get_width()):,}",va="center",fontsize=7.5,color=DARK)
        ax.tick_params(axis="y",labelsize=8.5,colors=DARK)
        ax.tick_params(axis="x",labelsize=7,colors=MUTED)
        ax.spines[["top","right","left"]].set_visible(False)
        ax.set_facecolor(CARD); fig1.patch.set_facecolor(CARD)
        self._embed(fig1, c1)

        # ── Time donut ────────────────────────────────────────────
        c2 = card_frame(r1); c2.grid(row=0,column=1,sticky="nsew",padx=3,pady=4)
        self._card_title(c2, "TIME OF DAY BREAKDOWN")
        tb_o=["Morning","Afternoon","Evening","Late Night"]
        tb={k:0 for k in tb_o}
        for r in D: tb[r.get("time_bucket","")]=tb.get(r.get("time_bucket",""),0)+1
        sz=[tb[k] for k in tb_o]; tot=sum(sz)
        fig2=Figure(figsize=(3.3,3.4),facecolor=CARD)
        fig2.subplots_adjust(top=0.80,bottom=0.03,left=0.03,right=0.97)
        pcts=[f"■ {k[:4]}. {round(tb[k]/tot*100) if tot else 0}%" for k in tb_o]
        fig2.text(0.5,0.92,"  ".join(pcts[:2]),ha="center",fontsize=6.2,color=MUTED)
        fig2.text(0.5,0.86,"  ".join(pcts[2:]),ha="center",fontsize=6.2,color=MUTED)
        ax2=fig2.add_subplot(111)
        _,_,ats=ax2.pie(sz,colors=[TC[k] for k in tb_o],startangle=90,
                        autopct="%1.0f%%",pctdistance=0.76,
                        wedgeprops={"linewidth":3,"edgecolor":"white"})
        for at in ats: at.set_fontsize(8); at.set_color("white"); at.set_fontweight("bold")
        ax2.add_patch(plt.Circle((0,0),0.55,color="white"))
        ax2.set_facecolor(CARD); fig2.patch.set_facecolor(CARD)
        self._embed(fig2, c2)

        # ── Weekday/Weekend donut ────────────────────────────────
        c3 = card_frame(r1); c3.grid(row=0,column=2,sticky="nsew",padx=(6,0),pady=4)
        self._card_title(c3, "WEEKDAY VS WEEKEND")
        wk=sum(1 for r in D if r["is_weekend"]); wd=len(D)-wk; tot2=len(D)
        wkp=round(wk/tot2*100) if tot2 else 0
        fig3=Figure(figsize=(3.3,3.4),facecolor=CARD)
        fig3.subplots_adjust(top=0.82,bottom=0.03,left=0.03,right=0.97)
        fig3.text(0.5,0.91,f"■ Weekday {100-wkp}%   ■ Weekend {wkp}%",
                  ha="center",fontsize=7,color=MUTED)
        ax3=fig3.add_subplot(111)
        _,_,ats3=ax3.pie([wd,wk],colors=["#2980b9","#e67e22"],startangle=90,
                         autopct="%1.0f%%",pctdistance=0.76,
                         wedgeprops={"linewidth":3,"edgecolor":"white"})
        for at in ats3: at.set_fontsize(8); at.set_color("white"); at.set_fontweight("bold")
        ax3.add_patch(plt.Circle((0,0),0.55,color="white"))
        ax3.set_facecolor(CARD); fig3.patch.set_facecolor(CARD)
        self._embed(fig3, c3)

        # ── Row 2: hourly line + day of week bar ─────────────────
        r2=tk.Frame(host,bg=BG); r2.pack(fill="x",pady=(8,0))
        r2.columnconfigure(0,weight=5); r2.columnconfigure(1,weight=4)

        c4=card_frame(r2); c4.grid(row=0,column=0,sticky="nsew",padx=(0,6),pady=4)
        self._card_title(c4,"INCIDENTS BY HOUR OF DAY")
        hc=[0]*24
        for r in D:
            try: hc[int(float(r.get("hour",0)))]+=1
            except: pass
        fig4=Figure(figsize=(5.6,2.7),facecolor=CARD)
        fig4.subplots_adjust(left=0.07,right=0.97,top=0.93,bottom=0.20)
        ax4=fig4.add_subplot(111)
        ax4.fill_between(range(24),hc,alpha=0.09,color=NAVY)
        ax4.plot(range(24),hc,color=NAVY,linewidth=2.2,marker="o",markersize=3.8,
                 markerfacecolor="#2980b9",markeredgecolor="white",markeredgewidth=1.3)
        ax4.set_xticks(range(0,24,2))
        ax4.set_xticklabels([f"{h:02d}" for h in range(0,24,2)],fontsize=7.5,color=MUTED)
        ax4.tick_params(axis="y",labelsize=7.5,colors=MUTED)
        ax4.spines[["top","right"]].set_visible(False)
        ax4.set_facecolor(CARD); fig4.patch.set_facecolor(CARD)
        self._embed(fig4,c4)

        c5=card_frame(r2); c5.grid(row=0,column=1,sticky="nsew",padx=(6,0),pady=4)
        self._card_title(c5,"DAY OF WEEK PATTERN")
        dc={d2:0 for d2 in DAYS}
        for r in D: dc[r.get("day_of_week","")]=dc.get(r.get("day_of_week",""),0)+1
        bcols=[NAVY if d2 not in("Saturday","Sunday") else "#e67e22" for d2 in DAYS]
        fig5=Figure(figsize=(3.6,2.7),facecolor=CARD)
        fig5.subplots_adjust(left=0.08,right=0.97,top=0.93,bottom=0.24)
        ax5=fig5.add_subplot(111)
        ax5.bar([d2[:3] for d2 in DAYS],[dc[d2] for d2 in DAYS],
                color=bcols,edgecolor="white",linewidth=0.4)
        ax5.tick_params(axis="x",labelsize=8.5,colors=DARK)
        ax5.tick_params(axis="y",labelsize=7.5,colors=MUTED)
        ax5.spines[["top","right"]].set_visible(False)
        ax5.set_facecolor(CARD); fig5.patch.set_facecolor(CARD)
        ax5.legend(handles=[mpatches.Patch(color=NAVY,label="Weekday"),
                             mpatches.Patch(color="#e67e22",label="Weekend")],
                   fontsize=7,frameon=False,loc="upper right")
        self._embed(fig5,c5)

    # ═════════════════════════════════════════════════════════════
    # TIME PATTERNS PANEL
    # ═════════════════════════════════════════════════════════════
    def _panel_time(self, host):
        D=self.fdata
        c1=card_frame(host); c1.pack(fill="x",pady=(0,10))
        self._card_title(c1,"INCIDENT HEATMAP — DAY OF WEEK × HOUR OF DAY")
        gd={d2:[0]*24 for d2 in DAYS}
        for r in D:
            try:
                day=r.get("day_of_week",""); hr=int(float(r.get("hour",0)))
                if day in gd: gd[day][hr]+=1
            except: pass
        mat=np.array([[gd[d2][h] for h in range(24)] for d2 in DAYS],dtype=float)
        cmap=LinearSegmentedColormap.from_list("psu",["#e8f3fb","#85b7eb","#2980b9","#1b2d45"])
        fig1=Figure(figsize=(11,3.4),facecolor=CARD)
        fig1.subplots_adjust(left=0.07,right=0.97,top=0.92,bottom=0.12)
        ax1=fig1.add_subplot(111)
        im=ax1.imshow(mat/max(mat.max(),1),cmap=cmap,aspect="auto")
        ax1.set_xticks(range(24)); ax1.set_xticklabels([str(h) for h in range(24)],fontsize=7.5)
        ax1.set_yticks(range(7))
        ax1.set_yticklabels([d2[:3] for d2 in DAYS],fontsize=9,fontweight="bold",color=DARK)
        for i,d2 in enumerate(DAYS):
            for h in range(24):
                v=int(mat[i][h])
                if v:
                    nr=mat[i][h]/max(mat.max(),1)
                    ax1.text(h,i,str(v),ha="center",va="center",fontsize=5.5,
                             color="white" if nr>0.45 else MUTED)
        fig1.colorbar(im,ax=ax1,shrink=0.7,label="Relative intensity")
        ax1.set_facecolor(CARD); fig1.patch.set_facecolor(CARD)
        self._embed(fig1,c1)

        c2=card_frame(host); c2.pack(fill="x",pady=(0,10))
        self._card_title(c2,"MONTHLY INCIDENT TREND")
        from collections import Counter
        mo_c=Counter()
        for r in D:
            y=str(r.get("year","")); m=str(r.get("month",""))
            if y.strip().isdigit() and m.strip().isdigit():
                mo_c[f"{y}-{int(m):02d}"]+=1
        periods=sorted(mo_c.keys()); counts=[mo_c[p] for p in periods]
        if len(counts)>=2:
            fig6=Figure(figsize=(11,2.8),facecolor=CARD)
            fig6.subplots_adjust(left=0.05,right=0.98,top=0.92,bottom=0.22)
            ax6=fig6.add_subplot(111)
            ax6.fill_between(range(len(counts)),counts,alpha=0.08,color=NAVY)
            ax6.plot(range(len(counts)),counts,color=NAVY,linewidth=2,marker="o",markersize=4,
                     markerfacecolor="#2980b9",markeredgecolor="white",markeredgewidth=1.5)
            step=max(1,len(periods)//12)
            ax6.set_xticks(range(0,len(periods),step))
            ax6.set_xticklabels(periods[::step],rotation=40,ha="right",fontsize=8)
            ax6.spines[["top","right"]].set_visible(False)
            ax6.set_facecolor(CARD); fig6.patch.set_facecolor(CARD)
            self._embed(fig6,c2)
        else:
            tk.Label(c2,text="Not enough data for monthly trend.",
                     font=("Helvetica",11),fg=MUTED,bg=CARD,pady=20).pack()

    # ═════════════════════════════════════════════════════════════
    # LOCATIONS PANEL
    # ═════════════════════════════════════════════════════════════
    def _panel_locations(self, host):
        D=self.fdata
        lc={}
        for r in D: lc[r.get("location","")]=lc.get(r.get("location",""),0)+1
        items=sorted(lc.items(),key=lambda x:-x[1])[:30]
        total=len(D); mx=items[0][1] if items else 1
        pm={}
        for r in D:
            loc=r.get("location",""); tb=r.get("time_bucket","")
            pm.setdefault(loc,{})[tb]=pm.get(loc,{}).get(tb,0)+1
        BDG={"Morning":("#ddf0fa","#1a6e8a"),"Afternoon":("#d8f5e8","#1a6e3a"),
             "Evening":("#fef3dc","#7a4e00"),"Late Night":("#ede8f8","#4a2d8a")}

        outer=card_frame(host); outer.pack(fill="both",expand=True)
        self._card_title(outer,f"TOP LOCATIONS  ({len(items)} shown)")
        cv=tk.Canvas(outer,bg=CARD,highlightthickness=0)
        sb=ttk.Scrollbar(outer,orient="vertical",command=cv.yview)
        cv.configure(yscrollcommand=sb.set); sb.pack(side="right",fill="y")
        cv.pack(side="left",fill="both",expand=True)
        inn=tk.Frame(cv,bg=CARD); cv.create_window((0,0),window=inn,anchor="nw")

        for col,(h,w) in enumerate(zip(["#","Location","Count","Frequency","Peak Time"],
                                        [4,44,10,24,14])):
            tk.Label(inn,text=h,font=("Helvetica",9,"bold"),fg=MUTED,
                     bg="#ecf3fa",width=w,anchor="w",padx=8,pady=7
                     ).grid(row=0,column=col,sticky="ew",padx=1)

        for idx,(loc,cnt) in enumerate(items):
            bg=CARD if idx%2==0 else "#f7fafe"
            pct=round(cnt/total*100,1) if total else 0
            bw=int(cnt/mx*110)
            tm=pm.get(loc,{}); pk=max(tm,key=tm.get) if tm else "—"
            bg2,fg2=BDG.get(pk,("#eee","#444"))
            tk.Label(inn,text=str(idx+1),font=("Helvetica",9,"bold"),fg=MUTED,
                     bg=bg,width=4,padx=8,pady=7).grid(row=idx+1,column=0,sticky="ew")
            tk.Label(inn,text=str(loc)[:54],font=("Helvetica",9),fg=DARK,
                     bg=bg,width=44,anchor="w",padx=8,pady=7).grid(row=idx+1,column=1,sticky="ew")
            tk.Label(inn,text=f"{cnt:,}",font=("Helvetica",9,"bold"),fg=DARK,
                     bg=bg,width=10,padx=8).grid(row=idx+1,column=2,sticky="ew")
            bf=tk.Frame(inn,bg=bg); bf.grid(row=idx+1,column=3,sticky="ew",padx=8)
            bo=tk.Frame(bf,bg="#e0eaf5",width=110,height=7)
            bo.pack(side="left",pady=9); bo.pack_propagate(False)
            tk.Frame(bo,bg="#2980b9",width=bw,height=7).pack(side="left")
            tk.Label(bf,text=f" {pct}%",font=("Helvetica",8),fg=MUTED,bg=bg).pack(side="left")
            tk.Label(inn,text=pk,font=("Helvetica",8,"bold"),fg=fg2,bg=bg2,
                     width=14,padx=6,pady=2).grid(row=idx+1,column=4,sticky="w",padx=8)

        inn.update_idletasks(); cv.configure(scrollregion=cv.bbox("all"))
        cv.bind("<MouseWheel>",lambda e: cv.yview_scroll(-1 if e.delta>0 else 1,"units"))

    # ═════════════════════════════════════════════════════════════
    # INCIDENT TYPES PANEL
    # ═════════════════════════════════════════════════════════════
    def _panel_types(self, host):
        D=self.fdata
        nc={}
        for r in D: nc[r.get("nature_of_incident","")]=nc.get(r.get("nature_of_incident",""),0)+1
        top=sorted(nc.items(),key=lambda x:-x[1])[:15]
        top5=[t[0] for t in top[:5]]

        row=tk.Frame(host,bg=BG); row.pack(fill="both",expand=True)
        row.columnconfigure(0,weight=3); row.columnconfigure(1,weight=2)

        c1=card_frame(row); c1.grid(row=0,column=0,sticky="nsew",padx=(0,6),pady=4)
        self._card_title(c1,"TOP 15 INCIDENT TYPES")
        labs=[str(t[0])[:55]+("…" if len(str(t[0]))>55 else "") for t in top][::-1]
        vals=[t[1] for t in top][::-1]
        fig1=Figure(figsize=(5.5,max(4,len(top)*0.48)),facecolor=CARD)
        fig1.subplots_adjust(left=0.42,right=0.95,top=0.97,bottom=0.04)
        ax1=fig1.add_subplot(111)
        ax1.barh(labs,vals,color=CHART[:len(vals)][::-1],edgecolor="white",linewidth=0.3,height=0.66)
        mv=max(vals) if vals else 1
        for bar in ax1.patches:
            ax1.text(bar.get_width()+mv*0.005,bar.get_y()+bar.get_height()/2,
                     f"{int(bar.get_width()):,}",va="center",fontsize=7.5,color=DARK)
        ax1.tick_params(axis="y",labelsize=7.5,colors=DARK)
        ax1.spines[["top","right","left"]].set_visible(False)
        ax1.set_facecolor(CARD); fig1.patch.set_facecolor(CARD)
        self._embed(fig1,c1)

        c2=card_frame(row); c2.grid(row=0,column=1,sticky="nsew",padx=(6,0),pady=4)
        self._card_title(c2,"TOP 5 TYPES BY TIME BUCKET")
        tbs=["Morning","Afternoon","Evening","Late Night"]
        x=np.arange(len(tbs)); bot=np.zeros(len(tbs))
        fig2=Figure(figsize=(3.4,max(4,len(top)*0.48)),facecolor=CARD)
        fig2.subplots_adjust(left=0.10,right=0.97,top=0.93,bottom=0.16)
        ax2=fig2.add_subplot(111)
        c5c=["#2980b9","#27ae60","#e67e22","#9b59b6","#e74c3c"]
        for i,nat in enumerate(top5):
            vs=[sum(1 for r in D if r.get("nature_of_incident","")==nat
                    and r.get("time_bucket","")==tb) for tb in tbs]
            lb=str(nat)[:28]+("…" if len(str(nat))>28 else "")
            ax2.bar(x,vs,bottom=bot,color=c5c[i],edgecolor="white",linewidth=0.3,label=lb)
            bot+=np.array(vs)
        ax2.set_xticks(x); ax2.set_xticklabels(["Morn","Aftn","Eve","Night"],fontsize=9,color=DARK)
        ax2.tick_params(axis="y",labelsize=7.5,colors=MUTED)
        ax2.spines[["top","right"]].set_visible(False)
        ax2.legend(fontsize=6.5,frameon=False,loc="upper right")
        ax2.set_facecolor(CARD); fig2.patch.set_facecolor(CARD)
        self._embed(fig2,c2)

    # ═════════════════════════════════════════════════════════════
    # FREQUENT PATTERNS PANEL
    # ═════════════════════════════════════════════════════════════
    def _panel_patterns(self, host):
        D=self.fdata
        from collections import Counter
        combos=Counter()
        for r in D:
            tb=r.get("time_bucket","Unknown")
            dt="Weekend" if r["is_weekend"] else "Weekday"
            nat=str(r.get("nature_of_incident",""))[:55]
            combos[(tb,dt,nat)]+=1
        top=combos.most_common(25)

        outer=card_frame(host); outer.pack(fill="both",expand=True)
        self._card_title(outer,"FREQUENT PATTERNS — Time Bucket × Day Type × Incident Type")
        cv=tk.Canvas(outer,bg=CARD,highlightthickness=0)
        sb=ttk.Scrollbar(outer,orient="vertical",command=cv.yview)
        cv.configure(yscrollcommand=sb.set); sb.pack(side="right",fill="y")
        cv.pack(side="left",fill="both",expand=True)
        inn=tk.Frame(cv,bg=CARD); cv.create_window((0,0),window=inn,anchor="nw")

        for col,(h,w) in enumerate(zip(["Rank","Time Bucket","Day Type","Incident Type","Count"],
                                        [5,14,10,62,8])):
            tk.Label(inn,text=h,font=("Helvetica",9,"bold"),fg=MUTED,
                     bg="#ecf3fa",width=w,anchor="w",padx=8,pady=7
                     ).grid(row=0,column=col,sticky="ew",padx=1)

        for idx,((tb,dt,nat),cnt) in enumerate(top):
            bg=CARD if idx%2==0 else "#f7fafe"
            tk.Label(inn,text=str(idx+1),font=("Helvetica",9,"bold"),fg=MUTED,
                     bg=bg,width=5,padx=8,pady=8).grid(row=idx+1,column=0,sticky="ew")
            tk.Label(inn,text=tb,font=("Helvetica",9),fg=DARK,bg=bg,
                     width=14,padx=8,anchor="w").grid(row=idx+1,column=1,sticky="ew")
            tk.Label(inn,text=dt,font=("Helvetica",9),fg=DARK,bg=bg,
                     width=10,padx=8,anchor="w").grid(row=idx+1,column=2,sticky="ew")
            tk.Label(inn,text=nat,font=("Helvetica",9),fg=DARK,bg=bg,
                     width=62,padx=8,anchor="w").grid(row=idx+1,column=3,sticky="ew")
            tk.Label(inn,text=f"{cnt:,}",font=("Helvetica",9,"bold"),
                     fg="#2980b9",bg=bg,width=8,padx=8).grid(row=idx+1,column=4,sticky="ew")

        inn.update_idletasks(); cv.configure(scrollregion=cv.bbox("all"))

    # ═════════════════════════════════════════════════════════════
    # PREDICTION PANEL
    # ═════════════════════════════════════════════════════════════
    def _panel_predict(self, host):
        D=self.fdata
        from collections import Counter
        mo_c=Counter()
        for r in D:
            y=str(r.get("year","")); m=str(r.get("month",""))
            if y.strip().isdigit() and m.strip().isdigit():
                mo_c[f"{y}-{int(m):02d}"]+=1
        periods=sorted(mo_c.keys()); counts=[mo_c[p] for p in periods]

        outer=card_frame(host); outer.pack(fill="both",expand=True)
        self._card_title(outer,"PREDICTION — Moving Average & Linear Regression")

        if len(counts)<4:
            tk.Label(outer,text="Not enough monthly data. Please use a wider date range or fewer filters.",
                     font=("Helvetica",12),fg=MUTED,bg=CARD,pady=30).pack()
            return

        window=3
        ma=np.convolve(counts,np.ones(window)/window,mode="valid")
        pred_ma=float(ma[-1])

        try:
            from sklearn.linear_model import LinearRegression
            X=np.arange(len(counts)).reshape(-1,1)
            lr=LinearRegression().fit(X,counts)
            pred_lr=float(lr.predict([[len(counts)]])[0])
            r2=lr.score(X,counts)
            mae=float(np.mean(np.abs(np.array(counts,float)-lr.predict(X))))
        except:
            pred_lr=pred_ma; r2=None; mae=None

        fig=Figure(figsize=(11,3.8),facecolor=CARD)
        fig.subplots_adjust(left=0.06,right=0.97,top=0.88,bottom=0.22)
        ax=fig.add_subplot(111)
        ax.plot(range(len(counts)),counts,color=NAVY,linewidth=2,
                marker="o",markersize=4,label="Actual")
        ax.plot(list(range(window-1,len(counts))),ma,
                color="#e67e22",linewidth=1.8,linestyle="--",
                label=f"Moving Avg (w={window})")
        ax.scatter([len(counts)],[pred_ma],color="#e67e22",s=90,zorder=5)
        ax.scatter([len(counts)],[pred_lr],color="#27ae60",s=90,zorder=5,
                   marker="D",label=f"LR next: {pred_lr:.0f}")
        ax.annotate(f"MA: {pred_ma:.0f}",xy=(len(counts),pred_ma),
                    xytext=(max(0,len(counts)-3),pred_ma+np.std(counts)*0.6),
                    fontsize=8,color="#e67e22",
                    arrowprops={"arrowstyle":"->","color":"#e67e22"})
        step=max(1,len(periods)//12)
        ax.set_xticks(range(0,len(periods),step))
        ax.set_xticklabels(periods[::step],rotation=40,ha="right",fontsize=8)
        ax.set_ylabel("Incidents"); ax.legend(frameon=False,fontsize=9)
        ax.spines[["top","right"]].set_visible(False)
        ax.set_facecolor(CARD); fig.patch.set_facecolor(CARD)
        self._embed(fig,outer)

        sf=tk.Frame(outer,bg=CARD,padx=20,pady=10); sf.pack(fill="x")
        for lbl,val in [("Moving Avg Prediction",f"{pred_ma:.0f} incidents"),
                         ("Linear Reg Prediction",f"{pred_lr:.0f} incidents"),
                         ("MAE (training)",f"{mae:.1f}" if mae is not None else "N/A"),
                         ("R² Score",f"{r2:.3f}" if r2 is not None else "N/A")]:
            f2=tk.Frame(sf,bg="#f0f6ff",highlightbackground=BORDER,
                        highlightthickness=1,padx=14,pady=8)
            f2.pack(side="left",padx=8)
            tk.Label(f2,text=lbl,font=("Helvetica",8,"bold"),fg=MUTED,bg="#f0f6ff").pack()
            tk.Label(f2,text=val,font=("Helvetica",14,"bold"),fg=NAVY,bg="#f0f6ff").pack()

    # ═════════════════════════════════════════════════════════════
    # ADD RECORD PANEL
    # ═════════════════════════════════════════════════════════════
    def _panel_add(self, host):
        outer=card_frame(host); outer.pack(fill="both",expand=True)
        self._card_title(outer,"ADD NEW INCIDENT RECORD")
        body=tk.Frame(outer,bg=CARD,padx=28,pady=20); body.pack(fill="both",expand=True)
        body.columnconfigure((0,1),weight=1)

        flds={}
        campuses=sorted(set(r.get("campus","") for r in self.all_data if r.get("campus")))
        specs=[
            ("campus","Campus","combo",["Unknown"]+campuses),
            ("loc","Location","entry",None),
            ("nature","Nature of Incident","entry",None),
            ("hour","Hour (0-23)","spin",None),
            ("day","Day of Week","combo",DAYS),
            ("month","Month","combo",[str(i) for i in range(1,13)]),
            ("year","Year","entry",None),
        ]
        for i,(k,lb,wt,opts) in enumerate(specs):
            ri,ci=divmod(i,2)
            f=tk.Frame(body,bg=CARD); f.grid(row=ri,column=ci,sticky="ew",padx=14,pady=7)
            tk.Label(f,text=lb.upper(),font=("Helvetica",8,"bold"),
                     fg=MUTED,bg=CARD,anchor="w").pack(fill="x")
            if wt=="combo":
                v=tk.StringVar(value=opts[0])
                w=ttk.Combobox(f,textvariable=v,values=opts,state="readonly",font=("Helvetica",10))
            elif wt=="spin":
                v=tk.StringVar(value="12")
                w=tk.Spinbox(f,from_=0,to=23,textvariable=v,font=("Helvetica",10),relief="solid",bd=1)
            else:
                v=tk.StringVar(value="2026" if k=="year" else "")
                w=tk.Entry(f,textvariable=v,font=("Helvetica",10),relief="solid",bd=1)
            w.pack(fill="x",ipady=5,pady=(3,0)); flds[k]=v

        def _add():
            campus=flds["campus"].get()
            loc=flds["loc"].get().strip().upper() or "UNKNOWN LOCATION"
            nat=flds["nature"].get().strip() or "Incident reported"
            try: hr=int(flds["hour"].get())
            except: hr=12
            day=flds["day"].get()
            try: mo=int(flds["month"].get())
            except: mo=1
            try: yr=int(flds["year"].get())
            except: yr=2026
            self.all_data.append({
                "campus":campus,"hour":str(hr),"day_of_week":day,
                "is_weekend":day in("Saturday","Sunday"),
                "time_bucket":_bucket(hr),"location":loc,
                "nature_of_incident":nat,"month":str(mo),"year":str(yr),
                "reported_datetime":"","id":str(len(self.all_data)+1),
                "incident_number":"MANUAL","campus_code":"",
                "offenses":"","occurred_datetime":"","date":"",
                "month_name":"","is_weekend":str(day in("Saturday","Sunday")),
                "final_campus":campus,
            })
            self._apply_filters()
            messagebox.showinfo("Success",
                f"Incident added!\nDataset now has {len(self.all_data):,} records.")

        br=tk.Frame(body,bg=CARD); br.grid(row=4,column=0,columnspan=2,sticky="w",padx=14,pady=(18,0))
        tk.Button(br,text="Add Incident",font=("Helvetica",10,"bold"),
                  fg=WHITE,bg=NAVY,activebackground="#2980b9",
                  relief="flat",padx=20,pady=9,cursor="hand2",command=_add).pack(side="left")
        tk.Button(br,text="Reset Filters",font=("Helvetica",10),
                  fg=DARK,bg=BG,relief="solid",bd=1,padx=14,pady=9,cursor="hand2",
                  command=self._reset).pack(side="left",padx=10)

    # ─────────────────────────────────────────────────────────────
    # EXPORT
    # ─────────────────────────────────────────────────────────────
    def _export_csv(self):
        if not self.fdata: return
        p=filedialog.asksaveasfilename(defaultextension=".csv",
                                        filetypes=[("CSV","*.csv")],
                                        title="Export Filtered Data")
        if not p: return
        with open(p,"w",newline="",encoding="utf-8") as f:
            w=csv.DictWriter(f,fieldnames=self.fdata[0].keys())
            w.writeheader(); w.writerows(self.fdata)
        messagebox.showinfo("Exported",f"Saved {len(self.fdata):,} records to:\n{p}")

    def _export_charts(self):
        out=filedialog.askdirectory(title="Select folder to save charts")
        if not out: return
        sys.path.insert(0,os.path.join(os.path.dirname(__file__),"../analysis"))
        try:
            import importlib.util, pandas as pd
            spec=importlib.util.spec_from_file_location(
                "analysis",os.path.join(os.path.dirname(__file__),"../analysis/analysis.py"))
            an=importlib.util.module_from_spec(spec); spec.loader.exec_module(an)
            df=pd.DataFrame(self.fdata)
            for col in ["hour","month","year"]:
                df[col]=pd.to_numeric(df[col],errors="coerce")
            df["is_weekend"]=df["is_weekend"].astype(bool)
            an.plot_campus_bar(df,out); an.plot_time_donut(df,out)
            an.plot_day_of_week(df,out); an.plot_hourly_line(df,out)
            an.plot_heatmap(df,out); an.plot_incident_types(df,out)
            an.plot_top_locations(df,out); an.plot_weekday_weekend(df,out)
            an.plot_monthly_trend(df,out)
            messagebox.showinfo("Exported",f"9 charts saved to:\n{out}")
        except Exception as e:
            messagebox.showerror("Error",f"Chart export failed:\n{e}")

    def _reset(self):
        self._vc.set("All campuses"); self._vt.set("All times")
        self._vd.set("All days"); self._vm.set("All months"); self._vy.set("All years")
        self._apply_filters()


def main():
    ap=argparse.ArgumentParser(description="PSU Crime Dashboard")
    ap.add_argument("--data",default=None,help="Path to incidents.csv")
    args=ap.parse_args()
    App(args.data).mainloop()


if __name__=="__main__":
    main()
