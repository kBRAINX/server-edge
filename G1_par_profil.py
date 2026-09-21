#!/usr/bin/env python3
"""
G1_par_profil.py — Pour CHAQUE profil, deux graphes « activité réelle vs
mesures transmises » : une vue complète et un zoom sur les 50 premières minutes.

Objectif : montrer que la sonde ne transmet qu'aux changements significatifs
(points sur les pics) et se tait dans les phases stables (courbe sans point).

PRINCIPE (important) :
  - La COURBE d'activité réelle est reconstruite en CONTINU et placée au TEMPS
    RÉEL serveur, en interpolant le temps de chaque cycle entre les trames émises
    (dont on connaît l'horodatage exact). Elle couvre donc toute la durée où la
    sonde a fonctionné, y compris les moments de silence.
  - Les POINTS transmis sont placés à leur horodatage réel.
  Ceci corrige le cas du profil en mode survie : ses cycles durent jusqu'à 10 s
  (au lieu de 2 s), il exécute donc moins de cycles, mais émet jusqu'à la fin —
  ce que le temps réel restitue correctement (à la différence de seq*2s).

  Le titre affiche le compteur « n trames / seq_max cycles (transmis X%) ».

Prérequis : pandas, numpy, matplotlib. Fichiers : simulation_3h.csv, traces_preview.csv.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

CSV_RESULTATS = "simulation_3h.csv"
CSV_TRACES    = "traces_preview.csv"
ZOOM_MIN      = 50

COUL = {"stable":"#2a78d6","montee_charge":"#eb6834","critique":"#e34948",
        "batterie_faible":"#1baf7a","nominal":"#6250d6"}
def coul(dev): return COUL.get(dev.replace("sim-",""), "#888")

df = pd.read_csv(CSV_RESULTATS)
for c in ["seq","cpu_usage_pct","ts_epoch"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["seq","device"]).sort_values(["device","seq"])
t0 = df["ts_epoch"].min()
df["t_min"] = (df["ts_epoch"] - t0) / 60.0
devices = sorted(df["device"].unique())
traces = pd.read_csv(CSV_TRACES)


def graphe_profil(dev, tmax=None, suffixe=""):
    prof = dev.replace("sim-", "")
    sub = df[df["device"] == dev].copy()
    n = len(sub); seq_max = int(sub["seq"].max())
    tx_pct = 100 * n / seq_max
    tr_col = traces[prof].values if prof in traces.columns else None

    # Reconstruction de l'activité réelle CONTINUE au temps réel :
    # chaque cycle 1..seq_max a une valeur (trace[seq-1]) et un temps réel
    # obtenu par interpolation entre les trames émises (seq -> t_min connus).
    if tr_col is not None and len(sub) >= 2:
        seqs = sub["seq"].values; tmins = sub["t_min"].values
        all_seq = np.arange(1, seq_max + 1)
        t_all = np.interp(all_seq, seqs, tmins)
        cpu_all = np.array([tr_col[min(int(s)-1, len(tr_col)-1)] for s in all_seq])
    else:
        t_all = sub["t_min"].values; cpu_all = sub["cpu_usage_pct"].values

    mask = t_all <= tmax if tmax else np.ones(len(t_all), bool)
    s = sub[sub["t_min"] <= tmax] if tmax else sub
    seg = cpu_all[mask] if len(cpu_all) else np.array([15])
    ymax = min(100, max(15, seg.max()*1.25 if len(seg) else 15,
                        s["cpu_usage_pct"].max()*1.3))

    fig, ax = plt.subplots(figsize=(11, 4))
    ax.plot(t_all[mask], cpu_all[mask], color="#bbbbbb", lw=0.8,
            label="activité réelle", zorder=1)
    ax.scatter(s["t_min"], s["cpu_usage_pct"], s=8, color=coul(dev),
               edgecolors="white", linewidths=0.3,
               label="mesures transmises", zorder=3)
    ax.set_ylabel("CPU %"); ax.set_xlabel("temps (minutes)"); ax.set_ylim(0, ymax)
    if tmax:
        ax.set_xlim(0, tmax)
    titre = f"{prof} — {n} trames / {seq_max} cycles  (transmis {tx_pct:.1f}%)"
    if tmax:
        titre += f"  — détail {tmax:.0f} min"
    ax.set_title(titre, loc="left", fontsize=10, color=coul(dev), fontweight="bold")
    ax.legend(fontsize=8, loc="upper right"); ax.grid(alpha=0.25)
    plt.tight_layout()
    out = f"G1_{prof}{suffixe}.png"
    plt.savefig(out, dpi=130, bbox_inches="tight"); plt.show()
    return out


for dev in devices:
    o1 = graphe_profil(dev)
    o2 = graphe_profil(dev, tmax=ZOOM_MIN, suffixe="_zoom50")
    print(f"{dev:22s} -> {o1}  +  {o2}")