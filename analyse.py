"""Pump-Labor Auswertung: Was hätte ein Sniper mit festen Regeln verdient?

Für jeden aufgezeichneten Coin wird durchgespielt: bei Sekunde S nach dem Start prüfen (Filter), mit Verzögerung kaufen,
nach Ziel, Notbremse oder fester Zeit verkaufen. Preise kommen direkt aus der Bonding-Curve (konstantes Produkt),
dadurch ist die Slippage für unseren Einsatz realistisch. Gebühren pro Kauf und Verkauf eingerechnet.

Ergebnis: results/summary.json (fürs Büro) und results/bericht.md
"""
import bisect
import glob
import gzip
import itertools
import json
import os
import random
import statistics
import time

STAKE_SOL = 0.1           # virtueller Einsatz pro Trade
FEE = 0.0125              # Pump.fun-Gebühr ca. 1 % plus Prioritätsgebühr, pro Seite
LATENCY_MS = 2000         # so lange dauert es, bis unser Kauf oder Verkauf ausgeführt ist
MAX_TOKENS = 25000        # Stichprobe, damit die Auswertung schnell bleibt
LOOKBACK_DAYS = 5

ENTRY_S = [5, 10, 20, 30, 60]
MIN_BUYERS = [0, 5, 10, 20]
CREATOR_FILTER = [False, True]   # True: nur wenn der Erfinder bis dahin nichts verkauft hat
MIN_NETFLOW = [0, 2, 5]          # Netto-Kaufvolumen in SOL bis zum Einstieg
EXITS = [(h, tp, sl) for h in (60, 180, 300) for tp in (None, 0.5, 1.0) for sl in (None, 0.3)]


def load():
    files = sorted(glob.glob("data/*/*.jsonl.gz"))
    cutoff = time.time() - LOOKBACK_DAYS * 86400
    seen, toks = set(), []
    for f in files:
        try:
            with gzip.open(f, "rt", encoding="utf-8") as fh:
                for line in fh:
                    t = json.loads(line)
                    if t["mint"] in seen or t["t0"] / 1000 < cutoff:
                        continue
                    seen.add(t["mint"])
                    toks.append(t)
        except Exception as e:
            print("Datei übersprungen", f, e)
    return toks


class Curve:
    """Zustand der Bonding-Curve über die Zeit (nach jedem Trade)."""

    def __init__(self, tok):
        tr = sorted(tok["trades"], key=lambda x: x[0])
        self.t = [0] + [x[0] for x in tr]
        self.vs = [tok.get("v_sol") or 30.0] + [x[4] or 0 for x in tr]
        self.vt = [tok.get("v_tok") or 1.073e9] + [x[5] or 0 for x in tr]
        self.tr = tr
        self.tracked = tok.get("tracked_ms", 600000)

    def state(self, ms):
        i = bisect.bisect_right(self.t, ms) - 1
        i = max(i, 0)
        return self.vs[i], self.vt[i]

    def buy(self, ms, sol):
        vs, vt = self.state(ms)
        if not vs or not vt:
            return 0
        eff = sol * (1 - FEE)
        return vt - vs * vt / (vs + eff)

    def sell_value(self, ms, tokens):
        vs, vt = self.state(ms)
        if not vs or not vt:
            return 0
        return (vs - vs * vt / (vt + tokens)) * (1 - FEE)


def features(tok, curve, s):
    """Was man bis Sekunde s über den Coin wissen kann."""
    ms = s * 1000
    buyers, net, creator_sold = set(), 0.0, False
    for t, side, sol, _tok, _vs, _vt, _mc, who in curve.tr:
        if t > ms:
            break
        sol = sol or 0
        if side == 1:
            buyers.add(who)
            net += sol
        else:
            net -= sol
            if who == tok.get("creator"):
                creator_sold = True
    return len(buyers), net, creator_sold


def simulate(curve, entry_ms, hold_s, tp, sl):
    buy_at = entry_ms + LATENCY_MS
    if buy_at > curve.tracked:
        return None
    q = curve.buy(buy_at, STAKE_SOL)
    if q <= 0:
        return None
    end = buy_at + hold_s * 1000
    exit_ms = end
    if tp is not None or sl is not None:
        i = bisect.bisect_right(curve.t, buy_at)
        while i < len(curve.t) and curve.t[i] <= end:
            val = curve.sell_value(curve.t[i], q) / STAKE_SOL - 1
            if (tp is not None and val >= tp) or (sl is not None and val <= -sl):
                exit_ms = curve.t[i] + LATENCY_MS
                break
            i += 1
    exit_ms = min(exit_ms, curve.tracked)
    return curve.sell_value(exit_ms, q) / STAKE_SOL - 1


def main():
    t0 = time.time()
    toks = load()
    if len(toks) > MAX_TOKENS:
        random.seed(1)
        toks = random.sample(toks, MAX_TOKENS)
    os.makedirs("results", exist_ok=True)
    if not toks:
        json.dump({"updated": time.time(), "tokens_total": 0}, open("results/summary.json", "w"))
        print("Noch keine Daten.")
        return
    span_days = max((max(t["t0"] for t in toks) - min(t["t0"] for t in toks)) / 86400000, 1 / 24)

    curves = [Curve(t) for t in toks]
    # Grundzahlen: wie viele Coins sind nach 5 Minuten tot, wie viele steigen stark?
    dead, x2 = 0, 0
    for tok, c in zip(toks, curves):
        vs0 = c.vs[0]
        vs5, _ = c.state(300000)
        late = [t for t in c.t if 120000 < t <= 300000]
        if not late or vs5 <= vs0 * 1.02:
            dead += 1
        if vs5 >= vs0 * 2:
            x2 += 1

    # Ergebnisse pro (Einstieg, Ausstieg) und Coin, Filter danach
    feats = {s: [features(t, c, s) for t, c in zip(toks, curves)] for s in ENTRY_S}
    outcome = {}
    for s in ENTRY_S:
        for ex in EXITS:
            outcome[(s, ex)] = [simulate(c, s * 1000, *ex) for c in curves]

    results = []
    for s, ub, cf, nf, ex in itertools.product(ENTRY_S, MIN_BUYERS, CREATOR_FILTER, MIN_NETFLOW, EXITS):
        rets = []
        for f, r in zip(feats[s], outcome[(s, ex)]):
            if r is None:
                continue
            nb, net, csold = f
            if nb < ub or net < nf or (cf and csold):
                continue
            rets.append(r)
        if len(rets) < 30:
            continue
        results.append({
            "einstieg_s": s, "min_kaeufer": ub, "erfinder_nicht_verkauft": cf, "min_netto_sol": nf,
            "halten_s": ex[0], "ziel": ex[1], "notbremse": ex[2],
            "trades": len(rets), "pro_tag": round(len(rets) / span_days, 1),
            "schnitt_pct": round(statistics.mean(rets) * 100, 2), "median_pct": round(statistics.median(rets) * 100, 2),
            "treffer_pct": round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1),
            "summe_sol": round(sum(rets) * STAKE_SOL, 3),
        })
    results.sort(key=lambda r: r["schnitt_pct"], reverse=True)
    plus = [r for r in results if r["schnitt_pct"] > 0]
    robust = [r for r in results if r["trades"] >= 200]
    baseline = next((r for r in results if r["einstieg_s"] == 5 and r["min_kaeufer"] == 0 and not r["erfinder_nicht_verkauft"]
                     and r["min_netto_sol"] == 0 and r["halten_s"] == 180 and r["ziel"] is None and r["notbremse"] is None), None)
    summary = {
        "updated": time.time(), "tokens_total": len(toks), "tage": round(span_days, 2), "coins_pro_tag": round(len(toks) / span_days),
        "tot_nach_5min_pct": round(dead / len(toks) * 100, 1), "verdoppelt_in_5min_pct": round(x2 / len(toks) * 100, 1),
        "varianten": len(results), "varianten_im_plus": len(plus),
        "beste": results[:10], "beste_robust": sorted(robust, key=lambda r: r["schnitt_pct"], reverse=True)[:5],
        "alles_kaufen": baseline, "einsatz_sol": STAKE_SOL, "gebuehr_pro_seite_pct": FEE * 100, "verzoegerung_s": LATENCY_MS / 1000,
        "laufzeit_s": round(time.time() - t0),
    }
    with open("results/summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=1)

    def row(r):
        return (f"| {r['einstieg_s']} s | {r['min_kaeufer']} | {'ja' if r['erfinder_nicht_verkauft'] else 'egal'} | {r['min_netto_sol']} | "
                f"{r['halten_s']} s | {'+' + str(int(r['ziel'] * 100)) + ' %' if r['ziel'] else 'nein'} | "
                f"{'-' + str(int(r['notbremse'] * 100)) + ' %' if r['notbremse'] else 'nein'} | {r['trades']} | {r['pro_tag']} | "
                f"{r['treffer_pct']} % | {r['schnitt_pct']:+.2f} % | {r['median_pct']:+.2f} % |")
    head = ("| Einstieg | min. Käufer | Erfinder hält | min. Netto-SOL | Halten | Ziel | Notbremse | Trades | pro Tag | Treffer | Schnitt | Median |\n"
            "|---|---|---|---|---|---|---|---|---|---|---|---|")
    lines = ["# Pump-Labor: Auswertung", "",
             f"{len(toks)} Coins aus {span_days:.1f} Tagen (ca. {summary['coins_pro_tag']} pro Tag). "
             f"Einsatz {STAKE_SOL} SOL, Gebühr {FEE * 100:.2f} % pro Seite, {LATENCY_MS / 1000:.0f} s Verzögerung.", "",
             f"Nach 5 Minuten tot: {summary['tot_nach_5min_pct']} %. In 5 Minuten verdoppelt: {summary['verdoppelt_in_5min_pct']} %.",
             f"Varianten im Plus: {len(plus)} von {len(results)}.", "",
             "## Beste Varianten", "", head, *[row(r) for r in results[:15]], "",
             "## Beste mit mindestens 200 Trades", "", head, *[row(r) for r in summary["beste_robust"]], "",
             "## Zum Vergleich: alles kaufen bei Sekunde 5, 3 Minuten halten", "", head, *( [row(baseline)] if baseline else []), "",
             f"_Laufzeit {summary['laufzeit_s']} s_"]
    with open("results/bericht.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("\n".join(lines[:8]))


if __name__ == "__main__":
    main()
