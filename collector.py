"""Pump-Labor Datensammler: zeichnet jeden neuen Pump.fun-Coin in den ersten 10 Minuten auf.

Hört über einen kostenlosen Datenstrom (PumpPortal, Ersatz: PumpDev) auf neue Coins, abonniert deren Trades und speichert
pro Coin die Erstellung und jeden Trade mit Zeitstempel (Millisekunden seit Erstellung). Alles nur lesen, kein Handel.

Läuft als GitHub-Action in Schichten. Schreibt stündlich eine gzip-JSONL-Datei nach data/JJJJ-MM-TT/ und pusht sie.
Aufruf: python collector.py <laufzeit_minuten>
"""
import asyncio
import gzip
import json
import os
import subprocess
import sys
import time

import websockets

FEEDS = ["wss://pumpportal.fun/api/data", "wss://pumpdev.io/ws"]
TRACK_SECONDS = 600          # jeden Coin 10 Minuten verfolgen
MAX_TRADES = 3000            # Obergrenze pro Coin, damit einzelne Riesen-Coins den Speicher nicht sprengen
FLUSH_EVERY = 3600           # stündlich speichern und pushen
SUB_BATCH = 100              # so viele Coins pro Abo-Nachricht

tokens = {}                  # mint -> Datensatz
pending_sub, pending_unsub = [], []
done = []
stats = {"neu": 0, "trades": 0, "reconnects": 0}


def now_ms():
    return int(time.time() * 1000)


def on_message(msg):
    t = now_ms()
    mint = msg.get("mint")
    if not mint:
        return
    if msg.get("txType") == "create":
        if mint in tokens:
            return
        tokens[mint] = {
            "mint": mint, "t0": t, "creator": msg.get("traderPublicKey"), "name": msg.get("name"), "symbol": msg.get("symbol"),
            "uri": msg.get("uri"), "initial_buy_sol": msg.get("solAmount"), "initial_buy_tokens": msg.get("initialBuy"),
            "mcap_sol": msg.get("marketCapSol"), "v_sol": msg.get("vSolInBondingCurve"), "v_tok": msg.get("vTokensInBondingCurve"),
            "pool": msg.get("pool"), "trades": [],
        }
        pending_sub.append(mint)
        stats["neu"] += 1
        return
    tok = tokens.get(mint)
    if not tok or len(tok["trades"]) >= MAX_TRADES:
        return
    side = 1 if msg.get("txType") == "buy" else -1 if msg.get("txType") == "sell" else 0
    if not side:
        return
    tok["trades"].append([t - tok["t0"], side, msg.get("solAmount"), msg.get("tokenAmount"), msg.get("vSolInBondingCurve"),
                          msg.get("vTokensInBondingCurve"), msg.get("marketCapSol"), msg.get("traderPublicKey")])
    stats["trades"] += 1


async def manager(ws):
    """Abos verwalten und fertige Coins (älter als TRACK_SECONDS) abschließen."""
    while True:
        await asyncio.sleep(1)
        while pending_sub:
            batch, pending_sub[:] = pending_sub[:SUB_BATCH], pending_sub[SUB_BATCH:]
            await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": batch}))
        t = now_ms()
        old = [m for m, tok in tokens.items() if t - tok["t0"] > TRACK_SECONDS * 1000]
        for m in old:
            done.append(tokens.pop(m))
            pending_unsub.append(m)
        while pending_unsub:
            batch, pending_unsub[:] = pending_unsub[:SUB_BATCH], pending_unsub[SUB_BATCH:]
            await ws.send(json.dumps({"method": "unsubscribeTokenTrade", "keys": batch}))


async def listen(deadline):
    i = 0
    while time.time() < deadline:
        url = FEEDS[i % len(FEEDS)]
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**22) as ws:
                print(time.strftime("%H:%M:%S"), "verbunden mit", url, flush=True)
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                if tokens:  # nach Neuverbindung laufende Coins wieder abonnieren
                    pending_sub.extend(list(tokens))
                mgr = asyncio.create_task(manager(ws))
                try:
                    while time.time() < deadline:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(1, deadline - time.time()))
                        try:
                            msg = json.loads(raw)
                        except ValueError:
                            continue
                        if isinstance(msg, dict):
                            on_message(msg)
                finally:
                    mgr.cancel()
        except asyncio.TimeoutError:
            break
        except Exception as e:
            stats["reconnects"] += 1
            print(time.strftime("%H:%M:%S"), "Verbindung weg:", type(e).__name__, e, flush=True)
            i += 1
            await asyncio.sleep(3)


def flush(final=False):
    """Fertige Coins in eine Datei schreiben und pushen."""
    global done
    t = now_ms()
    for tok in done:
        tok["tracked_ms"] = TRACK_SECONDS * 1000
    rest = list(tokens.values()) if final else []
    for tok in rest:
        tok["tracked_ms"] = t - tok["t0"]
    items = done + rest
    done = []
    if not items:
        return
    day = time.strftime("%Y-%m-%d", time.gmtime())
    os.makedirs(f"data/{day}", exist_ok=True)
    path = f"data/{day}/{time.strftime('%H%M%S', time.gmtime())}-{os.getenv('GITHUB_RUN_ID', 'lokal')}.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for tok in items:
            f.write(json.dumps(tok, separators=(",", ":")) + "\n")
    print(time.strftime("%H:%M:%S"), f"gespeichert: {len(items)} Coins -> {path}", stats, flush=True)
    if os.getenv("GITHUB_ACTIONS"):
        for cmd in (["git", "add", path], ["git", "commit", "-qm", f"Daten {path}"]):
            subprocess.run(cmd, check=False)
        for _ in range(5):
            subprocess.run(["git", "pull", "--rebase", "-q"], check=False)
            if subprocess.run(["git", "push", "-q"], check=False).returncode == 0:
                break
            time.sleep(5)


async def main(minutes):
    end = time.time() + minutes * 60
    while time.time() < end:
        chunk_end = min(end, time.time() + FLUSH_EVERY)
        await listen(chunk_end)
        flush(final=time.time() >= end - 5)


if __name__ == "__main__":
    asyncio.run(main(float(sys.argv[1]) if len(sys.argv) > 1 else 10))
