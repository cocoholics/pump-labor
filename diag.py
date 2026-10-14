"""Kurzer Test (2 Minuten): Wie viele Trades liefern die Datenquellen, und was steht in den Nachrichten ohne Coin?"""
import asyncio, json, time, collections
import websockets

async def run(url, secs=120, batch_every=1.0):
    out = {"url": url, "creates": 0, "trades": 0, "traded_mints": set(), "other": collections.Counter(), "samples": [], "subs_sent": 0}
    pend = []
    async with websockets.connect(url, max_size=2**22) as ws:
        await ws.send(json.dumps({"method": "subscribeNewToken"}))
        end = time.time() + secs
        nxt = time.time() + batch_every
        while time.time() < end:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                m = json.loads(raw)
                tx = m.get("txType")
                if tx == "create":
                    out["creates"] += 1; pend.append(m["mint"])
                elif tx in ("buy", "sell"):
                    out["trades"] += 1; out["traded_mints"].add(m.get("mint"))
                else:
                    key = m.get("type") or m.get("message", "")[:60] or ",".join(sorted(m)[:6])
                    out["other"][key] += 1
                    if len(out["samples"]) < 8 and m.get("type") != "subscribed":
                        out["samples"].append(json.dumps(m)[:300])
            except asyncio.TimeoutError:
                pass
            if time.time() >= nxt and pend:
                await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": pend[:100]})); del pend[:100]
                out["subs_sent"] += 1; nxt = time.time() + batch_every
    out["traded_mints"] = len(out["traded_mints"]); out["other"] = dict(out["other"])
    return out

async def main():
    res = []
    for url in ["wss://pumpdev.io/ws", "wss://pumpportal.fun/api/data"]:
        try:
            res.append(await run(url))
        except Exception as e:
            res.append({"url": url, "fehler": repr(e)})
    json.dump({"zeit": time.time(), "ergebnisse": res}, open("results/diag.json", "w"), indent=1)
    print(json.dumps(res, indent=1))

asyncio.run(main())
