# Pump-Labor

Datensammler und Auswertung für neue Pump.fun-Coins. **Nur lesen, kein Handel, kein Geld, keine Schlüssel.**

- `collector.py` hört rund um die Uhr auf neue Coins und speichert ihre ersten 10 Minuten (alle Trades) nach `data/`.
- `analyse.py` spielt jeden Morgen durch, was ein Sniper mit festen Regeln verdient hätte
  (Einstieg nach 5 bis 60 Sekunden, 2 Sekunden Verzögerung, Bonding-Curve-Preise, Gebühren) und schreibt `results/`.

Datenquelle: kostenloser Datenstrom von PumpPortal (Ersatz: PumpDev).
