#!/usr/bin/env python3
"""
Vegas Gaming Demo — Window-Aligned Sample Data Generator
=========================================================
Generates a batch of player_events JSON records aligned to the next
1-minute tumbling window boundary. Every event falls within a single
window so the Flink TUMBLE aggregation fires cleanly for a demo trigger.

Typical use:
  python generate_sample_data.py          # writes player_events_sample.json
  python generate_sample_data.py --print  # pretty-prints to stdout

The output JSON can be fed into produce_player_events.py via --sample-file
(one-shot replay mode) or used offline for manual inspection.

Window-alignment logic:
  - Rounds up to the next whole minute (current_minute + 1)
  - Spreads RISK events densely within the window so bet_count > 20
    and total_wagered > 10 000 in a 1-minute window
  - Appends one post-window event (6 seconds after window_end) to
    advance the Flink watermark and trigger window closure
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

# ── Player pools ──────────────────────────────────────────────────────────────

RISK_PLAYERS   = [f"PLAYER-RISK-0{i}" for i in range(1, 6)]
NORMAL_PLAYERS = [f"PLAYER-NRM-0{i}"  for i in range(1, 6)]

GAME_TYPES = ["BLACKJACK", "SLOTS", "ROULETTE", "POKER", "SPORTS_BOOK"]
CHANNELS   = ["FLOOR", "ONLINE", "MOBILE", "KIOSK"]
DEVICE_PREFIXES = ["TBL", "SLOT", "KIOSK", "MOB", "POS"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _next_window_start() -> datetime:
    """Return the start of the next whole-minute window (UTC)."""
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return now + timedelta(minutes=1)


def _ts_ms(dt: datetime) -> int:
    """Convert datetime to milliseconds-since-epoch integer."""
    return int(dt.timestamp() * 1000)


def _bet_amount(segment: str, game_type: str) -> float:
    if segment == "risk":
        ranges = {
            "BLACKJACK":   (200.00, 2500.00),
            "SLOTS":       (50.00,  800.00),
            "ROULETTE":    (150.00, 2000.00),
            "POKER":       (300.00, 3000.00),
            "SPORTS_BOOK": (500.00, 5000.00),
        }
    else:
        ranges = {
            "BLACKJACK":   (5.00,  100.00),
            "SLOTS":       (1.00,   50.00),
            "ROULETTE":    (5.00,   75.00),
            "POKER":       (10.00, 150.00),
            "SPORTS_BOOK": (10.00, 200.00),
        }
    low, high = ranges[game_type]
    return float(
        Decimal(str(random.uniform(low, high))).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
    )


def _make_event(player_id: str, session_id: str, seq: int, ts_ms: int) -> dict:
    game_type = random.choice(GAME_TYPES)
    segment   = "risk" if player_id.startswith("PLAYER-RISK") else "normal"
    return {
        "player_id":  player_id,
        "session_id": session_id,
        "bet_id":     f"BET-SAMPLE-{seq:08d}",
        "amount":     _bet_amount(segment, game_type),
        "game_type":  game_type,
        "channel":    random.choice(CHANNELS),
        "device_id":  f"{random.choice(DEVICE_PREFIXES)}-{random.randint(1000, 9999)}",
        "event_time": ts_ms,
    }


# ── Generator ─────────────────────────────────────────────────────────────────

def generate(
    risk_events_per_player: int = 25,
    normal_events_per_player: int = 3,
) -> list[dict]:
    """
    Build a window-aligned event batch.

    Risk players each get `risk_events_per_player` events spread evenly
    across the window (guaranteeing bet_count > 20 and total_wagered > 10 000).
    Normal players each get a handful of low-value events.
    A single watermark-trigger event is appended after window_end.
    """
    window_start = _next_window_start()
    window_end   = window_start + timedelta(minutes=1)
    window_ms    = 60_000  # 1 minute in ms

    events: list[dict] = []
    seq = 1

    # ── Risk players — dense, high-value events within window ─────────────────
    for player_id in RISK_PLAYERS:
        session_id = f"SESSION-{uuid.uuid4().hex[:8].upper()}"
        # Spread events: leave 2 s buffer at start and end
        usable_ms  = window_ms - 4_000
        spacing_ms = usable_ms // max(risk_events_per_player - 1, 1)

        for i in range(risk_events_per_player):
            ts = _ts_ms(window_start) + 2_000 + (i * spacing_ms)
            events.append(_make_event(player_id, session_id, seq, ts))
            seq += 1

    # ── Normal players — sparse, low-value events within window ───────────────
    for player_id in NORMAL_PLAYERS:
        session_id = f"SESSION-{uuid.uuid4().hex[:8].upper()}"
        for i in range(normal_events_per_player):
            offset_ms = random.randint(5_000, window_ms - 5_000)
            ts = _ts_ms(window_start) + offset_ms
            events.append(_make_event(player_id, session_id, seq, ts))
            seq += 1

    # ── Watermark trigger — one event after window_end to close the window ────
    trigger_ts = _ts_ms(window_end) + 6_000  # 6 s after window closes
    events.append(
        _make_event(
            player_id  = RISK_PLAYERS[0],
            session_id = "SESSION-WATERMARK",
            seq        = seq,
            ts_ms      = trigger_ts,
        )
    )

    # Shuffle so interleaving looks natural (but watermark trigger stays last)
    body     = events[:-1]
    trigger  = events[-1]
    random.shuffle(body)
    events   = body + [trigger]

    return events


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate window-aligned player_events sample data")
    parser.add_argument("--out",   default="player_events_sample.json", help="Output JSON file path")
    parser.add_argument("--print", dest="print_only", action="store_true", help="Print to stdout instead of writing file")
    parser.add_argument("--risk-events",   type=int, default=25, help="Events per risk player (default 25)")
    parser.add_argument("--normal-events", type=int, default=3,  help="Events per normal player (default 3)")
    args = parser.parse_args()

    events = generate(
        risk_events_per_player   = args.risk_events,
        normal_events_per_player = args.normal_events,
    )

    if args.print_only:
        print(json.dumps(events, indent=2))
    else:
        with open(args.out, "w") as f:
            json.dump(events, f, indent=2)
        print(f"✅ Written {len(events)} events → {args.out}")
        print(f"   Risk players    : {len(RISK_PLAYERS)} × {args.risk_events} = {len(RISK_PLAYERS) * args.risk_events} events")
        print(f"   Normal players  : {len(NORMAL_PLAYERS)} × {args.normal_events} = {len(NORMAL_PLAYERS) * args.normal_events} events")
        print(f"   Watermark event : 1")
        print(f"   Window start    : {(_next_window_start() - timedelta(minutes=1)).isoformat()}")

    return events


if __name__ == "__main__":
    main()
