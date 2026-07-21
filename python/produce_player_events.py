#!/usr/bin/env python3
"""
Vegas Gaming Demo — Continuous Player Events Producer
======================================================
Streams realistic casino player events to the `player_events` Kafka topic
using Schema Registry JSON serialization (json-registry format).

Mirrors python/produce_messages.py adapted for the vegas-vsi Confluent Platform
cluster (163.66.87.246) with SASL_PLAINTEXT over direct TCP brokers.

Traffic profile:
  - 5 risk-heavy players  (PLAYER-RISK-01..05) receive ~80% of events
  - 5 normal players      (PLAYER-NRM-01..05)  receive ~20% of events
  - Events emitted every 0.3–1.0 seconds
  - Timestamps: milliseconds-since-epoch (required by Flink watermarks)
  - Live console stats printed every 5 seconds

Schema (player_events):
  key   : { player_id }
  value : { session_id, bet_id, amount, game_type, channel, device_id, event_time }

Usage:
  # Continuous mode (Ctrl+C to stop):
  python produce_player_events.py

  # One-shot replay from a pre-generated sample file:
  python produce_player_events.py --sample-file player_events_sample.json

  # Generate sample data first, then replay:
  python generate_sample_data.py
  python produce_player_events.py --sample-file player_events_sample.json
"""

import argparse
import json
import random
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from itertools import count

import os
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

# ── Load environment ───────────────────────────────────────────────────────────

load_dotenv()

REQUIRED_ENV_VARS = [
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_API_KEY",
    "KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
]

missing = [v for v in REQUIRED_ENV_VARS if not os.getenv(v)]
if missing:
    print(f"❌ Missing required environment variables: {', '.join(missing)}")
    print("   Copy vegas/python/.env.example → vegas/python/.env and fill in values.")
    sys.exit(1)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_API_KEY   = os.getenv("KAFKA_API_KEY")
KAFKA_API_SECRET = os.getenv("KAFKA_API_SECRET")
SR_URL          = os.getenv("SCHEMA_REGISTRY_URL")
SR_API_KEY      = os.getenv("SCHEMA_REGISTRY_API_KEY")
SR_API_SECRET   = os.getenv("SCHEMA_REGISTRY_API_SECRET")

# ── Constants ──────────────────────────────────────────────────────────────────

TOPIC          = "player_events"
RISK_PLAYERS   = [f"PLAYER-RISK-0{i}" for i in range(1, 6)]
NORMAL_PLAYERS = [f"PLAYER-NRM-0{i}"  for i in range(1, 6)]
GAME_TYPES     = ["BLACKJACK", "SLOTS", "ROULETTE", "POKER", "SPORTS_BOOK"]
CHANNELS       = ["FLOOR", "ONLINE", "MOBILE", "KIOSK"]
DEVICE_PREFIXES = ["TBL", "SLOT", "KIOSK", "MOB", "POS"]

# Thresholds that trigger is_flagged=true in the Flink risk job
FLAG_BET_COUNT   = 20
FLAG_TOTAL_WAGER = 10_000.00

# ── Schema Registry client ─────────────────────────────────────────────────────

sr_client = SchemaRegistryClient(
    {
        "url": SR_URL,
        "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}",
    }
)

print("🔍 Fetching schemas from Schema Registry …")
try:
    key_schema   = sr_client.get_latest_version(f"{TOPIC}-key").schema
    value_schema = sr_client.get_latest_version(f"{TOPIC}-value").schema
    print("✅ Schemas retrieved successfully")
except Exception as exc:
    print(f"❌ Could not retrieve schemas: {exc}")
    print(
        "   Ensure the Flink CREATE TABLE statements have been submitted and the\n"
        "   schemas are registered in Schema Registry before running this producer.\n"
        f"   Schema Registry: {SR_URL}"
    )
    sys.exit(1)

# ── Serializers ────────────────────────────────────────────────────────────────

key_serializer   = JSONSerializer(key_schema.schema_str, sr_client)
value_serializer = JSONSerializer(value_schema.schema_str, sr_client)

# ── Kafka producer ─────────────────────────────────────────────────────────────
# Confluent Cloud brokers — SASL_SSL with API key credentials.

producer = Producer(
    {
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "security.protocol":  "SASL_SSL",
        "sasl.mechanisms":    "PLAIN",
        "sasl.username":      KAFKA_API_KEY,
        "sasl.password":      KAFKA_API_SECRET,
    }
)

# ── Runtime stats ──────────────────────────────────────────────────────────────

success_count  = 0
failure_count  = 0
player_counts  = Counter()
game_counts    = Counter()
wager_by_segment = Counter()
start_time     = time.time()


# ── Helpers ────────────────────────────────────────────────────────────────────

def choose_player():
    """80 % risk players, 20 % normal — mirrors the fraud demo weighting."""
    if random.random() < 0.80:
        return random.choice(RISK_PLAYERS), "risk"
    return random.choice(NORMAL_PLAYERS), "normal"


def bet_amount(segment: str, game_type: str) -> float:
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


# Active sessions: player_id → session_id (rotated occasionally)
_sessions: dict[str, str] = {}


def get_session(player_id: str) -> str:
    """Return a persistent session ID for the player, rotating ~5 % of the time."""
    if player_id not in _sessions or random.random() < 0.05:
        _sessions[player_id] = f"SESSION-{uuid.uuid4().hex[:8].upper()}"
    return _sessions[player_id]


def build_event(sequence_number: int) -> tuple[dict, dict, str]:
    """Return (key_obj, value_obj, segment)."""
    player_id, segment = choose_player()
    game_type = random.choice(GAME_TYPES)
    now = datetime.now(timezone.utc)

    key = {"player_id": player_id}
    value = {
        "session_id": get_session(player_id),
        "bet_id":     f"BET-LIVE-{sequence_number:08d}",
        "amount":     bet_amount(segment, game_type),
        "game_type":  game_type,
        "channel":    random.choice(CHANNELS),
        "device_id":  f"{random.choice(DEVICE_PREFIXES)}-{random.randint(1000, 9999)}",
        # Milliseconds since epoch — required by Flink TIMESTAMP(3) watermark
        "event_time": int(now.timestamp() * 1000),
    }
    return key, value, segment


def print_stats(last_key: dict, last_value: dict):
    elapsed    = max(time.time() - start_time, 1e-9)
    rate       = success_count / elapsed
    risk_total = sum(player_counts[p] for p in RISK_PLAYERS)
    norm_total = sum(player_counts[p] for p in NORMAL_PLAYERS)
    total      = max(success_count, 1)

    print("\n" + "─" * 72)
    print(
        f"📊 Live Stats | delivered={success_count} failed={failure_count} "
        f"rate={rate:.2f}/sec elapsed={elapsed:.1f}s"
    )
    print(
        f"🎰 Risk players  : {risk_total} events "
        f"({risk_total / total * 100:.1f}%)"
    )
    print(
        f"✅ Normal players: {norm_total} events "
        f"({norm_total / total * 100:.1f}%)"
    )
    print(
        "🃏 Game types: "
        + ", ".join(f"{g}={game_counts[g]}" for g in GAME_TYPES)
    )
    print(
        f"💰 Wager totals | risk=${wager_by_segment['risk']:.2f} "
        f"normal=${wager_by_segment['normal']:.2f}"
    )
    print(
        f"🧾 Last event: bet_id={last_value['bet_id']} "
        f"player={last_key['player_id']} "
        f"game={last_value['game_type']} "
        f"amount=${last_value['amount']:.2f}"
    )
    risk_flagged = [
        p for p in RISK_PLAYERS if player_counts[p] >= FLAG_BET_COUNT
    ]
    if risk_flagged:
        print(f"🚨 Flag threshold reached (>{FLAG_BET_COUNT} bets): {', '.join(risk_flagged)}")


def delivery_callback(err, msg):
    global success_count, failure_count
    if err:
        print(f"❌ Delivery failed: {err}")
        failure_count += 1
    else:
        success_count += 1


# ── Replay mode (--sample-file) ───────────────────────────────────────────────

def replay_from_file(sample_file: str):
    """Produce all events from a pre-generated JSON sample file, then exit."""
    print(f"📂 Replay mode — loading events from {sample_file} …")
    with open(sample_file) as f:
        events = json.load(f)

    print(f"   {len(events)} events loaded. Producing …")
    for event in events:
        player_id = event["player_id"]
        key   = {"player_id": player_id}
        value = {k: v for k, v in event.items() if k != "player_id"}

        try:
            producer.produce(
                topic=TOPIC,
                key=key_serializer(key, SerializationContext(TOPIC, MessageField.KEY)),
                value=value_serializer(value, SerializationContext(TOPIC, MessageField.VALUE)),
                callback=delivery_callback,
            )
            producer.poll(0)
        except Exception as exc:
            print(f"❌ Error producing {event.get('bet_id', '?')}: {exc}")
            failure_count += 1
        else:
            segment = "risk" if player_id.startswith("PLAYER-RISK") else "normal"
            player_counts[player_id]    += 1
            game_counts[event["game_type"]] += 1
            wager_by_segment[segment]   += event["amount"]

    producer.flush()
    print(f"\n✅ Replay complete — {success_count} delivered, {failure_count} failed")


# ── Continuous mode ────────────────────────────────────────────────────────────

def run_continuous():
    """Produce events indefinitely until Ctrl+C."""
    print("\n▶️  Starting continuous producer. Press Ctrl+C to stop.")

    last_stats_time = 0.0
    last_key        = None
    last_value      = None

    try:
        for sequence_number in count(1):
            key, value, segment = build_event(sequence_number)

            try:
                producer.produce(
                    topic=TOPIC,
                    key=key_serializer(key, SerializationContext(TOPIC, MessageField.KEY)),
                    value=value_serializer(value, SerializationContext(TOPIC, MessageField.VALUE)),
                    callback=delivery_callback,
                )
                producer.poll(0)
            except Exception as exc:
                print(f"❌ Error producing {value['bet_id']}: {exc}")
                failure_count += 1
            else:
                player_counts[key["player_id"]]   += 1
                game_counts[value["game_type"]]    += 1
                wager_by_segment[segment]          += value["amount"]
                last_key, last_value = key, value

            now = time.time()
            if last_key and now - last_stats_time >= 5:
                print_stats(last_key, last_value)
                last_stats_time = now

            time.sleep(random.uniform(0.3, 1.0))

    except KeyboardInterrupt:
        print("\n\n🛑 Stopping producer …")
    finally:
        producer.flush()
        if last_key:
            print_stats(last_key, last_value)
        print("✅ Producer stopped cleanly")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Vegas Gaming Demo — player_events Kafka producer"
    )
    parser.add_argument(
        "--sample-file",
        metavar="FILE",
        help="Replay events from a JSON sample file instead of generating live events",
    )
    args = parser.parse_args()

    if args.sample_file:
        replay_from_file(args.sample_file)
    else:
        run_continuous()


if __name__ == "__main__":
    main()
