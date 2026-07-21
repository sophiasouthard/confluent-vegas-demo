# Vegas Gaming Demo — Setup Guide

Real-time player risk detection demo for casino gaming.  
Apache Kafka + Flink SQL on Confluent Cloud → IBM watsonx Orchestrate agent.

---

## Architecture

```
Python producer
    │  player_events (Kafka topic)
    ▼
Confluent Cloud
    ├── Apache Flink SQL
    │       1-min tumbling window → player_risk_alerts (Kafka topic)
    └── Real-time Context Engine (RTCE)
            MCP endpoint → watsonx Orchestrate
                                │
                        player_risk_agent
                        (LLaMA 3.3 70B)
                                │
                    flag / suspend / escalate
```

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Terraform | 1.0+ | `brew install terraform` |
| Python | 3.9+ | `brew install python` |
| watsonx Orchestrate ADK | latest | `pip install ibm-watsonx-orchestrate` |
| Confluent Cloud account | — | [confluent.cloud](https://confluent.cloud) |
| Confluent Cloud API Key | Cloud-level | Menu → API Keys → Add key → Global access |

---

## Step 1 — Provision Infrastructure

```bash
cd vegas/terraform

cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — set api_key and api_secret

terraform init
terraform apply
```

`terraform apply` creates:
- `vegas-gaming-env` environment with Stream Governance (Essentials)
- `vegas-cluster` Basic Kafka cluster (AWS us-east-1)
- `vegas-gaming-pool` Flink compute pool (5 CFU)
- All API keys and RBAC role bindings
- Three Flink SQL statements (player_events table, player_risk_alerts table, risk detection job)
- **Auto-writes `vegas/python/.env`** with all connection credentials

---

## Step 2 — Enable Real-time Context Engine (RTCE)

RTCE exposes Kafka topics as an MCP endpoint that watsonx Orchestrate can query directly.

1. Open [confluent.cloud](https://confluent.cloud) → your environment → `vegas-cluster`
2. In the left nav, go to **Topics**
3. For **each** of the two topics below, open the topic → **Real-time Context Engine** tab → **Enable**:
   - `player_events`
   - `player_risk_alerts`

Once enabled, both topics are served from the same cluster-level MCP endpoint:

```
https://mcp.us-east-1.aws.confluent.cloud/mcp/v1/context-engine/organizations/<ORG_ID>/environments/<ENV_ID>/kafka-clusters/<CLUSTER_ID>
```

> Find your `ORG_ID`, `ENV_ID`, and `CLUSTER_ID` in the Confluent Cloud UI or from `terraform output`.

---

## Step 3 — Start the Python Producer

```bash
cd vegas/python

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt

# Continuous producer (Ctrl+C to stop)
python produce_player_events.py
```

Expected output every 5 seconds:
```
📊 Live Stats | delivered=47 failed=0 rate=1.23/sec elapsed=38.2s
🎰 Risk players  : 38 events (80.9%)
✅ Normal players: 9 events (19.1%)
🚨 Flag threshold reached (>20 bets): PLAYER-RISK-01, PLAYER-RISK-03
```

### One-shot demo replay (window-aligned)

For a guaranteed clean demo trigger, use the sample data generator:

```bash
python generate_sample_data.py
python produce_player_events.py --sample-file player_events_sample.json
```

---

## Step 4 — Verify in Confluent Cloud

1. Open [confluent.cloud](https://confluent.cloud) → `vegas-gaming-env` → `vegas-cluster`
2. **Topics** → `player_events` — messages should be flowing
3. **Topics** → `player_risk_alerts` — flagged players appear after ~1 minute
4. **Flink** → confirm `player_risk_detection_job` statement is **RUNNING**

---

## Step 5 — Register the Confluent MCP Toolkit in Orchestrate

```bash
# 1. Create the connection (once per Orchestrate environment)
orchestrate connections configure \
  -a confluent-cloud \
  --env draft \
  --type team \
  --kind key_value

# 2. Set credentials (Kafka API key from terraform output)
orchestrate connections set-credentials \
  -a confluent-cloud \
  --env draft \
  -e "KAFKA_API_KEY=<your_kafka_api_key>" \
  -e "KAFKA_API_SECRET=<your_kafka_api_secret>"

# 3. Register the RTCE MCP toolkit
orchestrate toolkits add \
  --kind mcp \
  --name confluent \
  --description "Confluent Cloud — Vegas gaming demo" \
  --url "https://mcp.us-east-1.aws.confluent.cloud/mcp/v1/context-engine/organizations/<ORG_ID>/environments/<ENV_ID>/kafka-clusters/<CLUSTER_ID>" \
  --transport streamable_http \
  --tools "*" \
  --app-id confluent-cloud
```

> Substitute `<ORG_ID>`, `<ENV_ID>`, `<CLUSTER_ID>` from the RTCE endpoint shown in the
> Confluent Cloud UI after enabling RTCE on the topics in Step 2.

---

## Step 6 — Import the Python Toolkit and Agent

```bash
# Import the player risk action tools (flag, suspend, notify, escalate)
orchestrate tools import \
  --kind python \
  --file vegas/orchestrate/player_risk_toolkit/player_risk_actions.py \
  --package-root vegas/orchestrate/player_risk_toolkit \
  --requirements-file vegas/orchestrate/player_risk_toolkit/requirements.txt

# Import the agent
orchestrate agents import -f vegas/orchestrate/player_risk_agent.agent.yaml
```

---

## Step 7 — Run the Demo

Open the watsonx Orchestrate chat UI and try these prompts:

**Prove the data is live:**
```
List all players currently flagged in the player_risk_alerts stream
```

**High risk investigation:**
```
Investigate player PLAYER-RISK-01 for suspicious activity
```

**Elevated risk:**
```
Run a risk check on PLAYER-RISK-03
```

**Clean player:**
```
What is the risk status of PLAYER-NORMAL-01?
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `Missing required environment variables` | Run `terraform apply` first — it writes `vegas/python/.env` automatically |
| `Could not retrieve schemas` | Flink CREATE TABLE statements haven't finished yet — wait 1–2 min and retry |
| Flink job stuck in PROVISIONING | Normal — takes ~2 min on first run; refresh the Confluent Cloud UI |
| Producer connects but no alerts appear | Wait for the 1-minute tumbling window to close |
| `SASL authentication failed` | API key may be deleted; run `terraform apply` again to regenerate |
| `Session terminated` 502 on toolkit add | RTCE not enabled on topics — complete Step 2 first |
| `No tools found with the name 'confluent:...'` | Toolkit registered with wrong URL or RTCE not initialised — re-run Step 5 |
| `No tools found with the name 'flag_player'` | Python tools not imported yet — run Step 6 toolkit import first |

---

## Reset Between Demo Runs

```bash
# Delete messages from both topics via Confluent Cloud UI:
# Topics → player_events → Actions → Delete all messages
# Topics → player_risk_alerts → Actions → Delete all messages

# Or destroy and re-apply for a completely clean slate:
cd vegas/terraform
terraform destroy
terraform apply
```

---

## Cleanup

```bash
cd vegas/terraform
terraform destroy
```

Removes all Confluent Cloud resources (environment, cluster, Flink pool, API keys).
