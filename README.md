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
    │       1-min tumbling window → player_risk_alerts_v2 (Kafka topic)
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
- `vegas-cluster` Standard Kafka cluster (AWS us-east-1)
- `vegas-gaming-pool` Flink compute pool (5 CFU)
- All API keys and RBAC role bindings
- Three Flink SQL statements (player_events table, player_risk_alerts table, risk detection job)
- **RTCE enabled on `player_events` and `player_risk_alerts_v2`** — no UI step needed
- **Auto-writes `python/.env`** with all connection credentials

> **Cluster tier:** RTCE requires a **Standard** cluster or higher. The Terraform config
> provisions Standard by default. Do not downgrade to Basic or RTCE will be unavailable.

---

## Step 2 — Get the RTCE MCP Endpoint

RTCE is enabled automatically by `terraform apply` — no Confluent Cloud UI step required.

Both topics (`player_events` and `player_risk_alerts_v2`) are wired up via the
`confluent_rtce_rtce_topic` Terraform resource and will show as **On** in the
Confluent Cloud UI once provisioning completes (~1–2 min).

Retrieve the MCP endpoint URL by running:

```bash
cd terraform

ORG_ID=$(terraform output -raw organization_id)
ENV_ID=$(terraform output -raw environment_id)
CLUSTER_ID=$(terraform output -raw cluster_id)

echo "https://mcp.us-east-1.aws.confluent.cloud/mcp/v1/context-engine/organizations/${ORG_ID}/environments/${ENV_ID}/kafka-clusters/${CLUSTER_ID}"
```

Copy the printed URL — you will need it in Step 5.

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
  --name player-risk-stream \
  --description "Confluent Cloud RTCE — Vegas gaming demo (player_risk_alerts_v2)" \
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
List all players currently flagged in the player_risk_alerts_v2 stream
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
| `Session terminated` 502 on toolkit add | RTCE not yet ready — wait 1–2 min after `terraform apply` and retry; or check that the cluster is Standard tier (not Basic) |
| `MT_UPSERT_NOT_SUPPORTED` on queryData | Topic is compacted — delete and recreate `player_risk_alerts_v2` without a PRIMARY KEY, then restart the Flink job |
| `No tools found with the name 'player-risk-stream:...'` | Toolkit registered with wrong name or URL — re-run Step 5 with `--name player-risk-stream` |
| `No tools found with the name 'flag_player'` | Python tools not imported yet — run Step 6 toolkit import first |

---

## Reset Between Demo Runs

```bash
# Delete messages from both topics via Confluent Cloud UI:
# Topics → player_events → Actions → Delete all messages
# Topics → player_risk_alerts_v2 → Actions → Delete all messages

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
