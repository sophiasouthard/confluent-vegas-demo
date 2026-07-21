# Vegas Demo — Lessons Learned

Problems encountered during the initial setup of this demo and exactly how each
was resolved. Read this before starting a new context window.

---

## 1. Wrong MCP URL → `Session terminated` 502

**Error:**
```
[ERROR] - Failed to create toolkit: Gateway creation failed: 502
{"message":"Failed to initialize gateway at https://mcp.us-east-1.aws.confluent.cloud/4411d9ce-.../mcp: Session terminated"}
```

**Cause:**  
The org-level shortlink URL (`https://mcp.us-east-1.aws.confluent.cloud/<ORG_ID>/mcp`) has
no cluster bound to it. Confluent accepts the connection then immediately terminates because
it doesn't know which cluster to attach to.

**Fix:**  
Use the full RTCE cluster-scoped URL format:
```
https://mcp.us-east-1.aws.confluent.cloud/mcp/v1/context-engine/organizations/<ORG_ID>/environments/<ENV_ID>/kafka-clusters/<CLUSTER_ID>
```
Copy this exact URL from the Confluent Cloud UI after enabling RTCE on the topic
(Topics → select topic → Real-time Context Engine tab).

---

## 2. RTCE Not Enabled → Toolkit Registers But Has No Tools

**Cause:**  
The RTCE endpoint URL exists at the org level but won't initialise until RTCE is
explicitly enabled per topic in the Confluent Cloud UI.

**Fix:**  
Go to Confluent Cloud → Topics → select each topic → **Real-time Context Engine** tab
→ click **Enable**. Do this for **both** `player_events` and `player_risk_alerts`.
Both topics share the same cluster-level endpoint URL once enabled.

> **Cluster tier:** RTCE requires **Standard or higher**. A Basic cluster will not show
> the Real-time Context Engine tab. Upgrade via Cluster settings → Edit → Upgrade.

---

## 3. Wrong Tool Names in Agent YAML → `No tools found`

**Error:**
```
[ERROR] - Failed to find tool. No tools found with the name 'confluent:list-topics'
```

**Cause:**  
The agent yaml was copied from the fraud detection demo which used the `mcp-confluent`
local server. That server exposes tools in kebab-case (`list-topics`, `get-topic-config`,
`consume-messages`). The Confluent RTCE MCP endpoint uses camelCase.

**Fix:**  
Check actual registered tool names with `orchestrate toolkits list`, then update the
agent yaml to match. The correct RTCE tool names are:
```yaml
tools:
  - confluent:listTopics
  - confluent:getMetadata
  - confluent:consumeMessages
```

---

## 4. Python Toolkit — `--tier` Required, Premium Plan Error

**Error:**
```
[ERROR] - Failed to create toolkit: --tier option is required for python toolkit
[ERROR] - Failed to create toolkit: Dedicated Deployment is only available for PREMIUM plan customers
```

**Cause:**  
`orchestrate toolkits add --kind python` requires `--tier` (small/medium/large), but
dedicated deployment tiers are a Premium-only feature. This account is not on Premium.

**Fix:**  
Use `orchestrate tools import` instead of `orchestrate toolkits add` for Python tools:
```bash
orchestrate tools import \
  --kind python \
  --file vegas/orchestrate/player_risk_toolkit/player_risk_actions.py \
  --package-root vegas/orchestrate/player_risk_toolkit \
  --requirements-file vegas/orchestrate/player_risk_toolkit/requirements.txt
```

---

## 5. Python Tools Not Imported Before Agent → `No tools found`

**Error:**
```
[ERROR] - Failed to find tool. No tools found with the name 'flag_player'
```

**Cause:**  
`orchestrate agents import` validates that every tool in the agent's `tools:` list
already exists. The Python tools (`flag_player`, `suspend_player`, etc.) must be
imported before the agent.

**Fix:**  
Always import in this order:
1. `orchestrate tools import` — Python toolkit
2. `orchestrate toolkits add` — Confluent MCP toolkit
3. `orchestrate agents import` — agent

---

## 6. `orchestrate toolkits import` Does Not Support Python

**Cause:**  
`orchestrate toolkits import -f toolkit.yaml` only handles MCP spec files.
It cannot import Python toolkits regardless of what the `toolkit.yaml` says.

**Fix:**  
Use `orchestrate tools import --kind python` (see Problem 4 above).

---

## 7. `orchestrate toolkits add` Has No `get` Subcommand

**Error:**
```
No such command 'get'
```

**Fix:**  
Use `orchestrate toolkits list` to see all registered toolkits and their tool names.

---

## 8. `consumeMessages` vs `queryData` on Compacted Topics

**Cause:**  
`player_risk_alerts` is an upsert/compacted topic (PRIMARY KEY defined in Flink DDL).
`queryData` does not support compacted topics and returns no results or errors.

**Fix:**  
Use `consumeMessages` for `player_risk_alerts`. The agent yaml and instructions
should explicitly call out `consumeMessages`, not `queryData`.

---

## Complete Working Registration Sequence

```bash
# 1. Create the Orchestrate connection
orchestrate connections configure \
  -a confluent-cloud --env draft --type team --kind key_value

# 2. Set Kafka credentials
orchestrate connections set-credentials \
  -a confluent-cloud --env draft \
  -e "KAFKA_API_KEY=<key>" \
  -e "KAFKA_API_SECRET=<secret>"

# 3. Register Confluent RTCE MCP toolkit
orchestrate toolkits add \
  --kind mcp \
  --name confluent \
  --description "Confluent Cloud — Vegas gaming demo" \
  --url "https://mcp.us-east-1.aws.confluent.cloud/mcp/v1/context-engine/organizations/<ORG_ID>/environments/<ENV_ID>/kafka-clusters/<CLUSTER_ID>" \
  --transport streamable_http \
  --tools "*" \
  --app-id confluent-cloud

# 4. Import Python tools
orchestrate tools import \
  --kind python \
  --file vegas/orchestrate/player_risk_toolkit/player_risk_actions.py \
  --package-root vegas/orchestrate/player_risk_toolkit \
  --requirements-file vegas/orchestrate/player_risk_toolkit/requirements.txt

# 5. Import the agent
orchestrate agents import -f vegas/orchestrate/player_risk_agent.agent.yaml
```
