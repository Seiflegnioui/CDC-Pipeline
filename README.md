# CDC Pipeline: MongoDB → Debezium → Kafka → Elasticsearch

A production-ready **Change Data Capture** pipeline that replicates every insert,
update, and delete from MongoDB to Elasticsearch in real time with sub-200ms
latency.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CDC Pipeline Architecture                        │
│                                                                         │
│  ┌──────────┐    ┌──────────────┐    ┌─────────┐    ┌───────────────┐  │
│  │          │    │   Debezium   │    │         │    │               │  │
│  │ MongoDB  │───▶│   Source     │───▶│  Kafka  │───▶│ Elasticsearch │  │
│  │  (rs0)   │    │  Connector   │    │ (KRaft) │    │   Sink Conn.  │  │
│  │          │    │              │    │         │    │               │  │
│  └──────────┘    └──────────────┘    └─────────┘    └───────┬───────┘  │
│    :27017        └── Kafka Connect ──┘  :9092               │          │
│                        :8083                          ┌─────▼─────┐    │
│                                                       │           │    │
│                  ┌──────────────┐                      │  Elastic  │    │
│                  │   Kafka UI   │                      │  search   │    │
│                  │    :8080     │                      │   :9200   │    │
│                  └──────────────┘                      └─────┬─────┘    │
│                                                             │          │
│                                                       ┌─────▼─────┐    │
│                                                       │  Kibana   │    │
│                                                       │   :5601   │    │
│                                                       └───────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

**Data Flow:**
1. Application writes to **MongoDB** (replica set `rs0`)
2. **Debezium** captures changes via MongoDB change streams
3. Events are published to **Kafka** topics (KRaft mode — no external coordinator)
4. **Elasticsearch Sink** connector consumes events and indexes them
5. Changes are queryable in **Elasticsearch** and visualizable in **Kibana**

## Prerequisites

| Tool             | Minimum Version | Check Command          |
|------------------|-----------------|------------------------|
| Docker           | 24.0+           | `docker --version`     |
| Docker Compose   | v2.20+          | `docker compose version` |
| Python           | 3.10+           | `python3 --version`    |
| curl             | any             | `curl --version`       |
| mongosh          | any             | `mongosh --version`    |

> **Note:** `mongosh` is needed only for `make test` and `make init-mongo` when
> run from the host. The Docker container has it pre-installed.

## Quick Start

Three commands to go from zero to a running pipeline:

```bash
# 1. Start all services (MongoDB, Kafka, Connect, ES, Kibana, Kafka UI)
make up

# 2. Wait ~90 seconds for Kafka Connect to install plugins, then:
make init

# 3. Run the end-to-end test
make test
```

That's it. The pipeline is now capturing changes from MongoDB and replicating
them to Elasticsearch.

## Verifying the Pipeline

### Kafka UI — [http://localhost:8080](http://localhost:8080)
- Navigate to **Topics** to see `dbserver1.appdb.users`, `dbserver1.appdb.products`,
  `dbserver1.appdb.orders`
- Click on a topic → **Messages** to see CDC events flowing in real time

### Kibana — [http://localhost:5601](http://localhost:5601)
1. Go to **Management → Stack Management → Data Views**
2. Create a data view with pattern `users*`
3. Go to **Discover** to browse the replicated documents

### Terminal Verification
```bash
# Check connector status
make status

# Query Elasticsearch directly
curl -s http://localhost:9200/users/_search?pretty

# Run the interactive demo (requires Python 3.10+)
make demo
```

## Project Structure

```
cdc-pipeline/
├── docker-compose.yml          # All services (KRaft Kafka, no ext. coordinator)
├── .env.example                # Environment variable template
├── Makefile                    # All automation targets
├── README.md                   # This file
├── connectors/
│   ├── mongodb-source.json     # Debezium MongoDB source configuration
│   └── elasticsearch-sink.json # ES sink configuration
├── scripts/
│   ├── init-mongo.js           # Replica set init + seed data
│   ├── init-connectors.sh      # Connector registration script
│   └── test-pipeline.sh        # End-to-end pipeline test
└── app/
    ├── demo.py                 # Python demo application
    └── requirements.txt        # Pinned Python dependencies
```

## Available Make Targets

| Target           | Description                                      |
|------------------|--------------------------------------------------|
| `make up`        | Start all Docker services                        |
| `make init`      | Initialize MongoDB RS + register connectors      |
| `make test`      | Run end-to-end INSERT→UPDATE→DELETE test         |
| `make demo`      | Run the Python demo app (continuous CRUD + verify)|
| `make logs-connect` | Tail Kafka Connect logs                       |
| `make status`    | Show connector status from Connect REST API      |
| `make down`      | Stop and remove all containers and volumes       |
| `make reset`     | Full teardown (including volumes) and restart     |

## Common Errors and Fixes

### 1. Replica Set Not Initialized

**Symptom:** Debezium connector fails with `not a replica set member`

**Fix:**
```bash
# Re-run MongoDB initialization
make init-mongo

# Or manually:
docker exec mongodb mongosh --eval "rs.initiate({_id:'rs0',members:[{_id:0,host:'mongodb:27017'}]})"
```

### 2. Connector Registration Fails (Kafka Connect Not Ready)

**Symptom:** `init-connectors.sh` exits with "Kafka Connect not ready after 60s"

**Cause:** Kafka Connect needs time to download and install the Debezium and ES
plugins on first startup (~60-90 seconds).

**Fix:**
```bash
# Check if Connect is ready
curl -s http://localhost:8083/connectors

# Watch Connect logs for "Kafka Connect started"
make logs-connect

# Once ready, re-run:
make init-connectors
```

### 3. CLUSTER_ID Mismatch on Kafka Restart

**Symptom:** Kafka fails to start with `Configured CLUSTER_ID ... doesn't match`

**Cause:** KRaft stores the cluster ID in its log directory. If you change the
`CLUSTER_ID` env var without clearing the volume, Kafka refuses to start.

**Fix:**
```bash
# Full reset — removes all volumes including Kafka logs
make reset
```

> **Important:** `make reset` deletes ALL data (MongoDB, Kafka, ES). Use it only
> when you need a clean slate.

### 4. Elasticsearch Index Not Created

**Symptom:** `curl http://localhost:9200/users/_search` returns `index_not_found`

**Cause:** The Elasticsearch Sink connector creates indices on first message.
If no CDC events have been produced yet, the index won't exist.

**Fix:**
```bash
# 1. Verify connectors are running
make status

# 2. Insert a document to trigger CDC
docker exec mongodb mongosh appdb --eval "db.users.insertOne({name:'test',email:'test@example.com'})"

# 3. Wait 2 seconds, then check
curl -s http://localhost:9200/users/_search?pretty
```

### 5. Port Conflicts

**Symptom:** `bind: address already in use`

**Fix:** Edit `.env` to change the conflicting port:
```bash
cp .env.example .env
# Edit .env and change the port, e.g.:
# KAFKA_UI_PORT=8081
```

## Adding a New MongoDB Collection

To add a new collection (e.g., `appdb.inventory`) to the pipeline:

### Step 1: Update the Source Connector
Edit `connectors/mongodb-source.json`:
```json
"collection.include.list": "appdb.users,appdb.products,appdb.orders,appdb.inventory"
```

### Step 2: Update the Sink Connector
Edit `connectors/elasticsearch-sink.json`:
```json
"topics": "dbserver1.appdb.users,dbserver1.appdb.products,dbserver1.appdb.orders,dbserver1.appdb.inventory"
```

### Step 3: Re-register the Connectors
```bash
# Delete existing connectors
curl -X DELETE http://localhost:8083/connectors/mongodb-source
curl -X DELETE http://localhost:8083/connectors/elasticsearch-sink

# Re-register with updated configs
make init-connectors
```

### Step 4: Create the Collection in MongoDB
```bash
docker exec mongodb mongosh appdb --eval "db.createCollection('inventory')"
```

New documents inserted into `appdb.inventory` will now flow through to the
`inventory` index in Elasticsearch.

## Technology Stack

| Component        | Image                                    | Version |
|------------------|------------------------------------------|---------|
| MongoDB          | `mongo`                                  | 6.0     |
| Kafka            | `confluentinc/cp-kafka`                  | 7.6.0   |
| Kafka Connect    | `confluentinc/cp-kafka-connect`          | 7.6.0   |
| Debezium MongoDB | `debezium-connector-mongodb`             | 2.5.4   |
| Elasticsearch    | `elasticsearch`                          | 8.13.0  |
| Kibana           | `kibana`                                 | 8.13.0  |
| Kafka UI         | `provectuslabs/kafka-ui`                 | latest  |

## License

This project is provided as-is for educational and development purposes.
# CDC-Pipeline
