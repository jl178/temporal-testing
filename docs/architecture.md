# Architecture — local platform ≙ prod

The design rule throughout: **the local default is the prod shape.** Workers
are light orchestrators; Spark is an external service; the catalog is
configured server-side; data moves cluster ↔ object store and never through
workers. Moving to AWS swaps *bindings* (a URI, a credential source, a
catalog type) — not topology.

## The local system

```mermaid
flowchart TB
    subgraph clients [Clients]
        CLI["starter.py · temporal CLI"]
        UI["Web UI :8080 · Dex login"]
    end

    subgraph temporal [Temporal cluster — prod-mimic]
        LB["nginx :7233"] --> FE1[frontend] & FE2[frontend]
        FE1 & FE2 --- SVC["history · matching · sys-worker"]
        SVC --- PG[(Postgres 16)]
        SVC --- ES[(Elasticsearch)]
    end

    subgraph fleets [Worker fleets — all light]
        IW["file-ingest: wf + activity workers"]
        EW["etl-pipeline: wf + light workers"]
        HW["etl-heavy: fallback only"]
    end

    subgraph data [Data plane]
        S3[("LocalEmu S3 :4566<br/>landing/ curated/ quarantine/ warehouse/")]
        SPARK["Spark Connect :15002<br/>S3A + lake catalog server-side"]
        ICE["Iceberg REST :8181"]
        SFTP["SFTP :2222"]
        EMR["EMR Serverless API<br/>(control plane, emulated)"]
    end

    CLI -->|start workflow| LB
    fleets -->|long-poll task queues| LB
    fleets -->|"dbt → SQL over Connect"| SPARK
    fleets -->|"land · copy · validate (bytes/metadata)"| S3
    fleets -->|start_job_run + poll| EMR
    SFTP -->|via land activity| S3
    SPARK <-->|"s3a:// data plane (bulk data — the only bulk path)"| S3
    SPARK -->|table commits| ICE
    ICE -->|warehouse/ data + metadata| S3
```

The one claim to internalize: the **Spark ↔ S3 edge is the only path bulk
data travels** — executors read and write object storage in parallel. Worker
edges carry SQL text, control-plane calls, and single-file streams.

### Ports

| Endpoint | Service |
|---|---|
| `:7233` | Temporal gRPC (nginx → load-balanced frontends) |
| `:8080` | Temporal UI (Dex-protected) |
| `:15002` | Spark Connect server (`sc://localhost:15002`) |
| `:8181` | Iceberg REST catalog |
| `:4566` | LocalEmu (S3, EMR Serverless API, +130 services) |
| `:2222` | SFTP test server (`demo`/`demo`) |
| `:5556` | Dex OIDC |
| `:9090` / `:8085` | Prometheus / Grafana |

## Local ⇄ prod binding map

Each row is one concern. The middle column is the contract that stays fixed;
local and prod are two bindings of it — which is why nothing re-architects
on the way up.

| Concern | Local binding | Invariant contract | Prod binding |
|---|---|---|---|
| File delivery | SFTP container, worker pulls | `landing/` zone in S3 | AWS Transfer Family → S3 event starts the workflow (worker touches no bytes) |
| Object store | LocalEmu `:4566` | `s3://` URIs · `AWS_ENDPOINT_URL` | S3 |
| Temporal cluster | compose prod-mimic | gRPC `:7233` · namespaces | CDK: ECS Fargate + internal NLB + Aurora Serverless v2 |
| Worker fleets | host processes | task-queue names | CDK `TemporalWorkerService`: ECS fleets + backlog autoscaling |
| Interactive Spark | `spark:4.1.1` Connect container | `spark_remote = sc://…` | EMR Serverless session endpoint (pre-initialized capacity) |
| Batch Spark | emulated control plane | `start_job_run` + artifacts in `jobs/` | EMR Serverless job runs (same `spark_job.py` actually executes) |
| Table catalog | Iceberg REST, configured on the Spark server | spec `catalog` / `defaultCatalog` | Glue Data Catalog, enabled on the EMR application |
| Identity (UI) | Dex, static test user | OIDC env vars | Okta / Cognito / Auth0 |
| Credentials | static test keys | SDK default credential chain | IAM task/execution roles + Secrets Manager |
| Visibility | Elasticsearch container | search attributes | managed OpenSearch/Elasticsearch |

## Prod deployment (what the CDK stacks build)

```mermaid
flowchart LR
    subgraph vpc [VPC — import-or-create]
        ALB["ALB (public, optional DNS/ACM)"] --> UIS["ECS: Temporal UI"]
        NLB["NLB internal<br/>:7233 gRPC · :7243 HTTP API"] --> TS["ECS: temporal server<br/>(auto-setup, Cloud Map)"]
        TS --> AUR[("Aurora Serverless v2<br/>creds in Secrets Manager")]
        WF["ECS worker fleets<br/>(TemporalWorkerService, all light)"] -->|long-poll| NLB
        LAM["backlog poller λ (1 min)<br/>DescribeTaskQueue via :7243"] -->|"ApproximateBacklogCount → CloudWatch"| SCALE["ECS step scaling<br/>+1 / +3 / drain"]
        SCALE --> WF
    end
    subgraph managed [Managed services]
        S3P[("S3: landing/ curated/<br/>quarantine/ warehouse/")]
        GLUE["Glue Data Catalog"]
        EMRP["EMR Serverless<br/>batch runs + sessions"]
        TF["Transfer Family (SFTP → S3)"]
        IDP["IdP: Okta / Cognito"]
    end
    WF -->|submit jobs · SQL sessions| EMRP
    EMRP --> GLUE & S3P
    TF --> S3P
    IDP -.->|OIDC + JWT| ALB
```

Every CDK dependency is **import-or-create**: pass `-c vpcId=… -c
ecsClusterName=… -c dbEndpoint=… -c hostedZoneId=…` to reuse existing
infrastructure; omit to create it. Architecture knobs: `-c publicUi=false`,
`-c serviceDiscovery=false`, `-c natGateways=0`. The heavy worker queue does
not exist in prod — workers submit, EMR computes.

See [security.md](security.md) for the auth chains and hardening map, and
[etl.md](etl.md) for the pipeline that runs on top of this platform.
