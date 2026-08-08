# temporal-testing

End-to-end [Temporal](https://temporal.io) example:

- **Local runtime** — full Temporal cluster (server, Postgres, web UI, admin tools) via Docker Compose.
- **AWS infrastructure** — CDK stacks with import-or-create abstractions: ECS Fargate + Aurora Serverless v2, internal NLB for gRPC, ALB for the UI, optional Route 53 DNS. Every major dependency (VPC, ECS cluster, database, hosted zone) can be **passed in if it already exists, or is created if not**.
- **Emulator validation** — the stacks deploy against a FOSS local AWS emulator (LocalStack was archived/paywalled in March 2026; this repo uses its successors).
- **Example workflows** — the same greeting workflow (workflow + activity + worker + starter) in **Python, Go, TypeScript, and C#**, each validated end-to-end against the local cluster.

## Prerequisites

- Docker (with Compose v2)
- [Nix](https://nixos.org) with flakes — provides the entire toolchain (Node 20, Go, Python 3.12, .NET 8, Temporal CLI, AWS CLI):

```sh
nix develop          # dev shell with all tools
direnv allow         # or use direnv (.envrc included)
```

Without Nix, install manually: Node 20+, Go 1.24+, Python 3.12+, .NET 8 SDK, [Temporal CLI](https://docs.temporal.io/cli), AWS CLI v2.

## Quickstart

```sh
nix run .#up          # start Temporal (server :7233, UI http://localhost:8080)
nix run .#examples    # run all 4 language examples e2e (starts the cluster if needed)
nix run .#down        # stop everything
```

Each example's `run.sh` is self-contained: installs dependencies, starts a worker, executes a workflow on its own task queue, and asserts the result (`Hello, Temporal!`). Individual runs:

```sh
./examples/python/run.sh
./examples/go/run.sh
./examples/typescript/run.sh
./examples/csharp/run.sh
```

Point any of them at another cluster with `TEMPORAL_ADDRESS=host:7233`.

## AWS deployment (CDK)

```
infra/
├── bin/app.ts                     # context-driven wiring
├── lib/constructs/
│   ├── temporal-network.ts        # VPC       — import or create
│   ├── temporal-database.ts       # Postgres  — import, or create Aurora Serverless v2
│   ├── temporal-cluster.ts        # ECS cluster — import or create; server + UI Fargate services
│   ├── temporal-dns.ts            # hosted zone — import or create; ui./grpc. alias records
│   └── temporal-worker.ts         # optional Fargate worker service construct
└── lib/stacks/                    # TemporalNetworkStack, TemporalStack
```

**Architecture (create-everything mode):** 2-AZ VPC → Aurora Serverless v2 PostgreSQL (credentials in Secrets Manager) → `temporalio/auto-setup` on Fargate (all four Temporal services in one container; creates/migrates schema on boot) registered in Cloud Map (`temporal-frontend.temporal.local`) and fronted by an **internal NLB** on :7233 → `temporalio/ui` on Fargate behind an **ALB**. `auto-setup` is a dev/example topology — production deployments should run the four Temporal services separately.

Everything that "might already exist" is a context flag — provided values are imported, omitted ones are created:

```sh
npx cdk deploy --all                                   # create everything
npx cdk deploy --all \
  -c vpcId=vpc-0123456789 \                            # reuse a VPC
  -c ecsClusterName=my-cluster \                       # reuse an ECS cluster
  -c dbEndpoint=mydb.cluster-x.us-east-1.rds.amazonaws.com \
  -c dbSecretArn=arn:aws:secretsmanager:...:secret:x \ # reuse a Postgres + creds
  -c dbSecurityGroupId=sg-0123 \                       # let the stack open DB ingress
  -c domainName=temporal.example.com \                 # enable DNS records
  -c hostedZoneId=Z... -c zoneName=example.com \       # reuse a hosted zone
  -c publicUi=false \                                  # keep the UI ALB internal
  -c serviceDiscovery=false \                          # skip Cloud Map, use the NLB
  -c natGateways=0                                     # no NAT (emulators/cost)
```

Validate without an AWS account:

```sh
nix run .#infra-test    # 13 assertion tests: create-everything / import-existing / DNS on-off / no-cloud-map
nix run .#synth         # clean cdk synth of both stacks
```

## Emulator validation

LocalStack archived its repos and paywalled the Community edition in March 2026. Two FOSS successors were evaluated:

| Emulator | Result |
|---|---|
| **[LocalEmu](https://github.com/localemu/localemu)** (Apache-2.0 LocalStack fork) | ✅ Both stacks deploy fully via CloudFormation (53 resources incl. Aurora, ECS services, ELBs, Cloud Map, IAM). Control-plane emulation: resources exist in CFN state but don't run. |
| **[MiniStack](https://github.com/ministackorg/ministack)** (MIT) | ⚠️ Network stack deploys; direct RDS/ECS APIs run **real Docker containers** (validated a live Postgres via the RDS API), but its CFN engine lacks `AWS::RDS::DBSubnetGroup`, `AWS::ServiceDiscovery::PrivateDnsNamespace`, `AWS::EC2::SecurityGroupIngress`. |

```sh
pip install localemu && localemu start    # gateway on :4566
nix run .#validate-emulator               # synth + deploy both stacks + report
```

The script deploys the synthesized templates with the plain CloudFormation API (no `cdk bootstrap` — emulator CFN engines mishandle the toolkit stack; the bootstrap-version SSM parameter is seeded directly). Real *runtime* validation is the Docker Compose path above — the emulators only prove out the infrastructure templates.

## Repository layout

```
docker-compose.yml     # postgres:16 + temporalio/auto-setup + temporalio/ui + admin-tools
dynamicconfig/         # Temporal dynamic config mounted into the server
examples/{python,go,typescript,csharp}/
infra/                 # CDK v2 app (TypeScript) + jest assertion tests
scripts/               # validate-local.sh, validate-emulator.sh
flake.nix              # dev shell + task-runner apps (nix run .#<up|down|examples|infra-test|synth|validate-emulator|validate>)
```
