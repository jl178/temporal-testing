# Documentation

| Doc | What it covers |
|---|---|
| [architecture.md](architecture.md) | The local platform, how the pieces connect, and the local ⇄ prod binding map |
| [examples.md](examples.md) | Every runnable example: Temporal as a service, the four SDK examples, infra validation |
| [etl.md](etl.md) | **The deep dive** — the `etl/` pipeline end to end: specs, canonical model, execution modes, ingest, workers, failure handling |
| [workers.md](workers.md) | The generic worker platform: size profiles, placement guide, what runs where |
| [security.md](security.md) | Auth chains, RBAC, and the local-vs-prod security posture |

## The 60-second map

This repo demonstrates Temporal at three levels, each usable on its own:

1. **Temporal as a service** — two Docker Compose stacks: a one-container dev
   stack, and a production-topology stack (load-balanced frontends, split
   services, Elasticsearch visibility, OIDC-protected UI, metrics). Plus CDK
   stacks that deploy the same thing to AWS with import-or-create
   dependencies.
2. **SDK examples** — the same greeting workflow in Python, Go, TypeScript,
   and C#: worker + starter + assertion, each runnable in one command.
3. **The ETL** — the real-world example: a multi-route file-ingestion and
   dbt-Spark transform platform orchestrated by Temporal, with parallel
   fan-out, per-route dispatch, quarantine, a canonical data model that
   generates the normalization, an Iceberg catalog, and three execution
   modes that map 1:1 onto AWS.

CI: `.github/workflows/ci.yml` runs the CDK assertion tests + synth and
the Python unit suite on every push. `.github/workflows/e2e.yml` spins up
the **full platform on the runner** (Temporal, LocalEmu, Iceberg catalog,
Spark Connect, SFTP) and runs the real pipelines with assertions — on PRs
and pushes touching the pipeline, plus manual dispatch.

Quick start for each level:

```sh
# 1 — the service
nix run .#up          # dev stack: Temporal + UI on :8080
nix run .#prod-up     # prod-topology stack (see examples.md)

# 2 — SDK examples (all four languages, e2e, asserted)
nix run .#examples

# 3 — the ETL
./etl/run.sh                                        # single transform, prod-shaped
ICEBERG_REST_URI=http://localhost:8181 \
  ./etl/ingest/run.sh                               # the full complex batch
```
