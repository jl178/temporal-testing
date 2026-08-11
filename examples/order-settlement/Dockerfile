# The settlement team's worker image: one image per codebase — every
# fleet runs it with a different worker_platform command (ECS task
# definition / TemporalWorkerService `command` prop):
#   ["python","-m","worker_platform","--queue","settlement-intake","--profile","small","--workflows","pricing:OrderBatchWorkflow"]
#   ["python","-m","worker_platform","--queue","settlement-intake","--profile","medium","--activities","pricing:split_batch,write_remittance,resolve_settlements_spec"]
#   ["python","-m","worker_platform","--queue","settlement-orders","--profile","small","--workflows","pricing:OrderPricingWorkflow"]
#   ["python","-m","worker_platform","--queue","settlement-contracts","--profile","medium","--activities","contracts:lookup_contract"]
#   (contracts.py would be ANOTHER team's image in production — bundled
#   here so the demo ships as one artifact.)
#
# Build from the REPO ROOT (needs worker_platform/ + the etl modules the
# spec resolver reads):
#   docker build -f examples/order-settlement/Dockerfile -t settlement-worker .
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir "temporalio>=1.9,<2" "boto3>=1.34,<2" \
    "asyncssh>=2.14,<3" "smbprotocol>=1.13,<2" "PyYAML>=6,<7"

COPY worker_platform/ worker_platform/
COPY etl/ingest/ etl/ingest/
COPY etl/runtime_env.py etl/runtime_env.py
COPY etl/specs/ etl/specs/
COPY etl/dbt/models/staging/schema.yml etl/dbt/models/staging/schema.yml
COPY examples/order-settlement/pricing.py examples/order-settlement/contracts.py examples/order-settlement/

ENV PYTHONPATH=/app:/app/etl:/app/examples/order-settlement
WORKDIR /app/examples/order-settlement

CMD ["python", "-m", "worker_platform", "--queue", "settlement-orders", "--profile", "small", "--workflows", "pricing:OrderPricingWorkflow"]
