# The billing tenant's worker image — deliberately tiny: the team's code,
# the platform runner, the SDK. Nothing ETL. Build from the REPO ROOT:
#   docker build -f examples/platform-demo/Dockerfile -t billing-worker .
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir "temporalio>=1.9,<2"

COPY worker_platform/ worker_platform/
COPY examples/platform-demo/billing.py billing.py
ENV PYTHONPATH=/app

CMD ["python", "-m", "worker_platform", "--queue", "billing", "--profile", "small", "--workflows", "billing"]
