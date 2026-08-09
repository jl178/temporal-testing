# The ETL team's worker image: one image per codebase — every fleet runs
# this same image with a different worker_platform command (set by the ECS
# task definition / CDK TemporalWorkerService `command` prop).
#
# Build from the REPO ROOT (needs worker_platform/):
#   docker build -f etl/Dockerfile -t etl-worker .
#
# Note: pure Spark Connect client — no JVM. A compute-large fallback fleet
# (in-process Spark) would layer a JRE on top of this image.
FROM python:3.12-slim

WORKDIR /app
COPY etl/requirements.txt etl/requirements.txt
RUN pip install --no-cache-dir -r etl/requirements.txt

COPY worker_platform/ worker_platform/
COPY etl/ etl/
ENV PYTHONPATH=/app
WORKDIR /app/etl

# Default fleet; ECS overrides per fleet, e.g.:
#   ["python","-m","worker_platform","--queue","etl-pipeline","--profile","small","--workflows","workflow"]
CMD ["python", "-m", "worker_platform", "--queue", "etl-pipeline", "--profile", "medium", "--activities", "activities"]
