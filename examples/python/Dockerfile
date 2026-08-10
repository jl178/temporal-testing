FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY activities.py workflows.py worker.py starter.py heartbeat_probe.py ./
ENV TEMPORAL_ADDRESS=temporal-nginx:7233
CMD ["python", "worker.py"]
