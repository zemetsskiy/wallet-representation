FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY worker_scheduled.py .

ENTRYPOINT ["python", "worker_scheduled.py"]
