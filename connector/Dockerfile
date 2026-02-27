FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY connector.py produce_events.py produce_events_avro.py ./

ENTRYPOINT ["python", "-u", "connector.py"]
