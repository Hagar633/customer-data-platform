
FROM python:3.12-slim

WORKDIR /app

COPY src/streaming/validate_cdc.py /app/validate_cdc.py

RUN pip install --no-cache-dir kafka-python psycopg2-binary

CMD ["python", "-u", "/app/validate_cdc.py"]
