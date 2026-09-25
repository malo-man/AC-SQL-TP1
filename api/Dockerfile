FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY tmdb_producer.py .

ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["python", "tmdb_producer.py"]
