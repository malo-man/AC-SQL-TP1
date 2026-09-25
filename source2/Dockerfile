FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY imdb_fetcher.py .

ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["python", "imdb_fetcher.py"]
