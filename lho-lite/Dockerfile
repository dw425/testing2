FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app/ app/
COPY app.yaml .

# Create data directory
RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1
ENV LHO_DATA_DIR=/app/data

EXPOSE 8050

CMD ["python3", "app/main.py"]
