FROM python:3.12.6

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN apt-get update && apt-get install -y dos2unix && \
    find . -maxdepth 1 -type f \( -name "*.py" -o -name "*.env" -o -name "*.json" \) -exec dos2unix {} + && \
    apt-get --purge remove -y dos2unix && \
    rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["python", "main.py"]