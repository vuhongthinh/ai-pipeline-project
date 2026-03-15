FROM python:3.10-slim

WORKDIR /app

# Cài đặt các thư viện hệ thống cần thiết cho psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Lệnh mặc định sẽ bị ghi đè bởi docker-compose (tùy theo là api hay worker)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]