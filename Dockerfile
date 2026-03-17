# Dockerfile.whisper
FROM python:3.11-slim

# apt‑get dependencies (ffmpeg)
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg libsndfile1 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Nếu muốn chạy LLaVA phía client (GPU) thì:
# RUN pip install torch==2.3.1+cu121 torchvision==0.18.1+cu121 -f https://download.pytorch.org/whl/cu121/torch_stable.html
# Whisper + torch (cpu) 

COPY . /app

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
RUN pip install --no-cache-dir "openai-whisper[ffmpeg]" torch==2.3.1

COPY whisper_service.py /app/
EXPOSE 8888
CMD ["uvicorn", "whisper_service:app", "--host", "0.0.0.0", "--port", "8888"]
