# tasks.py
from celery import Celery
import requests
import psycopg2
import json
import re
import os
from minio import Minio
from psycopg2 import pool as pg_pool

# Khởi tạo Celery kết nối với Redis Message Broker
app = Celery('tasks', broker='redis://redis:6379/0')

# Connection pool cho PostgreSQL — tránh mở/đóng connection mỗi task
db_pool = pg_pool.SimpleConnectionPool(
    1, 10,  # min=1, max=10 connections
    os.getenv("PG_URL", "postgresql://user:pass@postgres:5432/pipeline")
)


def safe_parse_llm_json(raw_text: str, fallback: dict) -> dict:
    """Parse JSON từ LLM output, tránh crash nếu output không hợp lệ"""
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        # LLM đôi khi bọc JSON trong markdown: ```json {...} ```
        match = re.search(r'\{.*\}', raw_text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return fallback  # Trả về fallback thay vì crash toàn bộ worker


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def process_file(self, task_id: str, file_url: str, mime_type: str):
    """Task xử lý ngầm: Phân phối tệp đến Model AI phù hợp dựa trên mime_type"""
    result = {
        "task_id": task_id,
        "file_url": file_url,
        "labels": [],
        "confidence": 0.0
    }

    try:
        if "image" in mime_type:
            result.update(process_image(file_url))
        elif "audio" in mime_type:
            result.update(process_audio(file_url))
        elif "text" in mime_type:
            result.update(process_text(file_url))

        save_metadata(result)

    except Exception as exc:
        print(f"Task {task_id} failed: {str(exc)}. Retrying ({self.request.retries}/{self.max_retries})...")
        raise self.retry(exc=exc)  # Celery tự retry, sau max_retries → Dead Letter Queue

    return result


def process_image(file_url: str):
    """Sử dụng LLaVA Vision Model qua Ollama"""
    prompt = """Analyze this image. Extract top-5 objects.
    Reply ONLY valid JSON: {"labels": ["car", "person"], "confidence": 0.92}"""

    response = requests.post("http://ollama:11434/api/generate", json={
        "model": "llava:13b",
        "prompt": prompt,
        "images": ["<base64_encoded_string_here>"],
        "stream": False
    }, timeout=120)

    return safe_parse_llm_json(
        response.json()['response'],
        fallback={"labels": [], "confidence": 0.0}
    )


def process_audio(file_url: str):
    """Sử dụng mô hình Whisper và sau đó dùng Llama 3"""
    # Gửi qua một endpoint Whisper giả định (có thể là một container khác)
    # transcript = requests.post("http://whisper-service/transcribe", files={"file": audio_data})
    transcript_text = "Sample transcribed text from audio"

    ner_prompt = f"Extract entities from this transcript: {transcript_text}. Reply ONLY valid JSON."
    response = requests.post("http://ollama:11434/api/generate", json={
        "model": "llama3",
        "prompt": ner_prompt,
        "stream": False
    }, timeout=120)

    return safe_parse_llm_json(
        response.json()['response'],
        fallback={"labels": [], "entities": [], "confidence": 0.0}
    )


def process_text(file_url: str):
    """Phân tích Text bằng Llama 3"""
    text = "Sample text content downloaded from MinIO"
    prompt = f"""Analyze text: {text}
    Return ONLY JSON: {{"sentiment": "positive", "labels": ["Apple", "iPhone"], "confidence": 0.9}}"""

    response = requests.post("http://ollama:11434/api/generate", json={
        "model": "llama3",
        "prompt": prompt,
        "stream": False
    }, timeout=120)

    return safe_parse_llm_json(
        response.json()['response'],
        fallback={"sentiment": "unknown", "labels": [], "confidence": 0.0}
    )


def save_metadata(result: dict):
    """Lưu trữ kết quả JSONB vào PostgreSQL dùng connection pool"""
    conn = db_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO metadata (id, file_url, labels, confidence, created_at)
            VALUES (%s, %s, %s, %s, NOW())
        """, (result['task_id'], result['file_url'],
              json.dumps(result['labels']), result.get('confidence', 0.0)))
        conn.commit()
        cur.close()
    finally:
        db_pool.putconn(conn)  # Trả về pool dù có lỗi hay không