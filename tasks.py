# tasks.py
import os, json, base64, re, logging, requests
from io import BytesIO
from urllib.parse import urlparse, unquote

from celery import Celery
from celery.result import AsyncResult
from celery import current_app as celery_app
from minio import Minio
from psycopg2 import pool as pg_pool

# ----------------------------------------------------------------------
# Celery config
# ----------------------------------------------------------------------
app = Celery(
    "tasks",
    broker=os.getenv("REDIS_URL", "redis://redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/1")
)

# ----------------------------------------------------------------------
# DB connection pool
# ----------------------------------------------------------------------
db_pool = pg_pool.SimpleConnectionPool(
    1, 10,
    os.getenv(
        "PG_URL",
        "postgresql://user:pass@postgres:5432/pipeline"
    )
)

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
log = logging.getLogger("celery")
log.setLevel(logging.INFO)

# ----------------------------------------------------------------------
# MinIO helper
# ----------------------------------------------------------------------
def get_minio_client() -> Minio:
    return Minio(
        os.getenv("MINIO_URL", "minio:9000"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=False,
    )

def download_object(file_url: str) -> bytes:
    """
    file_url dạng http://minio:9000/uploads/<key>
    """
    p = urlparse(file_url)
    bucket = p.path.split("/")[1]           # "uploads"
    key = "/".join(p.path.split("/")[2:])
    key = unquote(key)

    client = get_minio_client()
    resp = client.get_object(bucket, key)
    data = resp.read()
    resp.close()
    resp.release_conn()
    return data

# ----------------------------------------------------------------------
# Ollama wrapper (LLM & vision)
# ----------------------------------------------------------------------
def ask_ollama(model: str, prompt: str, images_b64: list[str] | None = None) -> str:
    payload = {"model": model, "prompt": prompt, "stream": False}
    if images_b64:
        payload["images"] = images_b64
    resp = requests.post("http://ollama:11434/api/generate", json=payload, timeout=120)
    resp.raise_for_status()
    return resp.json().get("response", "")

# ----------------------------------------------------------------------
# Prompt helpers (Vietnamese)
# ----------------------------------------------------------------------
def build_label_prompt(descriptions: list[str]) -> str:
    """
    Nhận danh sách mô tả tiếng Việt (tối đa 5 phần). 
    Trả về prompt để LLM sinh nhãn ngắn (≤3 từ).
    """
    ctx = "\n".join(descriptions)
    return f"""Bạn là trợ lý gán nhãn ngắn gọn cho nội dung đa phương tiện.
Dưới đây là các mô tả (có thể đến 5 phần) bằng tiếng Việt.
Hãy trả lời **DUY NHẤT** một nhãn ngắn, không quá 3 từ, mô tả tổng quan nhất.
Nếu không chắc chắn, trả lời "khác".
{ctx}
Nhãn:"""

# ----------------------------------------------------------------------
# JSON‑safe parser (các LLM thường trả về JSON trong markdown)
# ----------------------------------------------------------------------
def safe_parse_llm_json(raw_text: str, fallback: dict) -> dict:
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        # Tìm JSON trong markdown/code block
        m = re.search(r'\{.*\}', raw_text, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except json.JSONDecodeError:
                pass
    return fallback

# ----------------------------------------------------------------------
# -----------------  PROCESSOR PER MODAL ------------------------------
# ----------------------------------------------------------------------
def process_image(file_url: str) -> dict:
    """
    1) Tải file, 2) Base64, 3) Hỏi LLaVA để lấy caption + objects.
    Kết quả trả về dict có ít nhất: description (string) và raw_response.
    """
    img_bytes = download_object(file_url)
    img_b64 = base64.b64encode(img_bytes).decode()

    # Prompt ngắn gọn, LLaVA sẽ trả JSON {"labels": [...], "description": "..."}
    prompt = "Mô tả ngắn gọn ảnh, liệt kê tối đa 5 đối tượng, trả về JSON {\"labels\": [...], \"description\": \"...\"}."

    raw = ask_ollama(model="llava:13b", prompt=prompt, images_b64=[img_b64])
    parsed = safe_parse_llm_json(raw, fallback={"labels": [], "description": ""})
    description = parsed.get("description") or ", ".join(parsed.get("labels", []))
    return {
        "type": "image",
        "raw_response": raw,
        "description": description.strip(),
        "labels": parsed.get("labels", []),
    }

def process_audio(file_url: str) -> dict:
    """
    1) Tải âm thanh, 2) Gửi tới Whisper service, 3) Hỏi Llama3 để trích thập “entities”.
    """
    audio_bytes = download_object(file_url)
    files = {"file": ("audio.wav", audio_bytes, "audio/wav")}
    resp = requests.post("http://whisper:8888/transcribe", files=files, timeout=120)
    resp.raise_for_status()
    transcript = resp.json().get("text", "")

    # Prompt LLM: trích 5 thực thể/keyword
    prompt = f"""Dưới đây là nội dung âm thanh đã được chuyển thành văn bản. 
Hãy liệt kê tối đa 5 thực thể, từ khóa quan trọng, trả về JSON {{ "entities": [...] }}.\n\n{transcript[:2000]}"""
    raw = ask_ollama(model="llama3", prompt=prompt)
    parsed = safe_parse_llm_json(raw, fallback={"entities": []})
    description = ", ".join(parsed.get("entities", []))
    return {
        "type": "audio",
        "raw_response": raw,
        "description": description,
        "entities": parsed.get("entities", []),
    }

def process_text(file_url: str) -> dict:
    """
    1) Tải file text, 2) Hỏi Llama3 để có sentiment + keywords.
    """
    txt_bytes = download_object(file_url)
    text = txt_bytes.decode(errors="ignore")

    # Giới hạn 2000 ký tự để tránh vượt token limit
    prompt = f"""Phân tích nội dung sau và trả về JSON:
{{"sentiment": "positive|negative|neutral", "labels": ["..."]}}.\n\n{text[:2000]}"""
    raw = ask_ollama(model="llama3", prompt=prompt)
    parsed = safe_parse_llm_json(raw, fallback={"sentiment": "unknown", "labels": []})
    description = f"Sentiment: {parsed.get('sentiment')}, " \
                  f"keywords: {', '.join(parsed.get('labels', []))}"
    return {
        "type": "text",
        "raw_response": raw,
        "description": description,
        "sentiment": parsed.get("sentiment"),
        "labels": parsed.get("labels", []),
    }

# ----------------------------------------------------------------------
# Aggregation → short label
# ----------------------------------------------------------------------
def aggregate_and_label(step_results: list[dict]) -> dict:
    descriptions = [r["description"] for r in step_results if r.get("description")]
    prompt = build_label_prompt(descriptions)

    raw_label = ask_ollama(model="llama3", prompt=prompt)
    # Dọn dẹp kết quả (có thể có dấu ngoặc kép/đơn)
    short = raw_label.strip().strip('"').strip("'")
    # Loại ký tự đặc biệt, giữ lại chữ, số, dấu cách
    short = re.sub(r"[^\w\sàáảãạâăđêếềọôơưừýỵ]", "", short, flags=re.IGNORECASE)
    # Giới hạn ≤3 từ
    short = " ".join(short.split()[:3])
    return {
        "short_label": short if short else "khác",
        "raw_label": raw_label,
        "detail": step_results,
    }

# ----------------------------------------------------------------------
# DB persistence
# ----------------------------------------------------------------------
def save_metadata(payload: dict):
    """
    payload gồm:
        task_id, file_url, mime_type, short_label, detail (JSON)
    """
    conn = db_pool.getconn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO metadata (id, file_url, mime_type, short_label, detail, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            """,
            (
                payload["task_id"],
                payload["file_url"],
                payload["mime_type"],
                payload["short_label"],
                json.dumps(payload["detail"]),
            ),
        )
        conn.commit()
        cur.close()
    finally:
        db_pool.putconn(conn)

# ----------------------------------------------------------------------
# -------------------- CELERY TASK ------------------------------------
# ----------------------------------------------------------------------
@app.task(bind=True, max_retries=3, default_retry_delay=60)
def process_file(self, task_id: str, file_url: str, mime_type: str):
    """Orchestrates per‑modal processing → aggregate → store."""
    step_results = []
    try:
        if "image" in mime_type:
            step_results.append(process_image(file_url))
        elif "audio" in mime_type:
            step_results.append(process_audio(file_url))
        elif "text" in mime_type or mime_type == "application/json":
            step_results.append(process_text(file_url))
        else:
            raise ValueError(f"Unsupported MIME type: {mime_type}")

        agg = aggregate_and_label(step_results)

        # Ghi vào DB
        db_payload = {
            "task_id": task_id,
            "file_url": file_url,
            "mime_type": mime_type,
            "short_label": agg["short_label"],
            "detail": agg["detail"],
        }
        save_metadata(db_payload)

    except Exception as exc:
        log.exception("Task %s failed", task_id)
        raise self.retry(exc=exc)

    # Celery sẽ tự lưu `result` vào backend (Redis), trả về cho UI
    return {
        "task_id": task_id,
        "file_url": file_url,
        "mime_type": mime_type,
        "short_label": agg["short_label"],
        "detail": agg["detail"],
    }
