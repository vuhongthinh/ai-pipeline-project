from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import os
from minio import Minio
from io import BytesIO   
import uuid 
from tasks import process_file
from celery.result import AsyncResult
from celery import current_app as celery_app

app = FastAPI(title="Multi-modal Ingestion API")

# Bật CORS để cho phép Giao diện gọi API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Khởi tạo MinIO
minio_client = Minio(
    os.getenv("MINIO_URL", "minio:9000"),
    access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
    secure=False,
)
#end point để kiểm tra trạng thái
@app.get("/health")
async def health():
    return {"status": "ok", "minio": minio_client.bucket_exists("uploads")}
if not minio_client.bucket_exists("uploads"):
    minio_client.make_bucket("uploads")


@app.get("/")
async def serve_ui():
    try:
        with open("app_interface.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Thiếu tệp app_interface.html. Hãy kiểm tra lại thư mục!</h1>", status_code=404)

@app.get("/task/{task_id}")
async def get_task_status(task_id: str):
    """Trả về trạng thái và kết quả (nếu có)"""
    async_res = AsyncResult(task_id, app=celery_app)
    state = async_res.state
    if state == "PENDING":
        return {"status": "queued"}
    elif state == "STARTED":
        return {"status": "processing"}
    elif state == "FAILURE":
        return {"status": "failed", "reason": str(async_res.result)}
    else:  # SUCCESS
        return {"status": "finished", "result": async_res.result}

# -----------------------------------

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    task_id = str(uuid.uuid4())
        
    file_id = f"{task_id}.{file.filename.split('.')[-1]}"
    file_content = await file.read()
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
    if len(file_content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="File too large. Maximum size is 50MB.")
    
    minio_client.put_object("uploads", file_id, BytesIO(file_content), len(file_content))
    file_url = f"http://minio:9000/uploads/{file_id}"
        
    # Đẩy vào Celery
    process_file.delay(task_id, file_url, file.content_type)
        
    return {
        "status": "accepted",
        "task_id": task_id,
        "file_url": file_url,
        "message": "File queued for AI processing in the background."
    }