# main.py
from fastapi import FastAPI, UploadFile, File , HTTPException
import os
from minio import Minio
from io import BytesIO   
import uuid 
from tasks import process_file  # Import Celery task

app = FastAPI(title="Multi-modal Ingestion API")

# Khởi tạo kết nối tới MinIO
minio_client = Minio(
    os.getenv("MINIO_URL", "minio:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False
)
if not minio_client.bucket_exists("uploads"):
    minio_client.make_bucket("uploads")

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    # 1. Tạo ID duy nhất cho tác vụ
    task_id = str(uuid.uuid4())
        
    # 2. Lưu tệp thô vào MinIO (Object Storage)
    file_id = f"{task_id}.{file.filename.split('.')[-1]}"
    file_content = await file.read()
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
    if len(file_content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="File too large. Maximum size is 50MB.")
    
    # Đảm bảo bucket 'uploads' đã tồn tại trước khi put_object (thêm logic thực tế nếu cần)
    minio_client.put_object("uploads", file_id, BytesIO(file_content), len(file_content))
    file_url = f"http://localhost:9000/uploads/{file_id}"
        
    # 3. Đẩy tác vụ vào Redis Queue cho Celery Worker (BẤT ĐỒNG BỘ)
    # Hàm .delay() giúp FastAPI trả về response ngay lập tức mà không chờ AI xử lý
    process_file.delay(task_id, file_url, file.content_type)
        
    # 4. Trả về cho User ngay lập tức (Zero Wait)
    return {
        "status": "accepted",
        "task_id": task_id,
        "file_url": file_url,
        "message": "File queued for AI processing in the background."
    }