import time
import requests
import io
import os

# Gọi thẳng vào container API nội bộ của hệ thống
API_UPLOAD_URL = os.getenv("API_URL", "http://api:8000/upload")

def crawl_and_upload():
    print("🤖 Mini Clawbot: Đang thu thập dữ liệu thị trường...")
    
    # Giả lập hành động cào dữ liệu text (Ở thực tế, bạn dùng requests.get(url) để cào TradingView/Binance)
    timestamp = int(time.time())
    mock_data = f"[{timestamp}] Thị trường biến động mạnh. Bitcoin tăng vọt vượt mốc kháng cự. Apple công bố AI mới."
    file_name = f"market_news_{timestamp}.txt"

    # Tạo file in-memory (Không cần ghi ra ổ cứng bot để tiết kiệm tài nguyên)
    file_obj = io.BytesIO(mock_data.encode('utf-8'))

    try:
        # Bắn file thẳng vào luồng Ingestion của FastAPI
        files = {'file': (file_name, file_obj, 'text/plain')}
        response = requests.post(API_UPLOAD_URL, files=files)
        
        if response.status_code == 200:
            print(f"✅ Mini Clawbot: Đã đẩy vào Pipeline -> Task ID: {response.json().get('task_id')}")
        else:
            print(f"❌ Mini Clawbot: API từ chối -> {response.text}")
            
    except Exception as e:
        print(f"❌ Mini Clawbot: Lỗi kết nối tới API -> {e}")

if __name__ == "__main__":
    print("🚀 Mini Clawbot (Alpine) Khởi động! Chu kỳ: 60 giây/lần")
    # Đợi 15 giây để API Gateway khởi động hoàn toàn trước khi bot bắt đầu bắn file
    time.sleep(15) 
    
    while True:
        crawl_and_upload()
        time.sleep(60) # Cứ 60s bot tự động cào 1 lần