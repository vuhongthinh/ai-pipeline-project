# dashboard.py
import streamlit as st
import psycopg2
import pandas as pd
import json

import os

@st.cache_data(ttl=30)          # ← THÊM ttl=30 (tự refresh mỗi 30 giây)
def load_metadata():
    try:
        conn = psycopg2.connect(
            os.getenv("PG_URL", "postgresql://user:pass@postgres:5432/pipeline")
            #                                              ↑ "postgres" thay vì "localhost"
        )
        df = pd.read_sql(
            "SELECT * FROM metadata ORDER BY created_at DESC LIMIT 500",  # ← THÊM LIMIT
            conn
        )
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return pd.DataFrame()

st.title("🔍 Multi-modal Data Explorer")

df = load_metadata()

if not df.empty:
    # Thanh tìm kiếm
    query = st.text_input("Tìm kiếm theo nhãn (labels)")
    
    if query:
        # Lọc dataframe dựa trên JSONB chứa keyword
        filtered = df[df['labels'].apply(lambda x: query.lower() in json.dumps(x).lower())]
        st.dataframe(filtered)
    else:
        st.dataframe(df)

    # Hiển thị Metrics (Chỉ số tổng quan)
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Processed Files", len(df))
    col2.metric("Avg AI Confidence", f"{df['confidence'].mean() * 100:.1f}%" if 'confidence' in df else "N/A")
    col3.metric("System Status", "Healthy 🟢")
else:
    st.info("Chưa có dữ liệu nào trong hệ thống. Hãy upload file qua API!")