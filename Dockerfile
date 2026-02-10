# ใช้ภาพพื้นฐานจาก Airflow
FROM apache/airflow:2.8.1

# สลับเป็น Root เพื่อลงเครื่องมือสำหรับ Build (จำเป็นสำหรับบางไลบรารีบน Mac)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# สลับกลับเป็น user airflow
USER airflow

# คัดลอก requirements.txt และติดตั้ง
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt