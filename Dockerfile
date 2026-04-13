# Streamlit app — build context must be this project root (no `merged/` folder required).
FROM python:3.12-slim-bookworm

WORKDIR /app

# Railway: أنشئ Volume واضبط Mount path = /data (أو أي مسار؛ عرّف نفس القيمة في Variables كـ DATA_DIR).
ENV DATA_DIR=/data

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

# تشغيل افتراضي؛ railway.json يضبط نفس الأمر في deploy.startCommand
CMD ["python3", "docker_entrypoint.py"]
