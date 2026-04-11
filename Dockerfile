FROM python:3.9-slim

# ffmpeg + ffprobe
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY fakeiptv/ ./fakeiptv/
COPY run.py config.yaml ./

# HLS segments live in a tmpfs volume mounted at runtime (see docker-compose).
# Cache (SQLite) lives in a named volume for persistence across restarts.
ENV FAKEIPTV_TMP_DIR=/tmp/fakeiptv \
    FAKEIPTV_CACHE_DIR=/cache

# Port is set at runtime via FAKEIPTV_PORT — declared in docker-compose.yml
CMD ["python", "run.py"]
