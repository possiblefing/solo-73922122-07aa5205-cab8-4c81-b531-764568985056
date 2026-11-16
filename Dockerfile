FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 共享卷：原件字节、SQLite 库与临时文件均在其中。
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# 两实例使用同一命令、同一挂载卷，仅端口不同。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
