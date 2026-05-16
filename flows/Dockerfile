# Prefect worker / deploy 镜像：官方 Prefect 3 镜像 + minio 客户端
FROM prefecthq/prefect:3-latest

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
# 实际 flow 代码与 prefect.yaml 由 docker-compose 以卷挂载进 /app，
# 改代码无需重建镜像。
