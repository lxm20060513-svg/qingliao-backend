FROM python:3.11-slim

WORKDIR /app

# 轻聊后端：纯标准库为主，仅两个可选依赖
# - paramiko：路由器管理模块（SSH 连接路由器执行 Clash 启停/状态查询）
# - cryptography：密码管理模块（Fernet 加密凭据存储）
RUN pip install --no-cache-dir paramiko cryptography

COPY backend/ /app/

# 数据目录（挂载卷，存放会话/上传/日志/密钥等）
VOLUME ["/data"]

EXPOSE 9125 9127 9128 9129 9131 9132 9133 9135 9136 9137 9138 9139 9140 9141 9142

CMD ["python3", "qingliao_all.py"]
