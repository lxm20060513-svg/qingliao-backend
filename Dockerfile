FROM python:3.11-slim

WORKDIR /app

# 轻聊后端：纯标准库为主，三个依赖
# - paramiko：路由器管理模块（SSH 连接路由器执行 Clash 启停/状态查询）
# - cryptography：密码管理模块（Fernet 加密凭据存储）
# - pyyaml：BE20 —— provider_admin / channel_api / stream_api / usage_api 都要 import yaml，
#   原镜像没装：provider_admin 直接回「yaml 模块不可用」（删内置 provider 功能全废），
#   其余三处静默降级成「配置读不到」
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY backend/ /app/

# 数据目录（挂载卷，存放会话/上传/日志/密钥等）
VOLUME ["/data"]

EXPOSE 9125 9127 9128 9129 9131 9132 9133 9135 9136 9137 9138 9139 9140 9141 9142 9143 9147 9149

CMD ["python3", "qingliao_all.py"]
