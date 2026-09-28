---
name: qingliao-deploy
description: Deploy the Qingliao backend and Hermes plugin for users.
version: 0.1.0
author: lxm20060513 (lxm20060513-svg), Hermes Agent
license: MIT
platforms: [linux, macos]
metadata:
  hermes:
    tags: [Qingliao, Deploy, Docker]
    related_skills: []
---

# Qingliao Deploy Skill

把「轻聊」后端（AI 助手服务）与 Hermes 平台插件部署到本机。用户只需提供：想设置的
访问密码（或让它自动生成）与上游 AI 端点。全部用 `terminal` 工具执行；本 skill 假定
宿主机已装 Docker 与 docker compose 插件（未装则先装 Docker，不要自行换部署形态）。

## When to Use

- 用户说「帮我部署轻聊 / 部署轻聊后端 / 装 qingliao」。
- 用户已装好 App，要求把服务端配起来。
- Don't use for: 已部署实例的升级/排障（只做全新部署；排障按用户现场情况单独判断）。

## Prerequisites

- Docker + `docker compose` 插件可用：`docker version`、`docker compose version`。
- 网络可达 GitHub 与镜像源（`python:3.11-slim`）。
- 上游 AI 端点（OpenAI 兼容 `/v1/chat/completions`）与其 API Key——**先问用户要**，
  拿不到就先用 Hermes 网关本机地址（见步骤 2 的默认值）。
- 需要确定的两个用户输入：`QL_PASSWORD`（登录密码；用户不指定就自动随机生成并告知）、
  上游端点 URL 与 Key。

## How to Run

```bash
bash install.sh                       # 交互式；见 Procedure 里的非交互替代
```

交互式脚本会向 stdin 提问；用 Hermes 执行时**走非交互路径**：先写好 `.env` 再运行
（脚本检测到已有有效 `.env` 会自动跳过全部提问）。

## Procedure

1. **拉仓库**（用 `terminal` 执行）
   ```bash
   git clone https://github.com/lxm20060513-svg/qingliao-backend.git
   cd qingliao-backend
   ```
   完成判据：目录存在且含 `install.sh`、`docker-compose.yml`。

2. **写 `.env`**（生成随机 token 用 `openssl rand -hex 16`）
   ```bash
   cat > .env <<EOF
   QL_PASSWORD=<用户密码或随机值>
   QL_INBOX_TOKEN=<随机 hex>
   QL_PUSH_TOKEN=<随机 hex>
   QL_HERMES_URL=http://host.docker.internal:9123/v1/chat/completions
   QL_HERMES_KEY=<上游 Key，可空>
   EOF
   ```
   完成判据：`grep -c '=' .env` ≥ 4。

3. **执行安装**
   ```bash
   bash install.sh
   ```
   脚本自身会：构建镜像 → `docker compose up -d` → 轮询 `127.0.0.1:9127` 就绪（最多 60s）
   → 打印 `docker compose ps`。完成判据：`curl -s -m 2 http://127.0.0.1:9127/` 有响应。

4. **（可选）部署 Hermes 平台插件**——让轻聊作为 Hermes 的一个通道接入 agent 循环。
   若用户的上游 AI 就是本机 Hermes 网关，且希望轻聊消息进 agent 工具循环，执行：
   ```bash
   bash <(curl -fsSL https://raw.githubusercontent.com/lxm20060513-svg/qingliao-hermes-plugin/main/install.sh) \
     <profile 目录>/plugins/qingliao-platform
   ```
   然后在 Hermes 配置 `platforms.qingliao.enabled: true` 并重启 gateway。
   完成判据：`GET :9130/health` 返回 `{"ok": true}`，gateway 日志出现
   `qingliao: HTTP listener up on port 9130`。用户不需要通道式接入时**跳过此步**
   （后端直连上游端点即可用）。

5. **交付信息**——向用户回报（可复制进 App 登录页）：
   - 服务器地址：`http://<本机局域网IP>:9127`（同网段直连）；
   - 用户名：`qingliao`；密码：步骤 2 的 `QL_PASSWORD`；
   - 提醒：App 里先点「测试连接」再登录。

## Pitfalls

- **别改用 `docker run`**：compose 文件里端口映射（9125-9141）、数据卷、健康检查都已配好。
- `host.docker.internal` 在 Linux 上靠 compose 的 `extra_hosts` 映射；若上游端点不在
  本机，直接写完整 URL。
- 首次构建拉镜像慢不是卡死；`docker compose logs -f qingliao` 看进度。
- 密码含特殊字符时写 `.env` 不用加引号（compose 逐行读），但别含换行。
- 9127 起了一部分端口没起是正常的：各模块独立监听，App 核心只依赖 9127/9132。

## Verification

```bash
docker compose ps                 # qingliao 容器 Up
curl -s http://127.0.0.1:9127/    # 有 HTTP 响应（非 connection refused）
curl -s "http://127.0.0.1:9127/api/auth/login_get?u=qingliao&p=<QL_PASSWORD>"
# 返回含 "ok": true 的 JSON（GET + query，不是 POST body）
```
三项都过 = 部署成功，把服务器地址 + 初始账号告诉用户即可。
