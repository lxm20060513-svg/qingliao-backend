---
name: qingliao-deploy
description: Deploy the Qingliao backend and Hermes plugin for users.
version: 0.2.0
author: lxm20060513 (lxm20060513-svg), Hermes Agent
license: MIT
platforms: [linux, macos]
metadata:
  hermes:
    tags: [Qingliao, Deploy, Docker, SelfUpdate]
    related_skills: []
---

# Qingliao Deploy Skill

把「轻聊」后端（AI 助手服务）与 Hermes 平台插件部署到本机。用户只需提供：想设置的
访问密码（或让它自动生成）与上游 AI 端点。全部用 `terminal` 工具执行；本 skill 假定
宿主机已装 Docker 与 docker compose 插件（未装则先装 Docker，不要自行换部署形态）。

> 适用版本：后端 **v4.0.14 起**（含 `/api/selfupdate` 一键更新链路）。最后更新 2026-10-05。

## When to Use

- 用户说「帮我部署轻聊 / 部署轻聊后端 / 装 qingliao」。
- 用户已装好 App，要求把服务端配起来。
- Don't use for: 已部署实例的升级/排障（只做全新部署；排障按用户现场情况单独判断）。

## Prerequisites

- Docker + `docker compose` 插件可用：`docker version`、`docker compose version`。
- 网络可达 GitHub 与镜像源（`python:3.11-slim`；「一键更新」还要能拉官方 `docker` cli 镜像）。
- 上游 AI 端点（OpenAI 兼容 `/v1/chat/completions`）与其 API Key——**先问用户要**，
  拿不到就先用 Hermes 网关本机地址（见步骤 2 的默认值）。
- 需要确定的两个用户输入：`QL_PASSWORD`（登录密码；用户不指定就自动随机生成并告知）、
  上游端点 URL 与 Key。
- 若部署机已有 nginx 等反代把 9127 转给 App 用：**要给 `/api/selfupdate` 单列转发**，
  否则 App「一键更新」会 404（见「一键更新」一节与 Pitfalls）。

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
   QL_REPO_DIR=<宿主上 qingliao-backend 仓的绝对路径（上一步 cd 进去后的 pwd）>
   EOF
   ```
   `QL_REPO_DIR` 是**「一键更新」用的宿主仓根**：更新时容器经 docker socket 起 helper 容器，
   把这个目录挂进去跑 `update.sh`。`install.sh` 会自己补写这一行（`grep -q '^QL_REPO_DIR='`），
   但自己手写 `.env` 时别漏——漏了只会退化成「手动更新」，不算报错（见「一键更新」一节）。
   完成判据：`grep -c '=' .env` ≥ 5。

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

5. **（可选·用反代时必做）给 `/api/selfupdate` 补入口**
   仓库 `deploy/nginx/` 里的 conf 只是**参考件**（其中 `hermes-webui.conf` 决定 `/api/*` 往哪转）：
   `install.sh` **不装** nginx conf，`update.sh` 也只更 `backend/` + 重建容器，
   **入口层不随任何更新下发** ⇒ 用反代的实例要按自己的布局手工补一行（缩进同邻居），
   放在兜底 `location /api/` **之前**（nginx 最长前缀优先）：
   ```nginx
   location /api/selfupdate { proxy_pass http://127.0.0.1:9127; }
   ```
   改完 `nginx -t` 通过再 reload，并用 `nginx -T | grep -n selfupdate` 回读确认位置正确。

6. **交付信息**——向用户回报（可复制进 App 登录页）：
   - 服务器地址：`http://<本机局域网IP>:9127`（同网段直连）；
   - 用户名：`qingliao`；密码：步骤 2 的 `QL_PASSWORD`；
   - 提醒：App 里先点「测试连接」再登录。

## 一键更新（`/api/selfupdate`）怎么工作

- 接口（**都要鉴权**，未带凭据回 401）：
  - `POST /api/selfupdate {"action":"check"}` → 同步查远端有没有新版本（秒回）；
  - `POST /api/selfupdate {"action":"run"}` → 后台起更新，返回 `{"task":"<id>"}`；
  - `GET /api/selfupdate?task=<id>` → 轮询进度/结果；`GET /api/selfupdate` → 当前状态
    （idle/running/done/failed + 上次结果）。
- 执行方式：容器经挂载的 `/var/run/docker.sock`（compose 已带）起 helper 镜像 → 在宿主仓跑
  `update.sh`，**等价于用户 SSH 上去敲 `./update.sh`**。更新期间容器会重建，App 短暂连不上是正常的。
- 版本戳：**版本号由 `update.sh` 自动写 `QL_VERSION`**（不需要手动注入），`GET /api/version` 可回读。
- **非 git 装法不是故障**：手动拷代码跑、没有 `QL_REPO_DIR` 的实例，`check` 会回
  `{"ok": true, "update_available": false, "manual": true}`（**不是** `ok:false`），App 不报红；
  这种情况只能在仓里手跑 `./update.sh` —— 别去改代码「修」它。

## Pitfalls

- **别改用 `docker run`**：compose 文件里端口映射（9125-9141）、数据卷、健康检查都已配好。
- `host.docker.internal` 在 Linux 上靠 compose 的 `extra_hosts` 映射；若上游端点不在
  本机，直接写完整 URL。
- 首次构建拉镜像慢不是卡死；`docker compose logs -f qingliao` 看进度。
- 密码含特殊字符时写 `.env` 不用加引号（compose 逐行读），但别含换行。
- 9127 起了一部分端口没起是正常的：各模块独立监听，App 核心只依赖 9127/9132。
- **反代下 App「一键更新」报 404，按字面量定层**：`404 + text/plain`（body 形如 `404: Not Found`，
  十几字节）= 入口没给 `/api/selfupdate` 单列转发，落到了别的服务的兜底路由；`404 + JSON` = 已到后端
  但路由表里没这个键；上层反代（如 Lucky / 云厂商网关）白名单拦下则常见 403 或警告页。
  注意 `/api/version` 可能早先单独补过 ⇒「检查更新」正常、**只有「一键更新」404**，别只看一个接口。
- 改入口 conf 时：确认改动在 `location /api/` 兜底**之前**，`nginx -t` 门禁后再 reload，
  并用 `grep -c` 回读确认写入真生效（别 `cp` 完就以为好了；有些环境 `cp` 会静默不生效）。

## Verification

```bash
docker compose ps                 # qingliao 容器 Up
curl -s http://127.0.0.1:9127/    # 有 HTTP 响应（非 connection refused）
curl -s "http://127.0.0.1:9127/api/auth/login_get?u=qingliao&p=<QL_PASSWORD>"
# 返回含 "ok": true 的 JSON（GET + query，不是 POST body）
curl -s http://127.0.0.1:9127/api/version     # {"ok":true,"version":"v4.0.x",...} 版本戳随更新自动写
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  -H 'Content-Type: application/json' -d '{"action":"check"}' \
  http://127.0.0.1:9127/api/selfupdate
# 401 = 路由已通（需鉴权，正常）；404 = 入口或路由缺 → 见 Pitfalls
```
前三项都过 = 部署成功，把服务器地址 + 初始账号告诉用户即可。
