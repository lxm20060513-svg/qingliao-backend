# 轻聊后端（Qingliao Backend）

家庭 NAS 上的 AI 助手后端服务，纯 Python 标准库实现（仅路由器/密码管理两个可选依赖）。为 [轻聊 iOS/Web 客户端提供 AI 对话流式代理、会话同步、文件管理、智能家居（Home Assistant）代理、Docker 管理、知识库、定时任务、密码管理等 API。

## ✨ 功能

| 端口 | 服务 | 说明 |
|---|---|---|
| 9125 | cron | 定时任务管理 |
| 9127 | ha | Home Assistant 代理（智能家居） |
| 9128 | logs | 系统日志（含崩溃上报） |
| 9129 | files | 文件管理（上传/下载/重命名） |
| 9131 | sessions | 会话同步 |
| 9132 | stream | AI 流式代理（后端持流，客户端轮询） |
| 9133 | auth | 登录/Token/密码管理 |
| 9135 | secrets | 凭据加密存储（Fernet） |
| 9136 | router | 路由器状态 / Clash 快捷启停 |
| 9137 | docker | Docker Compose 部署管理 |
| 9138 | kb | 知识库（文档检索注入） |
| 9139 | hw | NAS 硬件温度 |
| 9149 | local | 本地模型管理（Ollama 状态/开关/模型/拉取/删除） |
| 9140 | memory | AI 记忆 |
| 9141 | weather | 天气（Open-Meteo，无 key） |
| 9142 | scenes | 智能家居场景（AI 生成动作组，一键执行） |
| 9143 | asr | 语音转文字（faster-whisper 转写，供 App 上传录音） |
| 9145 | agent | Agent 记忆规则管理（「以后XX都用agent」话术） |
| 9146 | automation | 定时自动化（「X分钟后执行Y」延迟动作，到点自动执行后消失） |
| 9147 | push | 微信推送队列（enqueue/pending/done + 推送开关，X-Push-Token 鉴权） |
| — | inbox | 收件箱（外部 agent 出站消息，App 收件箱页轮询） |
| — | life | 生活数据（备忘录/便签/生活记录） |
| — | mcp | MCP 工具服务（App「MCP工具服务」配置写入 Hermes） |
| — | channel | 渠道管理（微信通道模型路由） |
| — | usage | 用量统计 |
| — | media | MEDIA: 协议转换（AI 回复内图片/文件渲染） |

## 🚀 快速开始（一键安装）

```bash
git clone https://github.com/lxm20060513-svg/qingliao-backend.git
cd qingliao-backend
bash install.sh
```

安装脚本会引导设置访问密码与上游 LLM 端点，生成 `.env`，构建并启动容器，最后做健康检查。

<details>
<summary>手动部署（不用脚本）</summary>

```bash
git clone https://github.com/lxm20060513-svg/qingliao-backend.git
cd qingliao-backend
# 编辑 docker-compose.yml 设置 QL_PASSWORD 等环境变量
docker compose up -d
```

</details>

服务启动后，各 API 以 `/api/<模块>/` 前缀对外暴露（建议用 nginx 反代统一入口）。

> ⚠️ **nginx 路由部署须知（踩坑记录）**：新增 API 模块时，必须在**全部入口**配置特化 location：
> - `8080`（hermes-webui.conf）——**它有 `location /api/` 泛匹配兜底转发到 Hermes(9123)，新增路由不配特化会被 Hermes 404 吞掉**（scenes/asr/agent 都踩过）
> - `16668`（qingliao_http.conf）与 `443`（webui_443.conf）——按模块名加 `location /api/xxx { proxy_pass http://127.0.0.1:PORT; }`
>
> ⚠️ **场景动作 service 格式**：动作里存 `climate.turn_off`（点分隔），执行时后端自动拆为 HA 路径 `/api/services/climate/turn_off`（斜杠），勿直接拼接点号字符串（会 404）。

## 📖 踩坑记录在哪

完整踩坑实录（sudo/nginx/systemd/后端 patch/鉴权 token/PWA 缓存/ASR 自愈/docker 解析/看门狗）沉淀在 Hermes 技能 `qingliao-webui`（开发/调试/部署轻聊必读）与 NAS `轻聊app/避坑指南.md`（iOS 端）。

## ⚙️ 环境变量

| 变量 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `QL_PASSWORD` | ✅ | `change-me` | 统一访问密码（所有 API 密码鉴权） |
| `QL_DATA_DIR` | | `/data` | 数据目录（会话/上传/日志/密钥） |
| `QL_UPLOAD_DIR` | | `$QL_DATA_DIR/uploads` | 文件上传目录 |
| `QL_HERMES_URL` | ✅ | — | 上游 LLM（OpenAI 兼容端点） |
| `QL_HERMES_KEY` | | 空 | 上游 LLM API Key |
| `QL_HA_URL` | | `http://localhost:8123` | Home Assistant 地址（可选） |
| `QL_HA_TOKEN` | | 空 | HA 长期访问令牌 |
| `QL_ROUTER_HOST` | | 空 | 路由器 SSH 地址（可选，Clash 管理） |
| `QL_ROUTER_USER` | | `root` | 路由器 SSH 用户 |
| `QL_ROUTER_PASSWORD` | | 空 | 路由器 SSH 密码 |
| `QL_DOCKER_ROOT` | | `/data/docker` | Docker Compose 项目目录 |
| `STREAM_DATA_DIR` | | `$QL_DATA_DIR/streams_data` | 流式任务数据 |
| `SESSIONS_DATA_DIR` | | `$QL_DATA_DIR/sessions` | 会话数据 |
| `QL_LOG_CONTAINER` | | `hermes` | 日志模块查询的容器名 |
| `QL_AGENT_URL` | | DeepSeek 官方 | Agent 模式模型端点（需支持 function calling） |
| `QL_AGENT_KEY` | | 空 | Agent 模式 API Key |
| `QL_AGENT_MODEL` | | `deepseek-chat` | Agent 模式模型名 |

## 🤖 Agent 模式（工具调用）

消息含控制/查询意图（如"帮我查磁盘""把空调关了""生成离家模式"）时，自动切换 **Agent 通道**：直连支持 function calling 的模型（默认 DeepSeek 官方 API），模型可调用工具执行后回填结果：

- `get_time` / `get_disk_usage` / `get_service_status` / `get_temperature`
- `docker_ps` / `docker_action`（容器启停）
- `ha_list_entities` / `ha_call`（智能家居控制）
- `get_weather` / `scene_save` / `scene_run` / `scene_list`（场景）

**场景**：聊天里说「帮我生成离家模式：关灯、关空调、布防」→ Agent 查询 HA 实体 → 生成动作组存 `scenes.json` → 看板点场景卡一键执行（`POST /api/scenes/run`）。

## 🔗 上游依赖

- **LLM 上游**：任意 OpenAI 兼容端点（`/v1/chat/completions`）。支持流式输出与多模型 provider 路由。可与 [Hermes](https://hermes-agent.nousresearch.com) 网关、DeepSeek 官方 API、OpenCode Go 订阅等对接。
- **Home Assistant**（可选）：智能家居模块，未配置时该模块返回空数据。
- **路由器**（可选）：Clash 管理模块，需要容器能 SSH 访问路由器（装好 `paramiko` 依赖）。
- **Docker**（可选）：容器管理模块，需要挂载 `/var/run/docker.sock`。

## 📦 非 Docker 部署

```bash
pip install paramiko cryptography   # 可选依赖
cd backend
export QL_PASSWORD=your-password
export QL_HERMES_URL=http://127.0.0.1:9123/v1/chat/completions
python3 qingliao_all.py
```

> 注意：`files_api` 使用了 `cgi` 模块（Python 3.13 已移除），请使用 **Python 3.11/3.12** 运行。

## 🧱 架构

- 单进程多线程：`qingliao_all.py` 启动全部服务（`http.server.ThreadingHTTPServer`）
- 纯标准库（HTTP 服务/JSON/线程），无 Web 框架
- 流式输出：后端持流（上游 SSE → 落盘 JSON），客户端按 `taskId` 轮询增量
- 数据落盘：`$QL_DATA_DIR` 下的 JSON 文件（会话/流式任务/配置）

## 🆕 2026-08-16 变更（本地模型 + 修复，接手必读）

- **stream_api**：新增 `provider=local` 分支——直连 Ollama（`http://127.0.0.1:11434/v1/chat/completions`，不经 Hermes 9123，断网可用）；`/api/stream/sync-models` 的 provider key 从 `/data/hermes_config.yaml` 读取（含 deepseek/stepfun/xiaomi/opencode key；文件缺失时同步返回 `ok:false`——已生成）
- **local_api.py**（新，9149）：`/api/local/status|toggle|models|update|delete`（docker exec ollama 封装）
- **docker_api**：镜像 `in_use` 匹配修复（兼容 repo:tag / repo 无 tag / repo@digest / 镜像 ID；原 `endswith(":tag")` 把所有同 tag 镜像误标绿点）
- **Ollama 容器**：`docker run -d --name ollama --restart=always -p 11434:11434 -v /path/to/ollama:/root/.ollama ollama/ollama`；模型 qwen3:4b / qwen2.5:1.5b
- **Hermes 配置**（config.yaml）：providers.ollama 已配（手动可选）
- 部署方式：改文件 → 写入挂载目录 → `systemctl restart qingliao`

## 📄 License

MIT
