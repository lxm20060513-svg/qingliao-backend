#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""轻聊流式执行代理（第 7 个服务，端口 9132）。

架构：前端不持有流式连接。NAS 后端持流向 Hermes 请求，输出写入 NAS 文件，
前端轮询增量读取。iOS 杀前端连接不影响 NAS 上的流——后台期间模型照常输出。

接口：
  POST /api/stream/start   {sessionId, model, messages} -> {taskId}
  GET  /api/stream/{taskId}?offset=N -> {content: 增量, done, status}
  POST /api/stream/{taskId}/stop -> {ok:true}

鉴权：X-Stream-Password: QL_PASSWORD
数据：{DATA_DIR}/streams/{taskId}.json（节流写盘）
上游：http://127.0.0.1:9123/v1/responses（Hermes 容器 docker-proxy）。v3.5.2 起 Agent 路径走
      Responses 协议（自带 final_response 兜底，与微信通道对齐）；STREAM_HERMES_PROTOCOL=chat 可回退。
"""
import base64
import hashlib
import hmac
import json
import os
import kb_inject
import memory_store
import media_convert  # v2.0.130: MEDIA:路径→data URL 图片
import re
try:
    import yaml as _yaml  # V1.5.9 同步模型列表用（读 config.yaml 的 provider key）
except Exception:
    _yaml = None
import subprocess
import threading
import time
import urllib.request
import urllib.error
import urllib.parse
import uuid
import socket
from http.server import BaseHTTPRequestHandler

# v2.0.116 review：流式密码默认置空（只走 X-Auth-Token 鉴权）；需要密码兜底时注入强 STREAM_PASS
# （原硬编码 "123" 弱口令，生产未注入——已核实）
STREAM_PASS = os.environ.get("STREAM_PASS", "")
MAX_CONTENT_LEN = 200_000   # v2.0.116 review：回复内容上限（防无限输出）
MAX_CONTEXT_MESSAGES = int(os.environ.get("STREAM_MAX_CONTEXT_MSGS", 40))  # 上下文消息上限，超出自动截断旧消息
DATA_DIR = os.environ.get("STREAM_DATA_DIR", "/data/streams_data")
STREAM_DIR = os.path.join(DATA_DIR, "streams")
HERMES_URL = os.environ.get("STREAM_HERMES_URL", "http://127.0.0.1:9123/v1/chat/completions")
HERMES_KEY = os.environ.get("STREAM_HERMES_KEY", "")


def _reasoning_options(mode=None):
    """v3.6.4：按次指定思考强度 —— 只影响轻聊发出的请求，不动 Hermes 全局 config.yaml。

    背景：Hermes agent 全局是 reasoning_effort=medium，模型每轮先吐几百字思考再出正文。
    轻聊后端此前**完全丢弃** reasoning（grep 全无处理），等于用户白等：实测同一问题
    （9123 实打 2 轮）medium 正文首字 3.4~3.8s → low 1.6~2.2s → enabled=false 0.9~1.7s。
    Hermes 的 OpenAI 兼容层不发射 reasoning 事件（源码 api_server_openai_routes.py 无相关
    event 构造），拿不到思考内容给 App 显示，故只能调强度。

    QL_REASONING 环境变量：low（默认）/ none|off|disabled（完全禁思考）/ 其它档位透传。
    """
    mode = (mode or os.environ.get("QL_REASONING") or "low").strip().lower()
    if mode in ("none", "off", "disabled", "no"):
        return {"reasoning": {"enabled": False}}
    return {"reasoning": {"enabled": True, "effort": mode}}
# v3.5.2：Agent 路径改走 Hermes `/v1/responses`（官方客户端协议）。根因：chat/completions 的 SSE
# writer 只转发 agent 循环里的 delta，非流式阶段产生的 final_response（典型：agent.max_turns 耗尽的
# 收尾总结）落不到那条流；responses 出口的 collect_result 会在「本轮无 delta」时补发 final_response，
# 于是轻聊与微信通道的观感对齐（微信侧能看到「先回一版总结」，轻聊以前是空回复）。
# 回退：STREAM_HERMES_PROTOCOL=chat（容器 env）。
HERMES_RESPONSES_URL = os.environ.get(
    "STREAM_HERMES_RESPONSES_URL",
    (HERMES_URL[:-len("/v1/chat/completions")] + "/v1/responses")
    if HERMES_URL.endswith("/v1/chat/completions")
    else HERMES_URL.rsplit("/", 1)[0] + "/v1/responses")
HERMES_PROTOCOL = (os.environ.get("STREAM_HERMES_PROTOCOL", "responses") or "responses").strip().lower()
WRITE_INTERVAL = 0.3  # 写盘节流
TASK_TTL = 1800       # 任务完成后内存保留 30 分钟
# V1.4 微信接力推送：回复完成 → webhook deliver-only → 微信
WEBHOOK_URL = os.environ.get("STREAM_WEBHOOK_URL", "http://172.21.0.2:8644/webhooks/qingliao-push")
WEBHOOK_SECRET = os.environ.get("STREAM_WEBHOOK_SECRET", "")
PUSH_IDLE_SECONDS = 30  # 用户超过 30s 未轮询才推送（在看的用户不打扰）

# ==== v3.7.1 进度推送（长任务中途把「已生成内容」推到收件箱 + 任务中心）====
# 背景：v3.7.0 按用户要求下线了对话正文里的进度行（💭/🔧），于是长任务里模型一旦进入
# 工具期/思考期就完全静默——用户看不到"跑到哪了"。改法是把进度挪出对话正文：
#   ① 任务中心「进行中」卡片（_stream_progress_detail，App 每 2s 轮询）
#   ② 收件箱 task_type="progress" 推送（App 注入 🔔 进度气泡 + 本地通知）
# 触发条件（全部满足才推，防刷屏）：内容静默满 PROGRESS_SILENCE_SEC、自上次推送有新增、
# 距上次推送满 PROGRESS_MIN_GAP_SEC、且用户不在看（lastPollAt 落后 ≥ PUSH_IDLE_SECONDS）。
PROGRESS_SILENCE_SEC = 30     # 内容静默满多少秒才推一条
PROGRESS_MIN_GAP_SEC = 30     # 两条进度推送的最小间隔
PROGRESS_MIN_CHARS = 60       # 内容太短（一句话就完事）不值得推进度
PROGRESS_TAIL_CHARS = 120     # 进度气泡文案里附带的正文尾部长度
PROGRESS_DETAIL_TAIL = 40     # 任务中心卡片那行的尾部长度（卡片窄，太长会撑坏布局）
PROGRESS_MAX_PUSHES = 20      # 单任务进度推送上限（长任务防刷屏的兜底）


def _progress_tick(st, ps, now=None):
    """一次进度判定（纯逻辑，无 IO，便于单测）。命中返回要推送的文案，未命中返回 None。

    ps 是调用方持有的进度状态（dict，线程内可变）：
      last_len  上次判定时见过的内容长度（用于识别"是否有新增"）
      last_grow 最后一次观察到内容增长的时刻（静默计时锚点）
      last_push 上次进度推送时刻
      last_push_len 上次进度推送时的内容长度（保证"自上次推送以来有新增"）
      pushes    本任务已推进度条数
    """
    now = now or time.time()
    content = st.get("content") or ""
    cur = len(content)
    if cur > (ps.get("last_len") or 0):
        # 有新增 → 只能算"还在吐字"，重置静默锚点，不推
        ps["last_len"] = cur
        ps["last_grow"] = now
        return None
    if cur < PROGRESS_MIN_CHARS:
        return None
    if cur <= (ps.get("last_push_len") or 0):
        # 自上次进度推送以来没有新增 → 不重复推（任务中心卡片仍会显示"静默 N 分"，不靠刷屏报平安）
        return None
    if (ps.get("pushes") or 0) >= PROGRESS_MAX_PUSHES:
        return None
    if now - (ps.get("last_grow") or now) < PROGRESS_SILENCE_SEC:
        return None
    if now - (ps.get("last_push") or 0) < PROGRESS_MIN_GAP_SEC:
        return None
    # 用户在看着（30s 内有轮询）→ 不打扰，他自己能看到流式
    if now - (st.get("lastPollAt") or 0) < PUSH_IDLE_SECONDS:
        return None
    tail = re.sub(r"\s+", " ", content[-PROGRESS_TAIL_CHARS:]).strip()
    _tb = _tool_brief(st, now)          # v3.9.15：在干什么（不给用户看工具名时为空）
    _lead = "⏳ AI 正在回复（已生成 %d 字" % cur
    if _tb:
        _lead += "，" + _tb
    text = _lead + "）\n\n…%s" % tail
    ps["last_push"] = now
    ps["last_len"] = cur
    ps["last_push_len"] = cur
    ps["pushes"] = (ps.get("pushes") or 0) + 1
    return text


def _tool_brief(st, now=None):
    """v3.9.15：把「当前/最近一次工具调用」翻成一句中文简述，供任务中心卡片与进度推送使用。

    长任务里用户唯一能看到的进度就是任务中心「进行中」卡片（App 每 2s 轮询），
    而 v3.7.0 下线正文进度行后卡片只剩「已生成 N 字 · 静默 X 秒」，看不出在干什么。
    这里不改 content（offset 增量协议：已送达字节不可撤回），只把工具名接到卡片文案上。
    返回空串表示"本任务还没跑过工具"。
    """
    name = str(st.get("lastTool") or "")
    if not name:
        return ""
    zh = _TOOL_NAME_ZH.get(name, name)
    seq = int(st.get("toolSeq") or 0)
    if seq > 1:
        return "第 %d 步 %s" % (seq, zh)
    return zh


def _stream_progress_detail(st, now=None):
    """v3.7.1：任务中心「进行中」卡片的一行进度文案（App 每 2s 拉 /api/tasks/active）。

    原为「N 字回复中 / 思考中」——长任务里看不出跑到哪了。现补静默时长与最近生成片段。
    `st["updatedAt"]` 由 _write_state 在每次内容追加时刷新 → 它比 now 落后多少秒即"静默多久"。
    v3.9.15：再前置「第 N 步 工具名」——用户能直接看出卡在哪一类操作上（卡片窄，故压缩字数）。
    """
    now = now or time.time()
    content = st.get("content") or ""
    tool_txt = _tool_brief(st, now)
    if not content:
        return ("工具：%s" % tool_txt) if tool_txt else "思考中"
    silent = max(0, int(now - (st.get("updatedAt") or now)))
    silent_txt = "%d 秒" % silent if silent < 60 else "%d 分" % (silent // 60)
    tail = re.sub(r"\s+", " ", content[-PROGRESS_DETAIL_TAIL:]).strip()
    if tool_txt:
        return "%s · %d 字 · 静默 %s · 最近：%s" % (tool_txt, len(content), silent_txt, tail)
    return "已生成 %d 字 · 静默 %s · 最近：%s" % (len(content), silent_txt, tail)

# v3.7.1-PROGRESS-PUSH-PATCH-APPLIED

# v3.6.1 工具进度行：Hermes responses 流里 function_call item → 追加「🔧 xx…」进度行
_TOOL_NAME_ZH = {
    "terminal": "执行命令", "web_search": "搜索网页", "web_extract": "读取网页",
    "read_file": "读取文件", "write_file": "写入文件", "patch": "修改文件",
    "edit": "修改文件", "delegate_task": "派发子任务", "execute_code": "运行代码",
    "skill_view": "加载技能", "search_files": "搜索文件", "vision_analyze": "识别图片",
    "memory": "存取记忆", "cronjob": "定时任务", "clarify": "向用户确认",
}


def _tool_progress_line(name):
    # v3.7.0：已下线（stream 不再注入工具进度行）；保留定义以防历史引用，勿重新接入流式输出。
    return "🔧 %s…" % _TOOL_NAME_ZH.get(str(name or ""), str(name or "处理中"))


# v3.6.1 进度类追问识别：命中且同会话有 running 任务 → 秒回进度摘要，不再开新 Hermes 请求
_PROGRESS_WORDS = ("进度", "怎么样了", "好了吗", "好了么", "完成了吗", "完成了么",
                   "多久", "跑到哪", "卡了", "还在吗", "还在跑", "进度如何",
                   "处理完了吗", "处理完了么", "干完了吗", "啥时候")


def _is_progress_question(text):
    t = str(text or "").strip()
    return bool(t) and len(t) <= 40 and any(w in t for w in _PROGRESS_WORDS) \
        and not any(w in t for w in ("天气", "股价", "股票"))   # 排除同名普通查询


def _find_running_task_for_session(session_id, exclude_task_id):
    """v3.6.1：找同 sessionId 下仍在 streaming 的任务（排除本次追问自己）。
    返回 (task_id, task) 或 (None, None)。"""
    with _tasks_lock:
        for tid, t in list(_tasks.items()):
            if tid == exclude_task_id:
                continue
            try:
                if t.get("cancelled"):
                    continue
                s = t.get("state") or {}
                if s.get("sessionId") == session_id and s.get("status") == "streaming":
                    return tid, t
            except Exception:
                continue
    return None, None

# 模块级初始化（qingliao_all.py 用 importlib 加载，__main__ 块不会执行，
# 目录创建与清理线程必须放在模块顶层）
os.makedirs(STREAM_DIR, exist_ok=True)
_cleanup_started = False

# V1.5.9：各 provider 的模型列表端点 + config key 路径
SYNC_ENDPOINTS = {
    "opencode": ("https://opencode.ai/zen/go/v1/models", ["providers", "opencode", "api_key"]),
    "opencode-apple": ("https://opencode.ai/zen/go/v1/models", ["providers", "opencode-apple", "api_key"]),
    "stepfun": ("https://api.stepfun.com/step_plan/v1/models", ["providers", "stepfun", "api_key"]),
    "deepseek": ("https://api.deepseek.com/v1/models", ["providers", "deepseek", "api_key"]),
    "xiaomi": ("https://token-plan-cn.xiaomimimo.com/v1/models", ["providers", "xiaomi", "api_key"]),
    "sensenova": ("https://token.sensenova.cn/v1/models", ["providers", "sensenova", "api_key"]),
    "zai-coding": ("https://api.z.ai/api/coding/paas/v4/models", ["providers", "zai-coding", "api_key"]),
}

def _load_cfg_key(keypath):
    if _yaml is None:
        return ""
    # Hermes 配置路径（QL_HERMES_CONFIG 环境变量指定）
    for path in (os.environ.get("QL_HERMES_CONFIG", "/data/hermes_config.yaml"),):
        try:
            with open(path, encoding="utf-8") as f:
                cfg = _yaml.safe_load(f)
            cur = cfg
            for k in keypath:
                cur = cur.get(k) if isinstance(cur, dict) else None
            if isinstance(cur, str) and cur:
                return cur
        except Exception:
            continue
    return ""


def _provider_base_url(pid):
    """读 providers.<pid>.base_url（config.yaml）；读不到返回 None"""
    if _yaml is None:
        return None
    try:
        with open(os.environ.get("QL_HERMES_CONFIG", "/data/hermes_config.yaml"), encoding="utf-8") as f:
            cfg = _yaml.safe_load(f)
        p = (cfg.get("providers") or {}).get(pid)
        if isinstance(p, dict) and p.get("base_url"):
            return str(p["base_url"]).rstrip("/")
    except Exception:
        pass
    return None

# V1.7.2：NAS 面板——收集宿主系统与服务状态
def _collect_nas_status():
    def rd(p):
        try:
            with open(p) as f:
                return f.read()
        except Exception:
            return ""
    out = {"ok": True, "ts": int(time.time())}
    # 主机/运行时间
    out["hostname"] = socket.gethostname()
    try:
        up = float(rd("/proc/uptime").split()[0])
        d, h, m = int(up // 86400), int(up % 86400 // 3600), int(up % 3600 // 60)
        out["uptime"] = "%d天%d小时%d分" % (d, h, m)
    except Exception:
        out["uptime"] = "?"
    # CPU：两次采样（0.4s）算使用率
    def _cpu():
        s = rd("/proc/stat")
        for line in s.splitlines():
            if line.startswith("cpu "):
                v = [int(x) for x in line.split()[1:]]
                idle = v[3] + v[4]
                return sum(v), idle
        return 0, 0
    try:
        t1, i1 = _cpu()
        time.sleep(0.4)
        t2, i2 = _cpu()
        total = max(t2 - t1, 1)
        out["cpu"] = round(100 * (1 - (i2 - i1) / total), 1)
    except Exception:
        out["cpu"] = 0
    # 内存
    try:
        m = {}
        for line in rd("/proc/meminfo").splitlines():
            k, _, v = line.partition(":")
            m[k] = int(v.strip().split()[0])  # kB
        mem_total = m.get("MemTotal", 0)
        mem_avail = m.get("MemAvailable", m.get("MemFree", 0))
        out["mem"] = {
            "total": mem_total * 1024,
            "used": max(mem_total - mem_avail, 0) * 1024,
            "avail": mem_avail * 1024,
        }
    except Exception:
        out["mem"] = {"total": 0, "used": 0, "avail": 0}
    # 磁盘
    # v3.0.36：优先宿主视角——容器挂载 /:/host_root:ro 后，df 指定宿主挂载点路径可穿透显示系统盘
    try:
        import subprocess
        if os.path.isdir("/host_root"):
            # 宿主候选挂载点（系统盘 + 数据盘；/tmp 相对次要）
            cands = ["/boot", "/rootfs", "/ugreen", "/mnt/factory", "/overlay",
                     "/data", "/volume2", "/volume3"]
            df = subprocess.run(["df", "-B1"] + ["/host_root" + c for c in cands],
                                capture_output=True, text=True, timeout=10)
        else:
            df = subprocess.run(["df", "-B1"], capture_output=True, text=True, timeout=8)
        disks = []
        for line in df.stdout.splitlines()[1:]:
            parts = line.split()
            if len(parts) >= 6:
                mnt = parts[5]
                fs = parts[0]
                # v3.0.36：/host_root 前缀剥掉还原真实挂载点
                if mnt.startswith("/host_root") and mnt != "/host_root":
                    mnt = mnt[len("/host_root"):]
                # 过滤：tmpfs/udev/overlay/squashfs 伪设备 + 非根容器的重复挂载（只留物理分区与主要挂载点）
                if fs.startswith(("tmpfs", "udev", "overlay", "squashfs", "shm", "devtmpfs", "/dev/loop")):
                    continue
                if mnt.startswith(("/var/lib/docker", "/proc", "/sys", "/dev", "/run", "/etc/resolv", "/etc/hostname", "/etc/hosts", "/mnt/@remote")):
                    continue
                # v3.0.36：标注磁盘类型（system=系统盘 eMMC / data=数据卷）
                kind = "system" if fs.startswith(("/dev/mmcblk", "/dev/sd", "/dev/nvme")) and not mnt.startswith("/volume") else "data"
                disks.append({"fs": fs, "total": int(parts[1]), "used": int(parts[2]), "avail": int(parts[3]), "pct": parts[4].rstrip("%"), "mnt": mnt, "kind": kind})
        out["disks"] = disks
    except Exception:
        out["disks"] = []
    # 服务状态 + 内存（V1.7.4）
    def _proc_rss(pid_str):
        try:
            pid = int(pid_str)
            with open("/proc/%d/status" % pid) as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) * 1024  # kB -> bytes
        except Exception:
            return None
        return None
    services = {}
    # v3.4.10：轻聊后端=docker 容器 qingliao（qingliao_all.py，宿主 bind mount）。
    # 容器内无 systemd，systemctl is-active 恒失败 → qingliao 误判离线/qingliao_mem None；
    # 状态改 docker inspect Running；内存改 docker stats 实际占用（App 看板语义）。
    def _parse_mem_bytes(s):
        """'17.21MiB'/'1.2GiB'/'512KiB' → bytes（docker stats 单位，1024 进制）"""
        try:
            import re as _re
            m = _re.match(r"\s*([\d.]+)\s*([KMG]?i?B|B)?\s*", s or "")
            if not m:
                return None
            val = float(m.group(1))
            unit = (m.group(2) or "B").lower()
            mult = {"b": 1, "kb": 1024, "kib": 1024, "mb": 1024 ** 2, "mib": 1024 ** 2,
                    "gb": 1024 ** 3, "gib": 1024 ** 3, "tb": 1024 ** 4, "tib": 1024 ** 4}
            return int(val * mult.get(unit, 1))
        except Exception:
            return None
    try:
        import subprocess
        insp = subprocess.run(["docker", "inspect", "--format", "{{.State.Running}}", "qingliao"],
                              capture_output=True, text=True, timeout=10)
        services["qingliao"] = (insp.stdout or "").strip().lower() == "true"
        try:
            m = subprocess.run(["docker", "stats", "qingliao", "--no-stream", "--format", "{{.MemUsage}}"],
                               capture_output=True, text=True, timeout=15)
            mem_bytes = _parse_mem_bytes((m.stdout or "").split("/")[0])
        except Exception:
            mem_bytes = None
        # 两字段同值：qingliao_mem=旧 App 直接读；qingliao_docker_mem=新 App 明确 Docker 语义
        services["qingliao_mem"] = mem_bytes
        services["qingliao_docker_mem"] = mem_bytes
    except Exception:
        services["qingliao"] = None
        services["qingliao_mem"] = None
        services["qingliao_docker_mem"] = None
    try:
        # v2.0.102c：读 STREAM_HERMES_KEY（qingliao.service 注入；原 HERMES_KEY 为空 → 401）+
        #           健康检查打 /health（原打 /v1/chat/completions 是 POST 端点，GET 恒 405 → 永远误判离线）
        hkey = os.environ.get("STREAM_HERMES_KEY", "")
        health_url = os.environ.get("STREAM_HERMES_HEALTH_URL", "http://127.0.0.1:9123/health")
        r = subprocess.run(["curl", "-s", "-m", "3", "-o", "/dev/null", "-w", "%{http_code}",
                            "-H", "Authorization: Bearer " + hkey, health_url], capture_output=True, text=True, timeout=8)
        services["hermes"] = r.stdout.strip() == "200"
        # v3.0.36 fix：容器极简镜像无 pgrep/ps（FileNotFoundError 曾致整个 try 异常 → hermes 恒 null）
        #           内存改 docker exec hermes 容器内 ps -eo rss（Hermes 容器是完整镜像，ps 可用）
        try:
            p = subprocess.run(["docker", "exec", os.environ.get("QL_HERMES_CONTAINER", "hermes-container"), "ps", "-eo", "pid,rss,comm"],
                               capture_output=True, text=True, timeout=10)
            rss = 0
            for ln in (p.stdout or "").splitlines()[1:]:
                parts = ln.split()
                if len(parts) >= 3 and ("hermes" in parts[2].lower() or "node" in parts[2].lower()):
                    try:
                        rss += int(parts[1])
                    except ValueError:
                        pass
            services["hermes_mem"] = rss * 1024 if rss else None
        except Exception:
            services["hermes_mem"] = None
        # v3.0.8：Hermes 容器版本（docker exec hermes --version 首行，如 "Hermes Agent v0.20.4 ..."）
        try:
            v = subprocess.run(["docker", "exec", os.environ.get("QL_HERMES_CONTAINER", "hermes-container"), "hermes", "--version"],
                               capture_output=True, text=True, timeout=10)
            first = (v.stdout or "").strip().splitlines()[0] if v.stdout else ""
            services["hermes_version"] = first if first else None
        except Exception:
            services["hermes_version"] = None
    except Exception:
        services["hermes"] = None
        services["hermes_mem"] = None
        services["hermes_version"] = None
    out["services"] = services
    return out

def _collect_diagnose():
    """v3.0.18：设备一键体检——聚合 服务/磁盘/Docker/负载/内存/温度 六维诊断。
    每项返回 {id,name,status:ok|warn|error,detail,advice}；advice 为内置规则建议（不调 LLM）。
    v3.0.28 review：复用 _collect_nas_status() 结果（服务+磁盘+内存），避免重复读 /proc。"""
    import subprocess
    items = []

    def add(iid, name, status, detail, advice):
        items.append({"id": iid, "name": name, "status": status,
                      "detail": detail, "advice": advice})

    # 复用 nas/status（含 qingliao.service 状态、hermes 状态、磁盘、内存）
    try:
        _status = _collect_nas_status()
    except Exception:
        _status = {}

    # 1) 服务健康（复用 _collect_nas_status 结果）
    ql_up = (_status.get("services") or {}).get("qingliao")
    if ql_up is None:
        add("svc_ql", "轻聊后端服务", "warn", "无法检测", "检查 systemctl 可用性")
    else:
        add("svc_ql", "轻聊后端服务", "ok" if ql_up else "error",
            "qingliao.service 运行中" if ql_up else "qingliao.service 未运行",
            "重启服务：systemctl restart qingliao.service")
    h_up = (_status.get("services") or {}).get("hermes")
    if h_up is None:
        add("svc_hermes", "Hermes Agent", "warn", "无法检测", "检查 9123 端口")
    else:
        add("svc_hermes", "Hermes Agent", "ok" if h_up else "error",
            "9123 健康检查通过" if h_up else "9123 健康检查失败",
            "Hermes 容器异常，等看门狗自动重启，或 docker restart hermes-container")

    # v3.4.x：轻聊后端端口自检——9127 统一路由 / 9132 流式直连（App 长连接独立端口）
    # 容器内对本机端口 TCP 连通探测：通=监听中；拒=服务挂；超时=异常
    import socket as _sock
    for _pid, _pname, _pport, _proute in [
        ("port_9127", "轻聊端口 9127（统一路由）", 9127, "Web/App 全部 /api/* 请求入口"),
        ("port_9132", "轻聊端口 9132（流式）", 9132, "App 流式长连接 /r/stream/* 入口"),
    ]:
        try:
            _s = _sock.create_connection(("127.0.0.1", _pport), timeout=3)
            _s.close()
            add(_pid, _pname, "ok", "监听中 · " + _proute, "")
        except ConnectionRefusedError:
            add(_pid, _pname, "error", "连接被拒（未监听）",
                "qingliao 容器内 %d 服务未启动，查 docker logs qingliao" % _pport)
        except OSError as _e:
            add(_pid, _pname, "warn", "探测异常：%s" % str(_e)[:40],
                "确认 qingliao 容器网络状态")

    # 2) 磁盘（复用 nas/status 采集，阈值 80/90）
    try:
        disks = (_status.get("disks") or [])
        for d in disks:
            pct = d.get("pct")
            try:
                p = int(pct)
            except Exception:
                continue
            mnt = d.get("mnt", "?")
            if p >= 90:
                add("disk_" + mnt.replace("/", "_"), "磁盘 " + mnt, "error",
                    "已用 %d%%（剩 %s）" % (p, d.get("avail", "?")),
                    "占用超90%：docker system prune、清理旧日志/大镜像")
            elif p >= 80:
                add("disk_" + mnt.replace("/", "_"), "磁盘 " + mnt, "warn",
                    "已用 %d%%（剩 %s）" % (p, d.get("avail", "?")),
                    "接近满载，建议清理不用的 Docker 镜像和日志")
            else:
                add("disk_" + mnt.replace("/", "_"), "磁盘 " + mnt, "ok",
                    "已用 %d%%" % p, "")
    except Exception:
        add("disk", "磁盘", "warn", "检测失败", "无法读取磁盘状态")

    # 3) Docker 容器异常（Restarting/unhealthy/Exited）
    try:
        import docker_api
        containers = docker_api._ps()
        abnormal = 0
        for c in containers:
            st = c.get("status", "")
            name = c.get("name", "?")
            if "Restarting" in st:
                abnormal += 1
                add("dk_" + name, "容器 " + name, "error", st,
                    "重启循环：docker logs " + name + " 看原因")
            elif "unhealthy" in st:
                abnormal += 1
                add("dk_" + name, "容器 " + name, "error", st,
                    "健康检查失败：docker start/restart " + name)
            elif st.startswith("Exited"):
                abnormal += 1
                add("dk_" + name, "容器 " + name, "warn", st,
                    "已停止：docker start " + name + "（一次性任务容器可忽略）")
        if abnormal == 0:
            add("docker_all", "Docker 容器", "ok", "%d 个容器全部正常" % len(containers), "")
    except Exception as e:
        add("docker_all", "Docker 容器", "warn", "检测失败: %s" % str(e)[:60], "检查 docker 服务")

    # 4) 系统负载（1min vs 核数×0.75 阈值）
    try:
        with open("/proc/loadavg") as f:
            l1 = float(f.read().split()[0])
        nproc = os.cpu_count() or 4
        if l1 > nproc * 1.0:
            add("load", "系统负载", "error", "负载 %.2f（%d 核）" % (l1, nproc),
                "负载过高：top 看占用进程，必要时停用占资源的容器")
        elif l1 > nproc * 0.5:
            add("load", "系统负载", "warn", "负载 %.2f（%d 核）" % (l1, nproc),
                "负载偏高，可关注是否有后台任务在跑")
        else:
            add("load", "系统负载", "ok", "负载 %.2f（%d 核）" % (l1, nproc), "")
    except Exception:
        add("load", "系统负载", "warn", "检测失败", "")

    # 5) 内存（复用 nas/status 采集；可用 <15% error / <25% warn）
    try:
        mem = (_status.get("mem") or {})
        total = mem.get("total", 0)
        avail = mem.get("avail", 0)
        pct = (avail / total * 100) if total else 0
        if pct < 15:
            add("mem", "内存", "error", "可用仅 %.0f%%（%dG/%dG）" % (pct, avail // (1 << 30), total // (1 << 30)),
                "内存不足：看板 Docker 页停用非必要容器")
        elif pct < 25:
            add("mem", "内存", "warn", "可用 %.0f%%（%dG/%dG）" % (pct, avail // (1 << 30), total // (1 << 30)),
                "内存偏紧，留意后台任务")
        else:
            add("mem", "内存", "ok", "可用 %.0f%%（%dG/%dG）" % (pct, avail // (1 << 30), total // (1 << 30)), "")
    except Exception:
        add("mem", "内存", "warn", "检测失败", "")

    # 6) 温度（CPU>80 error / >70 warn；SSD>65 warn）
    try:
        import hw_api
        cpu = hw_api._hw_status().get("cpu_temp")
        ssd = hw_api._hw_status().get("ssd_temp")
        if cpu is not None:
            if cpu > 80:
                add("temp_cpu", "CPU 温度", "error", "%.1f°C" % cpu, "过热：检查散热/风扇/环境温度")
            elif cpu > 70:
                add("temp_cpu", "CPU 温度", "warn", "%.1f°C" % cpu, "温度偏高，留意负载")
            else:
                add("temp_cpu", "CPU 温度", "ok", "%.1f°C" % cpu, "")
        if ssd is not None:
            add("temp_ssd", "SSD 温度", "warn" if ssd > 65 else "ok", "%.1f°C" % ssd,
                "SSD 过热：检查散热" if ssd > 65 else "")
    except Exception:
        add("temp", "温度", "warn", "检测失败", "")

    n_ok = sum(1 for i in items if i["status"] == "ok")
    n_warn = sum(1 for i in items if i["status"] == "warn")
    n_err = sum(1 for i in items if i["status"] == "error")
    level = "error" if n_err else ("warn" if n_warn else "ok")
    return {"items": items, "level": level,
            "summary": "%d 项正常 · %d 项提醒 · %d 项异常" % (n_ok, n_warn, n_err)}


def _start_cleanup():
    global _cleanup_started
    if _cleanup_started:
        return
    _cleanup_started = True
    threading.Thread(target=cleanup_old_tasks, daemon=True).start()

_tasks = {}           # taskId -> {"state": {...}, "cancelled": bool, "lock": Lock}
_tasks_lock = threading.Lock()
_ql_stream_task = {}           # sessionId -> taskId (ingest 帧增量路由)
_ql_stream_lock = threading.Lock()

# ---- v3.4.23 任务中心后台任务登记 ----
# background_jobs: jobId -> {"jobId","title","detail","status":running|done|error,
#                            "createdAt","updatedAt","result"}
# ①流式任务（App 发出的请求，streaming 即 AI 干活中）直接从 _tasks 收集；
# ②其他后台作业（webhook/cron 触发、不经 App 流式）由 _bgjob_register 登记。
# v3.4.25：QL_INBOX_TOKEN 环境变量在模块加载时读取（/api/tasks/bg 端点鉴权用）
INBOX_TOKEN_ENV = os.environ.get("QL_INBOX_TOKEN", "ql-inbox-default")
_background_jobs = {}
_background_jobs_lock = threading.Lock()
_BGJOB_TTL = 2 * 3600        # 完成作业保留 2h 供查看
_BGJOB_KEEP = 50             # 完成作业最多保留条数，防无限膨胀


def _bgjob_register(job_id, title, detail=""):
    """登记/刷新一条后台作业（已存在则更新标题，状态重置 running）。"""
    with _background_jobs_lock:
        old = _background_jobs.get(job_id)
        _background_jobs[job_id] = {
            "jobId": job_id, "title": title, "detail": detail,
            "status": "running",
            "createdAt": old.get("createdAt", time.time()) if old else time.time(),
            "updatedAt": time.time(),
        }
        done_jobs = [(k, v.get("updatedAt", 0)) for k, v in _background_jobs.items()
                     if v.get("status") != "running"]
        if len(done_jobs) > _BGJOB_KEEP:
            for k, _ in sorted(done_jobs, key=lambda kv: kv[1])[:len(done_jobs) - _BGJOB_KEEP]:
                _background_jobs.pop(k, None)


def _bgjob_finish(job_id, ok=True, result=""):
    with _background_jobs_lock:
        job = _background_jobs.get(job_id)
        if not job:
            return
        job["status"] = "done" if ok else "error"
        job["updatedAt"] = time.time()
        if result:
            job["result"] = str(result)[:2000]


def bg_register(title, job_id=None, detail=""):
    """v3.4.25：供 /api/tasks/bg 内部端点调用的登记入口（Hermes 派子代理/后台作业时上报进度）。
    job_id 不传则按标题+时间生成。返回 jobId 供后续更新状态。"""
    jid = job_id or ("bg-" + uuid.uuid4().hex[:10])
    _bgjob_register(jid, title, detail)
    return jid


def bg_update(job_id, status=None, detail=None, result=None):
    """v3.4.25：更新后台作业状态/详情。running→done/error 或刷新 detail。"""
    with _background_jobs_lock:
        job = _background_jobs.get(job_id)
        if not job:
            return False
        if detail is not None:
            job["detail"] = str(detail)[:200]
        if result is not None:
            job["result"] = str(result)[:2000]
        if status in ("done", "error"):
            job["status"] = status
        job["updatedAt"] = time.time()
        return True


def _collect_active_tasks():
    """任务中心数据源：进行中的流式任务 + 登记的后台作业。"""
    now = time.time()
    tasks = []
    with _tasks_lock:
        for tid, t in _tasks.items():
            st = t["state"]
            if st.get("status") == "streaming":
                last_user = ""
                for m in reversed(st.get("messages") or []):
                    if isinstance(m, dict) and m.get("role") == "user":
                        c = m.get("content", "")
                        if isinstance(c, list):
                            last_user = " ".join(str(b.get("text", "")) for b in c if isinstance(b, dict))
                        else:
                            last_user = str(c or "")
                        break
                tasks.append({
                    "jobId": tid,
                    "kind": "stream",
                    "title": (last_user.strip()[:80] or "正在处理"),
                    "detail": _stream_progress_detail(st, now),   # v3.7.1：字数 + 静默时长 + 最近片段
                    "status": "running",
                    "createdAt": st.get("createdAt", now),
                    "updatedAt": st.get("updatedAt", now),
                })
    with _background_jobs_lock:
        for j in _background_jobs.values():
            # 防呆: running 作业超过 TTL(2h) 视为收尾上报丢失(挂死), 任务中心不再显示
            if j.get("status") == "running" and now - j.get("createdAt", now) > _BGJOB_TTL:
                continue
            if j.get("status") == "running" or now - j.get("updatedAt", 0) < _BGJOB_TTL:
                tasks.append({
                    "jobId": j.get("jobId", ""), "kind": "bg",
                    "title": j.get("title", ""), "detail": j.get("detail", ""),
                    "status": j.get("status", "running"),
                    "result": j.get("result", ""),
                    "createdAt": j.get("createdAt", now),
                    "updatedAt": j.get("updatedAt", now),
                })
    tasks.sort(key=lambda x: x.get("createdAt", 0), reverse=True)
    return {"ok": True, "tasks": tasks,
            "running": sum(1 for t in tasks if t.get("status") == "running")}


def _auth(h):
    # v2.0.116 review：统一走 auth_api.check_auth（含 token 校验 + 可选密码兜底），
    # 去掉手动密码二次校验（ALLOW_PW_FALLBACK 为 false 时两者都走 token）。
    import auth_api
    return auth_api.check_auth(h.headers, "X-Stream-Password", STREAM_PASS)


def _write_state(task_id, task):
    st = task["state"]
    st["updatedAt"] = time.time()
    with task["lock"]:
        tmp = os.path.join(STREAM_DIR, task_id + ".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(st, f, ensure_ascii=False)
            os.replace(tmp, os.path.join(STREAM_DIR, task_id + ".json"))
        except Exception:
            pass


def _maybe_push(st):
    """V1.4 微信接力推送：pushEnabled && done && 有内容 && 用户≥30s 未轮询。
    经 Hermes webhook deliver-only 直发微信（零 LLM 成本）。"""
    try:
        if not st.get("pushEnabled"):
            return
        if st.get("status") != "done":
            return
        content = (st.get("content") or "").strip()
        if len(content) < 4:
            return
        last_poll = st.get("lastPollAt") or 0
        if time.time() - last_poll < PUSH_IDLE_SECONDS:
            return  # 用户仍在轮询（在看），不打扰
        brief = re.sub(r"\s+", " ", content)
        if len(brief) > 150:
            brief = brief[:150] + "…"
        msg = "💬 轻聊：你的问题已回复完成\n\n" + brief + "\n\n— 打开轻聊查看全文"
        body = json.dumps({"msg": msg, "event_type": "reply_done"}, ensure_ascii=False).encode("utf-8")
        ts = str(int(time.time()))
        signed = ts.encode() + b"." + body
        sig = hmac.new(WEBHOOK_SECRET.encode(), signed, hashlib.sha256).hexdigest()
        req = urllib.request.Request(WEBHOOK_URL, data=body, headers={
            "Content-Type": "application/json",
            "X-Webhook-Signature-V2": sig,
            "X-Webhook-Timestamp": ts
        })
        resp = urllib.request.urlopen(req, timeout=10)
        print("[push] 微信推送完成 HTTP", resp.status, flush=True)
    except Exception as e:
        print("[push] 微信推送失败:", str(e)[:200], flush=True)


def _last_user_text(st):
    """v3.6.1：取最后一条 user 消息纯文本（静默指令识别用）"""
    try:
        for _m in reversed(st.get("messages") or []):
            if isinstance(_m, dict) and _m.get("role") == "user":
                _c = _m.get("content") or ""
                if isinstance(_c, list):
                    _c = " ".join(str(b.get("text", "")) for b in _c if isinstance(b, dict))
                return str(_c)
    except Exception:
        pass
    return ""


def _maybe_push_app(st, task_id=None):
    """v3.0.83：AI 回复完成 → 推送到轻聊 App 收件箱（inbox）。
    v3.1.13 恢复：v3.1.x 防复读迭代重写 _worker 时丢失全部挂载，导致 App 后台收不到
    回复完成推送（2026-09-03 审核发现，对比 bak-v310 有 8 处挂载、线上 0 处）。

    与 _maybe_push（推微信，受"用户是否在看"/pushEnabled 门控）不同，
    本函数**只要求回复完成且有实质内容就推**——用户要求"AI回复每一条都推"
    无论用户是否正盯着 App 看。App 端 InboxStore 每 5s 轮询 /api/inbox 拉取，
    拉到后注入当前聊天会话 + 本地通知 + 标已读。

    服务内直调 inbox_api.push()（无鉴权，与 App 拉取签名无关）。
    v3.4.x：完成同时写入同问题短窗口幂等缓存，下次重复提问可直接复用。
    v3.4.23 去重闸门：App 最近轮询过（30s）= 回复已随流式送达 → 不推（防流式气泡+推送双份）。
    v3.6.1 根治「单次判定即永久放弃」：旧逻辑在 done 瞬间一次性决策，但那一刻 App 的
    lastPollAt 往往还是流式期的（用户已退出流式页/输入中样式已退出）→ 跳过推送后永不再试，
    消息就丢了。现改为延迟 6s 复检，按「内容是否真被 App 取走」（deliveredLen）判定。
    """
    try:
        if not st or st.get("status") != "done":
            return
        content = (st.get("content") or "").strip()
        if not content:
            return
        # v3.4.30：App「新建会话加号」= 静默重置——只投一条 /new 触发 gateway 重置上下文，
        # 不接流式也不轮询。于是下面的 lastPollAt 闸门会把它误判成"用户不在看"而推收件箱，
        # 用户就会在收件箱/新会话里看到凭空冒出来的 AI 回复。此类内部指令的回复不入队。
        try:
            if _last_user_text(st).strip() == "/new":
                print("[push] App收件箱跳过：静默重置指令 /new", flush=True)
                return
        except Exception:
            pass
        # v3.6.1 根治「干完活没消息」：旧闸门只看「30s内轮询过」就跳过推送，但用户可能已退出
        # 流式页（前台保活轮询仍在刷 lastPollAt），最终回复从未被 App 取走 → 既不显示也不推送。
        # 改为看「内容是否真送达」：done 时刻记录最终长度，App 轮询取到的 offset 达到该长度才算送达。
        final_len = len(content)
        last_poll = st.get("lastPollAt") or 0
        delivered = st.get("deliveredLen")
        if delivered is None:
            # 兜底：拿不到送达进度时沿用旧行为（30s 内轮询过=已送达）
            if time.time() - last_poll < PUSH_IDLE_SECONDS:
                print("[push] App收件箱跳过：用户刚轮询过（回复已流式送达）", flush=True)
                return
        elif delivered < final_len:
            print(f"[push] App收件箱补推：内容未送达（已取{delivered}/{final_len}字，完成{int(time.time()-last_poll)}s后）", flush=True)
        else:
            print("[push] App收件箱跳过：内容已完整送达", flush=True)
            return
        import inbox_api
        ok, msg = inbox_api.push(content, task_id=task_id, task_type="reply")
        print(f"[push] App收件箱推送 {ok}: {msg} ({len(content)}字)", flush=True)
    except Exception as e:
        print("[push] App收件箱推送失败:", str(e)[:200], flush=True)


def _maybe_push_app_later(task_id, task, delay=6.0):
    """v3.6.1：done 时刻起 delay 秒后复检推送。

    旧链路只在 done 瞬间判一次：那一刻 App 往往刚轮询过（流式期心跳还在刷
    lastPollAt，或已退出流式页但前台 5s 轮询仍在刷）→ 判「已送达」跳过推送，
    但用户其实没看到（App 已退出输入中样式/收起页面）→ 消息永久丢失。
    复检改为看 deliveredLen（App 实际取到的字节数），没取全就补推。
    """
    def _run():
        try:
            time.sleep(delay)
            if task.get("cancelled"):
                return
            _maybe_push_app(task["state"], task_id)
        except Exception as _e:
            print("[push] 延迟复检异常:", str(_e)[:150], flush=True)
    threading.Thread(target=_run, daemon=True).start()


# ---- v2.0.96 方案B：Agent（工具调用循环）----

AGENT_URL = os.environ.get("QL_AGENT_URL", "https://api.deepseek.com/v1/chat/completions")
AGENT_KEY = os.environ.get("QL_AGENT_KEY", "")   # 优先 env，否则读 config.yaml 的 providers.deepseek.api_key
AGENT_MODEL = os.environ.get("QL_AGENT_MODEL", "deepseek-chat")
# 强意图（控制/执行类，命中即走 Agent）vs 弱意图（查询类，需动词+主题双命中，防闲聊误伤）
AGENT_STRONG = ("更新", "压缩", "整理", "维护", "控制", "开关", "启动", "停止", "重启", "关灯", "开灯", "打开", "关闭", "清理", "清空", "执行", "设置", "布防", "离家", "空调", "风扇", "灯", "排气扇")
AGENT_VERBS = ("查", "看", "问", "多少", "怎么样", "状态", "情况", "使用率", "帮我", "运行", "温度")
AGENT_TOPICS = ("记忆", "对话", "历史", "上下文", "天气", "磁盘", "容器", "服务", "内存", "空间", "温度", "系统", "数据库", "文件", "日志", "代码", "端口", "进程", "网络", "仓库", "部署", "报错", "错误", "配置", "镜像", "会话", "状态", "数据", "目录")


def _agent_key():
    if AGENT_KEY:
        return AGENT_KEY
    try:
        import re
        txt = open(os.environ.get("QL_HERMES_CONFIG", "/etc/hermes/config.yaml"), encoding="utf-8").read()
        m = re.search(r"deepseek:\s*\n\s*api_key:\s*([A-Za-z0-9_\-]+)", txt)
        if m:
            return m.group(1)
    except Exception:
        pass
    return ""


def _is_agent_request(messages):
    """最后一条用户消息：强意图词直接触发；弱意图需动词+主题双命中（防闲聊误伤）"""
    try:
        last = None
        for m in reversed(messages):
            if isinstance(m, dict) and m.get("role") == "user":
                last = m
                break
        if not last:
            return False
        c = last.get("content", "")
        if isinstance(c, list):
            c = " ".join(str(b.get("text", "")) for b in c if isinstance(b, dict))
        c = str(c)
        # v2.0.105：合并设置页自定义关键词（agent_keywords.json）
        strong, verbs, topics = _effective_keywords()
        # v3.4.2：疑问句豁免（2026-09-04 实证：普通提问"后端有修复复读问题，有需要
        # 更新docker镜像吗"命中 strong"更新" -> 误判 agent -> Hermes 全量历史注入 ->
        # 复读两轮前旧回答"世界上最高的山"整段）。疑问句（以 吗/么/呢 或 要不要/
        # 需不需要/是不是/有没有 等结尾）视为普通问答，除非含明确委托词（帮我/请/
        # 麻烦）——真操作祈使（"重启服务""清理备份""帮我查内存"）不受影响。
        # v3.4.6（方案A补丁）：扩展闲聊询问豁免——非操作、非委托的"询问/求解"句
        # 即使命中话题词（如"天气怎么样"/"内存是多少"）也判为普通聊天，杜绝闲聊误入 agent
        # 工具循环（复读根因之一）。⚠️ 必须三条件同时满足才豁免：
        #   ① 含询问/求解词（怎么样/什么/怎么/多少/如何…）② 无明确委托词（帮我/请/麻烦…）
        #   ③ 无操作动作词（查/看/清理/重启/控制/打开/设置…）
        # 少任何一个都不豁免——保证"查内存多少""帮我查数据库""清理磁盘"等真操作仍走 agent。
        # 强委托词——出现即必为 agent 操作（"帮我/请/麻烦…"）
        _delegate = ('帮我', '请', '麻烦', '帮忙', '帮我查', '去给我', '帮我弄',
                     '动手', '立刻', '立即', '马上', '去执行', '去查')
        # 讲解类词——"帮我解释/讲讲/介绍"是求知识讲解，而非执行操作；含则豁免为普通聊天
        _explain = ('解释', '讲讲', '介绍', '说明', '科普', '说说', '讲一下',
                    '分析一下', '是什么', '什么是', '怎么回事', '什么意思', '怎么理解')
        # 明确动作词（祈使操作）——含"查/清理/重启/设置…"即为操作，不被"多少/怎么样"误豁免
        _action = ('查一下', '查内存', '查磁盘', '查存储', '查看', '查本机', '查NAS',
                   '查一下', '清理', '清空', '重启', '重开', '关闭', '打开', '关灯',
                   '开灯', '控制', '设置', '执行', '修复', '删除', '创建', '安装',
                   '卸载', '开关', '启动', '停止', '布防', '离家', '扫描', '统计',
                   '同步', '监控', '整理', '看看')
        # 纯询问词——命中且无委托+无动作词 → 普通询问，判普通聊天
        _ask_word = ('怎么样', '什么', '怎么', '如何', '吗', '呢', '么',
                     '哪些', '哪个', '多久', '能不能', '可以吗', '请问', '多少')
        is_q = any(t in c for t in _ask_word)
        is_del = any(a in c for a in _delegate)
        is_act = any(a in c for a in _action)
        is_explain = any(t in c for t in _explain)
        # 讲解类（哪怕带"帮我"）→ 求知识，判普通聊天
        if is_explain:
            return False
        # 闲聊询问豁免：非委托且非动作词的疑问句 → 普通聊天
        if is_q and not is_del and not is_act:
            return False
        if any(h in c for h in strong):
            t = c.rstrip('。.!！～~ ')
            is_q = t.endswith(('吗', '么', '呢')) or any(
                q in t for q in ('要不要', '需不需要', '是不是', '有没有',
                                  '好不好', '行不行', '可不可以'))
            if is_q and not any(a in c for a in ('帮我', '请', '麻烦')):
                return False
            return True
        return any(v in c for v in verbs) and any(t in c for t in topics)
    except Exception:
        return False


def _effective_keywords():
    """内置 + 用户自定义（agent_keywords.json，设置页管理）"""
    try:
        with open(os.path.join(DATA_DIR, "agent_keywords.json"), encoding="utf-8") as f:
            d = json.load(f)
        return (AGENT_STRONG + tuple(d.get("strong", [])),
                AGENT_VERBS + tuple(d.get("verbs", [])),
                AGENT_TOPICS + tuple(d.get("topics", [])))
    except Exception:
        return AGENT_STRONG, AGENT_VERBS, AGENT_TOPICS


def _is_auto_request(text):
    """定时/自动化意图话术（"X分钟后执行Y"等）——Agent 关闭时用于拦截防幻觉"""
    return any(k in text for k in ("分钟后", "定时", "自动化", "延时", "延迟", "秒后", "小时后再", "几小时后"))


def _chat_once(body, url=None, key=None):
    import urllib.request
    req = urllib.request.Request(url or AGENT_URL, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json",
                                          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                                          "Authorization": "Bearer " + (key or _agent_key())}, method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())


def _agent_endpoint(model, provider):
    """按请求携带的 model/provider 解析 agent 端点/key/模型（设置页 Agent 模型路由）。

    provider 在 config.yaml providers 段有配置（deepseek/stepfun/xiaomi/sensenova/
    opencode-apple/ollama）→ base_url + /chat/completions + 对应 api_key；
    provider=opencode → 归一化为 opencode-apple（google key 已删，所有 opencode 模型统一走 apple 订阅）；
    provider=local → 归一化为 ollama（本地 Ollama）；
    无 provider / 配置缺失 → 回退全局 AGENT_URL/AGENT_KEY/AGENT_MODEL（保持原行为）。
    """
    if provider == "opencode":
        provider = "opencode-apple"   # 2026-08：google 订阅 key 已删，双组统一走 apple
    if provider and provider not in ("", "local") and _yaml is not None:
        base = _provider_base_url(provider)
        if base:
            key = _load_cfg_key(["providers", provider, "api_key"])
            return base + "/chat/completions", key or "", model
    if provider == "local":
        base = _provider_base_url("ollama") or os.environ.get("QL_OLLAMA_URL", "http://host.docker.internal:11434/v1")
        key = _load_cfg_key(["providers", "ollama", "api_key"]) or "ollama"
        return base + "/chat/completions", key, model
    return AGENT_URL, AGENT_KEY, AGENT_MODEL


def _agent_loop(messages, task=None, model=None, provider=None):
    """工具调用循环：模型返回 tool_calls → 执行 → 回填 → 再问（上限 8 轮）
    v2.0.116 review：支持中途取消（task.cancelled）——原循环不读取消标志，用户 stop 最长 20 分钟无效
    v3.0.30 fix：model/provider 参数——Agent 分流使用设置页选定的模型（原恒用 AGENT_MODEL 默认 deepseek-chat）
    v3.1.12 fix：入口先净化历史（脏占位剔除 + 以 user 结尾 + 轮次收窄），
    与普通模式同规则——Agent 模式复读（旧回复被重复输出）同样源于脏历史。"""
    url, key, eff_model = _agent_endpoint(model, provider)
    try:
        import tool_executor
    except Exception as e:
        return f"Agent 工具模块不可用：{e}"
    try:
        msgs = _sanitize_history(messages)
        msgs = [m for m in msgs if isinstance(m, dict) and m.get("role") != "system"]
        import agent_rules
        hint = agent_rules.rules_hint()
        sys_content = ("你是家庭 NAS 管家 Agent。可以调用工具查询/控制 NAS、Docker、智能家居。"
                       "工具结果如实转达用户；失败要说明原因和建议。回答简洁中文，用 emoji 点缀。"
                       "重要：定时/自动化请求（如'X分钟后执行Y'、'定时关闭XX'）必须调用 automation_create 工具真正创建，"
                       "禁止仅文字回复'已设置/已安排'；只有工具返回成功才可向用户确认已创建。")
        # v2.0.105：本次请求含定时意图时再强化一次（防历史幻觉回复污染导致模型学样不调工具）
        last_u = ""
        for _m in reversed(messages):
            if isinstance(_m, dict) and _m.get("role") == "user":
                last_u = _m.get("content", "")
                if isinstance(last_u, list):
                    last_u = " ".join(str(b.get("text", "")) for b in last_u if isinstance(b, dict))
                break
        if last_u and _is_auto_request(str(last_u)):
            sys_content += ("\n本次用户请求包含定时/自动化意图（如'X分钟后执行Y'）。"
                            "即使对话历史中曾有类似回复，你也必须调用 automation_create 工具真实创建任务，"
                            "严禁仅用文字回复'已设置/已安排'。")
            # v2.0.105：user 级强制指令（比 system 权重高，防历史幻觉污染学样）
            msgs.append({"role": "user",
                         "content": "（系统指令：请立即调用 automation_create 工具创建上述定时任务，"
                                    "确认工具执行成功后再回复用户，禁止仅文字回复“已设置/已安排”。）"})
        if hint:
            sys_content += "\n" + hint
        # v3.2.1：断掉"最新 user 前紧贴 assistant"的续写诱因（防复读终极，Agent 路统一）
        # 注意：不做 KEEP_MSGS 收窄——Agent 有 tool_calls↔tool 配对，收窄会切断配对导致孤立 tool 消息
        # v3.2.4：全量压缩超长 assistant（Agent 全量历史 = 最多长文素材，只压 1 条无效）
        msgs = _compress_long_assistants(msgs)
        msgs = _break_repeat_seed(msgs)
        sys_content += QCARD_PROMPT   # v3.9.31 ql-card 协议
        sys_p = {"role": "system", "content": sys_content}
        msgs = [sys_p] + msgs
        _log_sent_messages("agent", msgs)
        for _ in range(8):
            # v2.0.116 review：每轮检查取消标志（用户点停止立即中断）
            if task is not None and task.get("cancelled"):
                return "已取消"
            body = {"model": eff_model, "messages": msgs,
                    "tools": tool_executor.TOOLS, "stream": False, "max_tokens": 1500}
            resp = _chat_once(body, url, key)
            msg = resp["choices"][0]["message"]
            if msg.get("tool_calls"):
                msgs.append({"role": "assistant", "content": msg.get("content") or "", "tool_calls": msg["tool_calls"]})
                for tc in msg["tool_calls"]:
                    fn = tc["function"]
                    try:
                        args = json.loads(fn.get("arguments") or "{}")
                    except Exception:
                        args = {}
                    result = tool_executor.execute(fn["name"], args, agent_model=eff_model, agent_provider=provider)
                    msgs.append({"role": "tool", "tool_call_id": tc["id"], "content": result})
                continue
            return msg.get("content") or "（模型无返回内容）"
        return "Agent 工具循环超过 8 轮，已停止"
    except Exception as e:
        return f"Agent 执行失败：{e}"


# ============ v3.1.12 防复读根治：历史净化 + 上下文整形 ============
# 复读根因（2026-09-03 实证）：模型"续写"历史里紧邻的旧 assistant 长回复，
# 而非回答新 user 消息。净化三原则：
#   1. 脏占位剔除 —— App 失败提示（⚠️/HTTP Error/连接中断）被 upsert 成 assistant 混入历史
#   2. 以 user 结尾 —— 末尾孤立的 assistant 尾巴会诱导模型续写旧回复
#   3. 轮次收窄 —— 只保留最近 N 轮完整对话对，模型无从复读更早内容

_DIRTY_MARKERS = ("⚠️", "HTTP Error", "连接中断", "请重试", "请求失败", "网络错误", "Unauthorized", "无返回内容")
# 保留最近几条消息作为上下文（v3.1.12：6 条 = 最近 3 轮对话；STREAM_KEEP_MSGS 可调）
KEEP_MSGS = int(os.environ.get("STREAM_KEEP_MSGS", "6"))


def _msg_text(m):
    """dict 消息 → 纯文本（兼容多模态 content 数组）"""
    if not isinstance(m, dict):
        return ""
    c = m.get("content", "")
    if isinstance(c, list):
        return " ".join(str(b.get("text", "")) for b in c if isinstance(b, dict))
    return str(c)


def _msg_has_image(m):
    """dict 消息是否含图片块（image_url/input_image 等视觉块）——纯图消息无 text，
    净化时不能按空文本剔除（v3.4.x fix：否则最新图片消息被丢，模型只看到 [图片] 占位）。"""
    if not isinstance(m, dict):
        return False
    c = m.get("content", "")
    if not isinstance(c, list):
        return False
    for b in c:
        if isinstance(b, dict) and str(b.get("type", "")).lower() in ("image_url", "image", "input_image"):
            return True
    return False


def _sanitize_history(raw_msgs):
    """v3.4.18 历史净化：剔脏占位 → 去连续重复 assistant/user → 保证以 user 结尾。"""
    msgs = []
    for m in raw_msgs:
        if not isinstance(m, dict) or not m.get("role"):
            continue
        role = m.get("role")
        has_img = _msg_has_image(m)
        text = _msg_text(m).strip()
        if not text and not has_img:
            continue
        # 1) 脏占位剔除（assistant 的错误/失败提示不进模型上下文）
        if role == "assistant" and text.startswith(_DIRTY_MARKERS):
            continue
        # 2) 连续相同 assistant 只留最后一条（复读产物）；含图消息不参与文本去重
        if (role == "assistant" and msgs and msgs[-1].get("role") == "assistant"
                and not has_img and not _msg_has_image(msgs[-1])
                and _msg_text(msgs[-1]).strip() == text):
            continue
        # 2.5) v3.4.18 复读根治：连续相同 user 只留最后一条。
        # 发送重试/恢复错位在历史里堆出 N 条相同 user（body_dump 实证 8 条重复
        # "你是什么模型" 淹没最新问题），全部原样进上下文 -> 模型把旧问题当最新问题作答。
        if (role == "user" and not has_img and msgs and msgs[-1].get("role") == "user"
                and not _msg_has_image(msgs[-1])
                and _msg_text(msgs[-1]).strip() == text):
            continue
        msgs.append(m)
    # 3) 以 user 结尾：剥离末尾孤立 assistant/system，防模型续写旧回复
    while msgs and msgs[-1].get("role") != "user":
        msgs.pop()
    # 边界保护：若剥离后全空（异常历史），保留最后一条原始消息，避免模型收到空上下文
    if not msgs and raw_msgs:
        last = raw_msgs[-1]
        if isinstance(last, dict) and last.get("content"):
            msgs = [last]
    return msgs


def _break_repeat_seed(msgs):
    """v3.2.1 防复读终极：断掉"紧贴最新 user 的 assistant 续写诱因"。

    实证根因（msg_debug 04:02:53 确凿）：模型收到 [..., assistant(完整长回复), user(新问题)]
    时，会续写/复读那条 assistant 而非回答新问题。v3.1.12 只保证列表"以 user 结尾"，
    没处理"最新 user 前紧贴的 assistant"——那正是可续写素材（"好的，简要说明我的上下文理解
    机制：1.系统提示词 2.记忆系统..."整段被原样复活）。

    把最新 user 前紧贴的 assistant 压缩为不可复读的占位（保留 user→assistant→user 结构，
    断掉可续写素材），普通/Agent 两路统一调用。
    """
    if not msgs:
        return msgs
    # 防御：调用方已保证以 user 结尾，这里兜底
    if msgs[-1].get("role") != "user":
        return msgs
    if len(msgs) >= 2 and msgs[-2].get("role") == "assistant":
        prev = dict(msgs[-2])
        prev["content"] = "（上一轮回复已省略，请直接回答最新用户消息，不要续写或复述此条内容）"
        msgs[-2] = prev
    return msgs


_REPEAT_PLACEHOLDER = "（上一轮回复已省略，请直接回答最新用户消息，不要续写或复述此条内容）"


_STATUS_VERBOSE_RE = re.compile(
    r"(内存|CPU|SSD|磁盘|硬盘|系统状态|负载|存储).*(已用|可用|共|使用|占用|%|GB|MB|TB|\u00b0C|健康|剩余)"
    r"|服务器.*(正常|不可用)|容器.*(运行|停止|Up|Exited)",
    re.S)


def _is_status_verbose(text):
    """v3.2.5 后端：工具转述型"状态播报"识别——短句也可能被模型整段复述。
    查内存/CPU/温度/容器这类管家播报每轮都能重新生成，是最易复读的模板；
    无论长短都压为占位，杜绝进入模型上下文成为复述素材。"""
    if not text:
        return False
    return bool(_STATUS_VERBOSE_RE.search(text))


def _is_open_greeting(text):
    """v3.4.17 复读定案：识别「模板化开场白/欢迎语/自我介绍」——第一条答案最常见的形态。

    根因 /new 后复读：模型在 `/new` 环节生成的欢迎语（"👋 新会话已开启！我是轻聊 AI
    助手，可以帮你查询/控制 NAS、Docker、智能家居…"）是短回复(约70字<80)，被
    _compress_long_assistants 的 keep_recent 窗口豁免保留 → 又因处于历史中间(非 msgs[-2])
    而不被 _break_repeat_seed 覆盖 → 弱模型每轮复述这条「第一条答案」。超长回复已被
    len(t)>max_len 压掉，唯独此类短开场白漏网，形成自强化复读死循环。

    这类开场白无任何上下文记忆价值(纯自我介绍/能力罗列)，无论长短一律压为占位，
    即切断复述素材。仅匹配强特征句，避免误伤实质短回复（如"好的""已经在做了"）。"""
    if not text:
        return False
    _OPEN_GREETING_RE = re.compile(
        r"新会话已开启|新会话开始|欢迎使用|你好，我是|你好我是|"
        r"我是[^，。]{0,12}(助手|AI|AI助手|助理)|"
        r"(我可以|我能|我来帮你)(为你)?(查询|控制|处理|管理|操作|做|帮)", re.S)
    return bool(_OPEN_GREETING_RE.search(text))


def _compress_long_assistants(msgs, max_len=80, keep_recent=3):
    """v3.x: 保留最近 keep_recent 条实质 assistant 完整(防失忆跑偏)，更早超长/状态播报仍压(防复读)。

    根因(2026-09-07 复读定案)：原版把窗口内所有超长(>80字) assistant 压成"已省略"占位——
    开发长回复全被压 → 模型看不到任何 AI 实质回答(只见占位) → 对"分享会话卡片优化"这类需结合前文
    的问题无上下文可接，退化复读历史里唯一实质话题(如"灵动岛")，每轮重复输出。
    改为：保留最近 keep_recent 条实质 assistant 完整(记忆在)，更早超长仍压(防复读)。
    注：紧贴最新 user 的 msgs[-2] 仍会被 _break_repeat_seed 另行压掉，故最终保留的是最近几轮中
    除 msgs[-2] 外的实质回答。
    """
    if not msgs:
        return msgs
    a_idx = [k for k, m in enumerate(msgs)
             if isinstance(m, dict) and m.get("role") == "assistant"]
    if not a_idx:
        return msgs
    keep = set(a_idx[-keep_recent:])
    out = []
    for k, m in enumerate(msgs):
        if not (isinstance(m, dict) and m.get("role") == "assistant"):
            out.append(m)
            continue
        t = _msg_text(m)
        # 状态播报型无记忆价值且是复读源，即使保留窗口内也压
        if _is_status_verbose(t):
            mm = dict(m)
            mm["content"] = _REPEAT_PLACEHOLDER
            out.append(mm)
            continue
        # v3.4.16 复读定案(2026-09-07)：keep_recent 不得豁免超长原文。
        # 超长(>max_len) assistant 无论是否在最近窗口内都压成占位——否则模型拿到
        # 完整旧长文(如1183字头脑风暴整段)当续写素材，每轮复述首轮长回答。
        # 短回复(<max_len)才按 keep_recent 保留，两全复读与记忆。
        if len(t) > max_len:
            mm = dict(m)
            mm["content"] = _REPEAT_PLACEHOLDER
            out.append(mm)
            continue
        # v3.4.17 复读定案(2026-09-07)：模板化开场白/欢迎语/自我介绍短回复一律压为占位。
        # 这类「第一条答案」(如 /new 后"👋 新会话已开启！我是轻聊 AI 助手…")是短回复(<80字)，
        # 落进 keep_recent 窗口被豁免保留、又非 msgs[-2] 不被 _break_repeat_seed 覆盖 → 弱模型
        # 每轮复述。内容无记忆价值(纯自我介绍/能力罗列)，压掉即断复述循环。
        if _is_open_greeting(t):
            mm = dict(m)
            mm["content"] = _REPEAT_PLACEHOLDER
            out.append(mm)
            continue
        out.append(m)
    return out
def _is_strong_model(provider="", model=""):
    """v3.2.6：区分强/弱模型，决定是否启用\"防复读压缩\"。

    弱模型(如 mimo-v2.5)看到历史里有完整长回复会**整段照抄**（经典复读），
    所以要用 _compress_long_assistants / _break_repeat_seed 把 assistant 压成占位、
    断掉续写诱因——对它们有效。

    强模型(如 deepseek-v4-flash)需要**完整语义上下文**才能把用户短消息(如"1"、
    "1.按此方案做")对号入座到上一条回复；一旦把紧贴的 assistant 压成\"（上一轮回复
    已省略）\"占位，它就失忆，只能反复回\"没对上号/抱歉我异常了\"来兜底，而这些兜底
    长句又再次被压缩 → 自我强化的复读死循环（09-04 00:26 日志 deepseek 自述实证）。

    因此：强模型不做该压缩，只保留 sanitize(剔脏) + KEEP_MSGS(轮次收窄)，保证它
    能看到窗口内完整上下文。
    """
    # v3.2.6：强模型不做"压缩历史长回复"（压掉原文→失忆→长兜底→死循环）。
    # v3.2.7x（方案A）：扩到 stepfun（step-3.7/flash 等推理型）——stepfun 此前被当弱模型
    # 强压缩，正是用户 stepfun 复读失忆死循环的根因之一。判定：provider 精确匹配，
    # 或 model 名含常见强模型前缀。
    p = (provider or "").lower()
    m = (model or "").lower()
    return (p in ("deepseek", "stepfun")
            or m.startswith("deepseek") or m.startswith("step")
            or m.startswith("gpt-5") or m.startswith("claude") or m.startswith("glm-5"))


# v3.9.31：ql-card 结果卡片协议（App 端 v3.5.0 起 AgentCardParser 已解析渲染）。
# 注入 5 处 system prompt 组装点；纪律段防滥用：仅结构化结果类回复收尾用，闲聊禁用。
QCARD_PROMPT = (
    "\n\n【结果卡片协议（可选）】当回复属于结构化结果类（体检/诊断/巡检/任务清单/对比表格/多步骤结果汇报），"
    "在文字总结之后追加一个 ```ql-card 代码围栏，围栏内是单个 JSON 对象（不要多对象）。字段（全部可选，有什么写什么）："
    'type(result|metrics|list|table|status)/title/subtitle/status({text,tone:ok|warn|error|info})/'
    'fields([{key,value}])/metrics([{label,value,unit?,tone?}])/list([{title,subtitle?,status?,tone?}])/'
    'table({columns,rows})/footer。'
    "围栏独占一行、必须闭合；围栏外文字照常写。纪律：闲聊/解释/短回复一律不要用卡片；"
    "一张卡片讲完当前这轮结果，不要多卡堆叠；JSON 必须合法（客户端解析失败会原样显示文本，不会报错）。"
)

def _log_sent_messages(tag, msgs):
    """诊断：记录实际发给模型的消息（角色+摘要+条数），用于验证防复读是否生效。
    v3.2.1 恢复写点（v3.1.12 曾删除该日志导致无法观察部署后发给模型的内容）。"""
    try:
        _p = "/tmp/stream_msg_debug.log"
        if os.path.exists(_p) and os.path.getsize(_p) > 200_000:
            os.rename(_p, _p + ".old")
        with open(_p, "a", encoding="utf-8") as _f:
            _f.write(f"\n=== [{time.strftime('%H:%M:%S')}] {tag} ===\n")
            for m in msgs[-12:]:
                role = m.get("role", "?")
                t = _msg_text(m)
                _f.write(f"  {role}: {t[:140]}\n")
            _f.write(f"Msg count: {len(msgs)}\n")
    except Exception:
        pass


def _build_messages(st):
    """v3.1.12：组装发往 9123 的 messages（防复读根治版，仅回退路径用）。
    v3.2.7（方案C）：bot 模式已废除，删除 bot 分支，只保留普通模式引导逻辑。"""
    msgs = _sanitize_history(st["messages"])
    # v3.1.12：轮次收窄——始终只保留最近 KEEP_MSGS 条（v3.1.10 的"最近2条"证明有效，
    # 但 2 条过激丢上下文，取 6 条平衡；旧历史用一条 system 标记占位）
    if len(msgs) > KEEP_MSGS:
        recent = msgs[-KEEP_MSGS:]
        marker = {"role": "system", "content": "（更早的对话已省略，请基于最近的对话继续回答，不要复述已省略内容）"}
        msgs = [marker] + recent
    # v3.2.1：断掉"最新 user 前紧贴 assistant"的续写诱因（防复读终极）
    # v3.2.4：先全量压缩窗口内所有超长 assistant（_break_repeat_seed 只压 1 条不够——
    # 实证 6 条窗口内仍含多条完整长文可被整段复述）
    # v3.2.6：按模型区分——弱模型(mimo)照抄旧长回复需压断续写诱因；强模型(deepseek)
    # 压缩会掐掉它"上一条回复"记忆导致失忆复读（只能反复"没对上号"），故不强压缩。
    if not _is_strong_model(st.get("provider", ""), st.get("model", "")):
        msgs = _compress_long_assistants(msgs)
        msgs = _break_repeat_seed(msgs)
    # 普通模式：防复读引导型 system prompt（引导指向最新消息，非身份设定、非禁思维链）
    base_sys = [{"role": "system",
                 "content": "你是轻聊的 AI 助手，用中文简洁友好地回答用户的问题。"
                            "每次回复只针对用户最新一条消息：先理解它问的是什么，再给出有针对性的回答。"
                            "不要重复、复述或续写对话历史中你已经回答过的内容。"}]
    base_sys[0]["content"] += QCARD_PROMPT   # v3.9.31 ql-card 协议
    final = base_sys + kb_inject.inject(msgs)
    _log_sent_messages("normal", final)
    return final


def _use_hermes_session():
    """v3.2.7（方案C）：是否启用 Hermes 会话托管（复读根治根因）。
    开=请求体只传最新 user 消息，上下文由 Hermes 9123 按 X-Hermes-Session-Id 从
    state.db 管理（微信通道同款，从不复读/丢上下文）；关=回退现有 _build_messages
    全量塞消息+轻聊侧防复读（现状，可一键回退）。"""
    return os.environ.get("STREAM_HERMES_SESSION", "0") == "1"


def _hermes_session_header(session_id):
    """构造发往 Hermes 的会话头。轻聊 sessionId 与 Hermes 内部 session 可能撞名，
    用 ql_ 前缀隔离，避免污染其他通道会话。返回 (headers_dict, 需不需要改sessionId)。"""
    sid = str(session_id or "").strip()
    if not sid:
        return {}, None
    # Hermes sessionId 校验：拒绝控制字符/路径转义（见 api_server.py 5261）
    import re as _re
    if _re.search(r'[\r\n\x00]', sid) or "/" in sid or ".." in sid:
        return {}, None
    return {"X-Hermes-Session-Id": "ql_" + sid}, None


def _build_hermes_agent_prompt(st, last_user):
    """方案C：Agent 统一走 Hermes 时，把轻聊侧 agent system prompt 作为请求体首条 system
    传给 Hermes，使其在 _run_agent 里成为 ephemeral system prompt（叠在 core 之上）。
    返回完整 messages 列表（[system] + [最新user]），或仅 [最新user]。"""
    # 轻聊 agent prompt（含容器工具清单 + 行为规则）
    sys_content = ("你是轻聊的 AI 助手，用中文简洁友好地回答用户的问题。"
                   "可以调用工具查询/控制 NAS、Docker、智能家居。"
                   "工具结果如实转达用户；失败要说明原因和建议。"
                   "回答简洁中文，用 emoji 点缀。"
                   "每次回复只针对用户最新一条消息，不要重复历史中已回答过的内容。")
    # v3.9.31 ql-card 协议
    sys_content += QCARD_PROMPT
    # v3.3.1：多模态 content 原样透传
    user_content = last_user if isinstance(last_user, list) else str(last_user or "")
    return [{"role": "system", "content": sys_content},
            {"role": "user", "content": user_content}]


def _compress_all_assistants(msgs):
    """v3.4.11 防复读终版：窗口内 ALL assistant 回复一律压为占位。
    根因(已定案)：短小 reply(如「我现在走的是 step-3.7-flash」)不在 msgs[-2] 且 <80 字，
    _compress_long_assistants 只压超长/状态播报 → 短种子原样进模型上下文 → 弱模型复读。
    改为：任何 assistant 回复都压为占位，模型上下文无「可复读旧答案」；user 轮仍保留(上下文经用户话轮延续)。"""
    if not msgs:
        return msgs
    out = []
    for m in msgs:
        if isinstance(m, dict) and m.get("role") == "assistant":
            mm = dict(m)
            mm["content"] = _REPEAT_PLACEHOLDER
            out.append(mm)
            continue
        out.append(m)
    return out


def _build_hermes_messages(st, last_user, is_agent):
    """v3.4.10 X方案：发「断种子净化完整历史」给 Hermes 9123（不再靠 state.db 重建）。

    根因（已定案）：Hermes 收到 X-Hermes-Session-Id 就用 state.db 未净化原始会话覆盖外部
    messages（api_server_openai_routes.py:476 history=db.get_messages_as_conversation），
    种子上一条 assistant 回复+工具调用/结果原样进上下文 → 弱模型续写 → 复读。
    改：去掉该头改发 app 全量传入的净化历史（_sanitize_history 剔脏占位/连续重复，
    _compress_long_assistants 压超长/状态播报，_break_repeat_seed 断紧贴最新 user 的续写诱因）。
    模型上下文=净化历史，不复读；模型选择/图片/流式/工具全部保留。
    v3.3.1：last_user 可能是 list（多模态 content 含图片），原样透传不压字符串。"""
    raw = st.get("messages") or []
    if raw:
        sanitized = _sanitize_history(raw)
        sanitized = _compress_long_assistants(sanitized)
        sanitized = _break_repeat_seed(sanitized)
    else:
        sanitized = []
    # 兜底：若净化后空或未以 user 结尾，补上当前最新 user 消息
    user_content = last_user if isinstance(last_user, list) else str(last_user or "")
    if not sanitized or sanitized[-1].get("role") != "user":
        sanitized = sanitized + [{"role": "user", "content": user_content}]
    if is_agent:
        sys_content = ("你是轻聊的 AI 助手，用中文简洁友好地回答用户的问题。"
                       "可以调用工具查询/控制 NAS、Docker、智能家居。"
                       "工具结果如实转达用户；失败要说明原因和建议。"
                       "回答简洁中文，用 emoji 点缀。"
                       "下面 messages 是会话历史（assistant 回复已压缩为占位符），仅作背景参考。"
                       "【关键】只回答最新一条 user 消息。绝对不要续写、复述、照抄历史上任何一条 "
                       "assistant 回复的内容或任何工具调用/结果——这会重复回答，务必直接给出新答案。")
        sys_content += QCARD_PROMPT   # v3.9.31 ql-card 协议
    else:
        sys_content = ("你是轻聊的 AI 助手，用中文简洁友好地回答用户的问题。"
                       "下面 messages 是会话历史（assistant 回复已压缩为占位符），仅作背景参考。"
                       "【关键】只回答最新一条 user 消息：先理解它问什么，再给有针对性的回答。"
                       "不要复述、续写或照抄历史里你已经回答过的内容。")
    sys_content += QCARD_PROMPT   # v3.9.31 ql-card 协议
    return [{"role": "system", "content": sys_content}] + sanitized


def _forward_qingliao(task_id, task, text):
    """v3.4.7x 方案C-改造：把轻聊消息转发给 Hermes qingliao(9130) 一等客户端通道。
    Hermes 全权处理（智能判断+工具执行），结果经 qingliao send() 推轻聊收件箱。"""
    try:
        import threading
        st = task["state"]
        chat_id = str(st.get("sessionId") or task_id)
        token = os.environ.get("QL_HERMES_TOKEN", "qingliao-token-9130")
        url = os.environ.get("QL_HERMES_URL", "http://172.21.0.2:9130/chat")
        body = json.dumps({
            "text": str(text or ""),
            "user_id": chat_id,
            "chat_id": chat_id,
        }, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST", headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer " + token,
        })
        threading.Thread(target=_do_qingliao_post, args=(req,), daemon=True).start()
    except Exception:
        pass


def _do_qingliao_post(req):
    try:
        urllib.request.urlopen(req, timeout=15).read()
    except Exception:
        pass



def _worker(task_id, task):
    st = task["state"]
    last_write = time.time()
    try:
        # v3.2.7（方案C）：统一走 Hermes 会话托管。上下文由 Hermes 按 sessionId 管理，
        # 轻聊不再自己拼历史+防复读（复读根因）。_apply_bot 已在方案C移除（bot 模式废除）。
        import agent_rules
        last_user = None   # 必须初始化（messages 无 user 时会走 or "" 兜底）
        last_user_text = ""  # v3.3.1：纯文本版（供规则判断/日志），保留原始 last_user 供图片透传
        for m in reversed(st["messages"]):
            if isinstance(m, dict) and m.get("role") == "user":
                last_user = m.get("content", "")
                if isinstance(last_user, list):
                    last_user_text = " ".join(str(b.get("text", "")) for b in last_user if isinstance(b, dict))
                else:
                    last_user_text = str(last_user or "")
                break
        new_rule = agent_rules.extract_from_text(last_user_text)
        if new_rule:
            agent_rules.add_rule(new_rule)
        agent_on = st.get("agentEnabled", True)
        # v2.0.105d：分流诊断日志（排查用户侧 agentEnabled 实际值）
        # v2.0.116 review：日志限 500 行轮转（防 /tmp 占满）
        try:
            _dbg = "/tmp/stream_agent_debug.log"
            if os.path.exists(_dbg) and os.path.getsize(_dbg) > 200_000:
                os.rename(_dbg, _dbg + ".old")
            with open(_dbg, "a", encoding="utf-8") as _df:
                _df.write(f"[{time.strftime('%H:%M:%S')}] agent_on={agent_on} is_agent={_is_agent_request(st['messages'])} "
                          f"rule={agent_rules.match(last_user or '')} model={st.get('model','?')} provider={st.get('provider','?')} "
                          f"msgs={len(st['messages'])} text={str(last_user or '')[:60]} hermes_session={_use_hermes_session()}\n")
        except Exception:
            pass
        is_agent_req = bool(agent_on and (_is_agent_request(st["messages"]) or agent_rules.match(last_user or "")))
        # v3.6.1 进度类追问秒回：用户问「进度/好了吗」时，若同会话有正在跑的任务，
        # 直接回该任务的实时进度摘要（不开新 Hermes 请求 → 不排队、不互卡、1 秒内可见）
        try:
            if _is_progress_question(last_user_text):
                _run_id, _run_st = _find_running_task_for_session(st.get("sessionId"), task_id)
                if _run_id:
                    _st_now = _run_st["state"] if isinstance(_run_st, dict) and "state" in _run_st else _run_st
                    _content = (_st_now.get("content") or "")
                    _tools = int(_st_now.get("toolSeq") or 0)   # v3.7.0：进度行下线，改用工具步数计数
                    _elapsed = int(time.time() - (_st_now.get("createdAt") or time.time()))
                    _tail = [ln for ln in _content.strip().splitlines() if ln.strip()]
                    _last_line = _tail[-1] if _tail else "仍在处理中"
                    st["agent"] = False
                    st["content"] = ("⏳ 任务还在跑：已进行 %d 分 %d 秒，执行了 %d 步工具。\n最新动态：%s\n"
                                     "完成后会自动推给你，不用一直问～" % (_elapsed // 60, _elapsed % 60, _tools, _last_line))
                    st["status"] = "done"
                    _write_state(task_id, task)
                    _maybe_push(st)
                    _maybe_push_app(st, task_id)
                    _maybe_push_app_later(task_id, task)
                    print("[progress] 进度类追问秒回: running=%s elapsed=%ss" % (_run_id, _elapsed), flush=True)
                    return
        except Exception as _pe:
            print("[progress] 进度秒回异常(降级普通请求):", str(_pe)[:150], flush=True)

        # v2.0.105：Agent 关闭时定时类话术明确提示（防普通 LLM 幻觉回复"已设置"实际未创建）
        if not agent_on and _is_auto_request(last_user_text):
            st["agent"] = False
            st["content"] = ("⏰ 定时自动化需要开启「Agent 智能回复」才能创建（设置 → 高级设置 → Agent 智能回复）。\n"
                             "打开开关后，对我说「X分钟后执行Y」即可自动生成倒计时卡片。")
            st["status"] = "done"
            _write_state(task_id, task)
            _maybe_push(st)
            _maybe_push_app(st, task_id)
            return
        # ============ 方案C + 方案A：按 is_agent 分流，普通聊天不进 Hermes agent 循环 ============
        # 方案C：默认走 Hermes 9123 会话托管（上下文由 state.db 续接）。
        # 方案A（v3.2.7x）：普通聊天（is_agent_req=False）不再发去 Hermes——因为 Hermes
        # _create_agent 会给所有 /v1/chat/completions 无条件注入全量平台工具集 + agent 循环，
        # 普通聊天因此被卷进工具循环 → 工具调用/结果堆进会话历史 → 模型下轮整段复述 → 复读。
        # 根治：普通聊天直连 provider 端点（经 _agent_endpoint 解析 base_url），
        # 请求体无 tools，模型单轮直答，彻底脱离工具循环。流式效果保持（复用 SSE 解析）。
        if _use_hermes_session():
            # -------- 所有聊天恒走 Hermes agent（平替微信/QQ，无论开关/指令）--------
            # v3.4.8：彻底移除"普通聊天直连 provider"——所有消息进 Hermes 9123 agent 循环
            #（agent 系统提示+工具契约+Hermes 按 sessionId 续接历史），与微信/QQ 通道一致。
            # v3.4.10 X方案：不再带 X-Hermes-Session-Id！根因：Hermes 收到该头即用 state.db
            # 未净化原始会话覆盖外部 messages → 种子上一条 assistant 回复+工具/结果进上下文 → 复读。
            # 现改为去该头 + 发断种子净化历史（_build_hermes_messages），模型用净化上下文，不复读。
            _amodel = st.get("model") or AGENT_MODEL
            # v3.5.2：净化历史先拼好（responses 路径要拆成 instructions + input）
            _amsgs = _build_hermes_messages(st, last_user, True)
            if HERMES_PROTOCOL == "responses":
                req_body = _hermes_responses_body(_amodel, st, _amsgs)
            else:
                req_body = {
                    "model": _amodel,
                    "messages": _amsgs,
                    "stream": True,
                    "max_tokens": int(os.environ.get("STREAM_MAX_TOKENS", 8192)),
                }
                if st.get("provider"):
                    req_body["provider"] = st["provider"]
                else:
                    req_body["provider"] = "deepseek"
                req_body["model_options"] = _reasoning_options(st.get("reasoning"))   # v3.6.5 档位优先
            # 注意：不再 update(session_headers) —— 去掉 X-Hermes-Session-Id，
            # Hermes 改走 api_server_openai_routes.py:476 的 conversation_messages[:-1]（我们发的净化历史）。
            full_headers = {"Authorization": "Bearer " + HERMES_KEY,
                            "Content-Type": "application/json"}
            st["agent"] = True
            # v3.4.11 DIAG: dump 实际请求 body（消息角色+截断内容）以便定位复读种子
            try:
                _dump = []
                for _m in _amsgs:
                    _c = _m.get("content", "")
                    if isinstance(_c, list):
                        _c = str(_c)[:120]
                    _dump.append({"role": _m.get("role"), "content": str(_c or "")[:200]})
                open("/tmp/body_dump.log", "a", encoding="utf-8").write(
                    json.dumps({"model": req_body.get("model"), "provider": req_body.get("provider"),
                                "hdr_keys": list(full_headers.keys()), "messages": _dump}, ensure_ascii=False) + "\n")
            except Exception:
                pass
            if HERMES_PROTOCOL == "responses":
                _hermes_responses_worker(task_id, task, req_body, full_headers, last_write)
            else:
                _hermes_stream_worker(task_id, task, req_body, full_headers, last_write)
            return
        # ============ 回退：现状（STREAM_HERMES_SESSION=0，不启用方案C） ============
        # v3.5.2：下面这条回退支路仍走 chat/completions（含 provider 直连/Ollama 直连），
        # 只有上面的 Agent 支路迁移到了 /v1/responses —— 生产 STREAM_HERMES_SESSION=1，走不到这里。
        if agent_on and (_is_agent_request(st["messages"]) or agent_rules.match(last_user or "")):
            st["agent"] = True
            st["content"] = media_convert.convert_media_marks(_agent_loop(st["messages"], task))  # v2.0.130: MEDIA→图片
            st["status"] = "done"
            _write_state(task_id, task)
            _maybe_push(st)
            _maybe_push_app(st, task_id)
            return
        # === 走Hermes 9123（跟微信通道同一条路）===
        _fresh_sid = "ql_" + uuid.uuid4().hex[:12]
        session_headers, _ = _hermes_session_header(_fresh_sid)
        req_body = {
            "model": st["model"],
            "messages": _build_hermes_messages(st, last_user, False),
            "stream": True,
            "max_tokens": int(os.environ.get("STREAM_MAX_TOKENS", 8192)),
                "frequency_penalty": 0.7,
                "presence_penalty": 0.3,
        }
        if st.get("provider"):
            req_body["provider"] = st["provider"]
        # 本地模型直连 Ollama（断网兜底）
        if st.get("provider") == "local":
            try:
                local_model = st.get("model") or "qwen3:4b"
                lbody = json.dumps({"model": local_model, "messages": req_body["messages"], "stream": True}).encode("utf-8")
                lreq = urllib.request.Request("http://127.0.0.1:11434/v1/chat/completions",
                                              data=lbody, headers={"Content-Type": "application/json"})
                lresp = urllib.request.urlopen(lreq, timeout=900)
                for raw in lresp:
                    if task["cancelled"]:
                        break
                    line = raw.decode("utf-8", "replace").strip()
                    if not line.startswith("data: "):
                        continue
                    payload = line[6:]
                    if payload == "[DONE]":
                        break
                    try:
                        j = json.loads(payload)
                        delta = j.get("choices", [{}])[0].get("delta", {}).get("content", "")
                        if delta:
                            if len(st["content"]) < MAX_CONTENT_LEN:
                                st["content"] += delta[:MAX_CONTENT_LEN - len(st["content"])]
                            else:
                                st["status"] = "done"
                                break
                            now = time.time()
                            if now - last_write >= WRITE_INTERVAL:
                                _write_state(task_id, task)
                    except Exception:
                        continue
                st["status"] = "cancelled" if task["cancelled"] else "done"
                _write_state(task_id, task)
                _maybe_push(st)
                _maybe_push_app(st, task_id)
                return
            except Exception as e:
                st["content"] = f"⚠️ 本地模型不可用：{str(e)[:120]}（请确认「设置 → 本地模型」已开启）"
                st["status"] = "done"
                _write_state(task_id, task)
                _maybe_push_app(st, task_id)   # 异常兜底也推（用户需知道本地模型不可用）
                return
        full_headers = {"Authorization": "Bearer " + HERMES_KEY, "Content-Type": "application/json"}
        # v3.4.10 X方案：回退分支同样去掉 X-Hermes-Session-Id（防 state.db 未净化会话重建→复读）
        st["agent"] = False
        _hermes_stream_worker(task_id, task, req_body, full_headers, last_write)
        return
    except Exception as e:
        st["status"] = "error"
        st["error"] = str(e)[:300]
    _write_state(task_id, task)
    _maybe_push(st)
    _maybe_push_app(st, task_id)


def _hermes_stream_worker(task_id, task, req_body, headers, last_write, url=None):
    """v3.2.7（方案C）：走 Hermes 9123 `/v1/chat/completions` 的流式worker。

    与 _worker 的现状普通路径同构（SSE 解析），但使用传入的 headers（含
    X-Hermes-Session-Id）→ Hermes 按 sessionId 从 state.db 续接会话历史，
    上下文由 Hermes 统一管理（根治复读/上下文丢失）。支持中途取消。

    v3.2.7x（方案A）：新增 url 参数——普通聊天直连 provider 端点（不经 Hermes agent
    循环），url=provider base+chat/completions；缺省仍走 HERMES_URL（agent 路径）。
    """
    st = task["state"]
    try:
        body = json.dumps(req_body).encode("utf-8")
        target = url or HERMES_URL
        req = urllib.request.Request(target, data=body, headers=headers)
        open("/tmp/hermes_worker_debug.log", "a").write("REQ session=" + headers.get("X-Hermes-Session-Id", "NONE") + " url=" + target + chr(10))
        resp = urllib.request.urlopen(req, timeout=900)
        for raw in resp:
            if task["cancelled"]:
                break
            line = raw.decode("utf-8", "replace").strip()
            if not line.startswith("data: "):
                continue
            payload = line[6:]
            if payload == "[DONE]":
                break
            try:
                j = json.loads(payload)
                delta = j.get("choices", [{}])[0].get("delta", {}).get("content", "")
                if delta:
                    # v2.0.116 review：内容上限 200k 字符（防无限输出撑爆内存/磁盘）
                    if len(st["content"]) < MAX_CONTENT_LEN:
                        st["content"] += delta[:MAX_CONTENT_LEN - len(st["content"])]
                    else:
                        st["status"] = "done"
                        break
                    now = time.time()
                    if now - last_write >= WRITE_INTERVAL:
                        _write_state(task_id, task)
                        last_write = now
            except Exception:
                pass
        st["status"] = "cancelled" if task["cancelled"] else "done"
    except Exception as e:
        open("/tmp/hermes_worker_debug.log", "a").write("ERROR: " + str(e) + chr(10))
        st["status"] = "error"
        st["error"] = str(e)[:300]
    _write_state(task_id, task)
    _maybe_push(st)
    _maybe_push_app(st, task_id)
    _maybe_push_app_later(task_id, task)   # v3.6.1 延迟复检：内容没送达就补推


def _hermes_responses_body(model, st, msgs):
    """v3.5.2：把 chat/completions 的 messages 转成 Responses API 请求体。

    首条 system 平移到 `instructions`（responses 路由把它当 ephemeral system prompt，与
    chat/completions 的等价），其余按序放进 `input` —— 路由取 input[:-1] 当历史、最后一条当
    本轮用户消息，而轻聊发的是「断种子净化历史」，正好对上。`max_tokens`/penalty 这类字段
    responses 路由不读（`_request_agent_overrides` 只认 model/provider/model_options），故不传。
    """
    items = list(msgs or [])
    sys_prompt = ""
    if items and isinstance(items[0], dict) and items[0].get("role") == "system":
        sys_prompt = items[0].get("content") or ""
        items = items[1:]
    body = {
        "model": model,
        "input": items,
        "stream": True,
        "store": False,   # 默认 true 会往 response_store.db 落条目，这里不需要
        "provider": st.get("provider") or "deepseek",
    }
    if sys_prompt:
        body["instructions"] = sys_prompt
    body["model_options"] = _reasoning_options(st.get("reasoning"))   # v3.6.5 档位优先（只影响轻聊）
    return body


def _hermes_responses_worker(task_id, task, req_body, headers, last_write):
    """v3.5.2：走 Hermes 9123 `/v1/responses` 的流式 worker（官方客户端协议）。

    与 `_hermes_stream_worker` 同构，只换上游协议与 SSE 解析：
      response.output_text.delta → 累加文本（唯一增量来源）
      response.output_text.done  → 兜底（本轮没出现过 delta 时用整段文本补上）
      response.failed/incomplete → 取 error.message（本轮无文本时判 error）
    收益：Hermes 侧 `collect_result` 会把非流式阶段产生的 final_response（典型：
    agent.max_turns 耗尽的收尾总结）补发成一发 delta —— 与微信通道观感对齐。
    """
    st = task["state"]
    err_text = ""
    tool_seq = 0          # v3.6.1 进度行序号（同名工具多次调用也要追加新行）

    # v3.6.4 心跳 v3（只追加，禁止原位刷新）：App 轮询是 offset 增量协议（offset 只增不减、
    # content 只能追加不能修改/删除）——v3.6.3 的"删旧行+加新行"会位移字符导致 App 端
    # offset 错乱（用户实拍最终消息里出现 💭10s/15s/30s 多行重复）。改为：每 12s 追加一条
    # 短心跳「💭 Ns」，仅当上一条心跳后没有任何真内容时才追加（真内容到达即重置锚点），
    # 心跳行数自然受控（真内容会不断把锚点前移）。收尾不再删除（已送达的字节无法撤回）。
    hb_state = {"start": time.time(), "last_anchor": 0}   # last_anchor=上次心跳时 content 长度
    hb_stop = threading.Event()

    def _hb_collapse():
        pass   # v3.6.4：无操作——追加式心跳不能删除（会破坏 offset 协议）

    def _heartbeat_loop():
        # v3.7.0：💭 进度心跳行已下线（用户要求 AI 回复里不再出现进度显示）——**正文永不写进度**。
        # v3.7.1：线程复用为「进度推送」——只在内容静默且用户不在看时，把已经生成的部分推到
        # 收件箱（task_type="progress" → App 注入 🔔 进度气泡，不进对话上下文：isPush 消息被
        # historyPayload 滤掉）。判定逻辑全在 _progress_tick（纯逻辑，可单测），这里只管定时驱动。
        # ⚠️ 绝不修改 st["content"]：App 轮询是 offset 增量协议，只能追加不能改/删。
        ps = {"last_len": len(st.get("content") or ""), "last_grow": time.time(),
              "last_push": 0, "last_push_len": 0, "pushes": 0}
        while not hb_stop.wait(3):
            try:
                if st.get("status") != "streaming":
                    break          # 已收尾（done/cancelled/error）→ 线程退出，绝不给已结束的任务推"进行中"
                text = _progress_tick(st, ps)
                if not text:
                    continue
                import inbox_api
                ok, msg = inbox_api.push(text, task_id=task_id, task_type="progress")
                print("[progress] push ok=%s %s" % (ok, str(msg)[:120]), flush=True)
            except Exception as e:
                print("[progress] push fail:", str(e)[:200], flush=True)

    hb_thread = threading.Thread(target=_heartbeat_loop, daemon=True)
    hb_thread.start()
    try:
        body = json.dumps(req_body).encode("utf-8")
        target = HERMES_RESPONSES_URL
        req = urllib.request.Request(target, data=body, headers=headers)
        open("/tmp/hermes_worker_debug.log", "a").write(
            "REQ(responses) session=" + headers.get("X-Hermes-Session-Id", "NONE") + " url=" + target + chr(10))
        resp = urllib.request.urlopen(req, timeout=900)
        for raw in resp:
            if task["cancelled"]:
                break
            line = raw.decode("utf-8", "replace").strip()
            if not line.startswith("data: "):   # `event:` 行与心跳注释一律跳过，只认 data 里的 type
                continue
            payload = line[6:]
            if payload == "[DONE]":
                break
            try:
                j = json.loads(payload)
            except Exception:
                continue
            jtype = j.get("type") or ""
            if jtype == "response.created":
                continue   # 事件壳，不算「真内容」
            _hb_collapse()   # 任何真实事件到达 → 折叠心跳行
            if jtype == "response.output_text.delta":
                delta = j.get("delta") or ""
                if not delta:
                    continue
                # v2.0.116 review：内容上限 200k 字符（防无限输出撑爆内存/磁盘）
                if len(st["content"]) < MAX_CONTENT_LEN:
                    st["content"] += delta[:MAX_CONTENT_LEN - len(st["content"])]
                else:
                    st["status"] = "done"
                    break
                now = time.time()
                if now - last_write >= WRITE_INTERVAL:
                    _write_state(task_id, task)
                    last_write = now
            elif jtype == "response.output_item.added":
                # v3.7.0：工具进度行（🔧 xx…）已下线——用户要求 AI 回复里不再出现工具进度显示。
                # 仍统计工具步数（toolSeq），供「进度类追问秒回」的步数摘要使用，但不写进 content。
                item = j.get("item") or {}
                if item.get("type") == "function_call" and item.get("name"):
                    tool_seq += 1
                    st["toolSeq"] = tool_seq
                    # v3.9.15：记下工具名（原先只留步数、名字丢弃）——任务中心卡片与进度推送
                    # 据此显示"在跑什么"。只存英文工具标识（非用户内容），无隐私面。
                    st["lastTool"] = str(item.get("name"))[:40]
                    st["lastToolAt"] = time.time()
                    try:
                        _hist = st.setdefault("toolHistory", [])
                        _hist.append(str(item.get("name"))[:40])
                        if len(_hist) > 20:
                            del _hist[:-20]
                    except Exception:
                        pass
            elif jtype == "response.output_item.done":
                # v3.7.0：工具进度行已下线 → ✅ 回填一并移除（已无进度行需要收尾）
                pass
            elif jtype == "response.output_text.done":
                # 兜底：本轮流里没出现过 delta 时，用 done 事件的整段文本补上
                text = j.get("text") or ""
                if text and not st["content"]:
                    st["content"] = text[:MAX_CONTENT_LEN]
            elif jtype in ("response.failed", "response.incomplete"):
                env = j.get("response") if isinstance(j.get("response"), dict) else {}
                e = env.get("error")
                if isinstance(e, dict):
                    err_text = e.get("message") or ""
                else:
                    err_text = str(e or "")
                if not err_text:
                    err_text = str(env.get("status") or jtype)
    except urllib.error.HTTPError as he:
        # 非 200（401 鉴权 / 400 参数）——body 带进 error 便于自检
        try:
            detail = he.read().decode("utf-8", "replace")[:300]
        except Exception:
            detail = ""
        open("/tmp/hermes_worker_debug.log", "a").write(
            "HTTPERROR(responses): %s %s" % (he.code, detail) + chr(10))
        st["status"] = "error"
        st["error"] = ("HTTP %s %s" % (he.code, detail))[:300]
    except Exception as e:
        open("/tmp/hermes_worker_debug.log", "a").write("ERROR(responses): " + str(e) + chr(10))
        st["status"] = "error"
        st["error"] = str(e)[:300]
    hb_stop.set()          # v3.6.2 停心跳
    hb_thread.join(timeout=2)
    _hb_collapse()         # 收尾：残留的「💭 还在处理中…」折叠成 ✨，不留进最终消息
    if task["cancelled"]:
        st["status"] = "cancelled"
    elif err_text and not st["content"]:
        st["status"] = "error"
        st["error"] = str(err_text)[:300]
    else:
        st["status"] = "done"
    _write_state(task_id, task)
    _maybe_push(st)
    _maybe_push_app(st, task_id)
    _maybe_push_app_later(task_id, task)   # v3.6.1 延迟复检：内容没送达就补推


# ==== iOS 2.0 Safari relay aliases (/r/stream/* -> /api/stream/*) ====
_RELAY_OPS = {"start", "stop", "poll"}

def _relay_alias(path):
    # /r/stream/start/{uid} -> /api/stream/start
    # /r/stream/stop/{uid}/{taskId} -> /api/stream/{taskId}/stop
    # /r/stream/poll/{uid}/{taskId}/{offset} -> /api/stream/{taskId}?offset={offset}
    parts = path.split("/")
    if len(parts) >= 4 and parts[1] == "r" and parts[2] == "stream" and parts[3] in _RELAY_OPS:
        op = parts[3]
        if op == "start":
            return "/api/stream/start"
        elif op == "stop" and len(parts) >= 6:
            return f"/api/stream/{parts[5]}/stop"
        elif op == "poll" and len(parts) >= 7:
            return f"/api/stream/{parts[5]}?offset={parts[6]}"
    return path

class StreamHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, Authorization, X-Stream-Password, X-Auth-Token")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, Authorization, X-Stream-Password, X-Auth-Token")
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def do_POST(self):
        if self.path.startswith("/r?") or self.path == "/r":
            _relay_query(self)
            return
            return
        self.path = _relay_alias(self.path)
        # v3.9.32：模型管理三端点（provider_admin 模块一直随包部署，却从未被 import →
        #          App 的「删除内置 provider / 自定义 provider / 拉取模型」三处入口必然 404）
        if self.path.split("?", 1)[0] in ("/api/stream/builtin-providers",
                                          "/api/stream/custom-providers",
                                          "/api/stream/fetch-models"):
            if not _auth(self):
                return self._send(401, {"error": "unauthorized"})
            try:
                n = int(self.headers.get("Content-Length", 0))
                dat = json.loads(self.rfile.read(n) or b"{}")
            except Exception:
                return self._send(400, {"error": "bad json"})
            import provider_admin as _pa
            _code, _obj = _pa.handle_post(self.path.split("?", 1)[0], dat)
            return self._send(_code, _obj)
        # /api/stream/ingest: 内部流式帧回传（Hermes qingliao native streaming → 写 task 增量）
        if self.path == "/api/stream/ingest":
            try:
                n = int(self.headers.get("Content-Length", 0))
                dat = json.loads(self.rfile.read(n) or b"{}")
            except Exception:
                return self._send(400, {"error": "bad json"})
            tk = (self.headers.get("X-Inbox-Token") or "").strip()
            # v3.9.41（B3）原来只判非空，垃圾 token 都能过；该分支写别人会话的流式正文。
            # 同文件 /api/tasks/bg 用的是 compare_digest，这里补齐同一口径。
            if not tk or not hmac.compare_digest(tk, INBOX_TOKEN_ENV):
                return self._send(401, {"error": "unauthorized"})
            chat_id = str(dat.get("chat_id", ""))
            delta = str(dat.get("delta", ""))
            finalize = bool(dat.get("finalize", False))
            task_id = None
            with _ql_stream_lock:
                task_id = _ql_stream_task.get(chat_id)
            if task_id:
                task = _tasks.get(task_id)
                if task:
                    st = task["state"]
                    cur = st.get("content", "")
                    if len(cur) < MAX_CONTENT_LEN:
                        st["content"] = cur + delta[:MAX_CONTENT_LEN - len(cur)]
                    if finalize:
                        st["status"] = "done"
                    _write_state(task_id, task)
            return self._send(200, {"ok": True})
        # v3.4.25 后台作业进度登记（Hermes 派子代理/后台任务时上报，任务中心可见进度）
        # 鉴权同 /api/stream/ingest：X-Inbox-Token 服务间 token
        if self.path == "/api/tasks/bg":
            try:
                n = int(self.headers.get("Content-Length", 0))
                dat = json.loads(self.rfile.read(n) or b"{}")
            except Exception:
                return self._send(400, {"error": "bad json"})
            tk = (self.headers.get("X-Inbox-Token") or "").strip()
            if not tk or not hmac.compare_digest(tk, INBOX_TOKEN_ENV):
                return self._send(401, {"error": "unauthorized"})
            action = str(dat.get("action", "register"))
            title = str(dat.get("title", "")).strip()
            job_id = str(dat.get("jobId", "")).strip() or None
            detail = dat.get("detail")
            result = dat.get("result")
            if action == "register":
                if not title:
                    return self._send(400, {"error": "title required"})
                jid = bg_register(title, job_id=job_id, detail=str(detail or ""))
                return self._send(200, {"ok": True, "jobId": jid})
            # update / finish
            if not job_id:
                return self._send(400, {"error": "jobId required"})
            status = str(dat.get("status", "")).strip() or None
            ok = bg_update(job_id, status=status, detail=detail, result=result)
            return self._send(200, {"ok": ok})
        if not _auth(self):
            return self._send(401, {"error": "unauthorized"})
        try:
            n = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return self._send(400, {"error": "bad json"})

        if self.path == "/api/stream/start":
            session_id = str(data.get("sessionId", ""))
            model = str(data.get("model", "deepseek-v4-flash"))
            messages = data.get("messages")
            push_enabled = bool(data.get("pushEnabled", False))  # V1.4 微信推送开关
            agent_enabled = bool(data.get("agentEnabled", True))  # v2.0.98 Agent 开关（设置页可关）
            provider = str(data.get("provider", "") or "")  # V1.5.3 模型精确路由（9123 需 provider 才不回退默认）
            # v3.6.5：App「模型思考胶囊」下发的思考档位（none/low/medium/high）——空则用服务端默认
            reasoning = str(data.get("reasoning", "") or "").strip().lower()
            # v3.2.7（方案C）：bot 模式已废除，不再读取 bot 字段（App 若仍传则被忽略，向后兼容）
            if not messages or not isinstance(messages, list):
                return self._send(400, {"error": "messages required"})
            task_id = uuid.uuid4().hex[:12]
            task = {
                "cancelled": False,
                "lock": threading.Lock(),
                "state": {
                    "sessionId": session_id,
                    "model": model,
                    "messages": messages,
                    "content": "",
                    "status": "streaming",
                    "pushEnabled": push_enabled,
                    "agentEnabled": agent_enabled,
                    "provider": provider,
                    "reasoning": reasoning,   # v3.6.5 思考档位（App 可调）
                    "createdAt": time.time(),
                    "updatedAt": time.time()
                }
            }
            with _tasks_lock:
                _tasks[task_id] = task
            threading.Thread(target=_worker, args=(task_id, task), daemon=True).start()
            return self._send(200, {"taskId": task_id})

        # stop
        if self.path.startswith("/api/stream/") and self.path.endswith("/stop"):
            task_id = self.path.split("/")[3]
            with _tasks_lock:
                task = _tasks.get(task_id)
            if not task:
                return self._send(404, {"error": "no such task"})
            task["cancelled"] = True
            return self._send(200, {"ok": True})

        # 服务控制（看板运维：重试/停止轻聊后端，V2.0）
        # v3.0.36：+ hermes 网关重启（service=hermes → channel_api._restart_gateway），仅 restart 不支持 stop
        if self.path.startswith("/api/nas/service/"):
            action = self.path.split("/")[-1]
            svc = str(data.get("service", "qingliao"))
            if action not in ("restart", "stop"):
                return self._send(400, {"error": "invalid action"})
            if svc == "hermes":
                if action == "stop":
                    return self._send(400, {"error": "hermes 不支持停止"})
                try:
                    import channel_api
                    channel_api._restart_gateway()
                    return self._send(200, {"ok": True, "action": action, "service": "hermes",
                                            "note": "gateway 重启触发（约 10-30 秒生效）"})
                except Exception as e:
                    return self._send(200, {"ok": False, "action": action, "service": "hermes",
                                            "error": str(e)[:150]})
            if svc != "qingliao":
                return self._send(400, {"error": "invalid service"})
            # v3.4.10：轻聊后端=docker 容器 qingliao → restart/stop 操作容器
            # （独立进程执行，start_new_session 防信号连带杀死请求线程）
            subprocess.Popen(
                ["docker", "restart" if action == "restart" else "stop", "qingliao"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True
            )
            return self._send(200, {"ok": True, "action": action, "service": svc})

        # v3.0.68：文本转语音（云端神经 TTS）——按 provider 分发（xiaomi mimo / zai glm-tts）
        # App 把 text 出 POST，后端按 provider+model 调对应厂商，返回 base64 音频
        if self.path == "/api/tts":
            text = str(data.get("text", ""))
            voice = str(data.get("voice", "") or "mimo_default")
            provider = str(data.get("provider", "") or "xiaomi")
            model = str(data.get("model", "") or "mimo-v2.5-tts")
            if not text.strip():
                return self._send(400, {"ok": False, "error": "text required"})
            try:
                # 阶跃 StepFun TTS（stepaudio-2.5-tts）：POST {base}/audio/speech，
                # body {model,input,voice,response_format}；非流式返回原始音频字节 → base64
                if provider == "stepfun":
                    key = _load_cfg_key(["providers", "stepfun", "api_key"])
                    if not key:
                        return self._send(500, {"ok": False, "error": "stepfun api_key 未配置"})
                    base = _provider_base_url("stepfun") or "https://api.stepfun.com/step_plan/v1"
                    if voice in ("", "mimo_default"):
                        voice = "cixingnansheng"
                    payload = {
                        "model": model or "stepaudio-2.5-tts",
                        "input": text,
                        "voice": voice,
                        "response_format": "mp3",
                    }
                    req = urllib.request.Request(
                        base.rstrip("/") + "/audio/speech",
                        data=json.dumps(payload).encode("utf-8"),
                        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=60) as r:
                        audio_bytes = r.read()
                    data_b64 = base64.b64encode(audio_bytes).decode("ascii")
                    return self._send(200, {"ok": True, "audio": data_b64, "format": "mp3"})
                # 智谱 glm-tts：POST {base}/audio/speech，body {model,input,voice,response_format:wav}
                # 非流式 wav 返回原始音频字节 → 转 base64
                if provider == "zai":
                    key = _load_cfg_key(["providers", "zai", "api_key"])
                    if not key:
                        return self._send(500, {"ok": False, "error": "zai api_key 未配置"})
                    base = _provider_base_url("zai") or "https://open.bigmodel.cn/api/paas/v4"
                    # fix：voice 合法 id 只有 female/male（中文展示名会 400）；默认兜底 female
                    if voice in ("", "mimo_default", "彤彤"):
                        voice = "female"
                    payload = {
                        "model": model or "glm-tts",
                        "input": text,
                        "voice": voice,
                        "response_format": "wav",
                    }
                    req = urllib.request.Request(
                        base.rstrip("/") + "/audio/speech",
                        data=json.dumps(payload).encode("utf-8"),
                        headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=60) as r:
                        audio_bytes = r.read()
                    data_b64 = base64.b64encode(audio_bytes).decode("ascii")
                    return self._send(200, {"ok": True, "audio": data_b64, "format": "wav"})
                # 默认：xiaomi mimo-v2.5-tts（chat/completions + audio 合约）
                key = _load_cfg_key(["providers", "xiaomi", "api_key"])
                if not key:
                    return self._send(500, {"ok": False, "error": "xiaomi api_key 未配置"})
                base = _provider_base_url("xiaomi") or "https://token-plan-cn.xiaomimimo.com/v1"
                payload = {
                    "model": model or "mimo-v2.5-tts",
                    "messages": [
                        {"role": "user", "content": "用自然、清晰的语气朗读内容。"},
                        {"role": "assistant", "content": text},
                    ],
                    "audio": {"format": "wav", "voice": voice},
                }
                req = urllib.request.Request(
                    base.rstrip("/") + "/chat/completions",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=60) as r:
                    resp = json.loads(r.read().decode("utf-8"))
                audio = (resp.get("choices") or [{}])[0].get("message", {}).get("audio", {})
                data_b64 = audio.get("data", "")
                if not data_b64:
                    return self._send(500, {"ok": False, "error": "TTS 未返回音频"})
                return self._send(200, {"ok": True, "audio": data_b64, "format": "wav"})
            except urllib.error.HTTPError as e:
                try:
                    err = e.read().decode("utf-8", errors="replace")[:300]
                except Exception:
                    err = str(e)
                return self._send(500, {"ok": False, "error": f"{provider} tts {e.code}: {err}"})
            except Exception as e:
                return self._send(500, {"ok": False, "error": str(e)[:200]})

        return self._send(404, {"error": "not found"})

    def do_GET(self):
        # v2.0.130：免鉴权 AI 图片端点（App 渲染 MEDIA: 路径时加载）——只允许 /data/hermes(hermes-data) 下图片
        if self.path.startswith("/api/stream/media"):
            _serve_media(self)
            return
        if self.path.startswith("/r?") or self.path == "/r":
            _relay_query(self)
            return
        self.path = _relay_alias(self.path)
        if not _auth(self):
            return self._send(401, {"error": "unauthorized"})
        # v3.9.32：自定义 provider 列表（App「新增 API」入口读这个）
        if self.path.split("?", 1)[0] == "/api/stream/custom-providers":
            import provider_admin as _pa
            return self._send(200, _pa.custom_list())
        # v3.4.23 任务中心：进行中任务列表（流式任务 streaming 中 + 登记的后台作业）
        if self.path.startswith("/api/tasks/active") or self.path.startswith("/api/agent/tasks/active"):
            return self._send(200, _collect_active_tasks())
        # V1.7.3：返回全部 provider 及可选模型聚合（app 通用渲染 + 新 provider 免改版）
        # v3.0.60：并发拉取各 provider 模型（串行7个provider×10s超时=70s挂起根因）
        if self.path.startswith("/api/stream/model-providers"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            with_models = q.get("with_models", ["0"])[0] == "1"
            providers = []
            if with_models:
                from concurrent.futures import ThreadPoolExecutor, as_completed
                def _fetch_provider(pid):
                    url, keypath = SYNC_ENDPOINTS[pid]
                    api_key = _load_cfg_key(keypath)
                    models = []
                    if api_key:
                        try:
                            req = urllib.request.Request(url, headers={"Authorization": "Bearer " + api_key, "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
                            resp = urllib.request.urlopen(req, timeout=5)
                            data = json.loads(resp.read().decode("utf-8", "replace"))
                            models = [m.get("id") for m in data.get("data", []) if isinstance(m, dict) and m.get("id")]
                        except Exception:
                            models = []
                    return {"id": pid, "models": models}
                with ThreadPoolExecutor(max_workers=7) as pool:
                    futures = {pool.submit(_fetch_provider, pid): pid for pid in SYNC_ENDPOINTS}
                    for f in as_completed(futures, timeout=12):
                        try:
                            providers.append(f.result())
                        except Exception:
                            providers.append({"id": futures[f], "models": []})
            else:
                for pid in SYNC_ENDPOINTS:
                    providers.append({"id": pid})
            return self._send(200, {"ok": True, "providers": providers})
        # V1.5.9：同步 provider 模型列表（调各官方 /v1/models）
        if self.path.startswith("/api/stream/sync-models"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            provider = q.get("provider", [""])[0]
            if provider not in SYNC_ENDPOINTS:
                return self._send(400, {"error": "unsupported provider"})
            url, keypath = SYNC_ENDPOINTS[provider]
            api_key = _load_cfg_key(keypath)
            if not api_key:
                return self._send(200, {"ok": False, "provider": provider, "error": "provider key 未配置", "models": []})
            try:
                req = urllib.request.Request(url, headers={"Authorization": "Bearer " + api_key, "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"})
                resp = urllib.request.urlopen(req, timeout=10)   # v2.0.87au：中转页超时缩短，弹窗更快收起
                data = json.loads(resp.read().decode("utf-8", "replace"))
                ids = [m.get("id") for m in data.get("data", []) if isinstance(m, dict) and m.get("id")]
                return self._send(200, {"ok": True, "provider": provider, "models": ids})
            except Exception as e:
                return self._send(200, {"ok": False, "provider": provider, "error": str(e)[:150], "models": []})
        # V1.7.2：NAS 面板状态（宿主系统 + 服务健康）
        if self.path.startswith("/api/nas/status"):
            return self._send(200, _collect_nas_status())
        # v3.0.36：模型使用量聚合（deepseek/stepfun 官方余额；无接口 provider 标 unsupported）
        if self.path.startswith("/api/nas/providers-usage"):
            try:
                import usage_api
                return self._send(200, usage_api.collect_usage())
            except Exception as e:
                return self._send(200, {"ok": False, "error": str(e)[:150], "providers": []})
        # v3.0.18：设备一键体检（六维诊断）
        if self.path.startswith("/api/nas/diagnose"):
            return self._send(200, _collect_diagnose())
        # V1.5.2：模型可用性探测（分组快捷切换状态标注用）——内部调 Hermes 验证
        if self.path.startswith("/api/stream/check-model"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            model = q.get("model", [""])[0]
            provider = q.get("provider", [""])[0]  # V1.5.6：带 provider 探测，避免 fallback 假绿灯
            if not model:
                return self._send(400, {"error": "model required"})
            try:
                req_body = {
                    "model": model,
                    "messages": [{"role": "user", "content": "ok"}],
                    "max_tokens": 1,
                    "stream": False
                }
                if provider:
                    req_body["provider"] = provider
                body = json.dumps(req_body).encode("utf-8")
                req = urllib.request.Request(HERMES_URL, data=body, headers={
                    "Authorization": "Bearer " + HERMES_KEY,
                    "Content-Type": "application/json"
                })
                resp = urllib.request.urlopen(req, timeout=30)
                data = json.loads(resp.read().decode("utf-8", "replace"))
                ok = bool(data.get("choices"))
                err_hint = ""
                if ok:
                    # V1.5.7：Hermes 会把上游错误（如 xiaomi 401）包装成 200+choices，
                    # choices 内容即错误文本——必须过滤，否则假绿灯
                    c = ""
                    try:
                        c = data["choices"][0]["message"].get("content", "") or ""
                    except Exception:
                        c = ""
                    if any(k in c for k in ["Invalid API Key", "invalid_key", "Unauthorized", "401", "403", "insufficient", "无权限", "余额不足"]):
                        ok = False
                        err_hint = c[:100]
                return self._send(200, {"ok": ok, "model": model, "error": err_hint})
            except Exception as e:
                return self._send(200, {"ok": False, "model": model, "error": str(e)[:150]})
        # 恢复接口：按 sessionId 找进行中/刚完成的任务（内存优先，磁盘兜底）
        if self.path.startswith("/api/stream/recover"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            session_id = q.get("sessionId", [""])[0]
            if not session_id:
                return self._send(400, {"error": "sessionId required"})
            # 1) 内存中查找（v3.5.2 fix：同会话可能有多条任务，必须取 createdAt 最新的一条）
            #    旧实现 `for tid, t in _tasks.items()` 取首个命中 = dict 插入序 = 最旧任务，
            #    会把早已完成的旧回答当"在途任务"返回，App 回前台 recover 时旧答案被复活（复读事故根因）。
            with _tasks_lock:
                _cand = None   # (createdAt, tid)；createdAt 缺失时退化为插入序最后一条
                for _tid, _t in _tasks.items():
                    if _t["state"].get("sessionId") != session_id:
                        continue
                    try:
                        _ts = float(_t["state"].get("createdAt") or 0)
                    except Exception:
                        _ts = 0
                    if _cand is None or _ts >= _cand[0]:
                        _cand = (_ts, _tid)
                if _cand is not None:
                    _tid = _cand[1]
                    st = _tasks[_tid]["state"]
                    return self._send(200, {
                        "taskId": _tid,
                        "content": st.get("content", ""),
                        "done": st["status"] != "streaming",
                        "status": st["status"],
                        # v3.9.14：纯增量字段，语义显式化（App 不用也无害）
                        "isWorking": st["status"] == "streaming",
                        "error": st.get("error", ""),
                        "fromMemory": True
                    })
            # 2) 磁盘兜底：扫描 streams/*.json 按 sessionId 匹配（v3.5.2 fix：同样取最新一条）
            try:
                if os.path.isdir(STREAM_DIR):
                    _best = None   # (createdAt, task_id, st)；createdAt 缺失时用文件 mtime 兜底
                    for fn in sorted(os.listdir(STREAM_DIR)):
                        if not fn.endswith(".json"):
                            continue
                        fp = os.path.join(STREAM_DIR, fn)
                        try:
                            with open(fp, encoding="utf-8") as f:
                                st = json.load(f)
                        except Exception:
                            continue
                        if st.get("sessionId") != session_id:
                            continue
                        try:
                            _ts = float(st.get("createdAt") or 0)
                        except Exception:
                            _ts = 0
                        if not _ts:
                            try:
                                _ts = os.path.getmtime(fp)
                            except Exception:
                                _ts = 0
                        if _best is None or _ts >= _best[0]:
                            _best = (_ts, fn[:-5], st)
                    if _best is not None:
                        _bid, _bst = _best[1], _best[2]
                        # v3.9.14 fix：done 曾硬编码 True，与文件里记录的真实 status 自相矛盾——
                        # 任务被容器重启/清理切断后 status 仍可能是 streaming，App 收到 done=true
                        # 会把半截内容当「最终答案」落库。改为与内存分支同一口径（按 status 算）。
                        _bst_status = str(_bst.get("status", "done") or "done")
                        return self._send(200, {
                            "taskId": _bid,
                            "content": _bst.get("content", ""),
                            "done": _bst_status != "streaming",
                            "status": _bst_status,
                            "isWorking": _bst_status == "streaming",
                            "error": _bst.get("error", ""),
                            "agent": _bst.get("agent", False),
                            "fromDisk": True
                        })
            except Exception:
                pass
            return self._send(200, {"taskId": None, "content": "", "done": True, "status": "none"})

        # /api/stream/{taskId}?offset=N
        if self.path.startswith("/api/stream/"):
            rest = self.path[len("/api/stream/"):].split("?", 1)
            task_id = rest[0]
            offset = 0
            if len(rest) > 1:
                for kv in rest[1].split("&"):
                    if kv.startswith("offset="):
                        try:
                            offset = int(kv[7:])
                        except Exception:
                            offset = 0
            with _tasks_lock:
                task = _tasks.get(task_id)
            if not task:
                return self._send(404, {"error": "no such task"})
            st = task["state"]
            st["lastPollAt"] = time.time()  # V1.4：记录用户轮询时间（推送判定用）
            content = st["content"]
            new = content[offset:] if offset < len(content) else ""
            # v3.6.1：记录 App 真实取到的内容位置（推送闸门按「是否真送达」判定，
            # 取代只看轮询时间戳的旧推断——用户退出流式页后前台轮询仍在刷 lastPollAt）
            try:
                _dl = offset + len(new)
                if _dl > (st.get("deliveredLen") or 0):
                    st["deliveredLen"] = _dl
            except Exception:
                pass
            # v3.4.23 推送滞后根治：搭载投递（piggyback）——App 轮询流式内容时顺带捎上
            # 收件箱待推消息。App 流式中 0.15-0.25s 高频轮询、前台 5s 一轮，
            # 搭在已有请求上，无需 App 增加任何请求即可把推送延迟从最坏 5s 压到 0。
            inbox_payload = []
            try:
                import inbox_api
                pending = inbox_api.peek_pending()
                if pending:
                    inbox_payload = pending
                    for it in pending:
                        inbox_api.mark_sending(it["id"])
            except Exception:
                pass
            # v3.9.16：带上工具事件，供 App 在对话里画工具卡（纯增量字段，旧版 App 忽略）
            #   —— 工具名一律给中文（App 不必再维护一份映射表，与 _TOOL_NAME_ZH 保持一处真相）
            #   —— 只回最近 10 个，防长任务把响应撑大（轮询频率 0.15-0.25s，体积要克制）
            _th = [str(x) for x in (st.get("toolHistory") or [])][-10:]
            return self._send(200, {
                "content": new,
                "done": st["status"] != "streaming",
                "status": st["status"],
                "sessionId": st["sessionId"],
                "error": st.get("error", ""),
                "agent": st.get("agent", False),   # v2.0.98：Agent 回复标记（设置页开关关闭时恒 false）
                "inbox": inbox_payload,             # v3.4.23：搭载的收件箱待推消息（可为空）
                "toolSeq": int(st.get("toolSeq") or 0),
                "lastTool": _TOOL_NAME_ZH.get(str(st.get("lastTool") or ""),
                                              str(st.get("lastTool") or "")),
                "toolNames": [_TOOL_NAME_ZH.get(x, x) for x in _th],
                "lastToolAt": st.get("lastToolAt") or 0
            })
        return self._send(404, {"error": "not found"})


def _serve_media(self):
    """v2.0.130：免鉴权图片服务——MEDIA:路径 → 图片字节。
    v3.0.28 security note：免鉴权设计（App 本地 localhost 调用），白名单限制只读允许目录下的图片扩展名。
    query: p=<base64url(宿主绝对路径)>；仅允许图片扩展名 + 容器 /data/hermes 映射目录。
    """
    import urllib.parse as _up
    q = _up.parse_qs(_up.urlparse(self.path).query)
    raw = q.get("p", [""])[0]
    if not raw:
        self._send(400, {"error": "missing p"})
        return
    try:
        b64 = raw.replace("-", "+").replace("_", "/")
        rem = len(b64) % 4
        if rem:
            b64 += "=" * (4 - rem)
        path = base64.b64decode(b64.encode()).decode("utf-8")
    except Exception:
        self._send(400, {"error": "bad p"})
        return
    # 容器路径 → 宿主路径（App 直接编码 MEDIA: 里的容器路径）
    for _pre, _host in [(os.environ.get("QL_DATA_DIR", "/data"), os.environ.get("QL_HERMES_DATA_DIR", "/data/hermes")),
                        ("/opt/hermes_host", os.environ.get("QL_HERMES_ROOT", "/data/hermes"))]:
        if path == _pre or path.startswith(_pre + "/"):
            path = _host + path[len(_pre):]
            break
    # 只允许 hermes-data(/data/hermes) 与轻聊 data 目录下的图片（防任意文件读取）
    allowed = [os.environ.get("QL_HERMES_DATA", "/data/hermes"), os.environ.get("QL_DATA_DIR", "/data")]
    if not any(path.startswith(a) for a in allowed):
        self._send(403, {"error": "forbidden path"})
        return
    ext = os.path.splitext(path)[1].lower()
    # v3.9.17：从「只允许图片」扩到「图片 + 文档/文本」——AI 生成物（报告 PDF、数据 CSV、
    # markdown 笔记、日志）也要能在 App 里打开（App 侧走 QuickLook）。
    # 刻意**不开放** .html/.svg：这两种能在 WebView 里跑脚本/内联事件，免鉴权接口暴露它们
    # 等于自己开一个 XSS 面（App 对 HTML 一律按纯文本预览）；也不开放可执行文件与压缩包，
    # 避免把数据目录变成任意文件下载口。
    ctype = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
             ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp",
             ".heic": "image/heic",
             ".pdf": "application/pdf",
             ".txt": "text/plain; charset=utf-8", ".md": "text/plain; charset=utf-8",
             ".csv": "text/csv; charset=utf-8", ".json": "application/json; charset=utf-8",
             ".log": "text/plain; charset=utf-8"}.get(ext)
    if not ctype or not os.path.isfile(path):
        self._send(404, {"error": "not found"})
        return
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError:
        self._send(404, {"error": "read fail"})
        return
    self.send_response(200)
    self.send_header("Content-Type", ctype)
    # v3.9.17：带上文件名——iOS QuickLook 与分享面板靠它定类型/显示名（原先只有裸字节流）
    try:
        _fn = os.path.basename(path).replace(chr(34), "")
        self.send_header("Content-Disposition",
                         "inline; filename*=UTF-8''%s" % _up.quote(_fn))
    except Exception:
        pass
    self.send_header("Cache-Control", "public, max-age=3600")
    self._cors() if hasattr(self, "_cors") else None
    self.send_header("Content-Length", str(len(data)))
    self.end_headers()
    try:
        self.wfile.write(data)
    except Exception:
        pass


def _relay_query(self):
    """Query-version Safari relay: /r?r=<base64url({m,p,h,b})>
    Decode payload, forward to internal nginx, 302 back qingliao://relay?r=<b64({s,b})>"""
    import urllib.parse as _up
    q = _up.parse_qs(_up.urlparse(self.path).query)
    raw = q.get("r", [""])[0]
    if not raw:
        _relay_reply(self, 400, "missing r")
        return
    try:
        b64 = raw.replace("-", "+").replace("_", "/")
        rem = len(b64) % 4
        if rem:
            b64 += "=" * (4 - rem)
        payload = json.loads(base64.b64decode(b64).decode("utf-8"))
        method = (payload.get("m") or "GET").upper()
        path = payload.get("p") or "/"
        headers = payload.get("h") or {}
        body = payload.get("b")
        if body is not None:
            body = body.encode("utf-8")
        if not path.startswith("/"):
            path = "/" + path
        # /r/ping -> direct pong (testRelay)
        if path == "/r/ping":
            _relay_reply(self, 200, "pong")
            return
        # 蜂窝 relay 白名单（v3.0.6 security review：原允许任意 /api/* 造成认证绕过链。
        # 收窄到 App 蜂窝真正会用到的接口；每个接口仍独立校验 X-Auth-Token（下游鉴权不降级））
        ALLOWED_RELAY = (
            "/api/stream/", "/api/auth/", "/api/sessions/",
            "/api/local/", "/api/weather", "/api/push/", "/api/scenes",
            "/api/automation", "/api/memory", "/api/kb", "/api/docker",
            "/api/secrets", "/api/ha/", "/api/cron", "/api/files", "/api/logs",
            "/api/router/", "/api/agent", "/api/tasks", "/api/mcp",
            "/api/diag",
            "/api/life",
            "/api/nas/", "/api/hw/", "/api/channel/",
            "/api/inbox", "/api/history", "/api/tts",
        )
        if not (path.startswith(ALLOWED_RELAY) or path.startswith("/r/")):
            _relay_reply(self, 403, "forbidden")
            return
        url = "http://127.0.0.1:16668" + path
        req = urllib.request.Request(url, data=body, method=method)
        for k, v in headers.items():
            if k.lower() in ("host", "content-length", "connection"):
                continue
            req.add_header(k, v)
        req.add_header("X-Stream-Password", STREAM_PASS)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:   # v2.0.87au：中转页超时缩短，弹窗更快收起
                data = resp.read()
                status = resp.status
        except urllib.error.HTTPError as e:
            data = e.read()
            status = e.code
        except Exception as e:
            _relay_reply(self, 500, "relay upstream: " + str(e)[:200])
            return
        _relay_reply(self, status, data.decode("utf-8", errors="replace"))
    except Exception as e:
        _relay_reply(self, 500, "relay error: " + str(e)[:200])

def _relay_reply(self, status, body):
    resp = json.dumps({"s": status, "b": body}, ensure_ascii=False).encode("utf-8")
    b64 = base64.b64encode(resp).decode("ascii").replace("+", "-").replace("/", "_").rstrip("=")
    self.send_response(302)
    self.send_header("Location", "qingliao://relay?r=" + b64)
    self.send_header("Content-Length", "0")
    self.end_headers()




def cleanup_old_tasks():
    """定期清理过期任务（内存）"""
    # v3.9.28 fix（App 侧"点会话自动变发送态"根因）：streaming 的任务/文件此前被
    # "一律跳过"，worker 若异常死掉（HTTP 900s 超时后的残留断连、进程内部状态错乱），
    # 状态永远停在 streaming → 僵尸任务。App 端 probeRemoteBusy 每次点开该会话都会
    # recover 到它（done=false, status=streaming）→ adoptRemoteStream 误接回 →
    # AI 凭空"自动发送"。护栏：streaming 但静默（updatedAt 不刷新）超过 30 分钟
    # → 判死，标 error，进入正常回收。30 分钟 = 上游 HTTP timeout=900s（15 分钟）
    # 的 2 倍余量，真任务单请求不会静默超过它；v3.9.28 首版 7200s 太宽，实测期间
    # 又出现一条僵尸挂着灵动岛 100 分钟才等到回收（用户拍板收紧）。
    ZOMBIE_SILENT = 1800
    while True:
        time.sleep(300)
        now = time.time()
        with _tasks_lock:
            for tid in list(_tasks.keys()):
                t = _tasks[tid]
                st = t["state"]
                if st["status"] != "streaming" and now - st["updatedAt"] > TASK_TTL:
                    del _tasks[tid]
                elif st["status"] == "streaming" and now - st["updatedAt"] > ZOMBIE_SILENT:
                    st["status"] = "error"
                    st["error"] = "任务静默超时（worker 异常终止）"
                    del _tasks[tid]
        # 清理 streams 目录里超过 2 小时的文件
        # v3.9.14 fix：原来只看文件 mtime、不看里面记录的 status。长任务在模型思考期可能
        # 静默 >2h（本仓实测出现过 1.5 小时零输出），期间不写盘、mtime 不刷新 → 一个**仍在
        # streaming 的任务文件被当垃圾删掉**，之后 App 回前台 /api/stream/recover 的磁盘兜底
        # 再也找不到它（内容丢失）。改为先读 status：streaming 的一律跳过。
        try:
            if os.path.isdir(STREAM_DIR):
                for fn in os.listdir(STREAM_DIR):
                    if not fn.endswith(".json"):
                        continue
                    fp = os.path.join(STREAM_DIR, fn)
                    try:
                        if time.time() - os.path.getmtime(fp) <= 7200:
                            continue
                        try:
                            with open(fp, encoding="utf-8") as _f:
                                _st = json.load(_f)
                        except Exception:
                            _st = None
                        if isinstance(_st, dict) and _st.get("status") == "streaming":
                            # v3.9.28：streaming 不再"一律跳过"——静默超 30 分钟（= 2×上游
                            # timeout 900s）即僵尸，直接删，防 App recover 误接回（"自动发送"根因）
                            if now - os.path.getmtime(fp) > ZOMBIE_SILENT:
                                os.remove(fp)
                            continue
                        os.remove(fp)
                    except Exception:
                        pass
        except Exception:
            pass


# 模块级启动清理线程（import 与 __main__ 两条路径都覆盖，防重复）
_start_cleanup()


if __name__ == "__main__":
    from http.server import ThreadingHTTPServer
    srv = ThreadingHTTPServer(("0.0.0.0", 9132), StreamHandler)
    print("[stream] listening on 9132, dir:", STREAM_DIR, flush=True)
    srv.serve_forever()

