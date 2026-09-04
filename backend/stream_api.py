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
上游：http://127.0.0.1:9123/v1/chat/completions（Hermes 容器 docker-proxy）
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
WRITE_INTERVAL = 0.3  # 写盘节流
TASK_TTL = 1800       # 任务完成后内存保留 30 分钟
# V1.4 微信接力推送：回复完成 → webhook deliver-only → 微信
WEBHOOK_URL = os.environ.get("STREAM_WEBHOOK_URL", os.environ.get("STREAM_WEBHOOK_URL", "http://localhost:8644/webhooks/qingliao-push"))
WEBHOOK_SECRET = os.environ.get("STREAM_WEBHOOK_SECRET", "")
PUSH_IDLE_SECONDS = 30  # 用户超过 30s 未轮询才推送（在看的用户不打扰）

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
    "zai": ("https://open.bigmodel.cn/api/paas/v4/models", ["providers", "zai", "api_key"]),
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
                     "/volume1", "/volume2", "/volume3"]
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
    try:
        import subprocess
        r = subprocess.run(["systemctl", "is-active", "qingliao.service"], capture_output=True, text=True, timeout=8)
        services["qingliao"] = r.stdout.strip() == "active"
        p = subprocess.run(["systemctl", "show", "qingliao.service", "-p", "MainPID", "--value"], capture_output=True, text=True, timeout=8)
        services["qingliao_mem"] = _proc_rss(p.stdout.strip())
    except Exception:
        services["qingliao"] = None
        services["qingliao_mem"] = None
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
            f"Hermes 容器异常，等看门狗自动重启，或 docker restart {os.environ.get('QL_HERMES_CONTAINER', 'hermes-container')}")

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


# ---- v2.0.96 方案B：Agent（工具调用循环）----

AGENT_URL = os.environ.get("QL_AGENT_URL", "https://api.deepseek.com/v1/chat/completions")
AGENT_KEY = os.environ.get("QL_AGENT_KEY", "")   # 优先 env，否则读 config.yaml 的 providers.deepseek.api_key
AGENT_MODEL = os.environ.get("QL_AGENT_MODEL", "deepseek-chat")
# 强意图（控制/执行类，命中即走 Agent）vs 弱意图（查询类，需动词+主题双命中，防闲聊误伤）
AGENT_STRONG = ("控制", "开关", "启动", "停止", "重启", "关灯", "开灯", "打开", "关闭", "清理", "清空", "执行", "设置", "布防", "离家", "空调", "风扇", "灯", "排气扇")
AGENT_VERBS = ("查", "看", "问", "多少", "怎么样", "状态", "情况", "使用率", "帮我", "运行", "温度")
AGENT_TOPICS = ("天气", "磁盘", "容器", "服务", "内存", "空间", "温度", "系统")


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
        if any(h in c for h in strong):
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
        base = _provider_base_url("ollama") or os.environ.get("QL_OLLAMA_URL", "http://localhost:11434/v1")
        key = _load_cfg_key(["providers", "ollama", "api_key"]) or "ollama"
        return base + "/chat/completions", key, model
    return AGENT_URL, AGENT_KEY, AGENT_MODEL


def _agent_loop(messages, task=None, model=None, provider=None):
    """工具调用循环：模型返回 tool_calls → 执行 → 回填 → 再问（上限 8 轮）
    v2.0.116 review：支持中途取消（task.cancelled）——原循环不读取消标志，用户 stop 最长 20 分钟无效
    v3.0.30 fix：model/provider 参数——Agent 分流使用设置页选定的模型（原恒用 AGENT_MODEL 默认 deepseek-chat）
    v3.1.6 fix：返回 (content, enriched_msgs) 元组——保留完整工具调用历史供后续请求上下文"""
    url, key, eff_model = _agent_endpoint(model, provider)
    try:
        import tool_executor
    except Exception as e:
        return f"Agent 工具模块不可用：{e}", []
    try:
        msgs = [m for m in messages if isinstance(m, dict) and m.get("role") != "system"]
        import agent_rules
        hint = agent_rules.rules_hint()
        sys_content = (
            "你是轻聊 AI Agent，运行在 NAS 上的 AI 助手。你有以下能力：\n"
            "1. 文件操作：read_file/list_files/search_files（直接操作 NAS 文件系统，/volume1 可直接访问）\n"
            "2. 代码执行：execute_code（直接运行 Python/Shell，无需让用户手动执行）\n"
            "3. 网络搜索：web_search（搜索互联网信息）\n"
            "4. 系统管理：get_disk_usage/docker_ps/docker_action（NAS 和 Docker 管理）\n"
            "5. 智能家居：ha_list_entities/ha_call（Home Assistant 控制）\n"
            "6. 定时任务：automation_create（创建定时自动化）\n"
            "重要规则：\n"
            "- 用户说'帮我做X'时，直接调用工具执行，不要说'我写个脚本你去跑'\n"
            "- /volume1 是你直接可访问的路径，不需要让用户'拿到NAS上跑'\n"
            "- 对于任务型请求，先执行再汇报结果，不要只描述你会怎么做\n"
            "- 工具结果如实转达；失败要说明原因和建议\n"
            "- 用中文交流，简洁直接，先给结论再解释"
        )
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
        sys_p = {"role": "system", "content": sys_content}
        msgs = [sys_p] + msgs
        for _ in range(8):
            # v2.0.116 review：每轮检查取消标志（用户点停止立即中断）
            if task is not None and task.get("cancelled"):
                return "已取消", msgs
            body = {"model": eff_model, "messages": msgs,
                    "tools": tool_executor.TOOLS, "stream": False, "max_tokens": 1500}
            # 推理模型必须带 reasoning_effort，否则退化复读
            _em = (eff_model or "").lower()
            if any(k in _em for k in ("mimo", "deepseek", "qwen3", "kimi", "o1", "o3", "r1")):
                body["reasoning_effort"] = "medium"
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
            return msg.get("content") or "（模型无返回内容）", msgs
        return "Agent 工具循环超过 8 轮，已停止", msgs
    except Exception as e:
        return f"Agent 执行失败：{e}", []


def _summarize_old_messages(msgs, model=None, provider=None):
    """v3.1.5 智能摘要：把超限旧消息压缩成一段摘要，替代硬截断丢弃。
    用当前模型自身做摘要（复用同一 provider，无需额外配置）。
    返回摘要文本；失败时返回 None（fallback 到硬截断）。"""
    try:
        # 格式化为对话文本
        lines = []
        for m in msgs:
            role = "用户" if m.get("role") == "user" else "助手"
            content = m.get("content", "")
            if isinstance(content, list):
                content = " ".join(str(b.get("text", "")) for b in content if isinstance(b, dict))
            lines.append(f"{role}：{str(content)[:500]}")  # 每条截500字防超长
        transcript = "\n".join(lines)
        if len(transcript) < 20:
            return None  # 太短无需摘要
        url, key, eff_model = _agent_endpoint(model, provider)
        body = {
            "model": eff_model,
            "messages": [
                {"role": "system", "content": "你是对话摘要助手。将以下对话压缩为简洁摘要（200字内），保留：1)讨论的核心话题 2)关键结论和决定 3)用户的需求和偏好 4)未完成的事项。用中文输出。"},
                {"role": "user", "content": f"请摘要以下对话：\n\n{transcript}"}
            ],
            "stream": False,
            "max_tokens": 300
        }
        _em = (eff_model or "").lower()
        if any(k in _em for k in ("mimo", "deepseek", "qwen3", "kimi", "o1", "o3", "r1")):
            body["reasoning_effort"] = "low"
        resp = _chat_once(body, url, key)
        summary = resp.get("choices", [{}])[0].get("message", {}).get("content", "")
        return summary.strip() if summary and len(summary.strip()) > 10 else None
    except Exception:
        return None


# 智能摘要缓存（防同一轮对话重复摘要）
_summary_cache = {}  # {session_id: {"hash": str, "summary": str}}
_summary_cache_lock = threading.Lock()


def _build_messages(st):
    """v3.0.6：组装发往 9123 的 messages。
    bot 模式：注入 bot.system_prompt 置顶 + 共享记忆（决策1）；普通模式：原 memory+kb 注入。
    v3.1.5：上下文超限时用智能摘要压缩旧消息（替代硬截断），保留最近 MAX_CONTEXT_MESSAGES 条。
    短回复理解增强：system prompt 引导模型将简短回复关联上文。"""
    # v3.1.7 fix：始终使用原始 messages，不再读 agent_msgs（避免旧工具调用上下文混入新请求）
    msgs = list(st["messages"])
    # v3.1.5 诊断：记录每次请求的消息数量和最后一条用户消息
    try:
        _last_user = ""
        for _m in reversed(msgs):
            if isinstance(_m, dict) and _m.get("role") == "user":
                _c = _m.get("content", "")
                if isinstance(_c, list):
                    _c = " ".join(str(b.get("text", "")) for b in _c if isinstance(b, dict))
                _last_user = str(_c)[:80]
                break
        # v3.1.6 诊断：记录最后3条消息的角色和内容摘要
        _tail = []
        for _m in msgs[-3:]:
            _role = _m.get("role", "?") if isinstance(_m, dict) else "?"
            _c = _m.get("content", "") if isinstance(_m, dict) else ""
            if isinstance(_c, list):
                _c = " ".join(str(b.get("text", "")) for b in _c if isinstance(b, dict))
            _tail.append(f"{_role}:{str(_c)[:60]}")
        with open("/tmp/stream_ctx_debug.log", "a", encoding="utf-8") as _f:
            _f.write(f"[{time.strftime('%H:%M:%S')}] msgs={len(msgs)} last_user={_last_user} tail=[{' | '.join(_tail)}]\n")
    except Exception:
        pass
    # v3.1.5：智能摘要——超限时压缩旧消息而非直接丢弃
    if len(msgs) > MAX_CONTEXT_MESSAGES:
        old_count = len(msgs) - MAX_CONTEXT_MESSAGES
        old_msgs = msgs[:old_count]
        recent = msgs[-MAX_CONTEXT_MESSAGES:]
        # 尝试智能摘要（带缓存，同一会话不重复摘要）
        sid = st.get("sessionId", "")
        cache_key = f"{sid}:{old_count}"
        summary = None
        with _summary_cache_lock:
            cached = _summary_cache.get(sid)
            if cached and cached.get("hash") == cache_key:
                summary = cached.get("summary")
        if summary is None:
            summary = _summarize_old_messages(old_msgs, model=st.get("model"), provider=st.get("provider"))
            if summary:
                with _summary_cache_lock:
                    _summary_cache[sid] = {"hash": cache_key, "summary": summary}
        if summary:
            marker = {"role": "system", "content": f"【早期对话摘要（共{old_count}条已压缩）】\n{summary}"}
        else:
            # 摘要失败，fallback 到保留首条+标记
            first_user_idx = next((i for i, m in enumerate(msgs)
                                   if isinstance(m, dict) and m.get("role") == "user"), 0)
            first_user = msgs[first_user_idx]
            marker = {"role": "system", "content": f"（早期{old_count}条对话已省略，从最近 {MAX_CONTEXT_MESSAGES} 条继续）"}
            recent = [first_user, marker] + recent
            marker = None  # 已直接拼入 recent
        if marker:
            msgs = [marker] + recent
        else:
            msgs = recent
    bot_id = st.get("bot") or ""
    if bot_id:
        # bot 模式：system_prompt 置顶（人设），共享用户记忆
        bot = None
        try:
            import bots_api
            bot = bots_api.find_bot(bot_id)
        except Exception:
            bot = None
        if bot and bot.get("system_prompt"):
            msgs = [{"role": "system", "content": bot["system_prompt"]}] + msgs
        return memory_store.inject(msgs)
    # 普通模式：原逻辑（记忆注入 + KB 注入）+ 禁思维链指令
    base_sys = [{"role": "system", "content": "你是轻聊 AI 助手，一个智能、高效、直接的 AI 助手。你乐于助人、知识丰富、行动力强。你帮助用户完成各种任务：回答问题、分析信息、执行操作。你沟通清晰，承认不确定性，优先做有用的事而非废话。用中文交流，简洁直接，先给结论再解释。当用户用简短词语（如\"需要\"\"好的\"\"可以\"\"是\"\"行\"\"对\"）回复你之前提出的问题或选项时，这是对该问题的肯定回答，请直接按肯定方向继续执行，不要反问\"需要什么\"。对于任务型请求（如\"帮我查下\"\"清理一下\"\"执行\"），直接执行并给出结果，不要只描述你会怎么做。"}]
    return base_sys + memory_store.inject(kb_inject.inject(msgs))


def _apply_bot(st):
    """v3.0.6：bot 模式下用 bot 的 model/provider 覆盖请求的 model/provider。
    返回 True 表示是 bot 模式且 bot 存在。"""
    bot_id = st.get("bot") or ""
    if not bot_id:
        return False
    try:
        import bots_api
        bot = bots_api.find_bot(bot_id)
    except Exception:
        bot = None
    if bot:
        if bot.get("model"):
            st["model"] = bot["model"]
        if bot.get("provider"):
            st["provider"] = bot["provider"]
        return True
    return False


def _worker(task_id, task):
    st = task["state"]
    last_write = time.time()
    try:
        # v3.0.6：bot 模式先用 bot 配置覆盖 model/provider（在 agent 分流判断前）
        _apply_bot(st)
        # v2.0.96 方案B：Agent 分流（工具调用循环，直连支持 function calling 的模型）
        # v2.0.98：设置开关 agentEnabled=false 时禁用；agent_rules 规则命中强制走 agent；
        #          用户声明「以后XX都用agent」→ 提取并存规则（下次同类直接 agent）
        import agent_rules
        last_user = None   # 必须初始化（messages 无 user 时会走 or "" 兜底）
        for m in reversed(st["messages"]):
            if isinstance(m, dict) and m.get("role") == "user":
                last_user = m.get("content", "")
                if isinstance(last_user, list):
                    last_user = " ".join(str(b.get("text", "")) for b in last_user if isinstance(b, dict))
                break
        new_rule = agent_rules.extract_from_text(last_user or "")
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
                          f"msgs={len(st['messages'])} text={str(last_user or '')[:60]}\n")
        except Exception:
            pass
        if agent_on and not st.get("bot") and (_is_agent_request(st["messages"]) or agent_rules.match(last_user or "")):
            # v3.0.6：bot 模式不走 agent（决策2）——bot 纯聊天，人设由独立 system_prompt 控制
            st["agent"] = True
            # v2.0.116 review：传 task 使 Agent 循环支持中途取消
            # v3.0.30 fix：传 st 的 model/provider——Agent 使用设置页选定的模型（原恒用 AGENT_MODEL 默认 deepseek）
            # v3.1.6 fix：接收完整对话历史（含工具调用），存储供后续请求上下文
            _agent_content, _agent_msgs = _agent_loop(st["messages"], task,
                                                       model=st.get("model"), provider=st.get("provider"))
            st["content"] = media_convert.convert_media_marks(_agent_content)  # v2.0.130: MEDIA→图片
            # v3.1.7 fix：agent_msgs 不持久化到 st——避免旧工具调用上下文混入下次请求导致模型忽略新消息
            # agent 的工具调用上下文仅在本次请求内有效，下次请求从原始 messages 重建
            st["status"] = "done"
            _write_state(task_id, task)
            _maybe_push(st)
            return
        # v2.0.105：Agent 关闭时定时类话术明确提示（防普通 LLM 幻觉回复"已设置"实际未创建）
        if not agent_on and not st.get("bot") and _is_auto_request(last_user or ""):   # v3.0.6：bot 模式不触发定时提示
            st["agent"] = False
            st["content"] = ("⏰ 定时自动化需要开启「Agent 智能回复」才能创建（设置 → 高级设置 → Agent 智能回复）。\n"
                             "打开开关后，对我说「X分钟后执行Y」即可自动生成倒计时卡片。")
            st["status"] = "done"
            _write_state(task_id, task)
            _maybe_push(st)
            return
        req_body = {
            "model": st["model"],
            "messages": _build_messages(st),   # v3.0.6：bot 模式注入人设在此组装
            "stream": True,
            "max_tokens": int(os.environ.get("STREAM_MAX_TOKENS", 8192))  # 显式设上限，防 gateway 默认值过低截断输出
        }
        # 推理模型必须带 reasoning_effort，否则退化复读（mimo/deepseek/qwen3/kimi 等）
        _model_lower = (st.get("model") or "").lower()
        if any(k in _model_lower for k in ("mimo", "deepseek", "qwen3", "kimi", "o1", "o3", "r1")):
            req_body["reasoning_effort"] = "medium"
        if st.get("provider"):
            req_body["provider"] = st["provider"]   # V1.5.3：精确路由，避免回退默认模型
        # v2.0.117：本地模型（provider=local）——直连 Ollama（11434 OpenAI 兼容端点），断网兜底不经 9123
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
                return
            except Exception as e:
                st["content"] = f"⚠️ 本地模型不可用：{str(e)[:120]}（请确认「设置 → 本地模型」已开启）"
                st["status"] = "done"
                _write_state(task_id, task)
                return
        body = json.dumps(req_body).encode("utf-8")
        req = urllib.request.Request(HERMES_URL, data=body, headers={
            "Authorization": "Bearer " + HERMES_KEY,
            "Content-Type": "application/json"
        })
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
        st["status"] = "error"
        st["error"] = str(e)[:300]
    _write_state(task_id, task)
    _maybe_push(st)



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

    def _proxy_asr(self):
        """蜂窝 relay ASR：raw 音频 body 透传 9127/api/asr/transcribe（Phase 3 端口终局）"""
        try:
            n = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(n)
            headers = {"Content-Type": "application/octet-stream"}
            tok = self.headers.get("X-Auth-Token", "")
            if tok:
                headers["X-Auth-Token"] = tok
            req = urllib.request.Request("http://127.0.0.1:9127/api/asr/transcribe",
                                         data=body, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=95) as r:
                resp = r.read()
                self.send_response(r.status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(resp)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(resp)
        except Exception as e:
            self._send(500, {"ok": False, "error": str(e)[:150]})

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
        # 蜂窝 relay ASR 转写（v2.0.98，Phase 3 端口终局）：/r/asr/transcribe/{uid} → 透传 9127 asr_api
        if self.path.startswith("/r/asr/transcribe/"):
            self._proxy_asr()
            return
        self.path = _relay_alias(self.path)
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
            bot_id = str(data.get("bot", "") or "")  # v3.0.6：Bot Mode——指定 bot（人设/模型/会话隔离）
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
                    "bot": bot_id,   # v3.0.6：bot 模式标识
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
            # 独立进程执行 systemctl（start_new_session 防信号连带杀死请求线程）
            subprocess.Popen(
                ["systemctl", action, "qingliao.service"],
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
                # 智谱 glm-tts：POST {base}/audio/speech，body {model,input,voice,response_format:wav}
                # 非流式 wav 返回原始音频字节 → 转 base64
                if provider == "zai":
                    key = _load_cfg_key(["providers", "zai", "api_key"])
                    if not key:
                        return self._send(500, {"ok": False, "error": "zai api_key 未配置"})
                    base = _provider_base_url("zai") or "https://open.bigmodel.cn/api/paas/v4"
                    payload = {
                        "model": model or "glm-tts",
                        "input": text,
                        "voice": voice or "彤彤",
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
        # v2.0.130：免鉴权 AI 图片端点（App 渲染 MEDIA: 路径时加载）——只允许数据目录下图片
        if self.path.startswith("/api/stream/media"):
            _serve_media(self)
            return
        if self.path.startswith("/r?") or self.path == "/r":
            _relay_query(self)
            return
        self.path = _relay_alias(self.path)
        if not _auth(self):
            return self._send(401, {"error": "unauthorized"})
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
            # 1) 内存中查找
            with _tasks_lock:
                for tid, t in _tasks.items():
                    if t["state"].get("sessionId") == session_id:
                        st = t["state"]
                        return self._send(200, {
                            "taskId": tid,
                            "content": st.get("content", ""),
                            "done": st["status"] != "streaming",
                            "status": st["status"],
                            "error": st.get("error", ""),
                            "fromMemory": True
                        })
            # 2) 磁盘兜底：扫描 streams/*.json 按 sessionId 匹配
            try:
                if os.path.isdir(STREAM_DIR):
                    for fn in sorted(os.listdir(STREAM_DIR)):
                        if not fn.endswith(".json"):
                            continue
                        fp = os.path.join(STREAM_DIR, fn)
                        try:
                            with open(fp, encoding="utf-8") as f:
                                st = json.load(f)
                            if st.get("sessionId") == session_id:
                                # 重新注册到内存（worker 可能已不在，但内容可读）
                                task_id = fn[:-5]
                                return self._send(200, {
                                    "taskId": task_id,
                                    "content": st.get("content", ""),
                                    "done": True,
                                    "status": st.get("status", "done"),
                                    "error": st.get("error", ""),
                                    "agent": st.get("agent", False),
                                    "fromDisk": True
                                })
                        except Exception:
                            continue
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
            return self._send(200, {
                "content": new,
                "done": st["status"] != "streaming",
                "status": st["status"],
                "sessionId": st["sessionId"],
                "error": st.get("error", ""),
                "agent": st.get("agent", False)   # v2.0.98：Agent 回复标记（设置页开关关闭时恒 false）
            })
        return self._send(404, {"error": "not found"})


def _serve_media(self):
    """v2.0.130：免鉴权图片服务——MEDIA:路径 → 图片字节。
    v3.0.28 security note：免鉴权设计（App 本地 localhost 调用），白名单限制只读允许目录下的图片扩展名。
    query: p=<base64url(宿主绝对路径)>；仅允许图片扩展名 + 容器 /opt/data 映射目录。
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
                        (os.environ.get("QL_HERMES_HOST_DIR", "/data/hermes_host"), os.environ.get("QL_HERMES_ROOT_DIR", "/data/hermes"))]:
        if path == _pre or path.startswith(_pre + "/"):
            path = _host + path[len(_pre):]
            break
    # 只允许数据目录下的图片（防任意文件读取）
    allowed = [os.environ.get("QL_HERMES_DATA_DIR", "/data/hermes"), os.environ.get("QL_DATA_DIR", "/data")]
    if not any(path.startswith(a) for a in allowed):
        self._send(403, {"error": "forbidden path"})
        return
    ext = os.path.splitext(path)[1].lower()
    ctype = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
             ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp"}.get(ext)
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
            "/api/stream/", "/api/asr/", "/api/auth/", "/api/sessions/",
            "/api/local/", "/api/weather/", "/api/push/", "/api/scenes",
            "/api/automation", "/api/memory", "/api/kb", "/api/docker",
            "/api/secrets", "/api/ha/", "/api/cron", "/api/files", "/api/logs",
            "/api/router/", "/api/agent", "/api/bots",
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
    while True:
        time.sleep(300)
        now = time.time()
        with _tasks_lock:
            for tid in list(_tasks.keys()):
                t = _tasks[tid]
                if t["state"]["status"] != "streaming" and now - t["state"]["updatedAt"] > TASK_TTL:
                    del _tasks[tid]
        # 清理 streams 目录里超过 2 小时的文件
        try:
            if os.path.isdir(STREAM_DIR):
                for fn in os.listdir(STREAM_DIR):
                    fp = os.path.join(STREAM_DIR, fn)
                    try:
                        if time.time() - os.path.getmtime(fp) > 7200:
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
