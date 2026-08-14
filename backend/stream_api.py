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

STREAM_PASS = "123"
DATA_DIR = os.environ.get("STREAM_DATA_DIR", "/data/streams_data")
STREAM_DIR = os.path.join(DATA_DIR, "streams")
HERMES_URL = os.environ.get("STREAM_HERMES_URL", "http://127.0.0.1:9123/v1/chat/completions")
HERMES_KEY = os.environ.get("STREAM_HERMES_KEY", "")
WRITE_INTERVAL = 0.3  # 写盘节流
TASK_TTL = 1800       # 任务完成后内存保留 30 分钟
# V1.4 微信接力推送：回复完成 → webhook deliver-only → 微信
WEBHOOK_URL = os.environ.get("STREAM_WEBHOOK_URL", "http://172.21.0.2:8644/webhooks/qingliao-push")
WEBHOOK_SECRET = os.environ.get("STREAM_WEBHOOK_SECRET", "597eb994f2ef13db489f511f2bc88566")
PUSH_IDLE_SECONDS = 30  # 用户超过 30s 未轮询才推送（在看的用户不打扰）

# 模块级初始化（qingliao_all.py 用 importlib 加载，__main__ 块不会执行，
# 目录创建与清理线程必须放在模块顶层）
os.makedirs(STREAM_DIR, exist_ok=True)
_cleanup_started = False

# V1.5.9：各 provider 的模型列表端点 + config key 路径
SYNC_ENDPOINTS = {
    "opencode": ("https://opencode.ai/zen/go/v1/models", ["providers", "opencode", "api_key"]),
    "stepfun": ("https://api.stepfun.com/step_plan/v1/models", ["providers", "stepfun", "api_key"]),
    "deepseek": ("https://api.deepseek.com/v1/models", ["providers", "deepseek", "api_key"]),
    "xiaomi": ("https://token-plan-cn.xiaomimimo.com/v1/models", ["providers", "xiaomi", "api_key"]),
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
    try:
        import subprocess
        df = subprocess.run(["df", "-B1"], capture_output=True, text=True, timeout=8)
        disks = []
        for line in df.stdout.splitlines()[1:]:
            parts = line.split()
            if len(parts) >= 6:
                mnt = parts[5]
                fs = parts[0]
                # 过滤：tmpfs/udev/overlay/squashfs 伪设备 + 非根容器的重复挂载（只留物理分区与主要挂载点）
                if fs.startswith(("tmpfs", "udev", "overlay", "squashfs", "shm", "devtmpfs", "/dev/loop")):
                    continue
                if mnt.startswith(("/var/lib/docker", "/proc", "/sys", "/dev", "/run", "/etc/resolv", "/etc/hostname", "/etc/hosts", "/mnt/@remote")):
                    continue
                disks.append({"fs": fs, "total": int(parts[1]), "used": int(parts[2]), "avail": int(parts[3]), "pct": parts[4].rstrip("%"), "mnt": mnt})
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
        hkey = os.environ.get("HERMES_KEY", "")
        r = subprocess.run(["curl", "-s", "-m", "3", "-o", "/dev/null", "-w", "%{http_code}", "-H", "Authorization: Bearer " + hkey, HERMES_URL], capture_output=True, text=True, timeout=8)
        services["hermes"] = r.stdout.strip() == "200"
        p = subprocess.run(["pgrep", "-f", "hermes gateway run"], capture_output=True, text=True, timeout=8)
        services["hermes_mem"] = _proc_rss(p.stdout.strip().splitlines()[0]) if p.stdout.strip() else None
    except Exception:
        services["hermes"] = None
        services["hermes_mem"] = None
    out["services"] = services
    return out

def _start_cleanup():
    global _cleanup_started
    if _cleanup_started:
        return
    _cleanup_started = True
    threading.Thread(target=cleanup_old_tasks, daemon=True).start()

_tasks = {}           # taskId -> {"state": {...}, "cancelled": bool, "lock": Lock}
_tasks_lock = threading.Lock()


def _auth(h):
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


def _worker(task_id, task):
    st = task["state"]
    last_write = time.time()
    try:
        req_body = {
            "model": st["model"],
            "messages": memory_store.inject(kb_inject.inject(st["messages"])),
            "stream": True
        }
        if st.get("provider"):
            req_body["provider"] = st["provider"]   # V1.5.3：精确路由，避免回退默认模型
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
                    st["content"] += delta
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
            provider = str(data.get("provider", "") or "")  # V1.5.3 模型精确路由（9123 需 provider 才不回退默认）
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
                    "provider": provider,
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
        if self.path.startswith("/api/nas/service/"):
            action = self.path.split("/")[-1]
            svc = str(data.get("service", "qingliao"))
            if action not in ("restart", "stop") or svc != "qingliao":
                return self._send(400, {"error": "invalid action"})
            # 独立进程执行 systemctl（start_new_session 防信号连带杀死请求线程）
            subprocess.Popen(
                ["systemctl", action, "qingliao.service"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                start_new_session=True
            )
            return self._send(200, {"ok": True, "action": action, "service": svc})

        return self._send(404, {"error": "not found"})

    def do_GET(self):
        if self.path.startswith("/r?") or self.path == "/r":
            _relay_query(self)
            return
        self.path = _relay_alias(self.path)
        if not _auth(self):
            return self._send(401, {"error": "unauthorized"})
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
                req = urllib.request.Request(url, headers={"Authorization": "Bearer " + api_key})
                resp = urllib.request.urlopen(req, timeout=10)   # v2.0.87au：中转页超时缩短，弹窗更快收起
                data = json.loads(resp.read().decode("utf-8", "replace"))
                ids = [m.get("id") for m in data.get("data", []) if isinstance(m, dict) and m.get("id")]
                return self._send(200, {"ok": True, "provider": provider, "models": ids})
            except Exception as e:
                return self._send(200, {"ok": False, "provider": provider, "error": str(e)[:150], "models": []})
        # V1.7.2：NAS 面板状态（宿主系统 + 服务健康）
        if self.path.startswith("/api/nas/status"):
            return self._send(200, _collect_nas_status())
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
                "error": st.get("error", "")
            })
        return self._send(404, {"error": "not found"})


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
        # only forward to internal nginx (no open proxy)
        if not (path.startswith("/api/") or path.startswith("/r/")):
            _relay_reply(self, 400, "bad path")
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
