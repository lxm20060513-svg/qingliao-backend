# -*- coding: utf-8 -*-
"""微信推送队列（v2.0.113）：事件方（自动化执行/定时提醒/异常告警）入队，
Hermes cron 脚本每分钟拉取并投递微信（2026-08-22 延迟优化：入队后立即经
Hermes 容器内 relay(9460) 投递，成功即标记 done；失败/relay 不可达时
队列保留，由 cron 每分钟兜底重投）。

存储：QL_DATA_DIR/push_queue.json  [{id, text, ts, status: pending|sending|done}]
"""
import json
import os
import threading
import time
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("QL_DATA_DIR", os.path.join(os.path.dirname(BASE), "data"))
QUEUE_FILE = os.path.join(DATA_DIR, "push_queue.json")
SETTINGS_FILE = os.path.join(DATA_DIR, "push_settings.json")
PUSH_TOKEN = os.environ.get("QL_PUSH_TOKEN", "ql-push-default")

_lock = threading.RLock()
RELAY_GRACE = 10          # 入队后给 relay 即时投递的宽限秒数，期间 cron 不拉（防竞态重复）
_RELAY_IP_CACHE = {"t": 0, "ip": None}   # docker 容器 IP 缓存（docker 重启后 IP 可能变）


def _resolve_relay_url():
    """解析 Hermes 容器内 relay 地址：docker inspect 动态查 IP（缓存 5 分钟），失败用默认"""
    now = time.time()
    if now - _RELAY_IP_CACHE["t"] > 300:
        try:
            out = os.popen("docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' " + os.environ.get("QL_HERMES_CONTAINER", "hermes-container") + " 2>/dev/null").read()
            ip = (out.strip().split() or [None])[0]
            if ip:
                _RELAY_IP_CACHE["ip"] = ip
                _RELAY_IP_CACHE["t"] = now
        except Exception:
            pass
    if _RELAY_IP_CACHE["ip"]:
        return "http://%s:9460" % _RELAY_IP_CACHE["ip"]
    return os.environ.get("QL_RELAY_URL", "http://172.21.0.2:9460")


def notify_relay(mid, text):
    """入队后立即投递（延迟优化：不等 cron 每分钟轮询）。成功→mark_done；失败→留队列给 cron 兜底。"""
    try:
        req = urllib.request.Request(
            _resolve_relay_url() + "/send",
            data=json.dumps({"text": text}).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-Push-Token": PUSH_TOKEN},
            method="POST")
        resp = json.loads(urllib.request.urlopen(req, timeout=10).read() or b"{}")
        if resp.get("ok") or resp.get("success"):
            mark_done(mid)
            return True
    except Exception:
        pass
    return False
  # RLock：enqueue 外层持锁内调 _save 也持锁（v3.0.28 review 嵌套导致 Lock 自死锁，2026-08-22 Phase 3 修复）


def _load_settings():
    """微信推送开关（App 设置页控制，默认开）"""
    try:
        with open(SETTINGS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"pushWeixin": True}


def _save_settings(d):
    with _lock:
        try:
            tmp = SETTINGS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(d, f, ensure_ascii=False, indent=1)
            os.replace(tmp, SETTINGS_FILE)
        except Exception:
            pass


def push_enabled():
    return bool(_load_settings().get("pushWeixin", True))


def _load():
    try:
        with open(QUEUE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def _save(items):
    with _lock:
        try:
            tmp = QUEUE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=1)
            os.replace(tmp, QUEUE_FILE)
        except Exception:
            pass


def enqueue(text):
    """事件方调用：入队一条推送消息（v2.0.113：App 开关关闭时不入队）"""
    if not push_enabled():
        return False, "微信推送已关闭（设置页可开启）"
    text = (text or "").strip()
    if not text:
        return False, "消息为空"
    # v3.0.28 review：整个 load→append→save 放在锁内，防并发丢条目
    with _lock:
        items = _load()
        items.append({"id": uuid.uuid4().hex[:12], "text": text,
                      "ts": time.time(), "status": "pending"})
        # 队列上限 50（防堆积）
        if len(items) > 50:
            items = items[-50:]
        _save(items)
        new_id = items[-1]["id"]
    # 2026-08-22 延迟优化：入队后立即通知 Hermes relay 投递（不阻塞响应；失败由 cron 兜底）
    threading.Thread(target=notify_relay, args=(new_id, text), daemon=True).start()
    return True, "已入队"


def pending():
    """拉取未发送消息并标记 sending（防重复投递）"""
    now = time.time()
    items = _load()
    picked = []
    rest = []
    for it in items:
        if it.get("status") == "pending" and now - (it.get("ts") or 0) < RELAY_GRACE:
            rest.append(it)  # 入队 <10s：relay 即时投递窗口内，cron 不拉（防竞态重复）
        elif it.get("status") == "pending":
            it["status"] = "sending"
            picked.append(it)
        else:
            # sending 超过 60s 视为僵尸（投递失败），重置回 pending
            if it.get("status") == "sending" and now - (it.get("ts") or 0) > 60:
                it["status"] = "pending"
                rest.append(it)
            elif it.get("status") == "done" and now - (it.get("ts") or 0) > 3600:
                continue  # 清理已完成的旧消息
            else:
                rest.append(it)
    _save(rest + picked)
    return picked


def mark_done(mid):
    items = _load()
    new = [it for it in items if it.get("id") != mid]
    if len(new) != len(items):
        _save(new)
        return True
    return False


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Push-Token")

    def _send(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def _auth(self):
        return self.headers.get("X-Push-Token") == PUSH_TOKEN

    # v3.0.6 security review：settings 接口需鉴权，但要兼容两种调用方——
    # App 设置页带 X-Auth-Token（登录 token）、微信推送 cron 带 X-Push-Token
    def _auth_or_login(self):
        if self._auth():
            return True
        try:
            import auth_api
            # 仅登录 token（密码头兜底默认关闭）
            return auth_api.check_auth(self.headers, "X-Push-Password", "")
        except Exception:
            return False

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/api/push/pending"):
            if not self._auth():
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            self._send(200, {"ok": True, "items": pending()})
        elif self.path.startswith("/api/push/settings"):
            # v3.0.6 security review：settings 必须鉴权（兼容 App 登录 token / 推送 token）
            if not self._auth_or_login():
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            # v2.0.113：微信推送开关（App 设置页读写）
            self._send(200, {"ok": True, **{k: v for k, v in _load_settings().items()}})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        if self.path.startswith("/api/push/enqueue"):
            if not self._auth():
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            try:
                n = int(self.headers.get("Content-Length") or 0)
                d = json.loads(self.rfile.read(n) or b"{}")
                ok, msg = enqueue(d.get("text", ""))
                self._send(200, {"ok": ok, "message": msg})
            except Exception as e:
                self._send(400, {"ok": False, "error": str(e)[:200]})
        elif self.path.startswith("/api/push/settings"):
            # v3.0.6 security review：settings 必须鉴权（兼容 App 登录 token / 推送 token）
            if not self._auth_or_login():
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            try:
                n = int(self.headers.get("Content-Length") or 0)
                d = json.loads(self.rfile.read(n) or b"{}")
                cur = _load_settings()
                if "pushWeixin" in d:
                    cur["pushWeixin"] = bool(d["pushWeixin"])
                _save_settings(cur)
                self._send(200, {"ok": True, **cur})
            except Exception as e:
                self._send(400, {"ok": False, "error": str(e)[:200]})
        elif self.path.startswith("/api/push/") and self.path.endswith("/done"):
            # v3.0.6 security review：done 必须鉴权（防未授权标记/操纵）
            if not self._auth():
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            mid = self.path.split("/")[3]
            ok = mark_done(mid)
            self._send(200, {"ok": ok})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, *a):
        pass
