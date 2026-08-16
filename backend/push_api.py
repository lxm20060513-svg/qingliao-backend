# -*- coding: utf-8 -*-
"""微信推送队列（v2.0.113）：事件方（自动化执行/定时提醒/异常告警）入队，
Hermes cron 脚本每分钟拉取并投递微信。

存储：QL_DATA_DIR/push_queue.json  [{id, text, ts, status: pending|sending|done}]
"""
import json
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("QL_DATA_DIR", os.path.join(os.path.dirname(BASE), "data"))
QUEUE_FILE = os.path.join(DATA_DIR, "push_queue.json")
SETTINGS_FILE = os.path.join(DATA_DIR, "push_settings.json")
PUSH_TOKEN = os.environ.get("QL_PUSH_TOKEN", "ql-push-default")

_lock = threading.Lock()


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
    items = _load()
    items.append({"id": uuid.uuid4().hex[:12], "text": text,
                  "ts": time.time(), "status": "pending"})
    # 队列上限 50（防堆积）
    if len(items) > 50:
        items = items[-50:]
    _save(items)
    return True, "已入队"


def pending():
    """拉取未发送消息并标记 sending（防重复投递）"""
    now = time.time()
    items = _load()
    picked = []
    rest = []
    for it in items:
        if it.get("status") == "pending":
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
            mid = self.path.split("/")[3]
            ok = mark_done(mid)
            self._send(200, {"ok": ok})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, *a):
        pass
