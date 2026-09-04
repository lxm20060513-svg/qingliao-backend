#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Bot 管理 API（v3.0.6）：轻量 Bot Mode——每 Bot 独立人设/模型/会话。
方案 B：单 Hermes + 后端隔离。Bot = {id,name,system_prompt,model,provider,avatar}。
- 存储 DATA_DIR/bots.json（原子写）
- 聊天时 stream_api 按 request 的 bot 注入 system_prompt + 用 bot 的 model/provider
- 端口 9150，全部接口需 X-Auth-Token（与其它服务一致）
"""
import os
import json, os, uuid, threading, tempfile, hmac
from http.server import BaseHTTPRequestHandler
import auth_api   # v3.0.6：顶层导入，避免 handler 线程内 import 竞态导致 check_auth 卡

DATA_DIR = os.environ.get("QL_DATA_DIR", "/data")
BOTS_FILE = os.path.join(DATA_DIR, "bots.json")
_lock = threading.Lock()

DEFAULT_SYSTEM = "你是轻聊的 AI 助手，用中文简洁友好地回答用户的问题。不要输出你的思考过程、推理步骤或内部分析，直接给出回答。"


def _load():
    try:
        with open(BOTS_FILE, encoding="utf-8") as f:
            d = json.load(f)
            return d.get("bots", []) if isinstance(d, dict) else []
    except Exception:
        return []


def _save(bots):
    os.makedirs(DATA_DIR, exist_ok=True)
    # v3.0.7 fix：不加锁——调用方 (do_POST/do_DELETE) 已在 _lock 内，重入死锁
    fd, tmp = tempfile.mkstemp(dir=DATA_DIR, suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump({"bots": bots}, f, ensure_ascii=False, indent=1)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, BOTS_FILE)


def _sanitize(b):
    # 列表视图（不返回无关敏感字段）
    return {
        "id": b.get("id"),
        "name": b.get("name", ""),
        "system_prompt": b.get("system_prompt", ""),
        "model": b.get("model", ""),
        "provider": b.get("provider", ""),
        "avatar": b.get("avatar", ""),
    }


class Handler(BaseHTTPRequestHandler):
    def _auth(self):
        return auth_api.check_auth(self.headers, "X-Bots-Password", "")

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Auth-Token")

    def _send(self, data, code=200):
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

    def _read_body(self):
        try:
            n = int(self.headers.get("Content-Length", 0))
            if n <= 0:
                return {}
            return json.loads(self.rfile.read(n).decode("utf-8"))
        except Exception:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._auth():
            return self._send({"ok": False, "error": "unauthorized"}, 401)
        with _lock:
            bots = _load()
        self._send({"ok": True, "bots": [_sanitize(b) for b in bots]})

    def do_POST(self):
        if not self._auth():
            return self._send({"ok": False, "error": "unauthorized"}, 401)
        body = self._read_body()
        bot = body.get("bot") or body
        bid = str(bot.get("id", "") or "").strip() or uuid.uuid4().hex[:10]
        name = str(bot.get("name", "")).strip()
        if not name:
            return self._send({"ok": False, "error": "bot name required"}, 400)
        entry = {
            "id": bid,
            "name": name,
            "system_prompt": str(bot.get("system_prompt", "")).strip() or DEFAULT_SYSTEM,
            "model": str(bot.get("model", "")).strip(),
            "provider": str(bot.get("provider", "")).strip(),
            "avatar": str(bot.get("avatar", "")).strip(),
        }
        with _lock:
            bots = _load()
            existed = [b for b in bots if b.get("id") == bid]
            if existed:
                existed[0].update(entry)
            else:
                bots.append(entry)
            _save(bots)
        self._send({"ok": True, "id": bid})

    def do_DELETE(self):
        if not self._auth():
            return self._send({"ok": False, "error": "unauthorized"}, 401)
        p = self.path[len("/api/bots/"):].split("?")[0]
        with _lock:
            bots = [b for b in _load() if b.get("id") != p]
            _save(bots)
        self._send({"ok": True, "deleted": 1})

    def log_message(self, fmt, *args):
        pass


def list_bots():
    """供其它模块（stream_api）读取 bot 列表"""
    with _lock:
        return _load()


def find_bot(bid):
    for b in list_bots():
        if b.get("id") == bid:
            return b
    return None


def run_server(port=9150):
    from http.server import ThreadingHTTPServer
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Bots API on :{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    run_server()
