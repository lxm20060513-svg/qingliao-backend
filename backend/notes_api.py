#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""便签 API（v3.0.8）：看板便签——新增/删除，存 NAS 本地 JSON。
- 默认存 QL_DATA_DIR/notes.json（默认 /data/notes.json）
- 支持自定义目录：请求头 X-Notes-Dir 指定 NAS 绝对路径（App 设置「便签存储地址」），
  便签文件写到 <dir>/notes.json（目录自动创建，写后 chmod 644 保证文件管理器可见）
- 端口 9151，全部接口需 X-Auth-Token
"""
import json
import os
import time
import uuid
import tempfile
import threading
from http.server import BaseHTTPRequestHandler

DATA_DIR = os.environ.get("QL_DATA_DIR", "/data")
DEFAULT_FILE = os.path.join(DATA_DIR, "notes.json")
_lock = threading.Lock()


def _notes_file(header_dir):
    """按 X-Notes-Dir 头解析便签文件路径；无/非法回退默认。"""
    d = (header_dir or "").strip()
    if d and d.startswith("/") and ".." not in d:
        return os.path.join(d, "notes.json")
    return DEFAULT_FILE


def _load(path):
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
            return d.get("notes", []) if isinstance(d, dict) else []
    except Exception:
        return []


def _save(path, notes):
    d = os.path.dirname(path)
    os.makedirs(d, exist_ok=True)
    # 调用方已在 _lock 内（do_POST/do_DELETE），不再重复加锁
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump({"notes": notes}, f, ensure_ascii=False, indent=1)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    # 文件管理器可见（root 属主目录如微信文件）
    try:
        os.chmod(path, 0o644)
        os.chmod(d, 0o755)
    except Exception:
        pass


class Handler(BaseHTTPRequestHandler):
    def _auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, "X-Notes-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Auth-Token, X-Notes-Dir")

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
        path = _notes_file(self.headers.get("X-Notes-Dir"))
        with _lock:
            notes = _load(path)
        self._send({"ok": True, "notes": notes, "path": path})

    def do_POST(self):
        if not self._auth():
            return self._send({"ok": False, "error": "unauthorized"}, 401)
        body = self._read_body()
        text = str(body.get("text", "") or "").strip()
        if not text:
            return self._send({"ok": False, "error": "note text required"}, 400)
        path = _notes_file(self.headers.get("X-Notes-Dir"))
        entry = {
            "id": uuid.uuid4().hex[:10],
            "text": text,
            "created": int(time.time()),
        }
        with _lock:
            notes = _load(path)
            notes.append(entry)
            _save(path, notes)
        self._send({"ok": True, "note": entry, "path": path})

    def do_DELETE(self):
        if not self._auth():
            return self._send({"ok": False, "error": "unauthorized"}, 401)
        p = self.path[len("/api/notes/"):].split("?")[0]
        path = _notes_file(self.headers.get("X-Notes-Dir"))
        with _lock:
            notes = _load(path)
            before = len(notes)
            notes = [n for n in notes if n.get("id") != p]
            if len(notes) == before:
                return self._send({"ok": False, "error": "not found"}, 404)
            _save(path, notes)
        self._send({"ok": True, "deleted": p})

    def log_message(self, fmt, *args):
        pass


def list_notes():
    with _lock:
        return _load(DEFAULT_FILE)


if __name__ == "__main__":
    from http.server import ThreadingHTTPServer
    server = ThreadingHTTPServer(("0.0.0.0", 9151), Handler)
    print(f"Notes API on :9151 (default={DEFAULT_FILE})", flush=True)
    server.serve_forever()