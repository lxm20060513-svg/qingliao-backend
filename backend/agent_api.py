# -*- coding: utf-8 -*-
"""Agent 规则记忆管理 API（v2.0.98）：GET/POST/DELETE /api/agent/rules

- GET    /api/agent/rules           → 规则列表
- POST   /api/agent/rules           {pattern: "查磁盘"} → 手动添加
- DELETE /api/agent/rules           {id: "xxxx"} → 删除
鉴权：X-Agent-Password: QL_PASSWORD（或 X-Auth-Token 非空）
"""
import json
from http.server import BaseHTTPRequestHandler

import agent_rules


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, Authorization, X-Agent-Password, X-Auth-Token")

    def _auth(self):
        pw = os_env_password()
        return (self.headers.get("X-Agent-Password") == pw
                or bool(self.headers.get("X-Auth-Token")))

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._auth():
            return self._send(401, {"ok": False, "error": "unauthorized"})
        if self.path.startswith("/api/agent/rules"):
            return self._send(200, {"ok": True, "rules": agent_rules.list_rules()})
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        if not self._auth():
            return self._send(401, {"ok": False, "error": "unauthorized"})
        if self.path.startswith("/api/agent/rules"):
            try:
                n = int(self.headers.get("Content-Length", 0))
                data = json.loads(self.rfile.read(n) or b"{}")
            except Exception:
                return self._send(400, {"ok": False, "error": "bad json"})
            ok, msg = agent_rules.add_rule(str(data.get("pattern", "")).strip())
            return self._send(200 if ok else 400, {"ok": ok, "error": "" if ok else msg})
        self._send(404, {"ok": False, "error": "not found"})

    def do_DELETE(self):
        if not self._auth():
            return self._send(401, {"ok": False, "error": "unauthorized"})
        if self.path.startswith("/api/agent/rules"):
            try:
                n = int(self.headers.get("Content-Length", 0))
                data = json.loads(self.rfile.read(n) or b"{}")
            except Exception:
                return self._send(400, {"ok": False, "error": "bad json"})
            ok, msg = agent_rules.delete_rule(str(data.get("id", "")))
            return self._send(200 if ok else 400, {"ok": ok, "error": "" if ok else msg})
        self._send(404, {"ok": False, "error": "not found"})


def os_env_password():
    import os
    return os.environ.get("QL_PASSWORD", "change-me")
