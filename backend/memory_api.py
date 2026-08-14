#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AI 记忆 API：GET /api/memory/list、POST /api/memory/add、POST /api/memory/delete"""
import json
import os
from http.server import BaseHTTPRequestHandler

import memory_store


class MemoryHandler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token")

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, "X-Memory-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/memory/list"):
            self._send(200, {"ok": True, "entries": memory_store.list_entries()})
            return
        self._send(404, {"error": "Not Found"})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        try:
            n = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(n) or b"{}") if n else {}
        except Exception:
            body = {}
        if parsed.path.startswith("/api/memory/add"):
            text = (body.get("text") or "").strip()
            if not text:
                self._send(200, {"ok": False, "message": "内容不能为空"})
                return
            memory_store.add_entry(text)
            self._send(200, {"ok": True, "message": "已记住", "entries": memory_store.list_entries()})
            return
        if parsed.path.startswith("/api/memory/delete"):
            text = (body.get("text") or "").strip()
            memory_store.delete_entry(text)
            self._send(200, {"ok": True, "message": "已删除", "entries": memory_store.list_entries()})
            return
        self._send(404, {"error": "Not Found"})

    def log_message(self, fmt, *args):
        pass
