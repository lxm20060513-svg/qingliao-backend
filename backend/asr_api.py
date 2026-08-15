# -*- coding: utf-8 -*-
"""ASR 代理 API（v2.0.96c）：POST /api/asr/transcribe（raw 音频 body）→ Hermes 容器 faster-whisper 转写。
容器挂载 /volume1/docker/hermes → /opt/hermes_host，音频经共享目录传递，一次 docker exec。端口 9143。"""
import json
import os
import subprocess
import uuid
from http.server import BaseHTTPRequestHandler

CONTAINER = os.environ.get("QL_ASR_CONTAINER", "hermes-hermes-1")
# 宿主共享目录（挂载进容器 /opt/hermes_host）
HOST_ASR_DIR = os.environ.get("QL_ASR_DIR", "/volume1/docker/hermes/微信文件/轻聊web/data/asr_tmp")
CONTAINER_ASR_DIR = os.environ.get("QL_ASR_CONTAINER_DIR",
                                   "/opt/hermes_host/微信文件/轻聊web/data/asr_tmp")
ASR_URL = "http://127.0.0.1:9144/transcribe"


def _ensure_server():
    """自愈：容器内 9144 未监听则拉起 asr_server"""
    try:
        r = subprocess.run(["docker", "exec", CONTAINER, "sh", "-c",
                            "pgrep -f asr_server >/dev/null || (nohup /opt/data/whisper_venv/bin/python /opt/data/asr_server.py >/tmp/asr_server.log 2>&1 & sleep 10)"],
                           capture_output=True, text=True, timeout=40)
        return True
    except Exception:
        return False


def transcribe(audio_bytes):
    os.makedirs(HOST_ASR_DIR, exist_ok=True)
    fn = uuid.uuid4().hex + ".m4a"
    host_p = os.path.join(HOST_ASR_DIR, fn)
    cpath = os.path.join(CONTAINER_ASR_DIR, fn)
    try:
        with open(host_p, "wb") as f:
            f.write(audio_bytes)
        _ensure_server()
        r = subprocess.run(["docker", "exec", CONTAINER, "sh", "-c",
                            f"curl -s -m 90 -X POST --data-binary @{cpath} {ASR_URL}"],
                           capture_output=True, text=True, timeout=120)
        out = r.stdout.strip()
        if not out:
            return {"ok": False, "error": "转写服务无响应：" + r.stderr[:150]}
        return json.loads(out)
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}
    finally:
        try:
            os.remove(host_p)
        except Exception:
            pass


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-ASR-Password")

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
        pw = os.environ.get("QL_PASSWORD", "change-me")
        return self.headers.get("X-ASR-Password") == pw or self.headers.get("X-Auth-Token")

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_POST(self):
        if self.path.startswith("/api/asr/transcribe"):
            if not self._auth():
                self._send(401, {"ok": False, "error": "unauthorized"})
                return
            try:
                n = int(self.headers.get("Content-Length") or 0)
                data = self.rfile.read(n)
                if len(data) < 100:
                    self._send(200, {"ok": False, "error": "音频太短"})
                    return
                result = transcribe(data)
                self._send(200, result)
            except Exception as e:
                self._send(500, {"ok": False, "error": str(e)[:150]})
            return
        self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, fmt, *args):
        pass
