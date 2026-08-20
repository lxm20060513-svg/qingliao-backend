# -*- coding: utf-8 -*-
"""ASR 代理 API（v2.0.96c）：POST /api/asr/transcribe（raw 音频 body）→ Hermes 容器 faster-whisper 转写。
容器挂载 /volume1/docker/hermes → /opt/hermes_host，音频经共享目录传递，一次 docker exec。端口 9143。"""
import json
import hmac
import os
import subprocess
import time
import uuid
from http.server import BaseHTTPRequestHandler

CONTAINER = os.environ.get("QL_ASR_CONTAINER", "hermes-hermes-1")
# 宿主共享目录（挂载进容器 /opt/hermes_host）
HOST_ASR_DIR = os.environ.get("QL_ASR_DIR", "/volume1/docker/hermes/微信文件/轻聊web/data/asr_tmp")
CONTAINER_ASR_DIR = os.environ.get("QL_ASR_CONTAINER_DIR",
                                   "/opt/hermes_host/微信文件/轻聊web/data/asr_tmp")
ASR_URL = "http://127.0.0.1:9144/transcribe"


def _ensure_server():
    """自愈：容器内 9144 未监听则拉起 asr_server，并等待就绪
    v2.0.117：① pgrep 正则技巧防自匹配 ② nohup 分开执行（原 `pgrep || (nohup &)` 括号子 shell
    被 docker exec 退出清理——进程起不来）③ 轮询 9144 就绪（原固定 sleep 10——模型加载 30-60s 超时报无响应）"""
    try:
        # ① 已在跑？
        r = subprocess.run(["docker", "exec", CONTAINER, "sh", "-c",
                            "pgrep -f '[a]sr_server' >/dev/null && echo UP || echo DOWN"],
                           capture_output=True, text=True, timeout=20)
        if "UP" in r.stdout:
            return True
        # ② 不在跑：直接 nohup（无括号子 shell）
        subprocess.run(["docker", "exec", CONTAINER, "sh", "-c",
                        "nohup /opt/data/whisper_venv/bin/python /opt/data/asr_server.py >/tmp/asr_server.log 2>&1 &"],
                       capture_output=True, text=True, timeout=30)
        # ③ 轮询 9144 就绪（最多 60 秒，每 3 秒探测；非 000/空响应即就绪）
        for _ in range(20):
            r = subprocess.run(["docker", "exec", CONTAINER, "sh", "-c",
                                "curl -s -m 2 -o /dev/null -w '%{http_code}' http://127.0.0.1:9144/ 2>/dev/null"],
                               capture_output=True, text=True, timeout=10)
            if r.stdout.strip() not in ("000", ""):
                return True
            time.sleep(3)
        return False
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
        # v2.0.116 review：修复任意非空 X-Auth-Token 即放行的漏洞——token 必须真实有效
        pw = os.environ.get("QL_PASSWORD", "change-me")
        tok = self.headers.get("X-Auth-Token", "")
        if tok:
            import auth_api
            if auth_api.check_auth(self.headers, "X-ASR-Password", pw):
                return True
        return bool(self.headers.get("X-ASR-Password")) and \
            hmac.compare_digest(self.headers.get("X-ASR-Password", ""), pw)

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
