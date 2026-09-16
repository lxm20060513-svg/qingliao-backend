# -*- coding: utf-8 -*-
"""本地模型管理 API（v2.0.117）：Ollama 容器状态/开关/模型列表/更新检测。端口 9149。

- GET  /api/local/status       容器+模型状态
- POST /api/local/toggle       开关 {on: true/false}
- GET  /api/local/models       已安装模型列表
- GET  /api/local/check-update 模型更新检测（对比 registry digest）
- POST /api/local/update       更新模型 {model: "qwen3:4b"}
"""
import json
import os
import re
import subprocess
import time
import urllib.request
from http.server import BaseHTTPRequestHandler

OLLAMA_HOST = os.environ.get("QL_OLLAMA_HOST", "127.0.0.1:11434")
OLLAMA_CONTAINER = "ollama"


def _sh(args, timeout=30):
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception:
        return ""


def _container_up():
    out = _sh(["docker", "ps", "--format", "{{.Names}}"], timeout=10)
    return OLLAMA_CONTAINER in (out or "").splitlines()


def _ollama_cmd(args, timeout=60):
    """docker exec ollama ollama <args>"""
    try:
        r = subprocess.run(["docker", "exec", OLLAMA_CONTAINER, "ollama"] + args,
                           capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip(), r.stderr.strip()
    except Exception as e:
        return "", str(e)[:150]


_LIST_RE = re.compile(r"^(\S+)\s+(\S+)\s+([\d.]+\s*[KMGTP]?B)\s+(.+)$")


def _models():
    """解析 ollama list（NAME ID SIZE MODIFIED）。

    SIZE 在 ollama 输出里是「1.6 GB」两列、MODIFIED 是「3 minutes ago」多列，必须整行解析；
    按空格取 parts[2] / parts[3:] 会显示成 size=1.6、modified="GB 3 minutes ago"。
    """
    out, _ = _ollama_cmd(["list"])
    models = []
    for line in (out or "").splitlines()[1:]:
        line = line.strip()
        if not line:
            continue
        m = _LIST_RE.match(line)
        if m:
            models.append({"name": m.group(1),
                           "size": re.sub(r"\s+", " ", m.group(3)).strip(),
                           "modified": m.group(4).strip()})
            continue
        parts = line.split()
        if len(parts) >= 4:
            models.append({"name": parts[0], "size": parts[2], "modified": " ".join(parts[3:])})
    return models


def check_update(model):
    """用 ollama pull --dry-run 检测是否有更新（有则返回可更新提示）"""
    out, err = _ollama_cmd(["pull", model, "--dry-run"], timeout=90)
    text = (out + " " + err).lower()
    if "up to date" in text or "already" in text:
        return {"update": False, "message": "已是最新版本"}
    if "pulling" in text or "manifest" in text or "new" in text:
        return {"update": True, "message": "发现新版本，可更新"}
    return {"update": False, "message": "检查完成（未知状态）"}


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token")
        self.end_headers()

    def _auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, "X-Local-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def _read_json(self):
        try:
            n = int(self.headers.get("Content-Length", 0))
            return json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return {}

    def do_GET(self):
        if not self._auth():
            return self._send(401, {"ok": False, "error": "unauthorized"})
        p = self.path
        if p.startswith("/api/local/status"):
            up = _container_up()
            models = _models() if up else []
            return self._send(200, {"ok": True, "container": "up" if up else "down",
                                    "models": models,
                                    "loaded": _sh(["docker", "exec", OLLAMA_CONTAINER, "ollama", "ps"],
                                                  timeout=15) if up else ""})
        if p.startswith("/api/local/models"):
            up = _container_up()
            return self._send(200, {"ok": True, "models": _models() if up else []})
        if p.startswith("/api/local/check-update"):
            model = "qwen3:4b"
            return self._send(200, {"ok": True, **check_update(model)})
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        if not self._auth():
            return self._send(401, {"ok": False, "error": "unauthorized"})
        body = self._read_json()
        p = self.path
        if p.startswith("/api/local/toggle"):
            on = bool(body.get("on"))
            if on and not _container_up():
                r = subprocess.run(["docker", "start", OLLAMA_CONTAINER],
                                   capture_output=True, text=True, timeout=30)
                ok = _container_up()
                return self._send(200, {"ok": ok, "message": "已开启" if ok else "启动失败"})
            if not on and _container_up():
                subprocess.run(["docker", "stop", OLLAMA_CONTAINER],
                               capture_output=True, text=True, timeout=60)
                return self._send(200, {"ok": True, "message": "已关闭（释放内存）"})
            return self._send(200, {"ok": True, "message": "状态无变化"})
        if p.startswith("/api/local/update"):
            model = str(body.get("model") or "qwen3:4b")
            if not _container_up():
                return self._send(200, {"ok": False, "message": "本地模型未开启"})
            out, err = _ollama_cmd(["pull", model], timeout=1800)
            ok = "success" in (out + err).lower() or "up to date" in (out + err).lower()
            return self._send(200, {"ok": ok, "message": "更新完成" if ok else (err or out)[:150]})
        if p.startswith("/api/local/delete"):
            # v2.0.118：自主删除模型（ollama rm，释放磁盘）
            model = str(body.get("model") or "")
            if not model:
                return self._send(200, {"ok": False, "message": "缺少模型名"})
            if not _container_up():
                return self._send(200, {"ok": False, "message": "本地模型未开启"})
            out, err = _ollama_cmd(["rm", model], timeout=60)
            ok = "error" not in (out + err).lower()
            return self._send(200, {"ok": ok, "message": "已删除" if ok else (err or out)[:150]})
        self._send(404, {"ok": False, "error": "not found"})
