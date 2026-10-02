#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""后端版本 API：/api/version

GET /api/version → {"ok": true, "version": "4.0.13", "commit": "bfe45a0",
                    "built": "2026-09-30", "modules": 30}

用途：App「关于」页显示后端版本，用户装完 App 4.0.13 一眼确认后端配套的是哪版，
不用猜、不用翻 Docker 日志。

设计要点：
1. **免鉴权**。这是纯信息接口，返回的只有版本号，不含任何配置/路径/凭据。
   未登录时也要能看到（App 登录页也要显示后端版本才能排查"连不上"）。
2. 版本号真值优先级：**backend/QL_VERSION 文件 > QL_BACKEND_VERSION 环境变量 > .git**。
   文件优先是给 bind mount / 手动部署用的：部署脚本写一次 backend/QL_VERSION 即生效
   （读盘带 60 秒缓存），**不必 `docker compose up -d` 重建容器去刷 env**；
   镜像部署（Dockerfile 注入 env、盘上无该文件）时自动回落到环境变量。
   - backend/QL_VERSION：update.sh（git 装法）会写；手动/bind mount 部署自行写一份即可
     （第一行版本号，可选第二行 commit、第三行日期）
   - QL_BACKEND_COMMIT / QL_BACKEND_BUILT 同序遍历（各自缺失则单独回落）
   都没有则返回 version=""，App 侧显示"未知"而不是报错。
3. 只加字段不改语义，与任何现有接口零耦合。
"""
import json
import os
import subprocess
from http.server import BaseHTTPRequestHandler

# 镜像构建时注入（见 Dockerfile / docker-compose.yml）
VERSION_ENV = "QL_BACKEND_VERSION"
COMMIT_ENV = "QL_BACKEND_COMMIT"
BUILT_ENV = "QL_BACKEND_BUILT"

# 非镜像部署（源码直接 python3 跑）时可能存在的版本文件
# 注意：不要用裸 "VERSION" —— 轻聊后端目录里本来就有这么个文件（mail IMAP 客户端标识，
# 内容 "1.0.0"），读到它会把后端版本误报成 1.0.0。用带前缀的专用名。
_VERSION_FILES = (
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "QL_VERSION"),
    "/app/QL_VERSION",
)

_info_cache = {"ts": 0, "data": None}
_CACHE_TTL = 60   # 60 秒：update.sh 更新后最多 1 分钟生效，避免每次请求都读文件


def _read_version_file():
    """从 QL_VERSION 文件读版本（内容形如 `4.0.13` 或 `4.0.13\nbfe45a0\n2026-09-30`）

    文件名带 QL_ 前缀是必须的：轻聊后端目录里本来就有个裸 `VERSION` 文件
    （mail 模块的 IMAP 客户端标识，内容 "1.0.0"），读到它会把后端版本误报成 1.0.0。
    """
    for path in _VERSION_FILES:
        try:
            with open(path, encoding="utf-8") as f:
                lines = [l.strip() for l in f.read().splitlines() if l.strip()]
            if not lines:
                continue
            return {
                "version": lines[0],
                "commit": lines[1] if len(lines) > 1 else "",
                "built": lines[2] if len(lines) > 2 else "",
            }
        except Exception:
            continue
    return None


def _read_git():
    """源码直接跑时读 .git（镜像里没有 .git，会快速失败返回 None）"""
    try:
        d = os.path.dirname(os.path.abspath(__file__))
        commit = subprocess.run(
            ["git", "-C", d, "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=3).stdout.strip()
        if not commit:
            return None
        built = subprocess.run(
            ["git", "-C", d, "log", "-1", "--format=%cd", "--date=short"],
            capture_output=True, text=True, timeout=3).stdout.strip()
        return {"version": "", "commit": commit, "built": built}
    except Exception:
        return None


def _module_count():
    """统计同目录的 *_api.py 数量（粗略反映后端模块规模，便于确认版本新旧）"""
    try:
        d = os.path.dirname(os.path.abspath(__file__))
        n = sum(1 for f in os.listdir(d)
                if f.endswith("_api.py") and not f.startswith("_"))
        return n
    except Exception:
        return 0


def get_version_info():
    """取版本信息（带 60 秒缓存）"""
    import time
    now = time.time()
    if _info_cache["data"] is not None and now - _info_cache["ts"] < _CACHE_TTL:
        return _info_cache["data"]

    data = _read_version_file() or _read_git() or {}
    info = {
        "ok": True,
        # 真值优先级：文件（最贴近"当前跑着的这份代码"）> 环境变量（镜像构建时注入）> .git
        # 逐字段回落：文件只给了 version 时，commit/built 仍可用 env 补
        "version": data.get("version") or os.environ.get(VERSION_ENV, ""),
        "commit": data.get("commit") or os.environ.get(COMMIT_ENV, ""),
        "built": data.get("built") or os.environ.get(BUILT_ENV, ""),
        "modules": _module_count(),
    }
    _info_cache["ts"] = now
    _info_cache["data"] = info
    return info


class Handler(BaseHTTPRequestHandler):
    """GET /api/version"""

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "X-Auth-Token, Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")

    def do_OPTIONS(self):
        try:
            self.send_response(204)
            self._cors()
            self.end_headers()
        except Exception:
            pass

    def do_GET(self):
        try:
            body = json.dumps(get_version_info()).encode("utf-8")
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            # 版本接口本身绝不能把请求打挂
            try:
                body = json.dumps({"ok": False, "error": str(e),
                                   "version": "", "commit": "", "built": "",
                                   "modules": 0}).encode("utf-8")
                self.send_response(200)
                self._cors()
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception:
                pass

    def log_message(self, format, *args):
        pass   # 静默：这个接口不该刷日志
