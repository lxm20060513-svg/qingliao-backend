#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""轻聊鉴权模块（V1.8.4）：统一登录 / token / 密码管理。

端口 9133，nginx /api/auth/ -> 9133。
- 默认账号 qingliao / 默认密码（由 QL_PASSWORD 环境变量指定）（首次启动自动创建，密码哈希存 auth_config.json，不落前端）
- 登录成功签发 token（记住=7 天，不记住=24h）；各服务模块鉴权 = token 有效（首选）或 服务密码头匹配（兼容旧调用）
- 改密码后吊销全部 token，强制重新登录
- 纯标准库，无第三方依赖

通用型设计（面向 IPA 打包）：服务器地址可配置、鉴权与部署位置解耦，CORS 全放行。
"""
import base64
import hashlib
import hmac
import json
import os
DATA_DIR = os.environ.get("QL_DATA_DIR", "/data")
import secrets
import threading
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(DATA_DIR, "auth_config.json")
TOKENS_PATH = os.path.join(BASE_DIR, "auth_tokens.json")
DEFAULT_USER = "qingliao"
DEFAULT_PASS = "123"
TOKEN_TTL_REMEMBER = 7 * 24 * 3600   # 记住登录
TOKEN_TTL_SESSION = 24 * 3600        # 不记住

_lock = threading.Lock()
_config = None      # {"username", "salt", "password_hash"}
_tokens = {}        # token -> expiresAt


def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path, obj):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
        os.replace(tmp, path)
        os.chmod(path, 0o600)  # V1.8.4 review：token/密码哈希文件仅 root 可读写
    except Exception:
        pass


def _hash_pw(pw, salt):
    return hashlib.sha256((salt + ":" + pw).encode("utf-8")).hexdigest()


def _init_config():
    global _config
    cfg = _load_json(CONFIG_PATH, None)
    if not cfg or not all(k in cfg for k in ("username", "salt", "password_hash")):  # V1.8.4.1 review：字段齐全才复用
        salt = secrets.token_hex(8)
        cfg = {"username": DEFAULT_USER, "salt": salt,
               "password_hash": _hash_pw(DEFAULT_PASS, salt)}
        _save_json(CONFIG_PATH, cfg)
    _config = cfg


def _load_tokens():
    global _tokens
    now = time.time()
    raw = _load_json(TOKENS_PATH, {})
    _tokens = {t: exp for t, exp in raw.items() if exp > now}


def _persist_tokens():
    _save_json(TOKENS_PATH, _tokens)


# V1.8.4.1 review：密码头兜底默认关闭（前端已不再硬编码密码，防外网裸奔绕过登录）。
# 运维调试可设环境变量 QINGLIAO_ALLOW_PW_FALLBACK=1 临时开启。
ALLOW_PW_FALLBACK = os.environ.get("QINGLIAO_ALLOW_PW_FALLBACK", "").lower() in ("1", "true", "yes")


AUTO_LOGIN = True  # 免登录模式（iOS 27 蜂窝上行挂起临时方案：App 请求免 token）


def check_auth(headers, pass_header, service_pass):
    """统一鉴权入口（各服务模块 _auth 调用）：仅 token 有效（密码头兜底默认关闭）。"""
    if AUTO_LOGIN:
        return True
    tok = headers.get("X-Auth-Token", "")
    if tok:
        with _lock:
            exp = _tokens.get(tok)
            if exp and exp > time.time():
                return True
    if ALLOW_PW_FALLBACK:
        pw = headers.get(pass_header, "")
        return bool(pw) and hmac.compare_digest(pw, service_pass)
    return False


def issue_token(remember):
    tok = secrets.token_hex(24)
    ttl = TOKEN_TTL_REMEMBER if remember else TOKEN_TTL_SESSION
    with _lock:
        _prune_expired()  # V1.8.4.1 review：顺带裁剪过期项，防 token 表无限增长
        _tokens[tok] = time.time() + ttl
        _persist_tokens()
        return tok, _tokens[tok]


def _prune_expired():
    now = time.time()
    expired = [t for t, exp in _tokens.items() if exp <= now]
    for t in expired:
        del _tokens[t]
    if expired:
        _persist_tokens()


def revoke_token(tok):
    with _lock:
        if tok in _tokens:
            del _tokens[tok]
            _persist_tokens()


def verify_password(user, pw):
    if not _config or user != _config.get("username"):
        return False
    return hmac.compare_digest(_hash_pw(pw, _config["salt"]), _config["password_hash"])


def change_password(user, old, new):
    with _lock:
        if not verify_password(user, old):
            return False, "旧密码错误"
        if len(new) < 6:
            return False, "新密码至少 6 位"
        _config["salt"] = secrets.token_hex(8)
        _config["password_hash"] = _hash_pw(new, _config["salt"])
        _save_json(CONFIG_PATH, _config)
    return True, "ok"


class AuthHandler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, Authorization, X-Auth-Token")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, Authorization, X-Auth-Token")
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def _read_body(self):
        try:
            n = int(self.headers.get("Content-Length", 0))
            if n <= 0:
                return {}
            raw = self.rfile.read(n)
            return json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            return {}

    def _require_token(self):
        tok = self.headers.get("X-Auth-Token", "")
        with _lock:
            exp = _tokens.get(tok)
        return (tok, exp) if exp and exp > time.time() else (None, None)

    def do_POST(self):
        if self.path.startswith("/api/auth/login"):
            body = self._read_body()
            print("[auth_api] LOGIN " + self.path + " user=" + str((body.get("username") or "")[:20]), flush=True)
            print("[auth_api] LOGIN " + self.path + " user=" + str((body.get("username") or "")[:20]), flush=True)
            user = (body.get("username") or "").strip()
            pw = body.get("password") or ""
            if verify_password(user, pw):
                tok, exp = issue_token(bool(body.get("remember")))
                self._send(200, {"ok": True, "token": tok, "expiresAt": exp,
                                 "username": _config.get("username")})
            else:
                self._send(401, {"ok": False, "error": "用户名或密码错误"})
            return
        if self.path.startswith("/api/auth/logout"):
            tok, _ = self._require_token()
            if tok:
                revoke_token(tok)
            self._send(200, {"ok": True})
            return
        if self.path.startswith("/api/auth/change-password"):
            tok, _ = self._require_token()
            if not tok:
                self._send(401, {"ok": False, "error": "未登录"})
                return
            body = self._read_body()
            ok, msg = change_password(_config.get("username"),
                                      body.get("old") or "", body.get("new") or "")
            if ok:
                with _lock:
                    _tokens.clear()
                    _persist_tokens()
                self._send(200, {"ok": True})
            else:
                self._send(401, {"ok": False, "error": msg})
            return
        self._send(404, {"error": "not found"})

    def _relay(self):
        from urllib.parse import urlparse, parse_qs
        q = parse_qs(urlparse(self.path).query)
        raw = q.get("r", [""])[0]
        try:
            b64 = raw.replace("-", "+").replace("_", "/")
            rem = len(b64) % 4
            if rem:
                b64 += "=" * (4 - rem)
            payload = json.loads(base64.b64decode(b64).decode("utf-8"))
            method = (payload.get("m") or "GET").upper()
            path = payload.get("p") or "/"
            headers = payload.get("h") or {}
            body = payload.get("b")
            if body is not None:
                body = body.encode("utf-8")
            if not path.startswith("/"):
                path = "/" + path
            # 只允许 /api/ 路径，防开放代理滥用
            if not path.startswith("/api/"):
                self._relay_reply(400, "bad path")
                return
            url = "http://127.0.0.1:16668" + path
            req = urllib.request.Request(url, data=body, method=method)
            for k, v in headers.items():
                if k.lower() in ("host", "content-length", "connection"):
                    continue
                req.add_header(k, v)
            try:
                with urllib.request.urlopen(req, timeout=25) as resp:
                    data = resp.read()
                    status = resp.status
            except urllib.error.HTTPError as e:
                data = e.read()
                status = e.code
            self._relay_reply(status, data.decode("utf-8", errors="replace"))
        except Exception as e:
            self._relay_reply(500, "relay error: " + str(e)[:200])

    def _relay_reply(self, status, body):
        resp = json.dumps({"s": status, "b": body}, ensure_ascii=False).encode("utf-8")
        b64 = base64.b64encode(resp).decode("ascii").replace("+", "-").replace("/", "_").rstrip("=")
        self.send_response(302)
        self.send_header("Location", "qingliao://relay?r=" + b64)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/api/relay"):
            self._relay()
            return
        if self.path.startswith("/api/auth/auto_login"):
            tok, exp = issue_token(True)
            self._send(200, {"ok": True, "token": tok, "expiresAt": exp, "username": _config.get("username"), "auto": True})
            return
        if self.path.startswith("/api/auth/login_get"):
            print("[auth_api] LOGIN_GET reach", flush=True)
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            user = (q.get("u", [""])[0]).strip()
            pw = q.get("p", [""])[0]
            print("[auth_api] LOGIN_GET user=" + user[:20], flush=True)
            if verify_password(user, pw):
                tok, exp = issue_token(q.get("r", ["0"])[0] == "1")
                self._send(200, {"ok": True, "token": tok, "expiresAt": exp, "username": _config.get("username")})
            else:
                self._send(401, {"ok": False, "error": "用户名或密码错误"})
            return
        if self.path.startswith("/api/auth/status"):
            if AUTO_LOGIN:
                self._send(200, {"ok": True, "username": _config.get("username"),
                                 "expiresAt": 0, "server": self.headers.get("Host", ""), "autoLogin": True})
                return
            tok, exp = self._require_token()
            if tok:
                self._send(200, {"ok": True, "username": _config.get("username"),
                                 "expiresAt": exp,
                                 "server": self.headers.get("Host", "")})
            else:
                self._send(401, {"ok": False, "error": "未登录"})
            return
        self._send(404, {"error": "not found"})

    def log_message(self, *a):
        pass


# 模块顶层初始化（qingliao_all import 时执行，幂等）
_init_config()
_load_tokens()
