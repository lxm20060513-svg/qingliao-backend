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
# BE5：原来落在 BASE_DIR（= 镜像里的代码目录 /app），`docker compose up --build`
# 一次性重建就把全部已签发 token 抹掉，「token 跨重启有效」形同虚设、App 被踢回登录页。
TOKENS_PATH = os.path.join(DATA_DIR, "auth_tokens.json")
TOKENS_PATH_LEGACY = os.path.join(BASE_DIR, "auth_tokens.json")
DEFAULT_USER = "qingliao"
# v2.0.116 review：默认密码优先 QL_PASSWORD 环境变量；未注入则随机生成并写文件告知
# （原硬编码 "123" 弱口令，文档声称 QL_PASSWORD 实际代码 123——已核实）
DEFAULT_PASS = os.environ.get("QL_PASSWORD", "")
if not DEFAULT_PASS:
    DEFAULT_PASS = secrets.token_urlsafe(12)
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        _pw_file = os.path.join(DATA_DIR, "initial_password.txt")
        with open(_pw_file, "w", encoding="utf-8") as f:
            f.write("initial login: %s / %s\n" % (DEFAULT_USER, DEFAULT_PASS))
        os.chmod(_pw_file, 0o600)
    except Exception:
        pass
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
    # v2.0.116 review：pbkdf2 迭代哈希（原 sha256 单次无迭代，弱口令可离线秒破）
    return hashlib.pbkdf2_hmac("sha256", pw.encode("utf-8"), salt.encode("utf-8"), 200_000).hex()


def _hash_pw_legacy(pw, salt):
    # 旧 sha256 哈希（兼容已存配置，登录成功后自动迁移）
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
    # v2.0.116：磁盘存 sha256(token) 哈希表——重启后恢复，token 跨重启有效（校验时哈希比对）
    now = time.time()
    # BE5：老版本写在代码目录，一次性搬到 DATA_DIR，升级当次不掉线
    if not os.path.exists(TOKENS_PATH) and os.path.exists(TOKENS_PATH_LEGACY):
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            os.replace(TOKENS_PATH_LEGACY, TOKENS_PATH)
        except Exception:
            pass
    raw = _load_json(TOKENS_PATH, {})
    _tokens = {h: exp for h, exp in raw.items() if exp > now}


def _persist_tokens():
    # v2.0.116：落盘只存 sha256(token) 哈希（防文件泄露直接冒用）
    hashed = {h: exp for h, exp in _tokens.items()}
    _save_json(TOKENS_PATH, hashed)


# V1.8.4.1 review：密码头兜底默认关闭（前端已不再硬编码密码，防外网裸奔绕过登录）。
# 运维调试可设环境变量 QINGLIAO_ALLOW_PW_FALLBACK=1 临时开启。
ALLOW_PW_FALLBACK = os.environ.get("QINGLIAO_ALLOW_PW_FALLBACK", "").lower() in ("1", "true", "yes")


# v2.0.116 review：免登录模式改为环境变量控制，默认关闭
# （原硬编码 True 致全后端 check_auth 恒通过、鉴权体系失效；iOS 27 蜂窝如需可临时 QL_AUTO_LOGIN=1）
AUTO_LOGIN = os.environ.get("QL_AUTO_LOGIN", "0").lower() in ("1", "true", "yes")


def check_auth(headers, pass_header, service_pass):
    """统一鉴权入口（各服务模块 _auth 调用）：仅 token 有效（密码头兜底默认关闭）。"""
    if AUTO_LOGIN:
        return True
    tok = headers.get("X-Auth-Token", "")
    if tok:
        h = hashlib.sha256(tok.encode("utf-8")).hexdigest()
        with _lock:
            exp = _tokens.get(h)
            if exp and exp > time.time():
                return True
    if ALLOW_PW_FALLBACK:
        pw = headers.get(pass_header, "")
        return bool(pw) and hmac.compare_digest(pw, service_pass)
    return False


def issue_token(remember):
    tok = secrets.token_hex(24)
    ttl = TOKEN_TTL_REMEMBER if remember else TOKEN_TTL_SESSION
    # v2.0.116：内存/落盘统一用 sha256(token) 作键——重启后从哈希表恢复，token 跨重启有效
    h = hashlib.sha256(tok.encode("utf-8")).hexdigest()
    with _lock:
        _prune_expired()  # V1.8.4.1 review：顺带裁剪过期项，防 token 表无限增长
        _tokens[h] = time.time() + ttl
        _persist_tokens()
        return tok, _tokens[h]


def _prune_expired():
    now = time.time()
    expired = [t for t, exp in _tokens.items() if exp <= now]
    for t in expired:
        del _tokens[t]
    if expired:
        _persist_tokens()


def revoke_token(tok):
    h = hashlib.sha256(tok.encode("utf-8")).hexdigest()
    with _lock:
        if h in _tokens:
            del _tokens[h]
            _persist_tokens()


def verify_password(user, pw):
    if not _config or user != _config.get("username"):
        return False
    target = _config["password_hash"]
    if hmac.compare_digest(_hash_pw(pw, _config["salt"]), target):
        return True
    # v2.0.116 review：兼容旧 sha256 哈希，校验通过后自动迁移到 pbkdf2
    if hmac.compare_digest(_hash_pw_legacy(pw, _config["salt"]), target):
        with _lock:
            new_salt = secrets.token_hex(8)
            _config["salt"] = new_salt
            _config["password_hash"] = _hash_pw(pw, new_salt)
            _save_json(CONFIG_PATH, _config)
        return True
    return False


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
        h = hashlib.sha256(tok.encode("utf-8")).hexdigest() if tok else ""
        with _lock:
            exp = _tokens.get(h)
        return (tok, exp) if exp and exp > time.time() else (None, None)

    def do_POST(self):
        if self.path.startswith("/api/auth/login"):
            body = self._read_body()
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
            # v2.0.116 review：relay 转发前校验请求头 token（防开放代理被内网任意设备滥用）
            hdr = {str(k).lower(): str(v) for k, v in headers.items()}
            tok = hdr.get("x-auth-token", "")
            pw = hdr.get("x-stream-password", "") or hdr.get("x-scenes-password", "")
            ok = False
            if tok:
                ok = check_auth(hdr, "x-stream-password", os.environ.get("QL_PASSWORD", ""))
            if not ok and pw:
                ok = bool(pw) and hmac.compare_digest(pw, os.environ.get("QL_PASSWORD", ""))
            if not ok and not AUTO_LOGIN:
                self._relay_reply(401, "unauthorized")
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
            # v2.0.116 review：仅 QL_AUTO_LOGIN=1（免登录模式）时可用，否则拒绝签发
            if not AUTO_LOGIN:
                self._send(403, {"ok": False, "error": "auto_login disabled"})
                return
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
