#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""邮件接入 API v1：邮箱账号管理（可增删改）+ IMAP 收信 / SMTP 发信

端点（均需鉴权，与 life_api 一致走 auth_api.check_auth + X-Auth-Token）：
  GET    /api/mail/accounts                → {"ok":true,"accounts":[…]}  （绝不含 secret 明文，只给 has_secret）
  POST   /api/mail/accounts                → 新增/更新（body 带 secret 才覆盖；不带保留旧值）
  DELETE /api/mail/accounts?id=<id>        → 删除单个账号（连带 secret）
  POST   /api/mail/test                    → 连通性测试（可只传草稿参数，不落盘）
  GET    /api/mail/messages?account=&folder=INBOX&limit=20&unread=1&q=关键词
  GET    /api/mail/message?account=&uid=   → 读正文（text/plain + 纯文本化的 html）
  POST   /api/mail/send                    → 发信；allow_direct_send=false 时只回草稿不直发

安全边界：
  · 授权码/密码 Fernet 加密落盘（密钥 600，与 secrets_api 同一范式但独立密钥文件）
  · 响应与日志绝不出现 secret 明文
  · allow_direct_send 默认关 → 不连 SMTP，只回草稿让 App 用系统邮件界面预填
  · IMAP/SMTP 全部带硬超时（连接 8s / 读 15s），失败降级为空列表 + ok:false
仅标准库（imaplib / smtplib / email / cryptography）。
"""
import email
import imaplib
import json
import os
import re
import smtplib
import socket
import threading
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from cryptography.fernet import Fernet
except Exception:                                     # 容器内必装；本机自测缺则明确报错
    Fernet = None

# ---------------------------------------------------------------- 路径与常量
DATA_DIR = os.environ.get("QL_DATA_DIR", "/data")
STORE = os.path.join(DATA_DIR, "mail_accounts.json")
KEY_FILE = os.path.join(DATA_DIR, ".mail_key")

CONNECT_TIMEOUT = 8      # TCP 连接超时（秒）
READ_TIMEOUT = 15        # 收发读超时（秒）
MAX_LIMIT = 50           # 单次最多取多少封
MAX_BODY_CHARS = 20000   # 正文返回上限

_lock = threading.RLock()

# 常见邮箱的服务器猜测表（按地址后缀匹配；猜不到用 imap./smtp.<域名>）
_HINTS = {
    "qq.com":       {"imap": ("imap.qq.com", 993), "smtp": ("smtp.qq.com", 465)},
    "foxmail.com":  {"imap": ("imap.qq.com", 993), "smtp": ("smtp.qq.com", 465)},
    "163.com":      {"imap": ("imap.163.com", 993), "smtp": ("smtp.163.com", 465)},
    "126.com":      {"imap": ("imap.163.com", 993), "smtp": ("smtp.163.com", 465)},
    "yeah.net":     {"imap": ("imap.yeah.net", 993), "smtp": ("smtp.yeah.net", 465)},
    "gmail.com":    {"imap": ("imap.gmail.com", 993), "smtp": ("smtp.gmail.com", 465)},
    "outlook.com":  {"imap": ("outlook.office365.com", 993), "smtp": ("smtp.office365.com", 587)},
    "hotmail.com":  {"imap": ("outlook.office365.com", 993), "smtp": ("smtp.office365.com", 587)},
    "live.com":     {"imap": ("outlook.office365.com", 993), "smtp": ("smtp.office365.com", 587)},
}


def guess_servers(email_addr):
    """按邮箱后缀猜 IMAP/SMTP 服务器。返回 (host, port, security)。

    security: "ssl"（隐式 TLS，端口 993/465）| "starttls"（587 明文升级）
    """
    addr = (email_addr or "").strip().lower()
    domain = addr.split("@")[-1] if "@" in addr else ""
    hit = _HINTS.get(domain)
    if hit:
        imap_h, imap_p = hit["imap"]
        smtp_h, smtp_p = hit["smtp"]
    else:
        imap_h, imap_p = ("imap." + domain or "imap.local", 993)
        smtp_h, smtp_p = ("smtp." + domain or "smtp.local", 465)
    # 587 端口按 STARTTLS 处理，其余按隐式 SSL
    return (imap_h, int(imap_p), "ssl"), (smtp_h, int(smtp_p),
                                        "starttls" if int(smtp_p) == 587 else "ssl")


# ---------------------------------------------------------------- 凭据存储
def _load_key():
    if os.path.exists(KEY_FILE):
        return open(KEY_FILE).read().strip()
    if Fernet is None:
        raise RuntimeError("容器缺 cryptography，无法生成密钥")
    os.makedirs(DATA_DIR, exist_ok=True)
    key = Fernet.generate_key().decode()
    with open(KEY_FILE, "w") as f:
        f.write(key)
    os.chmod(KEY_FILE, 0o600)
    return key


_fernet = Fernet(_load_key().encode()) if Fernet else None
_read_failed = False          # 与 secrets_api 同一教训：读失败时拒绝写盘，别把已有凭据清空


def _load():
    global _read_failed
    _read_failed = False
    if not os.path.exists(STORE):
        return []
    try:
        with open(STORE, "rb") as f:
            raw = _fernet.decrypt(f.read())
        items = json.loads(raw)
        return items if isinstance(items, list) else []
    except Exception:
        _read_failed = True
        return []


def _save(items):
    """原子落盘；返回 False = 拒绝覆盖损坏文件。"""
    if _read_failed:
        return False
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = STORE + ".tmp"
    with open(tmp, "wb") as f:
        f.write(_fernet.encrypt(json.dumps(items, ensure_ascii=False).encode("utf-8")))
    os.chmod(tmp, 0o600)
    os.replace(tmp, STORE)
    return True


def _secret_of(account):
    return _fernet.decrypt(account["secret"].encode()).decode("utf-8", "ignore")


# ---------------------------------------------------------------- 账号 CRUD
def _public(a):
    """对外形态：绝不含 secret 明文。"""
    return {
        "id": a.get("id", ""),
        "email": a.get("email", ""),
        "nickname": a.get("nickname", ""),
        "imap_host": a.get("imap_host", ""),
        "imap_port": a.get("imap_port", 993),
        "imap_security": a.get("imap_security", "ssl"),
        "smtp_host": a.get("smtp_host", ""),
        "smtp_port": a.get("smtp_port", 465),
        "smtp_security": a.get("smtp_security", "ssl"),
        "allow_direct_send": bool(a.get("allow_direct_send")),
        "has_secret": bool(a.get("secret")),
        "default": bool(a.get("default")),
        "last_test": a.get("last_test") or {"ok": None, "ts": 0, "error": ""},
    }


def list_accounts():
    with _lock:
        return [_public(a) for a in _load()]


def normalize(body):
    """把 App 传来的表单规范化成内部账号形态（不含 secret 加密）。"""
    addr = (body.get("email") or "").strip().lower()
    nick = (body.get("nickname") or "").strip()
    imap_h = (body.get("imap_host") or "").strip()
    imap_p = body.get("imap_port")
    imap_sec = (body.get("imap_security") or "").strip().lower()
    smtp_h = (body.get("smtp_host") or "").strip()
    smtp_p = body.get("smtp_port")
    smtp_sec = (body.get("smtp_security") or "").strip().lower()
    (gi_h, gi_p, gi_sec), (gs_h, gs_p, gs_sec) = guess_servers(addr)

    def _port(raw, fallback):
        try:
            return int(raw) if raw not in (None, "") else fallback
        except (TypeError, ValueError):
            return fallback

    def _sec(raw, port, guessed):
        if raw in ("ssl", "starttls", "none"):
            return raw
        return "starttls" if port == 587 else guessed

    imap_p = _port(imap_p, gi_p)
    smtp_p = _port(smtp_p, gs_p)
    return {
        "email": addr,
        "nickname": nick,
        "imap_host": imap_h or gi_h,
        "imap_port": imap_p,
        "imap_security": _sec(imap_sec, imap_p, gi_sec),
        "smtp_host": smtp_h or gs_h,
        "smtp_port": smtp_p,
        "smtp_security": _sec(smtp_sec, smtp_p, gs_sec),
        "allow_direct_send": bool(body.get("allow_direct_send")),
        "default": bool(body.get("default")),
    }


def save_account(body):
    """新增或更新（body.id 为空 = 新增）。返回生效账号。

    编辑时邮箱地址可留空 → 沿用旧值（否则只改昵称会被判「地址不合法」）。
    """
    with _lock:
        items = _load()
        aid = (body.get("id") or "").strip()
        old = next((a for a in items if a.get("id") == aid), None) if aid else None
        if old and not (body.get("email") or "").strip():
            body = {**body, "email": old.get("email", "")}
            if not (body.get("imap_host") or "").strip():
                body["imap_host"] = old.get("imap_host", "")
            if body.get("imap_port") in (None, ""):
                body["imap_port"] = old.get("imap_port")
            if not (body.get("smtp_host") or "").strip():
                body["smtp_host"] = old.get("smtp_host", "")
            if body.get("smtp_port") in (None, ""):
                body["smtp_port"] = old.get("smtp_port")
        norm = normalize(body)
        if not norm["email"] or "@" not in norm["email"]:
            raise ValueError("邮箱地址不合法")
        secret = (body.get("secret") or "").strip()
        if old:
            if secret:
                norm["secret"] = _fernet.encrypt(secret.encode()).decode()   # v4.0.x: 编辑时填了新授权码也要落盘(旧码漏了这分支)
            else:
                norm["secret"] = old.get("secret", "")   # 留空 = 不修改
            norm["id"] = old["id"]
            norm["last_test"] = old.get("last_test") or {"ok": None, "ts": 0, "error": ""}
            items = [a for a in items if a.get("id") != aid] + [norm]
        else:
            if not secret:
                raise ValueError("新账号必须填写授权码/密码")
            norm["id"] = uuid.uuid4().hex[:12]
            norm["secret"] = _fernet.encrypt(secret.encode()).decode()
            norm["last_test"] = {"ok": None, "ts": 0, "error": ""}
            items = items + [norm]
        if norm.get("default"):
            for a in items:
                a["default"] = a.get("id") == norm["id"]
        if not _save(items):
            raise RuntimeError("凭据文件无法解密，已拒绝写入以免清空已有账号")
        return _public(norm)


def delete_account(aid):
    with _lock:
        items = _load()
        rest = [a for a in items if a.get("id") != aid]
        if len(rest) == len(items):
            return False
        if not _save(rest):
            raise RuntimeError("凭据文件无法解密，已拒绝写入")
        return True


def find_account(aid=None, email=None):
    with _lock:
        items = _load()
    if aid:
        hit = next((a for a in items if a.get("id") == aid), None)
        if hit:
            return hit
    if email:
        hit = next((a for a in items if a.get("email") == (email or "").strip().lower()), None)
        if hit:
            return hit
    return next((a for a in items if a.get("default")), None) or (items[0] if items else None)


def pick_account(aid=None, email=None):
    a = find_account(aid, email)
    if not a:
        raise ValueError("没有可用邮箱账号，请先在设置里添加")
    return a


# ---------------------------------------------------------------- IMAP
def _imap_login_with(a, pwd):
    host, port, sec = a["imap_host"], int(a["imap_port"]), a.get("imap_security", "ssl")
    M = imaplib.IMAP4_SSL(host, port) if sec != "none" else imaplib.IMAP4(host, port)
    # v4.0.x: py3.12+ 的 imaplib.socket() 是方法、3.11 及以下是属性——兼容两种形态
    sock = M.socket() if callable(M.socket) else M.socket
    sock.settimeout(READ_TIMEOUT)
    # v4.0.x: 163/126/yeah.net(Coremail)要求 LOGIN 前先发 IMAP ID(RFC2971)声明客户端身份，
    # 否则即使授权码正确也报「LOGIN Login error or password error」（网易风控拦截，非密码错误）。
    # 对其他邮箱发 ID 也无害（RFC 合法扩展，QQ/Gmail 等均忽略）。幂等：Commands 注册只做一次。
    if not hasattr(imaplib, "_ql_id_registered"):
        imaplib.Commands["ID"] = ("AUTH", "NONAUTH", "SELECTED")
        imaplib._ql_id_registered = True
    try:
        M._simple_command("ID", '("name" "qingliao" "version" "1.0.0" "vendor" "qingliao")')
        M._untagged_response("OK", ["ID completed"], "ID")
    except Exception:  # noqa: BLE001 — 老服务器不支持 ID 时照常登录，不阻断
        pass
    M.login(a["email"], pwd)
    return M


def _imap_connect(a):
    old = socket.getdefaulttimeout()
    socket.setdefaulttimeout(CONNECT_TIMEOUT)
    try:
        return _imap_login_with(a, _secret_of(a))
    finally:
        socket.setdefaulttimeout(old)


def _hdr(v):
    try:
        return str(make_header(decode_header(v or "")))
    except Exception:
        return v or ""


def _addr_list(v):
    out = []
    for name, mail in email.utils.getaddresses([v or ""]):
        out.append(("%s <%s>" % (name, mail)) if name else mail)
    return ", ".join(out)


def list_messages(aid=None, email_addr=None, folder="INBOX", limit=20, unread_only=False, query=""):
    a = pick_account(aid, email_addr)
    try:
        limit = max(1, min(int(limit or 20), MAX_LIMIT))
    except (TypeError, ValueError):
        limit = 20
    M = _imap_connect(a)
    try:
        M.select(folder or "INBOX")
        if unread_only:
            crit = ("UNSEEN",)
        elif query:
            crit = ('SUBJECT', '("%s")' % query.replace('"', " "))
        else:
            crit = ("ALL",)
        ids = M.search(None, *crit)[1][0].split()
        ids = [x.decode() for x in ids][-limit:][::-1]
        out = []
        for uid in ids:
            try:
                typ, data = M.fetch(uid, "(BODY.PEEK[HEADER.FIELDS (FROM TO SUBJECT DATE)] FLAGS)")
                blob = b"".join(p[1] for p in data if isinstance(p, tuple))
                msg = email.message_from_bytes(blob)
                flags = re.findall(r"\\Seen", str(data[0]))
                date = ""
                try:
                    date = parsedate_to_datetime(msg.get("Date")).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    date = msg.get("Date") or ""
                out.append({
                    "uid": uid,
                    "from": _addr_list(msg.get("From")),
                    "to": _addr_list(msg.get("To")),
                    "subject": _hdr(msg.get("Subject")) or "(无主题)",
                    "date": date,
                    "unread": not bool(flags),
                })
            except Exception:
                continue
        return {"ok": True, "account": a["id"], "email": a["email"], "count": len(out),
                "messages": out}
    finally:
        try:
            M.close()
        except Exception:
            pass
        try:
            M.logout()
        except Exception:
            pass


def read_message(aid=None, email_addr=None, uid=None, folder="INBOX"):
    a = pick_account(aid, email_addr)
    if not uid:
        raise ValueError("缺 uid")
    M = _imap_connect(a)
    try:
        M.select(folder or "INBOX")
        typ, data = M.fetch(str(uid), "(RFC822)")
        blob = b"".join(p[1] for p in data if isinstance(p, tuple))
    finally:
        try:
            M.close()
        except Exception:
            pass
        try:
            M.logout()
        except Exception:
            pass
    msg = email.message_from_bytes(blob)
    text, html, names = "", "", []
    for part in msg.walk():
        ct = part.get_content_type()
        fn = part.get_filename()
        if fn:
            names.append(_hdr(fn))
            continue
        if ct == "text/plain" and not text:
            text = part.get_payload(decode=True).decode(
                part.get_content_charset() or "utf-8", "ignore")
        elif ct == "text/html" and not html:
            html = part.get_payload(decode=True).decode(
                part.get_content_charset() or "utf-8", "ignore")
    body = text.strip() or re.sub(r"<[^>]+>", " ", re.sub(r"(?is)<head.*?</head>", " ", re.sub(r"(?is)<(style|script)[^>]*>.*?</\\1>", " ", html))).strip()
    return {
        "ok": True, "account": a["id"], "uid": uid,
        "from": _addr_list(msg.get("From")),
        "to": _addr_list(msg.get("To")),
        "subject": _hdr(msg.get("Subject")) or "(无主题)",
        "date": msg.get("Date") or "",
        "attachments": names,
        "text": body[:MAX_BODY_CHARS],
        "html": html[:MAX_BODY_CHARS] if not text else "",
    }


# ---------------------------------------------------------------- SMTP
def test_connection(a, pwd=None):
    """测 IMAP 登录 + SMTP 登录。返回 {"ok","imap","smtp","error"}，不落盘。"""
    pwd = pwd or _secret_of(a)
    res = {"ok": False, "imap": {"ok": False, "error": ""}, "smtp": {"ok": False, "error": ""},
           "error": ""}
    M = None
    try:
        old = socket.getdefaulttimeout()
        socket.setdefaulttimeout(CONNECT_TIMEOUT)
        try:
            M = _imap_login_with(a, pwd)
        finally:
            socket.setdefaulttimeout(old)
        res["imap"] = {"ok": True, "error": ""}
        try:
            M.logout()
        except Exception:
            pass
    except Exception as e:
        res["imap"] = {"ok": False, "error": _errstr(e)}
    try:
        s = _smtp_login(a, pwd)
        res["smtp"] = {"ok": True, "error": ""}
        try:
            s.quit()
        except Exception:
            pass
    except Exception as e:
        res["smtp"] = {"ok": False, "error": _errstr(e)}
    res["ok"] = res["imap"]["ok"] and res["smtp"]["ok"]
    if not res["ok"]:
        res["error"] = res["imap"]["error"] or res["smtp"]["error"]
    return res


def _smtp_login(a, pwd):
    host, port, sec = a["smtp_host"], int(a["smtp_port"]), a.get("smtp_security", "ssl")
    old = socket.getdefaulttimeout()
    socket.setdefaulttimeout(CONNECT_TIMEOUT)
    try:
        if sec == "none":
            s = smtplib.SMTP(host, port, timeout=CONNECT_TIMEOUT)
        else:
            s = smtplib.SMTP_SSL(host, port, timeout=CONNECT_TIMEOUT)
        if sec == "starttls":
            s.starttls()
        s.login(a["email"], pwd)
        return s
    finally:
        socket.setdefaulttimeout(old)


def _errstr(e):
    """错误文本：必须剔除凭据（smtplib/imaplib 认证异常里可能带用户名，绝不带密码）。"""
    s = str(e)
    s = re.sub(r"(?i)(password|passwd|pwd)\s*[=:]\s*\S+", r"\1=***", s)
    return s[:200]


def send_mail(aid=None, email_addr=None, to="", subject="", body="", direct=None):
    a = pick_account(aid, email_addr)
    to = (to or "").strip()
    if not to:
        raise ValueError("缺收件人")
    allow = a.get("allow_direct_send") if direct is None else bool(direct)
    if not allow:
        # 不直发：只回草稿，由 App 用系统邮件界面预填、用户点发送
        return {"ok": True, "draft": True, "from": a["email"], "to": to,
                "subject": subject or "", "body": body or "",
                "note": "该账号未开启「允许 AI 直接发送」，已生成草稿，请在邮件界面确认发送"}
    msg = EmailMessage()
    msg["From"] = a["email"]
    msg["To"] = to
    msg["Subject"] = subject or ""
    msg.set_content(body or "")
    pwd = _secret_of(a)
    s = _smtp_login(a, pwd)
    try:
        s.send_message(msg)
    finally:
        try:
            s.quit()
        except Exception:
            pass
    return {"ok": True, "draft": False, "from": a["email"], "to": to,
            "subject": subject or "", "ts": int(time.time())}


# ---------------------------------------------------------------- Handler
class MailHandler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token")

    def _send(self, code, obj):
        b = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))     # CFStream 按长度收满，缺了会截断
        self.end_headers()
        self.wfile.write(b)

    def _check_auth(self):
        try:
            import auth_api
        except Exception:
            return not os.environ.get("QL_PASSWORD")
        return auth_api.check_auth(self.headers, "X-Mail-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def _body(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except Exception:
            n = 0
        if n <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode("utf-8", "ignore") or "{}")
        except Exception:
            return {}

    def log_message(self, fmt, *args):        # 静默（避免噪声，错误已在响应里）
        pass

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"ok": False, "error": "未授权"})
            return
        p = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(p.query)
        one = lambda k, d=None: (q.get(k) or [d])[0]
        try:
            if p.path.startswith("/api/mail/accounts"):
                self._send(200, {"ok": True, "accounts": list_accounts(), "path": STORE})
            elif p.path.startswith("/api/mail/messages"):
                self._send(200, list_messages(
                    aid=one("account"), email_addr=one("email"),
                    folder=one("folder", "INBOX"), limit=one("limit", 20),
                    unread_only=one("unread", "0") in ("1", "true", "yes"),
                    query=one("q", "")))
            elif p.path.startswith("/api/mail/message"):
                self._send(200, read_message(aid=one("account"), email_addr=one("email"),
                                             uid=one("uid"), folder=one("folder", "INBOX")))
            else:
                self._send(404, {"ok": False, "error": "Not Found"})
        except Exception as e:
            self._send(200, {"ok": False, "error": _errstr(e)})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"ok": False, "error": "未授权"})
            return
        p = urllib.parse.urlparse(self.path)
        body = self._body()
        try:
            if p.path.startswith("/api/mail/accounts"):
                self._send(200, {"ok": True, "account": save_account(body)})
            elif p.path.startswith("/api/mail/test"):
                aid = (body.get("account") or "").strip()
                draft = bool(body.get("email")) and not aid
                if draft:
                    a = {**normalize(body), "id": "draft"}
                else:
                    a = pick_account(aid, body.get("email"))
                res = test_connection(a, pwd=(body.get("secret") or "").strip() or None)
                if not draft:               # 测完把结果记回账号卡片的连通状态点
                    with _lock:
                        items = _load()
                        for it in items:
                            if it.get("id") == a.get("id"):
                                it["last_test"] = {"ok": res["ok"],
                                                   "ts": int(time.time()),
                                                   "error": res["error"]}
                        _save(items)
                self._send(200, res)
            elif p.path.startswith("/api/mail/send"):
                self._send(200, send_mail(
                    aid=body.get("account"), email_addr=body.get("email"),
                    to=body.get("to"), subject=body.get("subject"),
                    body=body.get("body"), direct=body.get("direct")))
            else:
                self._send(404, {"ok": False, "error": "Not Found"})
        except Exception as e:
            self._send(200, {"ok": False, "error": _errstr(e)})

    def do_DELETE(self):
        if not self._check_auth():
            self._send(401, {"ok": False, "error": "未授权"})
            return
        p = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(p.query)
        aid = (q.get("id") or [""])[0]
        try:
            if p.path.startswith("/api/mail/accounts") and aid:
                ok = delete_account(aid)
                self._send(200, {"ok": ok,
                                 "error": "" if ok else "账号不存在",
                                 "accounts": list_accounts()})
            else:
                self._send(404, {"ok": False, "error": "Not Found"})
        except Exception as e:
            self._send(200, {"ok": False, "error": _errstr(e)})


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9170
    srv = ThreadingHTTPServer(("0.0.0.0", port), MailHandler)
    print("mail_api on :%d" % port, flush=True)
    srv.serve_forever()
