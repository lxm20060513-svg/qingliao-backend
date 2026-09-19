#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""App 诊断上报 API（崩溃 / 卡顿自上报 + App 内诊断页数据源）

接口：
  POST /api/diag/report   ← 上报一条崩溃/卡顿事件（App 端离线队列也会补传到这里）
  GET  /api/diag/recent   ← 最近 N 条事件（诊断页展示 / 事后排查），?limit=20&kind=crash|hang
  GET  /api/diag/ping     ← 连通性 + 延迟探针（诊断页「后端连通性」用，不落盘）
  GET  /api/diag/stats    ← 计数汇总（崩溃数/卡顿数/最近时间）

落盘（QL_DIAG_DIR，默认 /data/diag，不可写则退到 /tmp/qingliao_diag）：
  <DIAG_DIR>/reports.jsonl   每行一条事件（JSONL 追加写，超过 ROTATE_LINES 行自动裁剪）
  <DIAG_DIR>/diag.log        人类可读摘要日志（时间/类型/摘要）

隐私红线（与 App 端 DiagnosticsPayload.codingKeys 白名单双端一致）：
  只接受 版本 / 构建号 / 设备型号 / 系统版本 / 网络类型 / 时间 / 错误摘要 / 调用栈 / 时长 / id / kind / app。
  任何 token / password / content / message / text / prompt / chat / cookie 等字段一律丢弃，
  命中黑名单只记字段名（不记值），写入 dropped 字段用于审计。
"""
import json
import os
import time
import uuid
from http.server import BaseHTTPRequestHandler

# ---------------------------------------------------------------- 存储

MAX_STACK_CHARS = 8000
MAX_SUMMARY_CHARS = 300
MAX_BODY_BYTES = 256 * 1024      # 单条上报上限 256KB（防大 body 打爆内存）
ROTATE_LINES = 2000              # jsonl 超过该行数则裁剪
KEEP_LINES = 1000

_DEFAULT_DIR = os.environ.get("QL_DATA_DIR", "/data") + "/diag"
_FALLBACK_DIR = "/tmp/qingliao_diag"

# 字段白名单（与 iOS 端 DiagnosticsPayload.allowedKeys 保持一致）
_ALLOWED = (
    "id", "kind", "ts", "app", "version", "build",
    "device", "os", "network", "summary", "stack", "durationMs",
)
# 隐私黑名单：即使被塞进 body 也一律剔除（只记字段名，不记值）
_BLOCKED = (
    "token", "password", "passwd", "secret", "apikey", "api_key", "authorization",
    "auth", "cookie", "content", "message", "messages", "text", "prompt", "chat",
    "username", "user", "body", "request", "response", "image", "file", "audio",
)


def _store_dir():
    """诊断数据目录（优先 env QL_DIAG_DIR，不可写则退到 /tmp）。"""
    d = os.environ.get("QL_DIAG_DIR") or _DEFAULT_DIR
    try:
        os.makedirs(d, exist_ok=True)
        probe = os.path.join(d, ".w")
        with open(probe, "a"):
            pass
        os.remove(probe)
        return d
    except Exception:
        try:
            os.makedirs(_FALLBACK_DIR, exist_ok=True)
        except Exception:
            pass
        return _FALLBACK_DIR


def _reports_path():
    return os.path.join(_store_dir(), "reports.jsonl")


def _log_path():
    return os.path.join(_store_dir(), "diag.log")


def _sanitize(raw):
    """白名单过滤 + 黑名单剔除。返回 (clean_event, dropped_field_names)。"""
    if not isinstance(raw, dict):
        return {}, []
    dropped = []
    for k in raw.keys():
        lk = str(k).lower()
        if lk in _BLOCKED or any(b == lk or lk.startswith(b + "_") for b in _BLOCKED):
            dropped.append(str(k))
    clean = {}
    for k, v in raw.items():
        if k not in _ALLOWED:
            continue
        if isinstance(v, (str, int, float, bool)) or v is None:
            clean[k] = v
        else:
            clean[k] = str(v)[:MAX_SUMMARY_CHARS]
    # 摘要/栈长度收敛（防超大 payload 落盘）
    if isinstance(clean.get("summary"), str):
        clean["summary"] = clean["summary"][:MAX_SUMMARY_CHARS]
    if isinstance(clean.get("stack"), str):
        clean["stack"] = clean["stack"][:MAX_STACK_CHARS]
    return clean, dropped


def _rotate_if_needed(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        if len(lines) > ROTATE_LINES:
            with open(path, "w", encoding="utf-8") as f:
                f.writelines(lines[-KEEP_LINES:])
    except FileNotFoundError:
        pass
    except Exception as e:
        print("[diag] rotate failed: %s" % e, flush=True)


def store_event(raw):
    """落盘一条事件，返回 (ok, record, dropped)。"""
    clean, dropped = _sanitize(raw)
    rec = {
        "id": str(clean.get("id") or uuid.uuid4().hex[:16]),
        "kind": str(clean.get("kind") or "unknown")[:32],
        "ts": float(clean.get("ts") or time.time()),
        "app": str(clean.get("app") or "qingliao-ios")[:64],
        "version": str(clean.get("version") or "")[:32],
        "build": str(clean.get("build") or "")[:32],
        "device": str(clean.get("device") or "")[:64],
        "os": str(clean.get("os") or "")[:64],
        "network": str(clean.get("network") or "")[:32],
        "summary": str(clean.get("summary") or "")[:MAX_SUMMARY_CHARS],
        "stack": str(clean.get("stack") or "")[:MAX_STACK_CHARS],
        "durationMs": int(clean.get("durationMs") or 0),
        "receivedAt": time.time(),
        "dropped": dropped,
    }
    path = _reports_path()
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        print("[diag] write failed: %s" % e, flush=True)
        return False, rec, dropped
    _rotate_if_needed(path)
    try:
        with open(_log_path(), "a", encoding="utf-8") as f:
            f.write("%s [%s] %s %s | %s\n" % (
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(rec["receivedAt"])),
                rec["kind"], rec["version"], rec["build"],
                rec["summary"].replace("\n", " ")[:200]))
    except Exception:
        pass
    return True, rec, dropped


def read_recent(limit=20, kind=None):
    """读最近 limit 条（最新的在前）。"""
    path = _reports_path()
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return []
    out = []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except Exception:
            continue
        if kind and rec.get("kind") != kind:
            continue
        out.append(rec)
        if len(out) >= max(1, min(int(limit), 200)):
            break
    return out


def stats():
    path = _reports_path()
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = [l for l in f if l.strip()]
    except FileNotFoundError:
        lines = []
    crash = hang = other = 0
    last = None
    for line in lines:
        try:
            k = json.loads(line).get("kind")
        except Exception:
            continue
        if k == "crash":
            crash += 1
        elif k == "hang":
            hang += 1
        else:
            other += 1
    for line in reversed(lines):
        try:
            last = json.loads(line)
            break
        except Exception:
            continue
    return {
        "total": len(lines), "crash": crash, "hang": hang, "other": other,
        "lastAt": (last or {}).get("ts"),
        "lastSummary": (last or {}).get("summary", "")[:120],
    }


# ---------------------------------------------------------------- Handler

class DiagHandler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token, X-Diag-Password")

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _check_auth(self):
        """与其他模块一致：复用 auth_api.check_auth（主进程内存 token）。

        auth_api 仅在部署容器内存在；本机自测（源码树直接跑）无该模块 →
        退化为「未设置 QL_PASSWORD 时放行」，设置了密码则拒绝（不放口子）。
        """
        try:
            import auth_api
        except Exception:
            return not os.environ.get("QL_PASSWORD")
        return auth_api.check_auth(self.headers, "X-Diag-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        # ping 不需要鉴权：诊断页先用它判断「后端是否可达/延迟」（失败即离线）
        if parsed.path.startswith("/api/diag/ping"):
            self._send(200, {"ok": True, "pong": True, "ts": time.time(), "module": "diag"})
            return
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        if parsed.path.startswith("/api/diag/recent"):
            try:
                limit = int(params.get("limit", ["20"])[0])
            except ValueError:
                limit = 20
            kind = params.get("kind", [""])[0] or None
            items = read_recent(limit=limit, kind=kind)
            return self._send(200, {"ok": True, "items": items, "count": len(items)})
        if parsed.path.startswith("/api/diag/stats"):
            return self._send(200, {"ok": True, **stats()})
        self._send(404, {"error": "Not Found"})

    def do_POST(self):
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        if not parsed.path.startswith("/api/diag/report"):
            self._send(404, {"error": "Not Found"})
            return
        try:
            n = int(self.headers.get("Content-Length", 0))
        except ValueError:
            n = 0
        if n > MAX_BODY_BYTES:
            self._send(413, {"ok": False, "error": "payload too large"})
            return
        raw = b""
        try:
            raw = self.rfile.read(n) if n else b""
            body = json.loads(raw or b"{}")
        except Exception:
            self._send(400, {"ok": False, "error": "invalid json"})
            return
        if isinstance(body, dict) and isinstance(body.get("events"), list):
            # 支持批量补传（App 离线队列一次补多条）
            ids = []
            stored = 0
            for ev in body["events"]:
                ok, rec, _ = store_event(ev)
                if ok:
                    stored += 1
                    ids.append(rec["id"])
            return self._send(200, {"ok": True, "stored": stored, "total": len(body["events"]), "ids": ids})
        ok, rec, dropped = store_event(body)
        if not ok:
            return self._send(500, {"ok": False, "error": "storage failed"})
        self._send(200, {"ok": True, "id": rec["id"], "kind": rec["kind"],
                         "ts": rec["ts"], "dropped": dropped, "stored": 1})

    def log_message(self, fmt, *args):
        pass
