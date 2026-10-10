#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MCP 工具服务管理 API（v3.5.0 新增）

让轻聊 App 管理 Hermes 的 MCP servers：App 配 key → 本模块写 Hermes config.yaml
的 mcp_servers 段 → 重启 hermes 容器 → 本地模式聊天自动获得 MCP 工具
（Hermes 是 MCP host，工具在 agent loop 中自动可调用）。

端点（unified_router 9127 挂载 /api/mcp 前缀）：
  GET  /api/mcp/servers          列表（key 脱敏）+ 预置模板
  POST /api/mcp/save             {name, template+key 或 url, enabled}
  POST /api/mcp/delete           {name}
  GET  /api/mcp/restart_status   最近一次 hermes 重启结果

安全：只允许 http(s) URL；config 写入为原子替换（临时文件 + rename）；
     key 只写 Hermes config（内网隔离目录），接口返回时一律脱敏。
"""
import json
import os
import re
import subprocess
import tempfile
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler

import yaml

# Hermes config.yaml 路径（qingliao 容器已挂载 /data，hermes-data 同一宿主目录）
# ⚠️ 生产口径：本值是 Hermes 网关真正读取的 config.yaml（mcp_servers 写在这里），
# 不能改成 QL_HERMES_CONFIG（那是轻聊侧 provider key 台账，Hermes 不读）——否则
# App「MCP 工具服务」的保存会写进没人读的文件而静默失效。
HERMES_CONFIG_PATH = os.environ.get("QL_HERMES_DATA", "/data/hermes") + "/config.yaml"
HERMES_CONTAINER = os.environ.get("QL_HERMES_CONTAINER", "hermes-container")
RESTART_STATUS_FILE = "/data/streams_data/mcp_restart_status.json"

# 预置 MCP 模板（App 端展示用；key 由用户填入 URL 的 {key} 占位）
MCP_TEMPLATES = [
    {
        "id": "amap",
        "name": "高德地图",
        "desc": "天气/POI搜索/路径规划/导航/打车等地图能力",
        "url_template": "https://mcp.amap.com/mcp?key={key}",
        "key_hint": "高德开放平台 Web服务 Key（lbs.amap.com 免费申请）",
    },
]

_LOCK = threading.Lock()
_RESTARTING = False

# v4.0.58：改为直接读写 config.yaml **顶层 mcp_servers 段**（不再用 # == qingliao-mcp-*
# == 标记块）。旧的标记块实现只认自己写的那一段 → 手工注册进 mcp_servers 的 office /
# browser-agent / wenyan 在 App「MCP 工具服务」页完全不显示；且保存时会另追加一个重复的
# `mcp_servers:` 顶层键（原段一条不动）。现在：读 = 解析整段；写 = 只重建这一段，段内每个
# 条目的其它字段（headers / tools / command / args / env / timeout …）原样保留。
_SECTION_KEY = "mcp_servers"


def _read_config_text():
    with open(HERMES_CONFIG_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _write_config_text(text):
    # BE20：唯一 tmp + fsync，权限沿用原文件（config.yaml 含 API key，且 Hermes 侧要能读）
    try:
        _st = os.stat(HERMES_CONFIG_PATH)
        _mode = _st.st_mode & 0o777
        _uid, _gid = _st.st_uid, _st.st_gid
    except OSError:
        _mode = 0o644
        _uid = _gid = None
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(HERMES_CONFIG_PATH) or ".", suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    try:
        os.chmod(tmp, _mode)
    except OSError:
        pass
    os.replace(tmp, HERMES_CONFIG_PATH)
    # 2026-09-21 修：mkstemp 产物属 root，replace 后 owner 变 root → Hermes(uid 10000)读不了
    # 只沿用 mode 不够，必须复原 owner（否则 Hermes 报 config.yaml corrupt / EACCES）
    if _uid is not None:
        try:
            os.chown(HERMES_CONFIG_PATH, _uid, _gid)
        except OSError:
            pass


def _section_span(text):
    """定位顶层 mcp_servers 段：返回 (start, end) 字符区间；无该段返回 None。

    段 = 从顶层 `mcp_servers:` 行开始，到下一个「顶格非注释行」（下一个顶层 key）之前。
    """
    m = re.search(r"(?m)^%s:[ \t]*(?:#.*)?$" % re.escape(_SECTION_KEY), text)
    if not m:
        return None
    end = len(text)
    for lm in re.finditer(r"(?m)^(\S.*)$", text[m.end():]):
        end = m.end() + lm.start()
        break
    return m.start(), end


def _load_servers():
    """解析顶层 mcp_servers 段 → dict（保留全部字段）；无段/解析失败返回 {}"""
    try:
        text = _read_config_text()
    except Exception:  # noqa: BLE001
        return {}
    span = _section_span(text)
    if not span:
        return {}
    try:
        data = yaml.safe_load(text[span[0]:span[1]]) or {}
    except Exception:  # noqa: BLE001
        return {}
    servers = data.get(_SECTION_KEY) if isinstance(data, dict) else None
    return servers if isinstance(servers, dict) else {}


def _render_section(servers):
    """把 servers dict 渲染成顶层 mcp_servers 段文本（条目顺序按 dict 插入序）"""
    if not servers:
        return "%s: {}\n" % _SECTION_KEY
    body = yaml.safe_dump(servers, allow_unicode=True, default_flow_style=False,
                          sort_keys=False, width=10 ** 6)
    indented = "".join(("  " + ln if ln.strip() else ln) for ln in body.splitlines(True))
    return "%s:\n%s" % (_SECTION_KEY, indented)


def _save_servers(servers):
    """只替换 config.yaml 顶层的 mcp_servers 段，其余内容逐字节不动"""
    text = _read_config_text()
    block = _render_section(servers)
    span = _section_span(text)
    if span:
        text = text[:span[0]] + block + text[span[1]:]
    else:
        if not text.endswith("\n"):
            text += "\n"
        text += "\n" + block
    _write_config_text(text)


def _restart_hermes_async():
    """异步重启 hermes 容器；结果写状态文件供 /api/mcp/restart_status 查询"""
    global _RESTARTING
    with _LOCK:
        if _RESTARTING:
            return {"ok": False, "error": "重启已在进行中"}
        _RESTARTING = True
    status = {"ok": False, "ts": "", "error": ""}

    def _worker():
        global _RESTARTING
        try:
            import datetime
            status["ts"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            r = subprocess.run(["docker", "restart", HERMES_CONTAINER],
                               capture_output=True, text=True, timeout=120)
            if r.returncode == 0:
                status["ok"] = True
            else:
                status["error"] = (r.stderr or r.stdout or "restart failed")[:300]
        except Exception as exc:  # noqa: BLE001
            status["error"] = str(exc)[:300]
        finally:
            try:
                os.makedirs(os.path.dirname(RESTART_STATUS_FILE), exist_ok=True)
                with open(RESTART_STATUS_FILE, "w", encoding="utf-8") as f:
                    json.dump(status, f, ensure_ascii=False)
            except Exception:  # noqa: BLE001
                pass
            with _LOCK:
                _RESTARTING = False

    threading.Thread(target=_worker, daemon=True, name="mcp-hermes-restart").start()
    return {"ok": True, "msg": "重启已触发，约 30 秒后生效"}


def _mask_key(url):
    return re.sub(r"(key=)[^&]+", r"\1***", url)


class Handler(BaseHTTPRequestHandler):
    """unified_router 委托 handler（照 memory_api 模式）"""

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
        return auth_api.check_auth(self.headers, "X-Mcp-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/mcp/servers"):
            servers = _load_servers()
            safe = {}
            for name, e in servers.items():
                e = e if isinstance(e, dict) else {}
                url = str(e.get("url") or "")
                cmd = str(e.get("command") or "")
                if cmd:
                    kind = "command"
                    summary = "本地命令 · %s" % os.path.basename(cmd)
                else:
                    kind = "url"
                    summary = _mask_key(url) or "（无 url）"
                include = (e.get("tools") or {}).get("include") or []
                safe[name] = {
                    "url": _mask_key(url),
                    "has_key": "key=" in url,
                    "enabled": bool(e.get("enabled", True)),
                    # v4.0.58：新增字段（旧 App 忽略即可）——command 型服务没有 url，
                    # 光靠 url 字段 App 会以为是空条目，故补 type/summary 供展示。
                    "type": kind,
                    "summary": summary,
                    "tools_count": len(include) if isinstance(include, list) else 0,
                }
            self._send(200, {"ok": True, "servers": safe, "templates": MCP_TEMPLATES})
            return
        if parsed.path.startswith("/api/mcp/restart_status"):
            st = {}
            try:
                with open(RESTART_STATUS_FILE, encoding="utf-8") as f:
                    st = json.load(f)
            except Exception:  # noqa: BLE001
                pass
            self._send(200, {"ok": True, "status": st})
            return
        self._send(404, {"error": "Not Found"})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        parsed = urllib.parse.urlparse(self.path)
        try:
            n = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(n) or b"{}") if n else {}
        except Exception:  # noqa: BLE001
            body = {}

        if parsed.path.startswith("/api/mcp/save"):
            self._do_save(body)
            return
        if parsed.path.startswith("/api/mcp/delete"):
            self._do_delete(body)
            return
        self._send(404, {"error": "Not Found"})

    def _do_save(self, body):
        name = re.sub(r"[^A-Za-z0-9_-]", "", str(body.get("name", "")))[:40]
        key = str(body.get("key", "")).strip()
        template = str(body.get("template", "")).strip()
        url_in = str(body.get("url", "")).strip()
        has_enabled = "enabled" in body
        enabled = bool(body.get("enabled", True))

        if not name:
            self._send(400, {"ok": False, "error": "name 必填"})
            return

        # v4.0.58：url 只在本请求确实给了 template/key 或 url 时才动；只带 name(+enabled)
        # 的请求 = 开关已有条目，不得覆盖它原有的字段
        url = None
        if template:
            tpl = next((t for t in MCP_TEMPLATES if t["id"] == template), None)
            if not tpl:
                self._send(400, {"ok": False, "error": "未知模板"})
                return
            if not key:
                self._send(400, {"ok": False, "error": "key 必填"})
                return
            url = tpl["url_template"].replace("{key}", key)
        elif url_in:
            if not re.match(r"^https?://", url_in):
                self._send(400, {"ok": False, "error": "url 须以 http(s):// 开头"})
                return
            url = url_in

        try:
            servers = _load_servers()
            cur = servers.get(name)
            cur = dict(cur) if isinstance(cur, dict) else None
            if cur is not None and cur.get("command") and url:
                self._send(400, {"ok": False,
                                 "error": "该服务是本地命令型（command），不能改写成 URL"})
                return
            if cur is None:
                if not url:
                    self._send(400, {"ok": False, "error": "template+key 或 url 必填一个"})
                    return
                cur = {"url": url}
                servers[name] = cur
            elif url:
                cur["url"] = url
            if has_enabled:
                cur["enabled"] = enabled
            elif "enabled" not in cur:
                cur["enabled"] = True
            _save_servers(servers)
        except Exception as exc:  # noqa: BLE001
            self._send(500, {"ok": False, "error": "写配置失败: %s" % exc})
            return
        self._send(200, {"ok": True, "name": name, "restart": _restart_hermes_async(),
                         "hint": "约 30 秒后聊天即可使用新工具"})

    def _do_delete(self, body):
        name = str(body.get("name", ""))
        try:
            servers = _load_servers()
            if name not in servers:
                self._send(404, {"ok": False, "error": "不存在"})
                return
            servers.pop(name)
            _save_servers(servers)
        except Exception as exc:  # noqa: BLE001
            self._send(500, {"ok": False, "error": "写配置失败: %s" % exc})
            return
        self._send(200, {"ok": True, "restart": _restart_hermes_async()})

    def log_message(self, fmt, *args):
        pass
