#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""统一路由器：单端口 9127 按 /api/* 前缀分发到各模块 Handler。

Phase 2 of backend port consolidation (2026-08-22):
  - 所有 /api/* 请求由本模块统一分发
  - 各模块 Handler 代码零改动——只是不再各自监听端口
  - stream(9132) 保留独立端口（App 直连 + 长连接）
  - auth(9133) 保留独立端口（安全隔离，备用入口）

路由表：/api/<prefix>/ -> 对应模块 Handler

关键修复（v2）：重写 do_GET/do_POST 而非 handle()，
因为 BaseHTTPRequestHandler.handle() 在请求已解析后才被调用，
此时 socket 数据已被消费，子 Handler 无法重新 parse_request()。
"""
import importlib
import sys
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

ROUTE_TABLE = {
    "/api/ha":        ("ha_proxy",     "HAProxyHandler"),
    "/api/logs":      ("logs_api",     "LogsHandler"),
    "/api/files":     ("files_api",    "FilesHandler"),
    "/api/sessions":  ("sessions_api", "SessionsHandler"),
    "/api/auth":      ("auth_api",     "AuthHandler"),
    "/api/cron":      ("cron_api",     "Handler"),
    "/api/secrets":   ("secrets_api",  "Handler"),
    "/api/docker":    ("docker_api",   "DockerHandler"),
    "/api/kb":        ("kb_api",       "KBHandler"),
    "/api/hw":        ("hw_api",       "HwHandler"),
    "/api/memory":    ("memory_api",   "MemoryHandler"),
    "/api/weather":   ("weather_api",  "WeatherHandler"),
    "/api/scenes":    ("scenes_api",   "Handler"),
    "/api/agent":     ("agent_api",    "Handler"),
    "/api/agent/tool": ("stream_api",  "StreamHandler"),
    "/api/automations": ("automation_api", "Handler"),
    "/api/push":      ("push_api",     "Handler"),
    "/api/local":     ("local_api",    "Handler"),
    "/api/notes":     ("notes_api",    "Handler"),
    "/api/inbox":     ("inbox_api",    "Handler"),
    "/api/channel":   ("channel_api",  "Handler"),
    "/api/router":   ("router_api",  "Handler"),
    "/api/mcp":      ("mcp_api",     "Handler"),
    "/api/diag":     ("diag_api",    "DiagHandler"),
    "/api/life":      ("life_api",    "LifeHandler"),
    # v3.9.18 危险操作确认闸门：Hermes pre_tool_call 插件 ↔ 本后端 ↔ App 三方链路
    # 2026-08-22 docker 化后补挂：App/WebUI 主链路（/api/nas、/api/stream）
    # 此前 9127 直连这两个前缀 404，仅 nginx->9132 路径可用
    "/api/nas":     ("stream_api",  "StreamHandler"),
    "/api/stream":  ("stream_api",  "StreamHandler"),
    "/api/tasks":   ("stream_api",  "StreamHandler"),
    # v3.4.25: 16666(lucky) 别名（lucky 未放行 /api/tasks 前缀，借 /api/agent 前缀）
    "/api/agent/tasks": ("stream_api", "StreamHandler"),
    # v3.9.22（B3）TTS 能力探测：/api/agent/tts/probe（借 /api/agent 前缀，零 nginx/relay 改动）
    "/api/agent/tts": ("stream_api", "StreamHandler"),
    # v3.9.21（B2）用量统计：同样借 /api/agent 前缀（lucky 白名单 + relay 已含 /api/agent，
    # 故 nginx 三份 conf 无需改动；/api/usage 仅供内网直连调试）
    "/api/usage": ("stream_api", "StreamHandler"),
    "/api/agent/usage": ("stream_api", "StreamHandler"),
    # v3.9.41（修 bug 清单 C 的另一半）：这两个前缀 App 一直在调、蜂窝 relay 白名单
    # （stream_api.ALLOWED_RELAY）也已放行，但 9127 这里没有路由 → 蜂窝下必 404
    # （Wi-Fi 走 nginx 那条链才通）。
    # 实现本来就在下面这两个模块里，且各自 do_* 开头已有 _auth 闸门，故只需补挂前缀：
    #   /api/history → automation_api（do_GET 列历史 / do_DELETE 清空或按 ids 删）
    #   /api/tts     → stream_api.do_POST（云端神经 TTS，按 provider 分发）
    "/api/history": ("automation_api", "Handler"),
    "/api/tts":     ("stream_api",     "StreamHandler"),
}

# 缓存已导入的模块和 Handler 类
_handler_cache = {}


def _load_handler(prefix):
    """懒加载模块 Handler 类（失败返回 None）"""
    if prefix in _handler_cache:
        return _handler_cache[prefix]

    mod_name, cls_name = ROUTE_TABLE[prefix]
    try:
        mod = importlib.import_module(mod_name)
        cls = getattr(mod, cls_name)
        _handler_cache[prefix] = cls
        return cls
    except Exception as e:
        print(f"[router] load {mod_name}.{cls_name} FAILED: {e}", flush=True)
        _handler_cache[prefix] = None
        return None


def _resolve_handler(path):
    """根据请求路径匹配路由表（最长前缀优先）"""
    for prefix in sorted(ROUTE_TABLE.keys(), key=len, reverse=True):
        if path.startswith(prefix):
            return _load_handler(prefix)
    return None


def _delegate_to_handler(handler_cls, original_handler):
    """将请求委托给子 Handler 处理。

    子 Handler 与原 Handler 共享同一个 socket，但各自独立
    创建 makefile()，所以 HTTP/1.1 keep-alive 不会互相干扰。

    关键：子 Handler 的 do_GET/do_POST 直接操作 self.wfile，
    不需要重新 parse_request()——请求已经在 RouterHandler 中被解析过了。
    """
    if handler_cls is None:
        original_handler.send_response(404)
        original_handler.send_header("Content-Type", "text/plain")
        original_handler.end_headers()
        original_handler.wfile.write(b"Unknown API path")
        return

    # 创建子 Handler 实例，跳过 __init__ 避免重新读取 socket
    sub = handler_cls.__new__(handler_cls)
    sub.request = original_handler.request
    sub.client_address = original_handler.client_address
    sub.server = original_handler.server
    sub.close_connection = True

    # 复制已解析的请求属性
    sub.command = original_handler.command
    sub.path = original_handler.path
    sub.request_version = original_handler.request_version
    sub.headers = original_handler.headers
    sub.rfile = original_handler.rfile
    sub.wfile = original_handler.wfile
    sub._headers_buffer = []  # 子 Handler 自己的 header 缓冲
    # requestline 是 log_request() 所需（send_response 内部调用）
    if hasattr(original_handler, 'requestline'):
        sub.requestline = original_handler.requestline
    if hasattr(original_handler, 'raw_requestline'):
        sub.raw_requestline = original_handler.raw_requestline

    # 调用子 Handler 的 do_GET / do_POST
    method = original_handler.command
    do_method = getattr(sub, f"do_{method}", None)
    if do_method:
        try:
            do_method()
        except Exception as e:
            try:
                sub.send_response(500)
                sub.send_header("Content-Type", "text/plain")
                sub.end_headers()
                sub.wfile.write(f"Internal Server Error: {e}".encode())
            except Exception:
                pass
    else:
        sub.send_response(405)
        sub.send_header("Content-Type", "text/plain")
        sub.end_headers()
        sub.wfile.write(b"Method Not Allowed")


class RouterHandler(BaseHTTPRequestHandler):
    """统一路由 Handler：按 URL 前缀分发到各模块 Handler。

    通过重写 do_GET/do_POST 实现路由分发，而非重写 handle()。
    因为 handle() 在请求已解析后才被调用，此时 socket 数据已被消费。
    """

    def do_GET(self):
        handler_cls = _resolve_handler(self.path)
        _delegate_to_handler(handler_cls, self)

    def do_POST(self):
        handler_cls = _resolve_handler(self.path)
        _delegate_to_handler(handler_cls, self)

    def do_PUT(self):
        handler_cls = _resolve_handler(self.path)
        _delegate_to_handler(handler_cls, self)

    def do_PATCH(self):
        # v3.9.40（#17 前置）：原先**没有**这个 handler —— BaseHTTPRequestHandler 对未定义的
        # 方法直接回 "501 Unsupported method ('PATCH')"，请求根本到不了 cron_api。
        # cron_api.do_PATCH 一直是完整实现的（代理到 Hermes /api/jobs/{id}），所以 501 的根因
        # 在这里，不在 cron_api。补上即恢复定时任务的「编辑」能力。
        handler_cls = _resolve_handler(self.path)
        _delegate_to_handler(handler_cls, self)

    def do_DELETE(self):
        handler_cls = _resolve_handler(self.path)
        _delegate_to_handler(handler_cls, self)

    def log_message(self, format, *args):
        """抑制默认日志（各子 Handler 自己会记录）"""
        pass


def run_server(host="0.0.0.0", port=9127):
    """启动统一路由器"""
    # 预加载所有模块（启动时报错便于排查）
    for prefix, (mod_name, cls_name) in ROUTE_TABLE.items():
        cls = _load_handler(prefix)
        status = "ok" if cls else "FAILED"
        print(f"[router] {prefix} -> {mod_name}.{cls_name}: {status}", flush=True)

    srv = ThreadingHTTPServer((host, port), RouterHandler)
    print(f"[router] listening on {host}:{port}", flush=True)
    return srv


if __name__ == "__main__":
    srv = run_server()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()
