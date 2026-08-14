#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""轻聊后端统一入口：单进程运行全部 6 个 API 服务。

端口保持不变（nginx/前端零改动）：
  9125 cron      (127.0.0.1 仅本机)  -> Hermes API 代理
  9127 ha        (0.0.0.0)           -> Home Assistant 代理
  9128 logs      (0.0.0.0)           -> 系统日志
  9129 files     (0.0.0.0)           -> 文件管理
  9130 skills    (0.0.0.0)           -> 技能列表
  9131 sessions  (0.0.0.0)           -> 会话同步
  9132 stream    (0.0.0.0)           -> 流式执行代理（后端持流，前端轮询）

用法：python3 qingliao_all.py
systemd: qingliao.service (Type=simple, Restart=always)
"""
import importlib
import threading
import time
from http.server import ThreadingHTTPServer

MODULES = ["auth_api",
    "secrets_api",
    "router_api", "cron_api", "ha_proxy", "logs_api", "files_api", "sessions_api", "stream_api", "docker_api", "kb_api", "hw_api", "memory_api", "weather_api"]

# (name, host, port, module, handler_attr)
SERVICES = [
    ("cron",     "127.0.0.1", 9125, "cron_api",     "Handler"),
    ("ha",       "0.0.0.0",   9127, "ha_proxy",     "HAProxyHandler"),
    ("logs",     "0.0.0.0",   9128, "logs_api",     "LogsHandler"),
    ("files",    "0.0.0.0",   9129, "files_api",    "FilesHandler"),
    ("sessions", "0.0.0.0",   9131, "sessions_api", "SessionsHandler"),
    ("stream",   "0.0.0.0",   9132, "stream_api",   "StreamHandler"),
    ("auth",     "0.0.0.0",   9133, "auth_api",     "AuthHandler"),
    ("secrets",  "127.0.0.1", 9135, "secrets_api",  "Handler"),
    ("router",   "127.0.0.1", 9136, "router_api",   "Handler"),
    ("docker",   "0.0.0.0",   9137, "docker_api",   "DockerHandler"),
    ("kb",       "0.0.0.0",   9138, "kb_api",       "KBHandler"),
    ("hw",       "0.0.0.0",   9139, "hw_api",       "HwHandler"),
    ("memory",   "0.0.0.0",   9140, "memory_api",   "MemoryHandler"),
    ("weather",  "0.0.0.0",   9141, "weather_api",  "WeatherHandler"),
]


def main():
    # 逐个 import（各模块均有 __main__ 保护，import 无副作用）
    mods = {}
    for m in MODULES:
        mods[m] = importlib.import_module(m)
        print(f"[import] {m} ok", flush=True)

    servers = []
    threads = []
    for name, host, port, mod, attr in SERVICES:
        handler_cls = getattr(mods[mod], attr)
        srv = ThreadingHTTPServer((host, port), handler_cls)
        t = threading.Thread(target=srv.serve_forever, daemon=True, name=f"ql-{name}")
        t.start()
        servers.append(srv)
        threads.append(t)
        print(f"[listen] {name} on {host}:{port}", flush=True)

    print(f"[ready] all {len(servers)} services running", flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        for s in servers:
            s.shutdown()
        print("[stop] all services stopped", flush=True)


if __name__ == "__main__":
    main()
