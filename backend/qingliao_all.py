#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""轻聊后端统一入口：单进程运行全部 API 服务。

Phase 3 (2026-08-22)：端口终局 22 → 2
  9127 unified   (0.0.0.0)           -> 统一路由（/api/* 按前缀分发到所有模块）
  9132 stream    (0.0.0.0)           -> 流式执行代理（App 直连，长连接，保留独立）

Phase 1 已完成：14 个模块绑定改 127.0.0.1
Phase 2 已完成：其余模块统一由 9127 路由器分发，端口 22 → 4
Phase 3 已完成：cron(9125)/auth(9133) 独立监听撤除，全部走 9127 路由；端口 4 → 2

用法：python3 qingliao_all.py
systemd: qingliao.service (Type=simple, Restart=always)
"""
import importlib
import threading
import time
from http.server import ThreadingHTTPServer

# ── Phase 3：仅保留流式独立端口 ──
# cron/auth/其余全部模块均由 unified_router 在 9127 统一分发
# （cron_api 用 X-Cron-Password 鉴权，auth_api 用 X-Auth-Token，均已在路由表内）
MODULES = ["stream_api"]

# 保留独立端口的服务（其余由 unified_router 统一分发）
SERVICES = [
    ("stream", "0.0.0.0",   9132, "stream_api", "StreamHandler"),
]

# 统一路由器（Phase 2 核心：22 端口 -> 1 端口）
ROUTER_HOST = "0.0.0.0"
ROUTER_PORT = 9127


def main():
    # 逐个 import 独立服务模块
    mods = {}
    for m in MODULES:
        try:
            mods[m] = importlib.import_module(m)
            print(f"[import] {m} ok", flush=True)
        except Exception as e:
            print(f"[import] {m} FAILED: {e}", flush=True)

    servers = []
    threads = []

    # 启动独立服务
    for name, host, port, mod, attr in SERVICES:
        try:
            handler_cls = getattr(mods[mod], attr)
            srv = ThreadingHTTPServer((host, port), handler_cls)
            t = threading.Thread(target=srv.serve_forever, daemon=True, name=f"ql-{name}")
            t.start()
            servers.append(srv)
            threads.append(t)
            print(f"[listen] {name} on {host}:{port}", flush=True)
        except Exception as e:
            print(f"[listen] {name} on {host}:{port} FAILED: {e}（跳过）", flush=True)

    # 启动统一路由器（Phase 2：分发 /api/* 到所有模块）
    try:
        from unified_router import run_server
        router_srv = run_server(host=ROUTER_HOST, port=ROUTER_PORT)
        router_t = threading.Thread(target=router_srv.serve_forever, daemon=True, name="ql-router")
        router_t.start()
        servers.append(router_srv)
        threads.append(router_t)
    except Exception as e:
        print(f"[listen] router on {ROUTER_HOST}:{ROUTER_PORT} FAILED: {e}（跳过）", flush=True)

    # 主动建议引擎
    try:
        import suggest_engine
        suggest_engine.start_engine()
        print("[engine] suggest_engine started", flush=True)
    except Exception as e:
        print(f"[engine] suggest_engine failed: {e}", flush=True)

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
