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
    "secrets_api", "scenes_api", "asr_api", "agent_api",
    "router_api", "cron_api", "ha_proxy", "logs_api", "files_api", "sessions_api", "stream_api", "docker_api", "kb_api", "hw_api", "memory_api", "weather_api", "automation_api", "push_api", "local_api"]

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
    ("scenes",   "0.0.0.0",   9142, "scenes_api",   "Handler"),
    ("asr",      "0.0.0.0",   9143, "asr_api",      "Handler"),
    ("agent",    "0.0.0.0",   9145, "agent_api",    "Handler"),
    ("automation", "0.0.0.0", 9146, "automation_api", "Handler"),
    ("push", "0.0.0.0", 9147, "push_api", "Handler"),
    ("local", "0.0.0.0", 9149, "local_api", "Handler"),   # v2.0.117：本地模型管理（Ollama 状态/开关/更新）,
]


def main():
    # 逐个 import（各模块均有 __main__ 保护，import 无副作用）
    # v2.0.116 review：import 容错——单模块失败（语法/依赖）不拖垮全部服务
    mods = {}
    for m in MODULES:
        try:
            mods[m] = importlib.import_module(m)
            print(f"[import] {m} ok", flush=True)
        except Exception as e:
            print(f"[import] {m} FAILED: {e}", flush=True)

    servers = []
    threads = []
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
            # v2.0.116 review：端口冲突/模块缺失 → 该服务跳过，其余照常
            print(f"[listen] {name} on {host}:{port} FAILED: {e}（跳过）", flush=True)

    # v2.0.116：主动建议引擎（天气突变/设备长开/低电量巡检，import 时启动）
    try:
        import suggest_engine
        suggest_engine.start_engine()
        print("[engine] suggest_engine started", flush=True)
    except Exception as e:
        print(f"[engine] suggest_engine failed: {e}", flush=True)

    # v2.0.116 review：端口冲突/模块异常降级——失败的端口跳过不阻塞其余服务
    # （原任一端口被占 → 整体崩溃，systemd 无限重启循环）

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
