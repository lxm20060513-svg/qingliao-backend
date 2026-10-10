#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AI 记忆 API：GET /api/memory/list、POST /api/memory/add|delete|update"""
import json
import os
from http.server import BaseHTTPRequestHandler

import memory_store


class MemoryHandler(BaseHTTPRequestHandler):
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
        return auth_api.check_auth(self.headers, "X-Memory-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def _payload(self, ok=True, message="", **extra):
        """统一响应体（第 4 项）。

        🚨 兼容口径：`entries`（**纯字符串**数组）必须原样保留 —— WebUI 前端
        `qllm.js` 和 App 旧版 MemoryView 都直接 map 这个字段渲染；一旦改成 dict 列表，
        那些地方会静默渲染成一片空白（用户看着像"记忆全没了"）。
        新结构另开 `items` 字段（带 status/日期/来源），**新增而非替换**。
        """
        out = {"ok": ok, "entries": memory_store.list_entries(),
               "items": memory_store.list_meta()}
        if message:
            out["message"] = message
        out.update(extra)
        return out

    def _clear_followup(self, *texts):
        """v4.0.x 第 5 项：条目「不再待跟进」时清掉追问留痕。

        为什么要在这里清：追问留痕存在 proactive_followup.json，判定池在 memory.json。
        两份文件之间**没有任何自动同步** —— 用户在记忆页把条目勾销掉（状态改回 active、
        或直接删除、或改正文），proactive 侧下一次扫还会看到它已不在 pending 池里，
        于是靠 followup_event 的剪枝兜底；但剪枝只在 pending 条目存在时才走到
        （live 集合里没有它 → 会被清掉）。真正需要显式清的是这三种**用户主动动作**：
          ① 状态改回 active（勾销）② 删除 ③ 改正文（= 另一条新记忆，旧留痕成孤儿）
        在源头清掉 = 用户点了就立刻生效，不必等下一轮 5 分钟轮询。

        🚨 绝不能让这次清理失败把主流程带崩：proactive_agent 是**可选**模块
        （它 import 失败/被关掉时记忆页仍必须能改状态），所以整体吞异常。
        """
        try:
            import proactive_agent
        except Exception:
            return
        for t in texts:
            if not t:
                continue
            try:
                proactive_agent.clear_followup(t)
            except Exception:
                pass

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/memory/list"):
            self._send(200, self._payload(True))
            return
        self._send(404, {"error": "Not Found"})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        try:
            n = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(n) or b"{}") if n else {}
        except Exception:
            body = {}
        if parsed.path.startswith("/api/memory/add"):
            text = (body.get("text") or "").strip()
            if not text:
                self._send(200, self._payload(False, "内容不能为空"))
                return
            # v4.0.x 第 4 项：手动加的条目标 source="manual"（区别于聊天里自动记的 "chat"），
            # 这样 App 上能一眼看出「这条是我自己加的」还是「它从聊天里学来的」。
            ok = memory_store.add_entry(text, source=body.get("source") or "manual")
            self._send(200, self._payload(ok, "已记住" if ok else "已存在或写入失败"))
            return
        if parsed.path.startswith("/api/memory/delete"):
            text = (body.get("text") or "").strip()
            ok = memory_store.delete_entry(text)
            # v4.0.x 第 5 项：删掉条目 = 取消跟进，清掉它的追问留痕。
            self._clear_followup(text if ok else "")
            self._send(200, self._payload(ok, "已删除" if ok else "删除失败（条目不存在）"))
            return
        if parsed.path.startswith("/api/memory/status"):
            # v4.0.x 第 4 项：只翻状态（active/pending/stale），不动正文。
            # 单独开一个端点而不是塞进 update —— update 是**改写正文**，
            # 复用来改状态会顺手把条目顺序/内容搅乱（update_entry 是就地替换）。
            text = (body.get("text") or "").strip()
            st = (body.get("status") or "").strip()
            if st not in ("active", "pending", "stale"):
                self._send(200, self._payload(False, "状态取值非法（active/pending/stale）"))
                return
            ok = memory_store.set_meta(text, status=st)
            # v4.0.x 第 5 项：勾销语义 = 状态离开 pending（勾销回 active，或标 stale 说它过时了）。
            # 两种都不该再被追问 —— 留着留痕会让用户下次再标 pending 时**继承上一世的 asked 计数**，
            # 表现为「我明明刚标回去，它却从此不再问我了」。
            if ok and st != "pending":
                self._clear_followup(text)
            self._send(200, self._payload(ok, "已更新状态" if ok else "更新失败（条目不存在）"))
            return
        if parsed.path.startswith("/api/memory/update"):
            # v3.9.40（#19）：App 记忆面板就地编辑
            old = (body.get("old") or "").strip()
            new = (body.get("text") or "").strip()
            if not new or len(new) < 2:
                self._send(200, self._payload(False, "内容不能为空"))
                return
            ok = memory_store.update_entry(old, new)
            # v4.0.x 第 5 项：改正文 = 这条记忆换了个内容，旧留痕对不上新正文（孤儿）→ 清掉。
            # 只在成功时清：写失败保留留痕，否则用户白丢一次「已问过」的计数、
            # 下轮就又被问一遍同一条（同一件事被问两次 = 噪音，用户只会关掉主动功能）。
            if ok:
                self._clear_followup(old, new)
            self._send(200, self._payload(ok, "已更新" if ok else "更新失败（原条目不存在或写入出错）"))
            return
        self._send(404, {"error": "Not Found"})

    def log_message(self, fmt, *args):
        pass

