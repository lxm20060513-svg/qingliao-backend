#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""知识库 API（简化版 RAG）：文档管理 + 关键词检索，供聊天注入上下文

接口：
  GET  /api/kb/list                    文档列表
  POST /api/kb/upload {name, content}  上传文档（App 提取的文本/txt/md）
  POST /api/kb/delete {name}           删除文档
  POST /api/kb/search {q, top_k}       检索相关段落
  GET  /api/kb/search?q=xxx            检索（GET 形态）

检索策略：文档按 ~800 字切块（带重叠），查询词拆分后按块内命中计数评分，返回 top N。
纯标准库，零依赖；聊天时由 stream_api 在消息含「@知识库」时注入检索结果。
"""
import json
import os
import re
from http.server import BaseHTTPRequestHandler

KB_DIR = os.environ.get("QL_KB_DIR",
                        os.environ.get("QL_KB_DIR", "/data/kb"))
CHUNK_SIZE = 800
CHUNK_OVERLAP = 100
MAX_DOC = 50
MAX_DOC_SIZE = 300000
NAME_RE = re.compile(r"^[\w\u4e00-\u9fff._-]{1,80}$")


def _ensure_dir():
    os.makedirs(KB_DIR, exist_ok=True)


def _chunk(text):
    """段落感知切块（固定长度 + 重叠）"""
    paras = [p.strip() for p in re.split(r"\n+", text) if p.strip()]
    chunks, cur = [], ""
    for p in paras:
        if len(cur) + len(p) + 1 < CHUNK_SIZE:
            cur = cur + "\n" + p if cur else p
        else:
            if cur:
                chunks.append(cur)
            cur = p
    if cur:
        chunks.append(cur)
    result = []
    for c in chunks:
        if len(c) <= CHUNK_SIZE:
            result.append(c)
        else:
            for i in range(0, len(c), CHUNK_SIZE - CHUNK_OVERLAP):
                result.append(c[i:i + CHUNK_SIZE])
    return result


def _terms(q):
    """查询词拆分（空白/常见标点；2 字以上才算词）"""
    return [t for t in re.split(r"[\s,，。；;:：、.!?？()（）\"'\"']+", q)
            if len(t) >= 2]


def _search(q, top_k=4):
    """关键词命中评分检索"""
    terms = _terms(q)
    if not terms:
        return []
    results = []
    _ensure_dir()
    for fname in sorted(os.listdir(KB_DIR)):
        if not fname.endswith(".txt"):
            continue
        try:
            with open(os.path.join(KB_DIR, fname), encoding="utf-8") as f:
                text = f.read()
        except OSError:
            continue
        for i, c in enumerate(_chunk(text)):
            score = 0
            for t in terms:
                n = c.count(t)
                if n:
                    score += n * (2 if len(t) >= 4 else 1)
            if score > 0:
                results.append((score, fname, i, c))
    results.sort(key=lambda x: -x[0])
    return [{"doc": r[1], "chunk": r[2], "text": r[3][:500], "score": r[0]}
            for r in results[:top_k]]


def _list_docs():
    _ensure_dir()
    out = []
    for fname in sorted(os.listdir(KB_DIR)):
        if not fname.endswith(".txt"):
            continue
        p = os.path.join(KB_DIR, fname)
        try:
            out.append({"name": fname[:-4], "size": os.path.getsize(p),
                        "chunks": len(_chunk(open(p, encoding="utf-8").read()))})
        except OSError:
            continue
    return out


class KBHandler(BaseHTTPRequestHandler):
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
        return auth_api.check_auth(self.headers, "X-KB-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def _read_json(self):
        try:
            n = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return {}

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
        params = urllib.parse.parse_qs(parsed.query)
        if parsed.path.startswith("/api/kb/list"):
            self._send(200, {"ok": True, "docs": _list_docs()})
            return
        if parsed.path.startswith("/api/kb/search"):
            q = params.get("q", [""])[0]
            top = int(params.get("top_k", ["4"])[0])
            self._send(200, {"ok": True, "hits": _search(q, top)})
            return
        self._send(404, {"error": "Not Found"})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        body = self._read_json()
        _ensure_dir()
        if parsed.path.startswith("/api/kb/upload"):
            name = (body.get("name") or "").strip()
            content = body.get("content") or ""
            if not NAME_RE.match(name):
                self._send(200, {"ok": False, "message": "名称不合法"})
                return
            if len(content) > MAX_DOC_SIZE:
                self._send(200, {"ok": False, "message": "文档超过 300KB 限制"})
                return
            if len(_list_docs()) >= MAX_DOC and not os.path.exists(
                    os.path.join(KB_DIR, name + ".txt")):
                self._send(200, {"ok": False, "message": "文档数量已达上限(50)"})
                return
            try:
                with open(os.path.join(KB_DIR, name + ".txt"), "w",
                          encoding="utf-8") as f:
                    f.write(content)
                self._send(200, {"ok": True, "message": "已保存",
                                 "chunks": len(_chunk(content))})
            except OSError as e:
                self._send(200, {"ok": False, "message": str(e)[:150]})
            return
        if parsed.path.startswith("/api/kb/delete"):
            name = (body.get("name") or "").strip()
            if not NAME_RE.match(name):
                self._send(200, {"ok": False, "message": "名称不合法"})
                return
            try:
                os.remove(os.path.join(KB_DIR, name + ".txt"))
                self._send(200, {"ok": True, "message": "已删除"})
            except OSError:
                self._send(200, {"ok": False, "message": "文档不存在"})
            return
        self._send(404, {"error": "Not Found"})

    def log_message(self, fmt, *args):
        pass
