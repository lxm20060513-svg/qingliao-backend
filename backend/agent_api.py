# -*- coding: utf-8 -*-
"""Agent 关键词管理（v2.0.105）：设置页可查看/添加/删除分流关键词。

存储：QL_DATA_DIR/agent_keywords.json  {"strong": [...], "verbs": [...], "topics": [...]}
stream_api._needs_agent 读取时与内置 AGENT_STRONG/VERBS/TOPICS 合并。
"""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler

BASE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("QL_DATA_DIR", os.path.join(os.path.dirname(BASE), "data"))
KW_FILE = os.path.join(DATA_DIR, "agent_keywords.json")

_lock = threading.Lock()

# 内置默认关键词（与 stream_api 保持一致，仅用于展示"内置"分组）
BUILTIN_STRONG = ["控制", "开关", "启动", "停止", "重启", "关灯", "开灯", "打开", "关闭",
                  "清理", "清空", "执行", "设置", "布防", "离家", "空调", "风扇", "灯", "排气扇"]
BUILTIN_VERBS = ["查", "看", "问", "多少", "怎么样", "状态", "情况", "使用率", "帮我", "运行", "温度"]
BUILTIN_TOPICS = ["天气", "磁盘", "容器", "服务", "内存", "空间", "温度", "系统"]


def _load():
    try:
        with open(KW_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save(d):
    with _lock:
        try:
            tmp = KW_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(d, f, ensure_ascii=False, indent=1)
            os.replace(tmp, KW_FILE)
        except Exception:
            pass


def get_keywords():
    """返回 {内置分组 + 自定义分组}"""
    custom = _load()
    return {
        "builtin": {"strong": BUILTIN_STRONG, "verbs": BUILTIN_VERBS, "topics": BUILTIN_TOPICS},
        "custom": {"strong": custom.get("strong", []), "verbs": custom.get("verbs", []),
                   "topics": custom.get("topics", [])},
    }


def add_keyword(list_name, word):
    word = (word or "").strip()
    if not word:
        return False, "关键词不能为空"
    if list_name not in ("strong", "verbs", "topics"):
        return False, "分组无效（strong/verbs/topics）"
    d = _load()
    lst = d.setdefault(list_name, [])
    if word in lst:
        return False, f"「{word}」已在列表中"
    lst.append(word)
    _save(d)
    return True, f"已添加「{word}」"


def remove_keyword(list_name, word):
    if list_name not in ("strong", "verbs", "topics"):
        return False, "分组无效"
    d = _load()
    lst = d.get(list_name, [])
    if word not in lst:
        return False, "自定义列表中不存在该词（内置词不可删）"
    d[list_name] = [w for w in lst if w != word]
    _save(d)
    return True, f"已删除「{word}」"


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Auth-Token")

    def _send(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        try:
            self.wfile.write(body)
        except Exception:
            pass

    def _auth(self):
        pw = os.environ.get("QL_PASSWORD", "change-me")
        return self.headers.get("X-Auth-Token") == pw

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/api/agent/keywords"):
            self._send(200, {"ok": True, **get_keywords()})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        if self.path.startswith("/api/agent/keywords"):
            try:
                n = int(self.headers.get("Content-Length") or 0)
                d = json.loads(self.rfile.read(n) or b"{}")
                ok, msg = add_keyword(d.get("list", ""), d.get("word", ""))
                self._send(200, {"ok": ok, "message": msg, **get_keywords()})
            except Exception as e:
                self._send(400, {"ok": False, "error": str(e)[:200]})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def do_DELETE(self):
        if self.path.startswith("/api/agent/keywords"):
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            list_name = (q.get("list") or [""])[0]
            word = (q.get("word") or [""])[0]
            ok, msg = remove_keyword(list_name, word)
            self._send(200, {"ok": ok, "message": msg, **get_keywords()})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, *a):
        pass
