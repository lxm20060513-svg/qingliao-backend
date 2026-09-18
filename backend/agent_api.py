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
        # v2.0.116 review：统一走 auth_api 校验（原 X-Auth-Token==pw 永远不匹配）
        import auth_api
        return auth_api.check_auth(self.headers, "X-Agent-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        # v2.0.116 review：显式鉴权（原接口无鉴权，内网任意访问）
        if not self._auth():
            self._send(401, {"ok": False, "error": "unauthorized"})
            return
        if self.path.startswith("/api/agent/keywords"):
            self._send(200, {"ok": True, **get_keywords()})
        elif self.path.startswith("/api/agent/rules"):
            # v2.0.113：Agent 记忆规则可视化（agent_rules.py，重写时被覆盖补回）
            import agent_rules
            self._send(200, {"ok": True, "rules": agent_rules.list_rules()})
        elif self.path.startswith("/api/agent/last_suggestion"):
            # v2.0.116：最近主动建议（看板展示）
            import suggest_engine
            self._send(200, {"ok": True, "suggestion": suggest_engine.last_suggestion()})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        # v2.0.116 review：显式鉴权
        if not self._auth():
            self._send(401, {"ok": False, "error": "unauthorized"})
            return
        if self.path.startswith("/api/agent/keywords"):
            try:
                n = int(self.headers.get("Content-Length") or 0)
                d = json.loads(self.rfile.read(n) or b"{}")
                ok, msg = add_keyword(d.get("list", ""), d.get("word", ""))
                self._send(200, {"ok": ok, "message": msg, **get_keywords()})
            except Exception as e:
                self._send(400, {"ok": False, "error": str(e)[:200]})
        elif self.path.startswith("/api/agent/rules"):
            import agent_rules
            try:
                n = int(self.headers.get("Content-Length") or 0)
                d = json.loads(self.rfile.read(n) or b"{}")
                # v3.9.40（#19）：带 id = 改现有规则，不带 = 新增。
                # 走 POST 而不是新加 do_PATCH：/api/agent 已在 ROUTE_TABLE，relay/nginx/lucky
                # 白名单也只认既有方法，PATCH 要动的东西比这条功能本身多得多（见 cron PATCH 501）。
                if d.get("id"):
                    ok, msg = agent_rules.update_rule(d["id"], d.get("pattern", ""))
                else:
                    ok, msg = agent_rules.add_rule(d.get("pattern", ""))
                self._send(200, {"ok": ok, "message": msg, "rules": agent_rules.list_rules()})
            except Exception as e:
                self._send(400, {"ok": False, "error": str(e)[:200]})
        elif self.path.startswith("/api/agent/suggest"):
            # v2.0.116：看板智能建议（基于天气/NAS/设备状态生成简短建议）
            import stream_api
            try:
                n = int(self.headers.get("Content-Length") or 0)
                d = json.loads(self.rfile.read(n) or b"{}")
                context = (d.get("context") or "")[:400]
                prompt = ("你是家庭智能管家。基于以下家庭状态给出 1-2 条简短实用的建议，"
                          "中文，每条不超过 30 字，直接列点，不要客套。\n家庭状态：\n" + context)
                body = {"model": stream_api.AGENT_MODEL,
                        "messages": [{"role": "user", "content": prompt}],
                        "stream": False, "max_tokens": 300}
                resp = stream_api._chat_once(body)
                text = (resp.get("choices") or [{}])[0].get("message", {}).get("content", "")
                self._send(200, {"ok": True, "text": text.strip()[:400]})
            except Exception as e:
                self._send(200, {"ok": False, "text": "", "error": str(e)[:150]})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def do_DELETE(self):
        # v2.0.116 review：显式鉴权
        if not self._auth():
            self._send(401, {"ok": False, "error": "unauthorized"})
            return
        if self.path.startswith("/api/agent/keywords"):
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            list_name = (q.get("list") or [""])[0]
            word = (q.get("word") or [""])[0]
            ok, msg = remove_keyword(list_name, word)
            self._send(200, {"ok": ok, "message": msg, **get_keywords()})
        elif self.path.startswith("/api/agent/rules"):
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            rid = (q.get("id") or [""])[0]
            import agent_rules
            ok, msg = agent_rules.delete_rule(rid)
            self._send(200, {"ok": ok, "message": msg, "rules": agent_rules.list_rules()})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, *a):
        pass
