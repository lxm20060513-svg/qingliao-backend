# -*- coding: utf-8 -*-
"""场景管理 API（v2.0.96）：一键执行多设备动作组。
- 场景 = {name, actions:[{entity, service, data}]}，存 DATA_DIR/scenes.json
- 生成：Agent 对话（tool_executor.scene_save）或手动 API
- 执行：逐条调用 Home Assistant，汇总结果
端口 9142。"""
import json
import os
import urllib.request
from http.server import BaseHTTPRequestHandler

DATA_DIR = os.environ.get("QL_DATA_DIR", "/data")
SCENES_FILE = os.path.join(DATA_DIR, "scenes.json")
HA_URL = os.environ.get("QL_HA_URL", "http://localhost:8123")
HA_TOKEN = os.environ.get("QL_HA_TOKEN", "")
LOCK = __import__("threading").Lock()


def _load():
    try:
        with open(SCENES_FILE, encoding="utf-8") as f:
            d = json.load(f)
            return d if isinstance(d, list) else []
    except Exception:
        return []


def _save(scenes):
    os.makedirs(DATA_DIR, exist_ok=True)
    with LOCK:
        with open(SCENES_FILE, "w", encoding="utf-8") as f:
            json.dump(scenes, f, ensure_ascii=False, indent=1)


def list_scenes():
    return _load()


def save_scene(name, actions):
    """新增或覆盖场景。actions: [{entity, service, data?}]"""
    if not name or not isinstance(actions, list) or not actions:
        return False, "场景名和动作列表不能为空"
    scenes = _load()
    scenes = [s for s in scenes if s.get("name") != name]
    scenes.append({"name": name, "actions": actions, "created": __import__("time").strftime("%Y-%m-%d %H:%M")})
    _save(scenes)
    return True, f"场景「{name}」已保存（{len(actions)} 个动作）"


def delete_scene(name):
    scenes = [s for s in _load() if s.get("name") != name]
    _save(scenes)
    return True, f"场景「{name}」已删除"


def run_scene(name):
    """逐条执行 HA 调用，返回汇总"""
    scene = next((s for s in _load() if s.get("name") == name), None)
    if not scene:
        return False, f"场景「{name}」不存在"
    results = []
    ok_count = 0
    for act in scene.get("actions", []):
        entity = act.get("entity", "")
        service = act.get("service", "")
        data = act.get("data") or {}
        try:
            # v2.0.102c：service 可能是 "climate.turn_off"（点分隔）——HA API 路径是 /api/services/climate/turn_off（斜杠）
            domain, _, svc = service.partition(".")
            svc_path = svc or domain
            body = json.dumps({"entity_id": entity, **data}).encode()
            req = urllib.request.Request(f"{HA_URL}/api/services/{domain}/{svc_path}", data=body,
                                         headers={"Authorization": "Bearer " + HA_TOKEN,
                                                  "Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=15) as r:
                ok_count += 1
                results.append(f"✅ {service} {entity}")
        except Exception as e:
            results.append(f"❌ {service} {entity}：{str(e)[:80]}")
    summary = f"场景「{name}」执行完成：{ok_count}/{len(scene['actions'])} 成功"
    return ok_count == len(scene["actions"]), summary + "\n" + "\n".join(results)


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Scenes-Password")

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

    def _read_json(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return {}

    def _auth(self):
        # 密码鉴权（与其他模块一致）
        pw = os.environ.get("QL_PASSWORD", "change-me")
        return self.headers.get("X-Scenes-Password") == pw or self.headers.get("X-Auth-Token")

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/api/scenes/list"):
            self._send(200, {"ok": True, "scenes": list_scenes()})
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        body = self._read_json()
        p = self.path
        if p.startswith("/api/scenes/save"):
            ok, msg = save_scene(str(body.get("name") or ""), body.get("actions") or [])
            self._send(200, {"ok": ok, "message": msg})
        elif p.startswith("/api/scenes/delete"):
            ok, msg = delete_scene(str(body.get("name") or ""))
            self._send(200, {"ok": ok, "message": msg})
        elif p.startswith("/api/scenes/run"):
            ok, msg = run_scene(str(body.get("name") or ""))
            self._send(200, {"ok": ok, "message": msg})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, fmt, *args):
        pass
