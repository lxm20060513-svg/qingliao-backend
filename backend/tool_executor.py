# -*- coding: utf-8 -*-
"""Agent 工具执行器（v2.0.96 方案B）：供 stream_api 的 Agent 模式调用。
工具全部在 NAS 宿主执行（qingliao 跑宿主 systemd）；docker/HA/系统命令直连。"""

import json
import os
import subprocess
import time
import urllib.request

# ---- 工具 Schema（OpenAI function calling 格式）----

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_time",
            "description": "获取当前日期和时间",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_disk_usage",
            "description": "获取 NAS 磁盘使用情况（各分区已用百分比）",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_service_status",
            "description": "获取服务状态（qingliao 后端 / Hermes 网关 / Home Assistant）",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_temperature",
            "description": "获取 NAS CPU 和 SSD 温度",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "docker_ps",
            "description": "列出 Docker 容器及其运行状态",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "docker_action",
            "description": "启动或停止指定 Docker 容器",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "容器名"},
                    "action": {"type": "string", "enum": ["start", "stop"], "description": "操作"},
                },
                "required": ["name", "action"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ha_list_entities",
            "description": "列出 Home Assistant 的实体（灯 light./空调 climate./开关 switch./安防 alarm），生成场景或控制设备前先查询真实实体 ID",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "ha_call",
            "description": "控制 Home Assistant 实体（开关灯/空调/开关等）。service 如 light.turn_on / light.turn_off / climate.turn_off / switch.toggle",
            "parameters": {
                "type": "object",
                "properties": {
                    "entity": {"type": "string", "description": "实体 ID，如 light.living_room"},
                    "service": {"type": "string", "description": "服务，如 light.turn_on"},
                    "data": {"type": "object", "description": "可选参数（如 temperature）"},
                },
                "required": ["entity", "service"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "scene_save",
            "description": "保存智能家居场景（一组实体操作，一键执行）。如用户说'生成离家模式：关所有灯、关空调、布防'→ 先查实体再构造 actions=[{entity,service,data}]",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "场景名，如 离家模式"},
                    "actions": {"type": "array",
                                "description": "动作列表，每项 {entity: 实体ID, service: 服务如 light.turn_off, data: 可选参数}",
                                "items": {"type": "object"}},
                },
                "required": ["name", "actions"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "scene_run",
            "description": "执行已保存的场景（如 执行离家模式）",
            "parameters": {
                "type": "object",
                "properties": {"name": {"type": "string", "description": "场景名"}},
                "required": ["name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "scene_list",
            "description": "列出已保存的场景",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "获取指定城市的当前天气（中文城市名）",
            "parameters": {
                "type": "object",
                "properties": {"city": {"type": "string", "description": "城市名，如 上海"}},
                "required": ["city"],
            },
        },
    },
]

# ---- 工具执行 ----

HA_URL = os.environ.get("QL_HA_URL", "http://localhost:8123")
HA_TOKEN = os.environ.get("QL_HA_TOKEN", "")


def _sh(cmd, timeout=15):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return (r.stdout + r.stderr).strip()[:2000]
    except Exception as e:
        return f"执行失败: {e}"


def execute(name, args):
    """执行工具，返回字符串结果（供模型读取）"""
    try:
        if name == "get_time":
            return time.strftime("%Y-%m-%d %H:%M:%S %A")
        if name == "get_disk_usage":
            return _sh("df -hT | awk 'NR>1 && $2 !~ /overlay|rootfs|squashfs|tmpfs|devtmpfs/ {print $7, $6\" 已用\", $4\" 可用\"}' | head -8")
        if name == "get_service_status":
            q = _sh("systemctl is-active qingliao 2>/dev/null").strip()
            h = _sh("curl -s -m 3 -o /dev/null -w '%{http_code}' http://localhost:9123/health").strip()
            ha = _sh("curl -s -m 3 -o /dev/null -w '%{http_code}' http://localhost:8123/api/").strip()
            return f"qingliao={q}, hermes={'ok' if h=='200' else '异常('+h+')'}, HA={'ok' if ha=='401' else '异常('+ha+')'}"
        if name == "get_temperature":
            try:
                d = json.loads(_sh("curl -s -m 5 http://localhost:9139/api/hw/status") or "{}")
                return f"CPU {d.get('cpu_temp')}°C, SSD {d.get('ssd_temp')}°C"
            except Exception:
                return "温度查询失败"
        if name == "docker_ps":
            return _sh("docker ps -a --format '{{.Names}}|{{.Status}}' | head -15")
        if name == "docker_action":
            n, a = args.get("name", ""), args.get("action", "")
            r = _sh(f"docker {a} {n} 2>&1", timeout=60)
            return f"docker {a} {n}: {r or '成功'}"
        if name == "ha_call":
            return _ha_call(args.get("entity", ""), args.get("service", ""), args.get("data") or {})
        if name == "ha_list_entities":
            return _ha_list_entities()
        if name == "get_weather":
            city = args.get("city", "上海")
            import urllib.parse
            r = _sh(f"curl -s -m 8 'http://localhost:9141/api/weather?city={urllib.parse.quote(city)}'")
            try:
                d = json.loads(r)
                return f"{city}：{d.get('temp')}°C，天气码 {d.get('code')}" if d.get("ok") else f"查询失败：{d.get('error')}"
            except Exception:
                return f"天气查询失败：{r[:100]}"
        if name == "scene_save":
            import scenes_api
            ok, msg = scenes_api.save_scene(args.get("name", ""), args.get("actions") or [])
            return msg
        if name == "scene_run":
            import scenes_api
            ok, msg = scenes_api.run_scene(args.get("name", ""))
            return msg
        if name == "scene_list":
            import scenes_api
            scenes = scenes_api.list_scenes()
            return "、".join(s.get("name", "") for s in scenes) if scenes else "（暂无场景）"
        return f"未知工具 {name}"
    except Exception as e:
        return f"工具执行异常: {e}"


def _ha_call(entity, service, data):
    try:
        body = json.dumps({"entity_id": entity, **data}).encode()
        req = urllib.request.Request(f"{HA_URL}/api/services/{service}",
                                     data=body, headers={
                                         "Authorization": "Bearer " + HA_TOKEN,
                                         "Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=15) as r:
            return f"HA 调用成功（{service} {entity}）：HTTP {r.status}"
    except Exception as e:
        return f"HA 调用失败: {e}"


def _ha_list_entities():
    """列出 HA 实体（灯/空调/开关/安防/传感器电量）"""
    try:
        req = urllib.request.Request(HA_URL + "/api/states",
                                     headers={"Authorization": "Bearer " + HA_TOKEN}, method="GET")
        with urllib.request.urlopen(req, timeout=15) as r:
            states = json.loads(r.read())
        lines = []
        for s in states:
            eid = s.get("entity_id", "")
            if eid.startswith(("light.", "climate.", "switch.")) or "alarm" in eid \
                    or "battery_level" in eid or "temperature" in eid:
                lines.append(f"{eid} = {s.get('state')}")
        return "\n".join(lines[:60]) or "（无实体）"
    except Exception as e:
        return f"HA 实体查询失败: {e}"
