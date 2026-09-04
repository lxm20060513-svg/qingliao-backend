# -*- coding: utf-8 -*-
"""Agent 工具执行器（v2.0.96 方案B）：供 stream_api 的 Agent 模式调用。
工具全部在 NAS 宿主执行（qingliao 跑宿主 systemd）；docker/HA/系统命令直连。"""

import json
import os
import re
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
            "name": "automation_create",
            "description": "创建定时自动化（延迟执行动作，到点自动执行后消失，执行结果仅记录服务器日志、不会主动汇报）。用户说'X分钟后执行Y'（如 5分钟后关闭排气扇、10分钟后关空调）→ 先查实体再调用本工具。delay_seconds 为延迟秒数（如 5分钟=300）。注意：回复用户时如实说明'到点自动执行，无完成通知'，不要承诺'完成后汇报'",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "自动化名称，如 5分钟后关闭排气扇"},
                    "actions": {"type": "array",
                                "description": "动作列表，每项 {entity: 实体ID, service: 服务如 fan.turn_off, data: 可选参数}",
                                "items": {"type": "object"}},
                    "delay_seconds": {"type": "integer", "description": "延迟执行秒数（1分钟=60，5分钟=300，1小时=3600）"},
                },
                "required": ["name", "actions", "delay_seconds"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "automation_list",
            "description": "列出待执行的定时自动化（含剩余秒数）",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "hermes_execute",
            "description": "将超出本地工具能力的任务转交给 Hermes Agent 执行。Hermes 拥有完整工具链（联网搜索/网页、终端命令、文件读写、代码执行、浏览器等）。当用户任务需要这些能力（如查资料、写脚本、操作文件、执行命令）而本地工具无法完成时，调用此工具并把任务完整转述",
            "parameters": {
                "type": "object",
                "properties": {"task": {"type": "string", "description": "完整任务描述（用户原话或转述，含必要上下文）"}},
                "required": ["task"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_memory_usage",
            "description": "获取 NAS 内存使用情况（总量/已用/可用）",
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
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "搜索互联网获取最新信息。当用户询问新闻、实时信息、技术文档、教程、或任何需要联网查询的问题时使用此工具",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string", "description": "搜索关键词"}},
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": "读取文件内容。支持文本文件（代码、配置、日志等）。二进制文件会返回十六进制摘要",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "文件绝对路径"},
                    "offset": {"type": "integer", "description": "起始行号（从1开始，默认1）"},
                    "limit": {"type": "integer", "description": "最大读取行数（默认200）"},
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "写入文件内容（覆盖整个文件）。创建不存在的目录",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "文件绝对路径"},
                    "content": {"type": "string", "description": "要写入的内容"},
                },
                "required": ["path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_files",
            "description": "列出目录内容（文件和子目录）",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "目录绝对路径（默认当前目录）"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_files",
            "description": "按文件名或内容搜索文件。返回匹配的文件路径列表",
            "parameters": {
                "type": "object",
                "properties": {
                    "pattern": {"type": "string", "description": "搜索关键词（文件名或内容）"},
                    "path": {"type": "string", "description": "搜索目录（默认 /volume1）"},
                },
                "required": ["pattern"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "execute_code",
            "description": "执行 Python 或 Shell 代码并返回输出。用于数据分析、文件处理、计算、系统检查等任务",
            "parameters": {
                "type": "object",
                "properties": {
                    "code": {"type": "string", "description": "要执行的代码（Python 或 Shell）"},
                    "language": {"type": "string", "description": "语言：python 或 shell（默认 python）"},
                },
                "required": ["code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "delegate_task",
            "description": "将复杂任务委派给子 Agent 执行。子 Agent 拥有独立上下文和完整工具链，适合需要多步骤推理或长时间运行的任务",
            "parameters": {
                "type": "object",
                "properties": {
                    "task": {"type": "string", "description": "完整任务描述，包含所有必要上下文"},
                },
                "required": ["task"],
            },
        },
    },
]

# ---- 工具执行 ----

HA_URL = os.environ.get("QL_HA_URL", "http://localhost:8123")
HA_TOKEN = os.environ.get("QL_HA_TOKEN", "")


def _sh(cmd, timeout=15):
    """WARNING: shell=True — cmd 必须经过白名单/正则净化，禁止直接拼接用户输入。
    docker_action() 已做容器名正则净化；新增工具调用 _sh 前务必验证参数来源。"""
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return (r.stdout + r.stderr).strip()[:2000]
    except Exception as e:
        return f"执行失败: {e}"


def _fmt_kb(kb):
    kb = float(kb)
    for unit in ["K", "M", "G", "T"]:
        if kb < 1024:
            return f"{kb:.1f}{unit}"
        kb /= 1024
    return f"{kb:.1f}P"


def _disk_usage():
    """宿主存储卷磁盘状态：各存储卷容量/已用/可用/使用率 + 顶层主要占用。
    qingliao 跑在宿主 systemd，df/du 天然是宿主视角（区别于容器内 df 只见自身挂载）。
    注意：勿用 df -hT | head -8——系统分区会占满前 8 行，/volume3 被截断（历史 bug）。"""
    out = []
    df = _sh("df -h /volume1 /volume2 /volume3", timeout=20)
    for row in df.splitlines()[1:]:
        p = row.split()
        if len(p) < 6:
            continue
        mount, size, used, avail, pct = p[5], p[1], p[2], p[3], p[4]
        try:
            pnum = int(pct.rstrip("%"))
        except ValueError:
            pnum = 0
        flag = " 🔴" if pnum >= 85 else (" ⚠️" if pnum >= 75 else "")
        out.append(f"{mount}：{size} 总，已用 {used}，可用 {avail}（{pct}）{flag}")
        du = _sh(f"du -x --max-depth=1 {mount} 2>/dev/null | sort -rn | sed 1d | head -5", timeout=60)
        if du:
            for d in du.splitlines():
                parts = d.split(None, 1)
                if len(parts) == 2:
                    out.append(f"  - {parts[1]} {_fmt_kb(parts[0])}")
    return "\n".join(out) if out else "磁盘查询失败"


def execute(name, args, agent_model=None, agent_provider=None):
    """执行工具，返回字符串结果（供模型读取）
    v3.0.30 fix：agent_model/agent_provider——Agent 分流选定的模型透传给 hermes_execute（转交 9123 同模型）"""
    try:
        if name == "get_time":
            return time.strftime("%Y-%m-%d %H:%M:%S %A")
        if name == "get_disk_usage":
            return _disk_usage()
        if name == "get_service_status":
            q = _sh("systemctl is-active qingliao 2>/dev/null").strip()
            h = _sh("curl -s -m 3 -o /dev/null -w '%{http_code}' http://localhost:9123/health").strip()
            ha = _sh("curl -s -m 3 -o /dev/null -w '%{http_code}' http://localhost:8123/api/").strip()
            return f"qingliao={q}, hermes={'ok' if h=='200' else '异常('+h+')'}, HA={'ok' if ha=='401' else '异常('+ha+')'}"
        if name == "get_temperature":
            try:
                d = json.loads(_sh("curl -s -m 5 http://localhost:9127/api/hw/status") or "{}")
                return f"CPU {d.get('cpu_temp')}°C, SSD {d.get('ssd_temp')}°C"
            except Exception:
                return "温度查询失败"
        if name == "get_memory_usage":
            return _sh("free -h | awk 'NR==1 || /Mem/ {print}'")
        if name == "docker_ps":
            return _sh("docker ps -a --format '{{.Names}}|{{.Status}}' | head -15")
        if name == "docker_action":
            n, a = args.get("name", ""), args.get("action", "")
            # v2.0.116 review：命令注入修复——原 shell 拼接可注入（模型参数直接进 shell）；
            # 改 list 参数 + 动作白名单 + 名称净化
            if a not in ("start", "stop", "restart", "rm"):
                return f"不支持的 docker 动作：{a}"
            if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", n or ""):
                return f"非法的容器名：{n}"
            r = subprocess.run(["docker", a, n], capture_output=True, text=True, timeout=60)
            out = (r.stdout + r.stderr).strip()[:2000]
            return f"docker {a} {n}: {out or '成功'}"
        if name == "ha_call":
            return _ha_call(args.get("entity", ""), args.get("service", ""), args.get("data") or {})
        if name == "ha_list_entities":
            return _ha_list_entities()
        if name == "get_weather":
            city = args.get("city", "上海")
            import urllib.parse
            r = _sh(f"curl -s -m 8 'http://localhost:9127/api/weather?city={urllib.parse.quote(city)}'")
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
        if name == "automation_create":
            import automation_api
            ok, msg = automation_api.create_automation(args.get("name", ""), args.get("actions") or [],
                                                        int(args.get("delay_seconds") or 0))
            return msg
        if name == "automation_list":
            import automation_api
            items = automation_api.list_automations()
            if not items:
                return "当前没有待执行的定时自动化"
            return "\n".join(f"⏱ {a['name']}（剩余 {a['remaining'] // 60} 分 {a['remaining'] % 60} 秒）" for a in items)
        if name == "hermes_execute":
            return _hermes_execute(args.get("task", ""), agent_model, agent_provider)
        if name == "web_search":
            return _web_search(args.get("query", ""))
        if name == "read_file":
            return _read_file(args.get("path", ""), args.get("offset", 1), args.get("limit", 200))
        if name == "write_file":
            return _write_file(args.get("path", ""), args.get("content", ""))
        if name == "list_files":
            return _list_files(args.get("path", "."))
        if name == "search_files":
            return _search_files(args.get("pattern", ""), args.get("path", "/volume1"))
        if name == "execute_code":
            return _execute_code(args.get("code", ""), args.get("language", "python"))
        if name == "delegate_task":
            return _delegate_task(args.get("task", ""), agent_model, agent_provider)
        return f"未知工具 {name}"
    except Exception as e:
        return f"工具执行异常: {e}"


def _web_search(query):
    """DuckDuckGo 网页搜索（免费无需 API key）"""
    if not query:
        return "搜索词为空"
    try:
        import urllib.parse, re
        q = urllib.parse.quote(query)
        req = urllib.request.Request(
            f"https://html.duckduckgo.com/html/?q={q}",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        # 提取搜索结果标题和摘要
        results = []
        blocks = re.findall(r'class="result__a"[^>]*href="([^"]*)"[^>]*>([^<]+)</a>.*?class="result__snippet"[^>]*>(.*?)</(?:a|span|div)', html, re.DOTALL)
        for url, title, snippet in blocks[:5]:
            snippet = re.sub(r'<[^>]+>', '', snippet).strip()
            results.append(f"**{title.strip()}**\n{snippet}\n{url}")
        if not results:
            # fallback：简单提取
            titles = re.findall(r'class="result__a"[^>]*>([^<]+)', html)
            results = [f"- {t.strip()}" for t in titles[:5]]
        return "\n\n".join(results) if results else "未找到搜索结果"
    except Exception as e:
        return f"搜索失败：{str(e)[:150]}"


def _read_file(path, offset=1, limit=200):
    """读取文件内容"""
    if not path:
        return "路径为空"
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
        total = len(lines)
        start = max(0, offset - 1)
        end = min(total, start + limit)
        selected = lines[start:end]
        result = f"文件：{path}（共{total}行，显示{start+1}-{end}行）\n"
        result += "=" * 40 + "\n"
        for i, line in enumerate(selected, start=start + 1):
            result += f"{i:4d}| {line}"
        if end < total:
            result += f"\n... 还有 {total - end} 行未显示"
        return result
    except Exception as e:
        return f"读取失败：{str(e)[:150]}"


def _write_file(path, content):
    """写入文件"""
    if not path:
        return "路径为空"
    try:
        import os
        os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        return f"已写入 {path}（{len(content)} 字符）"
    except Exception as e:
        return f"写入失败：{str(e)[:150]}"


def _list_files(path='.'):
    """列出目录内容"""
    if not path:
        path = '.'
    try:
        import os
        entries = []
        for name in sorted(os.listdir(path)):
            full = os.path.join(path, name)
            if os.path.isdir(full):
                entries.append(f"📁 {name}/")
            else:
                size = os.path.getsize(full)
                entries.append(f"📄 {name} ({_fmt_kb(size / 1024)})")
        if not entries:
            return f"{path} 为空目录"
        return f"目录：{path}（{len(entries)} 项）\n" + "\n".join(entries[:50])
    except Exception as e:
        return f"列目录失败：{str(e)[:150]}"


def _search_files(pattern, path='/volume1'):
    """按文件名搜索"""
    if not pattern:
        return "搜索词为空"
    try:
        import os
        results = []
        for root, dirs, files in os.walk(path):
            # 跳过隐藏目录和系统目录
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ('proc', 'sys')]
            for name in files + dirs:
                if pattern.lower() in name.lower():
                    full = os.path.join(root, name)
                    results.append(full)
                    if len(results) >= 20:
                        break
            if len(results) >= 20:
                break
        if not results:
            return f"未找到匹配 '{pattern}' 的文件"
        return f"找到 {len(results)} 个匹配：\n" + "\n".join(results)
    except Exception as e:
        return f"搜索失败：{str(e)[:150]}"


def _execute_code(code, language='python'):
    """执行 Python 或 Shell 代码"""
    if not code:
        return "代码为空"
    try:
        import subprocess
        if language == 'shell':
            result = subprocess.run(
                code, shell=True, capture_output=True, text=True, timeout=30
            )
        else:
            result = subprocess.run(
                ['python3', '-c', code], capture_output=True, text=True, timeout=30
            )
        output = result.stdout
        if result.stderr:
            output += f"\n[stderr]\n{result.stderr}"
        if result.returncode != 0:
            output += f"\n[exit code: {result.returncode}]"
        return output.strip()[:3000] if output.strip() else "（无输出）"
    except subprocess.TimeoutExpired:
        return "执行超时（30秒限制）"
    except Exception as e:
        return f"执行失败：{str(e)[:150]}"


def _delegate_task(task, agent_model=None, agent_provider=None):
    """委派任务给子 Agent（通过 Hermes gateway）"""
    if not task:
        return "任务为空"
    try:
        import stream_api
        body = {
            "model": agent_model or "deepseek-v4-flash",
            "messages": [
                {"role": "system", "content": "你是轻聊子 Agent。用户把任务转交给你执行。直接完成任务，用中文简洁回复结果。"},
                {"role": "user", "content": task}
            ],
            "stream": False,
            "max_tokens": 3000
        }
        if agent_provider and agent_provider not in ("", "local"):
            body["provider"] = agent_provider
        req = urllib.request.Request(
            stream_api.HERMES_URL,
            data=json.dumps(body).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer " + stream_api.HERMES_KEY
            },
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=300) as r:
            d = json.loads(r.read())
        content = d.get("choices", [{}])[0].get("message", {}).get("content", "")
        return content or "（子 Agent 无返回内容）"
    except Exception as e:
        return f"委派失败：{str(e)[:150]}"


def _hermes_execute(task, agent_model=None, agent_provider=None):
    """转交 Hermes 执行（9123 完整工具链：联网/终端/文件/代码）
    v3.0.30 fix：使用 Agent 分流选定的模型/provider（原硬编码 deepseek-v4-flash 且不带 provider，
    导致转交 9123 时无论设置选什么模型都走 Hermes 默认模型）"""
    if not task:
        return "任务为空"
    try:
        # 复用 stream_api 的 Hermes 配置（运行时 import 避免循环依赖）
        import stream_api
        body = {"model": agent_model or "deepseek-v4-flash",
                "messages": [{"role": "system",
                              "content": "你是家庭 NAS 管家 Hermes。用户把任务转交给你执行，你有完整工具链（联网搜索/终端/文件/代码执行）。直接完成任务，用中文简洁回复结果。"},
                             {"role": "user", "content": task}],
                "stream": False, "max_tokens": 2500}
        if agent_provider and agent_provider not in ("", "local"):
            body["provider"] = agent_provider   # V1.5.3：精确路由，避免回退 Hermes 默认模型
        req = urllib.request.Request(stream_api.HERMES_URL, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json",
                                              "Authorization": "Bearer " + stream_api.HERMES_KEY},
                                     method="POST")
        with urllib.request.urlopen(req, timeout=300) as r:
            d = json.loads(r.read())
        c = ""
        try:
            c = d["choices"][0]["message"]["content"] or ""
        except Exception:
            pass
        return c or "（Hermes 无返回内容）"
    except Exception as e:
        return f"Hermes 转交失败：{str(e)[:150]}"


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
