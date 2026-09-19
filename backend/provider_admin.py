#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v3.0.82 后端补丁模块（部署到轻聊 backend 目录）：
1. POST /api/stream/builtin-providers  {action:"delete", id:"..."}
   真删 Hermes config.yaml providers 段的内置 provider。
   引用保护：providers 段之外任何 provider: <pid> 引用（model.provider / auxiliary / 通道等）拒绝删除。
   行级删除避免 yaml 整段 dump 重排/泄露 key；删后 safe_load 校验，失败还原备份。
2. POST /api/stream/fetch-models  {base_url, api_key}
   调 OpenAI 兼容 <base_url>/models 拉模型 id 列表（自定义 provider 勾选用）。
"""
import json
import os
import re
import urllib.request

try:
    import yaml as _yaml
except Exception:
    _yaml = None

HERMES_CFG = os.environ.get("QL_HERMES_CONFIG", "/data/hermes_config.yaml")


def _cfg_providers_block_lines():
    """返回 (lines, start, end) —— providers 段行范围 [start, end)（0基，end不含）"""
    with open(HERMES_CFG, encoding="utf-8") as f:
        lines = f.read().splitlines(keepends=True)
    start = None
    for i, ln in enumerate(lines):
        if ln.rstrip("\n") == "providers:":
            start = i
            break
    if start is None:
        return lines, None, None
    end = len(lines)
    for j in range(start + 1, len(lines)):
        ln = lines[j]
        if ln.strip() and not ln.startswith((" ", "\t")):  # 下一个顶层键
            end = j
            break
    return lines, start, end


def _provider_referenced_outside(pid: str) -> list:
    """扫描 providers 段之外对 pid 的引用（provider: xxx 形式），返回引用描述列表"""
    lines, ps, pe = _cfg_providers_block_lines()
    refs = []
    pat = re.compile(r"provider\s*:\s*['\"]?" + re.escape(pid) + r"['\"]?\s*(#.*)?$")
    for i, ln in enumerate(lines):
        if ps is not None and ps <= i < pe:
            continue
        if pat.search(ln):
            refs.append(f"L{i+1}: {ln.strip()[:60]}")
    return refs


def delete_builtin_provider(pid: str) -> dict:
    """真删 config.yaml providers 段的 pid 块（含引用保护+备份回滚）"""
    if not re.match(r"^[a-zA-Z0-9_-]+$", pid):
        return {"ok": False, "error": "非法 provider id"}
    if _yaml is None:
        return {"ok": False, "error": "yaml 模块不可用"}
    bak = HERMES_CFG + ".bak_del_provider"
    try:
        lines, ps, pe = _cfg_providers_block_lines()
        if ps is None:
            return {"ok": False, "error": "config.yaml 无 providers 段"}
        blk_start = None
        for j in range(ps + 1, pe):
            if lines[j].rstrip() == f"  {pid}:":
                blk_start = j
                break
        if blk_start is None:
            return {"ok": False, "error": f"provider「{pid}」不存在"}
        blk_end = pe
        for j in range(blk_start + 1, pe):
            ln = lines[j]
            if ln.strip() and not ln.startswith("    "):  # 下一个同级（2空格）键
                blk_end = j
                break
        refs = _provider_referenced_outside(pid)
        if refs:
            return {"ok": False,
                    "error": f"「{pid}」正被引用，无法删除：{'; '.join(refs[:3])}"}
        with open(bak, "w", encoding="utf-8") as f:
            f.write("".join(lines))
        new_content = "".join(lines[:blk_start] + lines[blk_end:])
        cfg = _yaml.safe_load(new_content)
        if not isinstance(cfg, dict):
            raise ValueError("删除后 YAML 解析失败")
        if pid in (cfg.get("providers") or {}):
            raise ValueError("删除后 provider 仍存在")
        with open(HERMES_CFG, "w", encoding="utf-8") as f:
            f.write(new_content)
        try:
            os.chmod(HERMES_CFG, 0o644)
        except Exception:
            pass
        return {"ok": True, "deleted": pid}
    except Exception as e:
        try:
            if os.path.exists(bak):
                with open(bak, encoding="utf-8") as f:
                    good = f.read()
                with open(HERMES_CFG, "w", encoding="utf-8") as f:
                    f.write(good)
        except Exception:
            pass
        return {"ok": False, "error": str(e)[:200]}


def fetch_models_from_endpoint(base_url: str, api_key: str) -> dict:
    """调 OpenAI 兼容 <base_url>/models 拉模型 id 列表"""
    base = (base_url or "").strip().rstrip("/")
    key = (api_key or "").strip()
    if not base.startswith(("http://", "https://")):
        return {"ok": False, "error": "base_url 必须以 http(s):// 开头"}
    if not key:
        return {"ok": False, "error": "api_key 必填"}
    try:
        req = urllib.request.Request(
            base + "/models",
            headers={
                "Authorization": "Bearer " + key,
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8", "replace"))
        ids = [m.get("id") for m in data.get("data", []) if isinstance(m, dict) and m.get("id")]
        return {"ok": True, "models": ids}
    except Exception as e:
        return {"ok": False, "error": str(e)[:150]}


# ==== v3.9.32：自定义 provider CRUD + HTTP 分发 ====
# 背景：本模块自 v3.0.82 起随包部署，却**从未被任何模块 import** →
#       /api/stream/{builtin-providers,custom-providers,fetch-models} 三个端点一律 404，
#       App「模型管理」里删除内置 provider / 自定义 provider / 拉取模型三处入口全是死路。
#       这里补上唯一入口，由 stream_api 在 /api/stream 前缀下调用（鉴权在 stream_api 侧已完成）。

CUSTOM_JSON_CANDIDATES = [
    # 与其它 App 数据同处（容器同名挂载；宿主可直接查看/备份）
    os.environ.get("QL_DATA_DIR", "/data") + "/custom_providers.json",
    # 兜底：容器 /data（NAS /data）
    "/data/custom_providers.json",
]


def _custom_path() -> str:
    for p_ in CUSTOM_JSON_CANDIDATES:
        if os.path.exists(p_):
            return p_
    return CUSTOM_JSON_CANDIDATES[0]


def load_custom_providers() -> list:
    try:
        with open(_custom_path(), encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_custom_providers(items: list) -> bool:
    p_ = _custom_path()
    try:
        d = os.path.dirname(p_)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = p_ + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        os.chmod(tmp, 0o600)   # 含 api_key，别给全局可读
        os.replace(tmp, p_)
        return True
    except Exception:
        return False


def custom_list() -> dict:
    return {"ok": True, "providers": load_custom_providers()}


def custom_add(provider: dict) -> dict:
    pid = str((provider or {}).get("id") or "").strip()
    if not pid:
        return {"ok": False, "error": "provider.id 必填"}
    items = [x for x in load_custom_providers() if x.get("id") != pid]
    items.append(provider)
    if not _save_custom_providers(items):
        return {"ok": False, "error": "写入 custom_providers.json 失败"}
    return {"ok": True, "id": pid, "count": len(items)}


def custom_delete(pid: str) -> dict:
    pid = (pid or "").strip()
    items = load_custom_providers()
    left = [x for x in items if x.get("id") != pid]
    if len(left) == len(items):
        return {"ok": False, "error": "未找到该自定义 provider"}
    if not _save_custom_providers(left):
        return {"ok": False, "error": "写入 custom_providers.json 失败"}
    return {"ok": True, "count": len(left)}


def custom_refresh_models(pid: str) -> dict:
    """v3.9.34：重新拉取自定义 provider 的模型列表（用存好的 base_url+api_key 调 /models，成功写回 JSON）"""
    items = load_custom_providers()
    p = next((x for x in items if x.get("id") == pid), None)
    if p is None:
        return {"ok": False, "error": "未找到该自定义 provider"}
    r = fetch_models_from_endpoint(str(p.get("base_url") or ""), str(p.get("api_key") or ""))
    if not r.get("ok"):
        return r
    items = [dict(x, models=r["models"]) if x.get("id") == pid else x for x in items]
    if not _save_custom_providers(items):
        return {"ok": False, "error": "写入 custom_providers.json 失败"}
    return {"ok": True, "id": pid, "models": r["models"]}


def handle_post(path: str, body: dict):
    """只处理模型管理三端点，返回 (http_code, payload)；其它路径返回 (404, ...)。"""
    body = body or {}
    if path == "/api/stream/builtin-providers":
        if (body.get("action") or "") != "delete":
            return 400, {"ok": False, "error": "action 仅支持 delete"}
        r = delete_builtin_provider(str(body.get("id") or ""))
        return (200 if r.get("ok") else 400), r
    if path == "/api/stream/fetch-models":
        r = fetch_models_from_endpoint(str(body.get("base_url") or ""),
                                       str(body.get("api_key") or ""))
        return (200 if r.get("ok") else 400), r
    if path == "/api/stream/custom-providers":
        act = (body.get("action") or "").strip()
        if act == "add":
            r = custom_add(body.get("provider") or {})
        elif act == "delete":
            r = custom_delete(str(body.get("id") or ""))
        elif act == "refresh":
            # v3.9.34：按 key 重新拉取模型列表（App 模型管理每个分组一行）
            r = custom_refresh_models(str(body.get("id") or ""))
        else:
            r = {"ok": False, "error": "action 仅支持 add/delete/refresh"}
        return (200 if r.get("ok") else 400), r
    return 404, {"ok": False, "error": "not found"}
