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
