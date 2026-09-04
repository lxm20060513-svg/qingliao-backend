# -*- coding: utf-8 -*-
"""模型用量聚合（v3.0.36）：查询各 provider 订阅/余额余量，供看板「模型使用量」栏。

支持策略（参考 TokenMeter 调研）：
  - deepseek           官方余额 GET /user/balance（total/granted/topped_up，字符串转 float）
  - stepfun            Step Plan 订阅（官方 /v1/accounts 余额；Step Plan 具体配额接口待官方文档核）
  无公开接口的 provider（opencode/xiaomi/sensenova）不查询 → 卡片显示「控制台查看」，
  由 App 侧降级展示，后端不伪造数据。

返回值统一结构（每条）：
  {provider, name, mode: payg|plan, available: bool,
   balance: {total, granted?, topped_up?, currency}, error?}

所有 key 从 config.yaml providers 段读取（QL_HERMES_CONFIG 指向）。
"""
import json
import os
import urllib.request

try:
    import yaml as _yaml
except Exception:
    _yaml = None


def _cfg() -> dict:
    if _yaml is None:
        return {}
    try:
        with open(os.environ.get("QL_HERMES_CONFIG", "/data/hermes_config.yaml"), encoding="utf-8") as f:
            cfg = _yaml.safe_load(f)
        return cfg or {}
    except Exception:
        return {}


def _provider_cfg(pid: str) -> dict:
    return (_cfg().get("providers") or {}).get(pid) or {}


def _get_json(url, key, timeout=12):
    """GET + Bearer，返回 (status_code, json_obj)；网络/解析错误抛异常由调用方兜底
    v3.0.36 fix：容器内 getent 优先返回 IPv6（opencode.ai 纯 v6 → 连接超时 000），强制 IPv4"""
    import socket
    orig_getaddrinfo = socket.getaddrinfo
    def _v4_only(host, port, family=0, type=0, proto=0, flags=0):
        return orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
    socket.getaddrinfo = _v4_only
    try:
        req = urllib.request.Request(url, headers={
            "Authorization": "Bearer " + key,
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode("utf-8", "replace"))
    finally:
        socket.getaddrinfo = orig_getaddrinfo


def _load_custom() -> list:
    """v3.1.1：自定义 provider（App 新增 API 入口，custom_providers.json，不入 config.yaml）"""
    try:
        with open("/data/streams_data/custom_providers.json", encoding="utf-8") as f:
            import json as _j
            data = _j.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _custom_key(pid: str) -> str:
    """自定义 provider 的 api_key（App 新增的 key 优先于 config.yaml）"""
    for p in _load_custom():
        if p.get("id") == pid:
            return p.get("api_key", "")
    return ""


def query_deepseek() -> dict:
    p = _provider_cfg("deepseek")
    key = _custom_key("deepseek") or p.get("api_key", "")
    if not key:
        return {"provider": "deepseek", "name": "DeepSeek", "mode": "payg",
                "available": False, "error": "未配置 api_key"}
    try:
        _, j = _get_json("https://api.deepseek.com/user/balance", key)
        infos = j.get("balance_infos") or []
        info = infos[0] if infos else {}
        def _f(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return 0.0
        return {
            "provider": "deepseek", "name": "DeepSeek", "mode": "payg",
            "available": bool(j.get("is_available", False)),
            "balance": {
                "total": _f(info.get("total_balance")),
                "granted": _f(info.get("granted_balance")),
                "topped_up": _f(info.get("topped_up_balance")),
                "currency": info.get("currency", "CNY"),
            },
        }
    except Exception as e:
        return {"provider": "deepseek", "name": "DeepSeek", "mode": "payg",
                "available": False, "error": str(e)[:150]}


def query_stepfun() -> dict:
    p = _provider_cfg("stepfun")
    key = _custom_key("stepfun") or p.get("api_key", "")
    if not key:
        return {"provider": "stepfun", "name": "阶跃 StepFun", "mode": "payg",
                "available": False, "error": "未配置 api_key"}
    try:
        # 官方账户余额（platform.stepfun.com 文档：GET /v1/accounts）
        _, j = _get_json("https://api.stepfun.com/v1/accounts", key)
        return {
            "provider": "stepfun", "name": "阶跃 StepFun", "mode": "payg",
            "available": j.get("balance", 0) > 0 if isinstance(j.get("balance"), (int, float)) else False,
            "balance": {
                "total": j.get("balance", 0) if isinstance(j.get("balance"), (int, float)) else 0,
                "currency": "CNY",
            },
            "raw": j,
        }
    except Exception as e:
        # Step Plan 订阅模式：/step_plan 域可能返回 404（无此接口），降级为不可查询
        return {"provider": "stepfun", "name": "阶跃 StepFun", "mode": "plan",
                "available": False, "error": str(e)[:150]}


def query_opencode(pid="opencode-apple") -> dict:
    """opencode zen 订阅用量：GET /zen/go/v1/usage → {usage:{rolling/weekly/monthly:{percent,resetsAt}}}。
    百分比配额（非余额）；必须带浏览器 UA（否则 Cloudflare 403）。
    v3.0.36 fix：容器内 getent 优先 IPv6 → 连接超时；强制 IPv4（socket.getaddrinfo AF_INET）+ 短超时 8s。
    v3.0.80：支持 opencode / opencode-apple 两个 key 复用同一接口。"""
    p = _provider_cfg(pid)
    key = p.get("api_key", "")
    if not key:
        return {"provider": pid, "name": "OpenCode", "mode": "plan",
                "available": False, "error": "未配置 api_key"}
    try:
        import socket
        orig = socket.getaddrinfo
        def _v4(host, port, family=0, type=0, proto=0, flags=0):
            return orig(host, port, socket.AF_INET, type, proto, flags)
        socket.getaddrinfo = _v4
        try:
            req = urllib.request.Request(
                "https://opencode.ai/zen/go/v1/usage",
                headers={
                    "Authorization": "Bearer " + key,
                    "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                },
            )
            with urllib.request.urlopen(req, timeout=8) as r:
                j = json.loads(r.read().decode("utf-8", "replace"))
        finally:
            socket.getaddrinfo = orig
        u = j.get("usage") or {}
        return {
            "provider": pid, "name": "OpenCode", "mode": "plan",
            "available": True,
            "usage": {
                "rolling": u.get("rolling") or {},
                "weekly": u.get("weekly") or {},
                "monthly": u.get("monthly") or {},
            },
        }
    except Exception as e:
        return {"provider": pid, "name": "OpenCode", "mode": "plan",
                "available": False, "error": str(e)[:150]}


def query_siliconflow() -> dict:
    """v3.1.1：硅基流动余额（官方 GET /v1/user/info → data.balance/totalBalance/chargeBalance）。
    key 优先自定义 provider（App 新增入口），兜底 config.yaml。"""
    p = _provider_cfg("siliconflow")
    key = _custom_key("siliconflow") or p.get("api_key", "")
    if not key:
        return {"provider": "siliconflow", "name": "硅基流动", "mode": "payg",
                "available": False, "error": "未配置 api_key"}
    try:
        _, j = _get_json("https://api.siliconflow.cn/v1/user/info", key)
        info = (j.get("data") or {}) if j.get("status") is True else {}
        def _f(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return 0.0
        total = _f(info.get("totalBalance"))
        topped = _f(info.get("chargeBalance"))
        return {
            "provider": "siliconflow", "name": "硅基流动", "mode": "payg",
            "available": bool(info),
            "balance": {
                "total": total,
                "topped_up": topped,
                "granted": max(total - topped, 0.0),
                "currency": "CNY",
            },
        }
    except Exception as e:
        return {"provider": "siliconflow", "name": "硅基流动", "mode": "payg",
                "available": False, "error": str(e)[:150]}


def collect_usage():
    """聚合全部可查询 provider；不可查询的标注 unsupported（App 显示控制台查看）
    v3.0.80：遍历 config providers 段——新增 provider 自动出现在看板，无需改代码。
    v3.1.1 fix：合并自定义 provider（App 新增 API，custom_providers.json）——新增 API 自动出卡片。
    已知接口的查官方接口，未知的降级 unsupported（App 显示「控制台查看」）。"""
    providers = dict(_cfg().get("providers") or {})
    # v3.1.1：合并自定义 provider（App「新增 API」入口写入 custom_providers.json）
    for cp in _load_custom():
        pid = cp.get("id")
        if pid and pid not in providers:
            providers[pid] = cp
    DISPLAY = {"deepseek": "DeepSeek", "stepfun": "阶跃 StepFun",
               "opencode": "OpenCode", "opencode-apple": "OpenCode",
               "xiaomi": "小米 MiMo", "sensenova": "商汤 SenseNova", "zai": "智谱 Z.ai",
               "siliconflow": "硅基流动"}
    out = []
    for pid in providers.keys():
        if pid == "deepseek":
            out.append(query_deepseek())
        elif pid == "stepfun":
            out.append(query_stepfun())
        elif pid in ("opencode", "opencode-apple"):
            out.append(query_opencode(pid))
        elif pid == "siliconflow":
            # v3.1.2：官方 /v1/user/info 已废弃(410 deprecated,2026-08实测)，无公开余额接口→降级 unsupported
            out.append({"provider": pid, "name": DISPLAY.get(pid, pid),
                        "mode": "payg", "available": False, "unsupported": True,
                        "error": "官方余额接口已下线，请控制台查看"})
        else:
            # 无公开用量接口的 provider → unsupported 降级（App 显示控制台查看）
            out.append({"provider": pid, "name": DISPLAY.get(pid, pid),
                        "mode": "plan", "available": False, "unsupported": True,
                        "error": "官方无公开用量接口"})
    return {"ok": True, "providers": out}