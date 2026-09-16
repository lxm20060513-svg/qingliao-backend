#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""微信通道模型设置 API（v3.0.19 方案B + v3.0.22 视觉模型）：
- GET  /api/channel/model          读 wechat-profile 当前 model/provider
- POST /api/channel/model          写 wechat-profile 的 model/provider（改 config.yaml + 重启 gateway）
- GET  /api/channel/vision-model   读 wechat-profile 的 auxiliary.vision（视觉模型 provider/model/base_url）
- POST /api/channel/vision-model   写 wechat-profile 的 auxiliary.vision（provider/model，base_url/api_key 自动从 providers 段解析）
- DELETE /api/channel/vision-model 清除 auxiliary.vision（恢复跟随主模型原生视觉/默认兜底）
- 端口 9152，需 X-Auth-Token（与其它服务一致）
wechat-profile = Hermes 独立 profile，微信通道经 profile_routes 路由到它，
改它的 config.yaml 即独立控制微信通道（不影响其它通道）。
视觉模型逻辑（Hermes agent.image_input_mode: auto 原生支持）：
主模型 supports_vision → 原生传图用主模型；主模型不支持 + auxiliary.vision 显式配置 → 用视觉模型兜底。
"""
import json, os, subprocess, threading, hmac
from http.server import BaseHTTPRequestHandler
import auth_api
try:
    import yaml as _yaml
except ImportError:
    _yaml = None

# 2026-09-09：wechat-profile 已删除，微信通道由 default profile（主 config.yaml）服务。
# 模型写入主 config.yaml 的 model 段，重启 default gateway 生效。
# v3.0.83：QL_WECHAT_PROFILE_CFG env 已在 compose 中改指主 config.yaml（旧值指向已删 profile 会 500）。
_DEFAULT_CFG = os.environ.get("QL_CONFIG_YAML", "/data/hermes_config.yaml")
_FALLBACK_CFG = "/data/hermes_config.yaml"
PROFILE_CFG = os.environ.get("QL_WECHAT_PROFILE_CFG") or (
    _DEFAULT_CFG if os.path.exists(_DEFAULT_CFG) else _FALLBACK_CFG
)

_lock = threading.Lock()

# 支持的 provider（与主 config providers 段对齐，防止写死错误 provider）
VALID_PROVIDERS = {
    "deepseek", "stepfun", "xiaomi", "opencode", "opencode-apple",
    "ollama", "sensenova", "zai", "glm", "kimi", "openrouter", "custom",
    "local",  # v3.0.22：App 端本地模型 provider 名 → 写 profile 时归一化为 ollama
}


def _normalize_provider(provider):
    """App 端 provider 名 → wechat-profile providers 段实际名（local → ollama）"""
    return "ollama" if provider == "local" else provider


def _config_provider_keys():
    """2026-09-09：读主 config.yaml providers 段的一级 key（动态白名单，
    防止硬编码集合漏掉 zai-coding 等实际存在的 provider 而误报 400）。"""
    try:
        with open(PROFILE_CFG, encoding="utf-8") as f:
            lines = f.read().splitlines()
    except Exception:
        return set()
    keys = set()
    in_providers = False
    for ln in lines:
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        if not (ln[0] in " \t"):
            in_providers = (s == "providers:")
            continue
        if in_providers and ln.startswith("  ") and not ln.startswith("   "):
            k = s.split(":", 1)[0].strip()
            if k:
                keys.add(k)
    return keys


def _is_model_header(ln):
    """顶层 model: 段头：原始行无缩进且 strip 后恰为 'model:'（防止误匹配 auxiliary.vision.model 等）"""
    return ln.strip() == "model:" and not ln[0] in " \t"


def _read_model():
    """读 wechat-profile config.yaml 的 model 段（无文件/无 model 段 → None）"""
    try:
        with open(PROFILE_CFG, encoding="utf-8") as f:
            raw = f.read()
        # v3.0.28 review：优先用 yaml.safe_load 解析（避免手工行级解析的缩进依赖），
        # 解析失败时 fallback 到原手工解析（保留旧逻辑兜底）。
        if _yaml is not None:
            try:
                cfg = _yaml.safe_load(raw)
                if isinstance(cfg, dict):
                    m = cfg.get("model") or {}
                    if isinstance(m, dict):
                        return {"model": m.get("default"), "provider": m.get("provider")}
            except Exception:
                pass  # yaml 解析失败，fallback 到手工解析
        lines = raw.splitlines()
        model = provider = None
        in_model = False
        for ln in lines:
            s = ln.strip()
            if _is_model_header(ln):
                in_model = True
                continue
            # 离开 model 段：非空行且无缩进（原始行不以空格/tab 开头）且非注释
            if in_model and s and not ln[0] in " \t" and not s.startswith("#"):
                in_model = False
            if in_model:
                if s.startswith("default:"):
                    model = s.split(":", 1)[1].strip().strip('"\'')
                elif s.startswith("provider:"):
                    provider = s.split(":", 1)[1].strip().strip('"\'')
        return {"model": model, "provider": provider}
    except Exception as e:
        return {"model": None, "provider": None, "error": str(e)[:120]}


def _write_model(model, provider):
    """改 wechat-profile config.yaml 的 model.default / model.provider（行级替换，保留注释）"""
    with open(PROFILE_CFG, encoding="utf-8") as f:
        lines = f.read().splitlines()
    # 找 model: 段（只认顶层 model:，防误匹配 auxiliary 段）——先删掉旧 default/provider 行再统一插入
    in_model = False
    out = []
    for ln in lines:
        s = ln.strip()
        if _is_model_header(ln):
            in_model = True
            out.append("model:")
            continue
        if in_model and s and not ln[0] in " \t" and not s.startswith("#"):
            in_model = False  # 离开 model 段（原始行无缩进）
        if in_model:
            if s.startswith("default:") or s.startswith("provider:"):
                continue  # 删除旧键，稍后统一写
        out.append(ln)
    # 在 model: 段首行后插入新的 default/provider（幂等：重复写不会叠加）
    for i, ln in enumerate(out):
        if ln.strip() == "model:" and not ln[0] in " \t":
            out.insert(i + 1, "  default: %s" % model)
            out.insert(i + 2, "  provider: %s" % provider)
            break
    else:
        out.insert(0, "  provider: %s" % provider)
        out.insert(0, "  default: %s" % model)
        out.insert(0, "model:")
    with open(PROFILE_CFG, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    return True


def _restart_gateway():
    """重启 Hermes gateway 使新模型生效（docker exec 容器内，异步不阻塞）"""
    try:
        subprocess.Popen(
            ["docker", "exec", os.environ.get("QL_HERMES_CONTAINER", "hermes-container"), "hermes", "gateway", "restart"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# v3.0.22：视觉模型配置（auxiliary.vision 段）
# ---------------------------------------------------------------------------

def _find_top_block(lines, header):
    """找顶层 ``header:`` 块，返回 [start, end) 行索引（含 header 行）；找不到返回 None。
    块结束 = 下一个无缩进的非注释非空行。"""
    for i, ln in enumerate(lines):
        if ln.strip() == header and not ln[0] in " \t":
            end = i + 1
            while end < len(lines):
                nxt = lines[end]
                if nxt.strip() and not nxt[0] in " \t" and not nxt.strip().startswith("#"):
                    break
                end += 1
            return i, end
    return None


def _read_vision():
    """读 auxiliary.vision 段（provider/model/base_url/api_key 任选；未配置 → None 字段）"""
    try:
        with open(PROFILE_CFG, encoding="utf-8") as f:
            raw = f.read()
    except Exception as e:
        return {"provider": None, "model": None, "base_url": None, "error": str(e)[:120]}
    # v3.0.28 review：优先 yaml.safe_load 解析，fallback 手工行级解析
    if _yaml is not None:
        try:
            cfg = _yaml.safe_load(raw)
            if isinstance(cfg, dict):
                aux = cfg.get("auxiliary") or {}
                vision = aux.get("vision") or {}
                if isinstance(vision, dict) and vision.get("provider"):
                    return {"provider": vision.get("provider"),
                            "model": vision.get("model"),
                            "base_url": vision.get("base_url")}
        except Exception:
            pass
    # fallback：手工行级解析
    lines = raw.splitlines()
    blk = _find_top_block(lines, "auxiliary:")
    if not blk:
        return {"provider": None, "model": None, "base_url": None}
    _, aux_end = blk
    out = {"provider": None, "model": None, "base_url": None}
    in_vision = False
    for ln in lines[1:aux_end]:
        indent = len(ln) - len(ln.lstrip(" "))
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        if indent == 2:
            in_vision = (s == "vision:")
            continue
        if in_vision and indent == 4:
            if s.startswith("provider:"):
                out["provider"] = s.split(":", 1)[1].strip().strip("\"'")
            elif s.startswith("model:"):
                out["model"] = s.split(":", 1)[1].strip().strip("\"'")
            elif s.startswith("base_url:"):
                out["base_url"] = s.split(":", 1)[1].strip().strip("\"'")
    return out


def _provider_creds(provider):
    """从 providers 段解析 base_url/api_key（支持 custom: 前缀别名）"""
    try:
        with open(PROFILE_CFG, encoding="utf-8") as f:
            raw = f.read()
    except Exception:
        return {}
    # v3.0.28 review：优先 yaml.safe_load 解析，fallback 手工行级解析
    if _yaml is not None:
        try:
            cfg = _yaml.safe_load(raw)
            if isinstance(cfg, dict):
                provs = cfg.get("providers") or {}
                # 支持 custom:<name> 别名
                p = provs.get(provider) or provs.get("custom:" + provider) or {}
                if isinstance(p, dict) and (p.get("base_url") or p.get("api_key")):
                    return {"base_url": str(p.get("base_url") or ""),
                            "api_key": str(p.get("api_key") or "")}
        except Exception:
            pass
    # fallback：手工行级解析
    lines = raw.splitlines()
    blk = _find_top_block(lines, "providers:")
    if not blk:
        return {}
    _, prov_end = blk
    cands = {provider, "custom:" + provider}
    cur = None
    creds = {}
    for ln in lines[1:prov_end]:
        indent = len(ln) - len(ln.lstrip(" "))
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        if indent == 2:
            name = s.rstrip(":")
            cur = name if name in cands else None
            creds = {} if cur else creds
        elif cur and indent == 4:
            if s.startswith("base_url:"):
                creds["base_url"] = s.split(":", 1)[1].strip().strip("\"'")
            elif s.startswith("api_key:"):
                creds["api_key"] = s.split(":", 1)[1].strip().strip("\"'")
    return creds


def _write_vision(provider, model):
    """写 auxiliary.vision 段（provider/model + base_url/api_key 自动从 providers 段解析）。
    providers 段缺 base_url/api_key → 只写 provider/model（Hermes 仍识别为显式配置）。"""
    provider = _normalize_provider(provider)
    creds = _provider_creds(provider)
    with open(PROFILE_CFG, encoding="utf-8") as f:
        lines = f.read().splitlines()

    new_block = ["  vision:", "    provider: %s" % provider, "    model: %s" % model]
    if creds.get("base_url"):
        new_block.append("    base_url: %s" % creds["base_url"])
    if creds.get("api_key"):
        new_block.append("    api_key: %s" % creds["api_key"])

    blk = _find_top_block(lines, "auxiliary:")
    if not blk:
        # 无 auxiliary 段 → 文件末尾追加（保留原内容，注释在末尾之后不影响 yaml 解析）
        out = lines + [""] + ["auxiliary:"] + new_block
    else:
        aux_start, aux_end = blk
        # 删掉旧 vision 子块（auxiliary 内缩进 2 的 vision: 及其 4 缩进子行）
        body = lines[aux_start + 1:aux_end]
        kept = []
        skip_vision = False
        for ln in body:
            indent = len(ln) - len(ln.lstrip(" "))
            s = ln.strip()
            if indent == 2:
                skip_vision = (s == "vision:")
                if not skip_vision:
                    kept.append(ln)
            elif indent == 4 and skip_vision:
                continue
            elif indent == 4:
                kept.append(ln)
            else:
                kept.append(ln)
        out = lines[:aux_start + 1] + new_block + kept + lines[aux_end:]
    with open(PROFILE_CFG, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    return True


def _clear_vision():
    """删除 auxiliary.vision 子块（保留 auxiliary 段内其它子块如 compression）"""
    with open(PROFILE_CFG, encoding="utf-8") as f:
        lines = f.read().splitlines()
    blk = _find_top_block(lines, "auxiliary:")
    if not blk:
        return True
    aux_start, aux_end = blk
    body = lines[aux_start + 1:aux_end]
    kept = []
    skip_vision = False
    for ln in body:
        indent = len(ln) - len(ln.lstrip(" "))
        s = ln.strip()
        if indent == 2:
            skip_vision = (s == "vision:")
            if not skip_vision:
                kept.append(ln)
        elif indent == 4 and skip_vision:
            continue
        else:
            kept.append(ln)
    if len(kept) == 0:
        # auxiliary 段空了 → 整段删除
        out = lines[:aux_start] + lines[aux_end:]
    else:
        out = lines[:aux_start + 1] + kept + lines[aux_end:]
    with open(PROFILE_CFG, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    return True


class Handler(BaseHTTPRequestHandler):
    def _auth(self):
        return auth_api.check_auth(self.headers, "X-Channel-Password", "")

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token, X-Channel-Password")
        self.end_headers()

    def do_GET(self):
        if self.path == "/api/channel/model":
            if not self._auth():
                self._send(401, {"error": "unauthorized"})
                return
            info = _read_model()
            self._send(200, {"ok": True, "channel": "wechat", "profile": "wechat-profile", **info})
        elif self.path == "/api/channel/vision-model":
            if not self._auth():
                self._send(401, {"error": "unauthorized"})
                return
            info = _read_vision()
            self._send(200, {"ok": True, "channel": "wechat", "profile": "wechat-profile", **info})
        else:
            self._send(404, {"error": "not found"})
            return

    def do_DELETE(self):
        if self.path != "/api/channel/vision-model":
            self._send(404, {"error": "not found"})
            return
        if not self._auth():
            self._send(401, {"error": "unauthorized"})
            return
        with _lock:
            try:
                _clear_vision()
                info = _read_vision()
            except Exception as e:
                self._send(500, {"error": "clear failed: %s" % str(e)[:150]})
                return
        restart_ok = _restart_gateway()
        self._send(200, {"ok": True, "channel": "wechat", **info,
                         "restart": "triggered" if restart_ok else "failed",
                         "note": "已清除微信通道视觉模型，gateway 重启后生效（约 10-30 秒）"})

    def do_POST(self):
        if self.path == "/api/channel/model":
            self._post_model()
        elif self.path == "/api/channel/vision-model":
            self._post_vision()
        else:
            self._send(404, {"error": "not found"})

    def _read_body(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            return json.loads(self.rfile.read(length).decode("utf-8", "replace") or "{}")
        except Exception:
            return None

    def _post_model(self):
        if not self._auth():
            self._send(401, {"error": "unauthorized"})
            return
        body = self._read_body()
        if body is None:
            self._send(400, {"error": "invalid json"})
            return
        model = (body.get("model") or "").strip()
        provider = (body.get("provider") or "").strip()
        if not model or not provider:
            self._send(400, {"error": "model and provider required"})
            return
        if provider not in (VALID_PROVIDERS | _config_provider_keys()):
            self._send(400, {"error": "unsupported provider: %s" % provider})
            return
        with _lock:
            try:
                _write_model(model, _normalize_provider(provider))
                info = _read_model()
            except Exception as e:
                self._send(500, {"error": "write failed: %s" % str(e)[:150]})
                return
        # 异步重启 gateway（模型切换需要 gateway 重新加载 profile 配置）
        restart_ok = _restart_gateway()
        self._send(200, {"ok": True, "channel": "wechat", **info,
                         "restart": "triggered" if restart_ok else "failed",
                         "note": "gateway 重启后生效（约 10-30 秒）"})

    def _post_vision(self):
        if not self._auth():
            self._send(401, {"error": "unauthorized"})
            return
        body = self._read_body()
        if body is None:
            self._send(400, {"error": "invalid json"})
            return
        model = (body.get("model") or "").strip()
        provider = (body.get("provider") or "").strip()
        if not model or not provider:
            self._send(400, {"error": "model and provider required"})
            return
        if provider not in (VALID_PROVIDERS | _config_provider_keys()):
            self._send(400, {"error": "unsupported provider: %s" % provider})
            return
        with _lock:
            try:
                _write_vision(provider, model)
                info = _read_vision()
            except Exception as e:
                self._send(500, {"error": "write failed: %s" % str(e)[:150]})
                return
        restart_ok = _restart_gateway()
        self._send(200, {"ok": True, "channel": "wechat", **info,
                         "restart": "triggered" if restart_ok else "failed",
                         "note": "微信通道视觉模型已设置，gateway 重启后生效（约 10-30 秒）"})
