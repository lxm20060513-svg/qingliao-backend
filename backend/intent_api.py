"""意图抽取云端兜底：POST /api/agent/intent/extract

定位：App 的「输入收口」管道里最后一层兜底。前四层（正则规则 / Vision OCR / iOS 26 端侧模型）
都在设备上跑，只有它们都认不出（或设备不支持端侧模型）时才打到这来。

契约（App 侧 Core/IntentExtractor.swift 对同一份契约）：
    入：{"text": "...", "image_b64": "...", "hint": "..."}   至少给一个 text 或 image_b64
    出：{"ok": true, "kind": <OneOf>, "title": "...", "fields": {...}, "confidence": 0.0~1.0, "source": "cloud"}
        kind ∈ express|address|contact|link|text|amount|datetime（与 App 的 IntentKind 逐字一致）
    失败：{"ok": false, "error": "..."}，**HTTP 一律 200**——App 侧把非 200 与 ok:false 都当静默降级，
          用 4xx/5xx 只会让客户端多一条无意义的错误日志。

两条口径（都是踩出来的）：
  1. **不猜**：模型给出的 kind 不在枚举里、或没给出任何可用字段 → 直接 ok:false，让 App 用本地兜底
     （宁可让用户点「问 AI」，也不要给一个假结论）。
  2. **不要思维链**：system 里明确禁掉思考过程/解释（DeepSeek/GLM 这类推理模型默认会吐 CoT，
     那会把 JSON 解析搞坏——本仓在 bots_api/stream_api 上已经踩过同一个坑）。

上游调用复用 stream_api 现成的 `_chat_once()` + `_agent_key()`（自带 key 解析，不重复造配置读取）。

通道与路由：本模块只被 unified_router 分派；ROUTE_TABLE 挂 `/api/intent` 与 `/api/agent/intent`，
借已放行的 `/api/agent` 前缀 → nginx 三份 conf / lucky 白名单 / relay ALLOWED_RELAY 全部零改动。
"""

import json
import os
import re
import sys
import urllib.error
from http.server import BaseHTTPRequestHandler

KINDS = ("express", "address", "contact", "link", "text", "amount", "datetime")

EXTRACT_PATHS = ("/api/intent/extract", "/api/agent/intent/extract")
HEALTH_PATHS = ("/api/intent/health", "/api/agent/intent/health")
BILL_PATHS = ("/api/intent/bill", "/api/agent/intent/bill")

MAX_TEXT = 2000          # 输入上限（Clippings 级别的短内容；超长的截断后判定）
MAX_IMG_B64 = 6_000_000  # ≈4.5MB 原图；App 侧已压到 1600px/JPEG0.7（通常 150~400KB）
TIMEOUT = 20             # 判定是用户点了「识别」在等，超过就早点告诉它失败

SYSTEM_PROMPT = """你是内容类型识别器。判断用户给出的一段内容属于哪种类型，并给出一行中文摘要与关键字段。

类型只能是以下之一：
- express：快递单号/取件码
- address：地址
- contact：电话或邮箱
- link：网址
- amount：金额或表读数（电表/水表等）
- datetime：日期时间/日程
- text：普通文本（以上都不是）

只输出 JSON，不要输出解释、思考过程或任何多余文字。JSON 结构：
{"kind": "...", "title": "一行中文摘要（≤20字）", "value": "关键值（金额给数字；快递给单号；电话/邮箱给号码；日期给ISO8601；其它留空）", "unit": "金额/读数单位（元/度/kWh），其它留空", "confidence": 0.0~1.0}

拿不准就填 kind="text"，不要硬猜。"""


def _strip_code_fence(s):
    s = (s or "").strip()
    m = re.match(r"^```(?:json)?\s*(.*?)\s*```$", s, re.S)
    return m.group(1) if m else s


def _clamp_conf(v):
    try:
        f = float(v)
    except Exception:
        return 0.6
    if f != f:            # NaN
        return 0.6
    return max(0.0, min(1.0, f))


def _normalize(obj, text):
    """把模型输出收敛成对外契约。返回 None 表示「不猜」→ 调用方回 ok:false。"""
    if not isinstance(obj, dict):
        return None
    kind = str(obj.get("kind") or "").strip().lower()
    if kind not in KINDS:
        return None
    title = str(obj.get("title") or "").strip()[:40]
    value = obj.get("value")
    value = str(value).strip()[:200] if value is not None else ""
    unit = str(obj.get("unit") or "").strip()[:16]

    fields = {}
    if value:
        fields["value"] = value
    if unit:
        fields["unit"] = unit
    if kind == "contact" and value and "@" in value:
        fields["type"] = "email"
    elif kind == "contact" and value:
        fields["type"] = "phone"
    if kind == "link":
        fields["url"] = (text or "").strip()[:500]

    # 有数值的类型必须真给出数值，否则视为没认出来（App 侧会拿不到可写入的字段）
    if kind == "amount" and not fields.get("value"):
        return None
    if not title:
        title = (text or "").strip()[:20]

    return {"kind": kind, "title": title, "fields": fields,
            "confidence": _clamp_conf(obj.get("confidence"))}


def _upstream(body, key=None):
    """直调上游文本模型（复用 stream_api 的端点与 key 解析）"""
    import stream_api
    return stream_api._chat_once(body, key=key)


VISION_REASON = "未探测"   # health 用：为什么没视觉 / 用了哪个模型（不静默，别让排障再猜）


def _aux_vision_endpoint():
    """解析辅助视觉模型端点；不可用返回 None，并把原因写进 VISION_REASON。

    三级回退（实测踩出来的顺序）：
      1) 容器内 Hermes 配置的 auxiliary.vision（provider/model/base_url）
      2) 环境变量 QL_INTENT_VISION_MODEL / QL_INTENT_VISION_PROVIDER
      3) 默认 step-3.7-flash / stepfun
    为什么不能只走第 1 步：后端容器读的是 /data/hermes_config.yaml，**只有 providers 段**
    （没有 auxiliary 段）→ 只认配置的话这条路永远走不通，而失败了还看不出原因（本模块初版就是这样，
    表现成「云端未配置视觉模型」）。
    """
    global VISION_REASON
    try:
        import stream_api
    except Exception as e:
        VISION_REASON = "stream_api 不可用：%s" % e
        return None

    model = provider = base_url = ""
    try:
        import yaml
        with open(stream_api._hermes_cfg_path(), encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        aux = ((cfg.get("auxiliary") or {}).get("vision") or {})
        model = str(aux.get("model") or "").strip()
        provider = str(aux.get("provider") or "").strip()
        base_url = str(aux.get("base_url") or "").strip()
    except Exception:
        pass
    src = "config" if model else ""
    if not model:
        model = os.environ.get("QL_INTENT_VISION_MODEL", "").strip()
        provider = provider or os.environ.get("QL_INTENT_VISION_PROVIDER", "").strip()
        src = "env" if model else ""
    if not model:
        model, provider, src = "step-3.7-flash", provider or "stepfun", "default"

    try:
        url, key, eff = stream_api._agent_endpoint(model, provider)
    except Exception as e:
        VISION_REASON = "端点解析失败：%s" % e
        return None
    if not key:
        VISION_REASON = "%s/%s 的 api_key 读不到" % (provider, model)
        return None
    # 陷阱：provider 在配置里找不到时 _agent_endpoint 会**静默回退主模型端点**，
    # 那就变成「把图发给看不见图的文本模型」，只会拿到空 content。这里挡掉。
    if url == getattr(stream_api, "AGENT_URL", None) and provider not in ("", "deepseek"):
        VISION_REASON = "provider %s 无配置（已回退主端点，拒绝当视觉模型用）" % provider
        return None
    if base_url:
        url = base_url.rstrip("/") + "/chat/completions"
    VISION_REASON = "%s（%s/%s）" % (src, provider, eff or model)
    return url, key, eff or model


def _ask_text(text):
    """文本判定：一次上游调用，要求 JSON。"""
    prompt = "内容：\n" + text[:MAX_TEXT]
    body = {
        "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                     {"role": "user", "content": prompt}],
        "temperature": 0,
        "max_tokens": 300,
        "response_format": {"type": "json_object"},
    }
    # 模型名不写死：_chat_once 缺省走 AGENT_URL/AGENT_KEY，其默认模型由 stream_api 决策
    try:
        import stream_api
        body["model"] = getattr(stream_api, "AGENT_MODEL", None) or "deepseek-chat"
    except Exception:
        body["model"] = "deepseek-chat"
    resp = _upstream(body)
    content = (((resp or {}).get("choices") or [{}])[0].get("message") or {}).get("content") or ""
    for cand in (_strip_code_fence(content), content):
        try:
            return json.loads(cand)
        except Exception:
            continue
    return None


RAW_LAST = ""   # 上游最后一次原始输出（截断）：解析失败时带进错误信息，排障不用猜


def _ask_image(image_b64):
    """看图判定：需要辅助视觉模型；未配置则返回 ('no_vision', None)，解析失败返回 ('parse_fail', None)。"""
    global RAW_LAST
    ep = _aux_vision_endpoint()
    if not ep:
        return "no_vision", None
    url, key, model = ep
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": [
                {"type": "text", "text": "识别这张图片里的内容并判定类型，只输出 JSON。"},
                {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + image_b64}},
            ]},
        ],
        "temperature": 0,
        # 4096（实测值，不是拍脑袋）：stepfun step-3.7-flash 关不掉思考
        # （thinking.type=disabled / enable_thinking=false 实测被忽略，reasoning_effort=none 也一样有 2400 字思考），
        # 每次调用**思考本身就占 2400~3100 字**，预算给 1500 会随机撞 finish_reason=length 返回空 content
        # → 表现成「未识别出类型」。给足预算后稳定 finish=stop。
        # 代价：单次 7~12s。所以云端看图**只当后台兜底**，App 侧 6s 超时到点就静默降级（拍照主路径是设备端 Vision OCR）。
        "max_tokens": 4096,
    }
    resp = _post_json(url, key, body)
    msg = (((resp or {}).get("choices") or [{}])[0].get("message") or {})
    content = msg.get("content") or ""
    if not content:
        RAW_LAST = "content 为空（finish_reason=%s，reasoning=%s 字）" % (
            ((resp or {}).get("choices") or [{}])[0].get("finish_reason"),
            len(str(msg.get("reasoning_content") or "")))
        return "parse_fail", None
    RAW_LAST = content[:300]
    try:
        return "ok", json.loads(_strip_code_fence(content))
    except Exception:
        return "parse_fail", None


def _post_json(url, key, body):
    import urllib.request
    req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json",
                                          "Authorization": "Bearer " + (key or "")},
                                 method="POST")
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read())


def extract(body):
    """对外契约实现（纯函数式：入 dict → 出 dict）。抛 ValueError → 400。"""
    if not isinstance(body, dict):
        raise ValueError("请求体必须是对象")
    text = body.get("text")
    text = str(text).strip() if text is not None else ""
    image_b64 = body.get("image_b64")
    image_b64 = str(image_b64).strip() if image_b64 is not None else ""
    if not text and not image_b64:
        raise ValueError("text 与 image_b64 至少要给一个")
    if len(image_b64) > MAX_IMG_B64:
        return {"ok": False, "error": "图片过大"}

    if text and not image_b64:
        obj = _ask_text(text)
        norm = _normalize(obj, text)
        if not norm:
            return {"ok": False, "error": "未识别出类型"}
        return {"ok": True, "source": "cloud", **norm}

    # 有图：先试图（图里信息通常比 OCR 出来的字更全）
    status, obj = _ask_image(image_b64)
    if status == "no_vision":
        # 未配置视觉模型时退回文本（App 传来的 text 往往是它 OCR 出来的字，聊胜于无）
        if text:
            norm = _normalize(_ask_text(text), text)
            if norm:
                return {"ok": True, "source": "cloud-ocr", **norm}
        return {"ok": False, "error": "云端视觉不可用：" + VISION_REASON}
    norm = _normalize(obj, text)
    if not norm:
        return {"ok": False, "error": "未识别出类型", "raw": RAW_LAST}
    return {"ok": True, "source": "cloud-vision", **norm}


# ───────────────── v4.0.20（#⑪）账单截图识别入账 ─────────────────
# 定位：App「拍照/相册选图 → 后端识别 → 返回结构化账单 → 用户确认后入账」。
# 后端只做「图 → 结构化字段」，**不写账本**（写入仍由 App 走 records 同步链路，
# 人工确认这一环留在 App，避免把识别错的金额自动记进账本）。
# 复用 intent 的视觉端点解析（_aux_vision_endpoint）与 _post_json，零新配置项；
# 路由借已注册的 /api/intent 前缀（ROUTE_TABLE 最长前缀命中）→
# nginx 三份 conf / lucky 白名单 / relay ALLOWED_RELAY 全部零改动。
BILL_CATEGORIES = ("餐饮", "购物", "交通", "医疗", "娱乐", "居住", "通讯", "其他")

BILL_SYSTEM_PROMPT = """你是账单/小票识别器。读用户给的账单截图或账单文字，抽取**一笔支出**的结构化字段。
只输出 JSON，不要解释、不要思考过程、不要多余文字。JSON 结构：
{"amount": 合计/实付金额（数字，单位元；认不出填 null）, "date": "消费日期 YYYY-MM-DD（认不出填空串）", "category": "只能是 餐饮/购物/交通/医疗/娱乐/居住/通讯/其他 之一", "item": "一句话摘要（≤20字，如「便利店购物」）", "confidence": 0.0~1.0}
要点：只认**最终合计/实付金额**，不要把小计、优惠前金额或单件单价当合计；认不出金额就把 amount 填 null。"""


def _img_mime(b64):
    """按 base64 魔数判图片类型（App 压缩后多为 JPEG，也可能直接给 PNG）。"""
    return "image/png" if str(b64 or "").startswith("iVBORw0KGgo") else "image/jpeg"


def _norm_bill(obj):
    """把模型输出收敛成对 App 的契约。返回 None 表示「没认出可用字段」→ 调用方回 ok:false。"""
    if not isinstance(obj, dict):
        return None
    amt = obj.get("amount")
    try:
        s = str(amt).replace(",", "").replace("¥", "").replace("￥", "").replace("元", "").strip()
        amt = float(s)
        if amt != amt or amt <= 0 or amt > 100000000:
            amt = None
    except Exception:
        amt = None
    date = str(obj.get("date") or "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        date = ""
    cat = str(obj.get("category") or "").strip()
    if cat not in BILL_CATEGORIES:
        cat = "其他"
    item = str(obj.get("item") or "").strip()[:20]
    if amt is None and not date and not item:
        return None
    return {"amount": amt, "date": date, "category": cat, "item": item,
            "confidence": _clamp_conf(obj.get("confidence"))}


def _ask_bill_image(image_b64):
    """账单看图：需要视觉端点。未配置 → ('no_vision', None)；解析失败 → ('parse_fail', None)。"""
    global RAW_LAST
    ep = _aux_vision_endpoint()
    if not ep:
        return "no_vision", None
    url, key, model = ep
    body = {
        "model": model,
        "messages": [{"role": "system", "content": BILL_SYSTEM_PROMPT},
                     {"role": "user", "content": [
                         {"type": "text", "text": "识别这张账单，提取金额/日期/分类，只输出 JSON。"},
                         {"type": "image_url",
                          "image_url": {"url": "data:" + _img_mime(image_b64) + ";base64," + image_b64}}]}],
        "temperature": 0,
        # 4096：step-3.7-flash 思考关不掉（实测），预算给少会随机撞 finish_reason=length 返空 content
        "max_tokens": 4096,
    }
    resp = _post_json(url, key, body)
    ch = ((resp or {}).get("choices") or [{}])[0]
    msg = (ch.get("message") or {})
    content = msg.get("content") or ""
    if not content:
        RAW_LAST = "content 为空（finish_reason=%s，reasoning=%s 字）" % (
            ch.get("finish_reason"), len(str(msg.get("reasoning_content") or "")))
        return "parse_fail", None
    RAW_LAST = content[:300]
    try:
        return "ok", json.loads(_strip_code_fence(content))
    except Exception:
        return "parse_fail", None


def _ask_bill_text(text):
    """账单纯文本（App 端 OCR 出来的字）：一次上游调用要 JSON。"""
    body = {"messages": [{"role": "system", "content": BILL_SYSTEM_PROMPT},
                         {"role": "user", "content": "账单文字：\n" + text[:MAX_TEXT]}],
            "temperature": 0, "max_tokens": 800,
            "response_format": {"type": "json_object"}}
    # 文本支路优先复用「账单看图」那个端点（实测 step-3.7-flash 支持纯文本、key 可用）；
    # 主 Agent 模型的 key 可能没余额（实测 402 Insufficient Balance）→ 不能只走 _upstream。
    resp = None
    ep = _aux_vision_endpoint()
    if ep:
        try:
            resp = _post_json(ep[0], ep[1], dict(body, model=ep[2]))
        except Exception:
            resp = None
    if not resp:
        try:
            import stream_api
            body["model"] = getattr(stream_api, "AGENT_MODEL", None) or "deepseek-chat"
        except Exception:
            body["model"] = "deepseek-chat"
        resp = _upstream(body)
    content = (((resp or {}).get("choices") or [{}])[0].get("message") or {}).get("content") or ""
    for cand in (_strip_code_fence(content), content):
        try:
            return json.loads(cand)
        except Exception:
            continue
    return None


def extract_bill(body):
    """账单识别对外契约（入 dict → 出 dict）。请求体问题抛 ValueError。

    出：{"ok": true, "amount": 数字|null, "date": "YYYY-MM-DD"|"", "category": "餐饮…",
         "item": "摘要", "confidence": 0-1, "source": "cloud-vision"|"cloud-ocr"|"cloud"}
    失败：{"ok": false, "error": "..."}，**HTTP 一律 200**（与 intent/extract 同口径）。
    """
    if not isinstance(body, dict):
        raise ValueError("请求体必须是对象")
    image_b64 = str(body.get("image_b64") or "").strip()
    text = str(body.get("text") or "").strip()
    if not image_b64 and not text:
        raise ValueError("image_b64 与 text 至少要给一个")
    if len(image_b64) > MAX_IMG_B64:
        return {"ok": False, "error": "图片过大"}
    if image_b64:
        status, obj = _ask_bill_image(image_b64)
        if status == "no_vision":
            # 没配视觉模型时退回文本（App 传来的 text 往往是它 OCR 出来的字，聊胜于无）
            if text:
                norm = _norm_bill(_ask_bill_text(text))
                if norm:
                    return {"ok": True, "source": "cloud-ocr", **norm}
            return {"ok": False, "error": "云端视觉不可用：" + VISION_REASON}
        norm = _norm_bill(obj)
        if not norm:
            return {"ok": False, "error": "未识别出账单字段", "raw": RAW_LAST}
        return {"ok": True, "source": "cloud-vision", **norm}
    norm = _norm_bill(_ask_bill_text(text))
    if not norm:
        return {"ok": False, "error": "未识别出账单字段"}
    return {"ok": True, "source": "cloud", **norm}


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token")

    def _send(self, code, obj):
        payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _path(self):
        return self.path.split("?", 1)[0].rstrip("/") or "/"

    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, "X-Intent-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        if self._path() in HEALTH_PATHS:
            ok_vision = bool(_aux_vision_endpoint())
            self._send(200, {"ok": True, "kinds": list(KINDS),
                             "bill": True, "vision": ok_vision,
                             "vision_reason": VISION_REASON})
            return
        self._send(404, {"error": "Not Found"})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        if self._path() in BILL_PATHS:
            try:
                n = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                n = 0
            raw = self.rfile.read(n) if n > 0 else b""
            try:
                body = json.loads(raw.decode("utf-8", "replace") or "{}")
            except Exception as e:
                self._send(200, {"ok": False, "error": "请求体不是合法 JSON：%s" % str(e)[:120]})
                return
            try:
                self._send(200, extract_bill(body))
            except ValueError as e:
                self._send(200, {"ok": False, "error": str(e)[:200]})
            except urllib.error.HTTPError as e:
                detail = ""
                try:
                    detail = e.read().decode("utf-8", "replace")[:200]
                except Exception:
                    pass
                self._send(200, {"ok": False, "error": "上游 HTTP %s" % e.code, "upstream": detail})
            except Exception as e:
                self._send(200, {"ok": False, "error": "%s: %s" % (type(e).__name__, str(e)[:200])})
            return
        if self._path() not in EXTRACT_PATHS:
            self._send(404, {"error": "Not Found"})
            return
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            n = 0
        raw = self.rfile.read(n) if n > 0 else b""
        try:
            body = json.loads(raw.decode("utf-8", "replace") or "{}")
        except Exception as e:
            self._send(200, {"ok": False, "error": "请求体不是合法 JSON：%s" % str(e)[:120]})
            return
        try:
            self._send(200, extract(body))
        except ValueError as e:
            self._send(200, {"ok": False, "error": str(e)[:200]})
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", "replace")[:200]
            except Exception:
                pass
            self._send(200, {"ok": False, "error": "上游 HTTP %s" % e.code, "upstream": detail})
        except Exception as e:
            self._send(200, {"ok": False, "error": "%s: %s" % (type(e).__name__, str(e)[:200])})

    def log_message(self, fmt, *args):
        pass


# ─────────────────────────── CLI（运维自测，不参与服务） ───────────────────────────

def _main(argv):
    if not argv or argv[0] == "health":
        print(json.dumps({"ok": True, "kinds": list(KINDS),
                          "vision": bool(_aux_vision_endpoint())}, ensure_ascii=False))
        return 0
    if argv[0] == "extract" and len(argv) >= 2:
        try:
            print(json.dumps(extract({"text": argv[1]}), ensure_ascii=False, indent=2))
            return 0
        except Exception as e:
            print(json.dumps({"ok": False, "error": "%s: %s" % (type(e).__name__, e)},
                             ensure_ascii=False))
            return 1
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
