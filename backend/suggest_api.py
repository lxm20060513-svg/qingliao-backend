# -*- coding: utf-8 -*-
"""提问推荐「猜你想问」（v4.0.42 待做池 ①）。

AI 答完一条后给 3 个追问候选，App 渲染成胶囊、点了直接接着问，
接住「追问问答链」这条链路已铺好但此前没有入口的通路。

设计要点（与 agent_api 的 /api/agent/suggest 同款范式，不另造调用件）：
  · 复用 stream_api._chat_once（已带 opencode 必需头 + 推理开关）
  · 端点挂 /api/agent 前缀 → 借前缀大法，nginx / lucky / ALLOWED_RELAY 零改动
  · 质量闸门「宁缺勿滥」：去重后不足 2 条直接返空数组；空数组是合法成功，不是错误
  · 与 exclude（近几轮已问过的原文）有交集的丢弃；换一批 = 再 POST 一次带新 exclude
"""
import json
import re

MAX_Q = 3
MIN_Q = 2          # 去重后不足这个数就当「没有好候选」，返空数组
MAX_Q_LEN = 24     # 单条问题字数上限（超长截断）


def _key(s):
    """比较用的规范化键：去掉首尾空白与句末标点。"""
    return re.sub(r"[\s？?。.!！]+$", "", (s or "").strip())


def _is_question(s):
    """是不是问句：末尾问号，或含疑问词（模型漏打问号时也不丢）。"""
    return bool(re.search(r"[?？]$", s.strip())) or \
        bool(re.search(r"(吗|呢|吧|怎么|如何|哪些|哪种|多少|为什么|为何|哪里|哪儿|几|是否|能不能|可不可以)", s))


def _normalize(items, exclude=None):
    """清洗 + 去重 + 截断 + 与 exclude 求交集过滤，返回规范问题列表（保留句末问号）。"""
    ex = set()
    for e in (exclude or []):
        k = _key(str(e or ""))
        if k:
            ex.add(k)
    out, seen = [], set()
    for raw in items or []:
        if not isinstance(raw, str):
            continue
        s = raw.strip()
        if not s:
            continue
        # 截断不能吃掉句末问号——超长时先留问号再截正文
        if len(s) > MAX_Q_LEN:
            s = s[:MAX_Q_LEN].rstrip("？?。.!！") + "？"
        if not _is_question(s):
            continue
        k = _key(s)
        if not k or k in seen or k in ex:
            continue
        seen.add(k)
        out.append(s)
        if len(out) >= MAX_Q:
            break
    return out


def _parse_questions(text):
    """从模型回复里提取问题数组：先整体 JSON.parse，失败再正则抓 [...] 兜底。"""
    text = (text or "").strip()
    if not text:
        return []
    try:
        d = json.loads(text)
        if isinstance(d, list):
            return d
        if isinstance(d, dict):
            for k in ("questions", "items", "list", "data"):
                if isinstance(d.get(k), list):
                    return d[k]
    except Exception:
        pass
    m = re.search(r"\[.*?\]", text, re.S)
    if m:
        try:
            d = json.loads(m.group(0))
            if isinstance(d, list):
                return d
        except Exception:
            pass
    return []


_PROMPT = (
    "你在给用户推荐接下来可以追问的问题。\n"
    "要求：\n"
    "1. 只输出 JSON 数组，不要任何解释、前后缀、Markdown 代码块。\n"
    "2. 恰好 {n} 条，每条是一个中文问句，不超过 {qlen} 字。\n"
    "3. 必须紧扣下面这段对话的主题，不要泛泛而谈。\n"
    "4. 不得复述用户已经问过的原句，不得与这些已问过的问题重复：{ex}\n"
    "5. 不要写「你能做什么」「有什么帮助」这类无信息量的问题。\n"
    "用户的问题：{u}\n"
    "AI 的回答摘要：{a}\n"
)


def build_questions(last_user, last_answer, exclude=None, n=MAX_Q):
    """生成 0~3 条追问候选。任何异常都吞成空数组（App 侧静默不渲染该区）。"""
    try:
        import stream_api
        last_user = (last_user or "").strip()[:600]
        last_answer = (last_answer or "").strip()[:1200]
        ex = [str(e or "").strip() for e in (exclude or []) if str(e or "").strip()]
        if not last_user and not last_answer:
            return []
        prompt = _PROMPT.format(
            n=n, qlen=MAX_Q_LEN,
            ex=("（无）" if not ex else "；".join(e[:40] for e in ex[:8])),
            u=last_user or "（无）",
            a=(last_answer[:600] or "（无）"),
        )
        # ⚠️ body 只能有 model/messages/stream/max_tokens（与 /api/agent/suggest 同款）：
        # 实测经 Hermes 网关时多带 `model_options` 会让上游返 HTTP 400 invalid_request，
        # 网关把错误文本塞回 content → 解析成空数组 → 候选恒为空（2026-10-04 容器内探针实锤）。
        body = {"model": stream_api.AGENT_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False, "max_tokens": 500}
        resp = stream_api._chat_once(body)
        # ⚠️ 网关失败时会把错误文本塞进 content 而不抛异常（hermes.failed=True / finish_reason=error）
        # → 必须先判网关状态，否则解析错误文案、候选恒空（2026-10-04 实锤）。
        if (resp.get("hermes") or {}).get("failed"):
            return []
        text = (resp.get("choices") or [{}])[0].get("message", {}).get("content", "")
        if text.startswith("custom rejected"):   # 老网关形态：失败文本直接进 content
            return []
        qs = _normalize(_parse_questions(text), exclude)
        if len(qs) < MIN_Q:
            return []
        return qs
    except Exception:
        return []