#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v3.9.80 上下文策略：最近 N 轮原样 + 早期转摘要。

为什么要有这个模块
------------------
轻聊每条消息都把**整段会话历史原样**发给上游（`stream_api._build_hermes_messages`，
刻意不带 X-Hermes-Session-Id，靠「断种子净化历史」防复读）→ 会话越长，每轮重发的历史
越大，token 大头全在「重发历史」上（cache_read）。本模块把「超出最近 N 轮」的早期消息
折成一条 AI 要点摘要，逐字保留的永远只有最近 N 轮。

策略（三条都是刻意的，别随手改）
--------------------------------
1. 最近 `STREAM_CTX_RECENT_TURNS` 轮**逐字保留**（1 轮 = user+assistant，默认 6 轮 ≈ 12 条），
   且保留窗口必须从 user 消息起头——半截轮次进上下文会让模型续写旧回复（本仓踩过多轮）。
2. 更早的部分 → 一段中文要点摘要，由 `stream_api` 拼进**首条 system prompt**。
   ⚠️ 不新插 system 消息：轻聊走 Responses 协议时只认首条 system（平移到 instructions），
   中间插 system 会被当历史消息发给模型。
3. 摘要**缓存**在 `data/ctx_summary.json`（键 = 会话 id，值 = 早期消息指纹 + 摘要文本），
   命中直接复用；指纹变了、且丢弃条数比上次多 `STREAM_CTX_SUMMARY_STEP` 条（默认 8）才
   后台重算。摘要调用**绝不阻塞本轮回复**：同一会话在飞任务去重 + 冷却期，首次遇到长会话
   本轮先用「（更早的 N 条对话已省略，摘要生成中）」占位，下一轮就有真摘要。
   重算粒度按条数步进而**不是每轮**：丢弃条数每轮都会 +2，若按指纹一变就算，等于每轮多打
   一次摘要模型（省下的 token 又花回去）。

配置（都有代码默认值；改 env 须同步 compose 三处 + `docker compose up -d`）
------------------------------------------------------------------------
  STREAM_CTX_RECENT_TURNS    最近保留轮数，默认 6；0 = 关闭本策略（回到原样全量发历史）
  STREAM_CTX_SUMMARY_CHARS   摘要目标字数，默认 600
  STREAM_CTX_SUMMARY_STEP    丢弃条数比上次多这么多才重算，默认 8
  STREAM_CTX_COOLDOWN_SEC    同一会话两次重算的最小间隔（秒），默认 30
  STREAM_CTX_MIN_DROP        少于这么多条早期消息就不折叠（不值得），默认 2
  STREAM_CTX_SRC_CHARS       送进摘要模型的早期文本上限（字），默认 12000
  STREAM_CTX_DATA_DIR        摘要缓存目录（缺省 = STREAM_DATA_DIR 或 <backend>/../data）
"""
import hashlib
import json
import os
import re
import threading
import time

_LOCK = threading.RLock()
_INFLIGHT = set()      # 正在后台重算的会话 id
_LAST_TS = {}          # 会话 id → 上次触发重算的时刻（冷却用）

MAX_SESSIONS = 200     # 缓存里最多保留多少个会话（超出按 ts 丢最旧）

SUMMARY_PROMPT = (
    "你是会话压缩器。把下面这段「更早的对话」压成要点摘要，供后续对话当背景使用。\n"
    "要求：\n"
    "1. 用中文，不超过 %d 字，条目式（每行一条，以「- 」开头）。\n"
    "2. 必须保留：用户的需求/偏好/身份信息、已达成的结论与关键数据（数字、编号、路径、"
    "专有名词）、未完成的事项与待办。\n"
    "3. 丢弃：寒暄与重复、工具调用的过程细节（只留结果）。\n"
    "4. 只输出摘要正文，不要输出思考过程、推理步骤、解释或任何前后缀。\n\n"
    "【更早的对话】\n%s"
)


def _env_int(name, default):
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default


def _data_dir(explicit=None):
    """摘要缓存落盘目录：显式参数 > STREAM_CTX_DATA_DIR > STREAM_DATA_DIR > <backend>/../data。

    最后一级兜底用的是「相对于本文件」的路径（容器里 backend/ 与 data/ 同级），
    与 stream_api 的 DATA_DIR 缺省口径一致，避免两个模块各指一处。
    """
    if explicit:
        return explicit
    d = os.environ.get("STREAM_CTX_DATA_DIR") or os.environ.get("STREAM_DATA_DIR")
    if d:
        return d
    return os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))


def _text(m):
    """dict 消息 → 纯文本（兼容多模态 content 数组）"""
    if not isinstance(m, dict):
        return ""
    c = m.get("content", "")
    if isinstance(c, list):
        return " ".join(str(b.get("text", "")) for b in c if isinstance(b, dict))
    return str(c)


def _role(m):
    return str(m.get("role") or "") if isinstance(m, dict) else ""


def _fingerprint(msgs):
    h = hashlib.sha1()
    for m in msgs:
        h.update((_role(m) + "\x1f" + _text(m) + "\x1e").encode("utf-8", "replace"))
    return h.hexdigest()


def _flatten(msgs, max_chars):
    """早期消息 → 一段文本（超长从**尾部**截：近期比远古重要）"""
    lines = []
    for m in msgs:
        t = re.sub(r"\s+", " ", _text(m)).strip()
        if not t:
            if m.get("content") and isinstance(m.get("content"), list):
                t = "[图片]"
            else:
                continue
        who = "用户" if _role(m) == "user" else ("AI" if _role(m) == "assistant" else _role(m))
        lines.append("%s: %s" % (who, t[:1500]))
    s = "\n".join(lines)
    if len(s) > max_chars:
        s = "…（更早内容已截断）\n" + s[-max_chars:]
    return s


def _clean(out, chars):
    """摘要回包净化：剥思考块/代码围栏、压空白、硬上限 chars*2"""
    if not isinstance(out, str):
        return ""
    s = re.sub(r"(?is)<think[^>]*>.*?</think\s*>", "", out)
    s = re.sub(r"(?is)```[a-zA-Z]*", "", s).strip()
    s = re.sub(r"\n{3,}", "\n\n", s)
    if len(s) > chars * 2:
        s = s[:chars * 2].rstrip() + "…"
    return s


def _load(path):
    """读缓存（每次读盘：文件很小，且后台线程/多线程写入后要立刻可见）"""
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save(path, payload):
    """原子写 + 保住原属主/mode（mkstemp+replace 会把 owner 变 root，本仓踩过）"""
    st_uid = st_gid = None
    try:
        st_uid, st_gid = os.stat(path).st_uid, os.stat(path).st_gid
    except Exception:
        pass
    tmp = path + ".tmp-%d" % os.getpid()
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
        if st_uid is not None:
            try:
                os.chown(path, st_uid, st_gid)
            except Exception:
                pass
    except Exception:
        try:
            os.remove(tmp)
        except Exception:
            pass


def _placeholder(n):
    return ("（更早的 %d 条对话已省略，摘要生成中；若用户追问早期细节，先请他重述）" % n)


def _should_spawn(sid, cooldown):
    with _LOCK:
        if sid in _INFLIGHT:
            return False
        if time.time() - _LAST_TS.get(sid, 0.0) < cooldown:
            return False
        _INFLIGHT.add(sid)
        _LAST_TS[sid] = time.time()
    return True


def _spawn(sid, older, fp, n, path, ask, chars, src_chars):
    def run():
        try:
            prompt = SUMMARY_PROMPT % (chars, _flatten(older, src_chars))
            out = _clean(ask(prompt), chars)
            if out:
                with _LOCK:
                    db = _load(path)          # 重新读盘：别覆盖别的会话刚写进去的条目
                    db[sid] = {"fp": fp, "n": n, "text": out, "ts": int(time.time())}
                    if len(db) > MAX_SESSIONS:
                        for k in sorted(db, key=lambda x: db[x].get("ts", 0))[:len(db) - MAX_SESSIONS]:
                            db.pop(k, None)
                    _save(path, db)
        except Exception:
            pass
        finally:
            with _LOCK:
                _INFLIGHT.discard(sid)

    threading.Thread(target=run, daemon=True, name="ctx-summary").start()


def apply(msgs, session_id, ask, recent_turns=None, data_dir=None):
    """最近 N 轮原样 + 早期转摘要。

    入参：msgs = 净化后的消息列表（dict）；session_id = 轻聊会话 id（缓存键）；
          ask = callable(prompt) -> str，摘要模型调用（由 stream_api 注入，后台线程里跑）。
    返回：((保留后的消息列表), 摘要文本或占位文案或空串, meta)
    """
    msgs = list(msgs or [])
    turns = _env_int("STREAM_CTX_RECENT_TURNS", 6) if recent_turns is None else int(recent_turns)
    meta = {"dropped": 0, "source": "off", "cached_n": None}
    if turns <= 0 or len(msgs) <= turns * 2:
        return msgs, "", meta

    kept = msgs[-turns * 2:]
    while kept and _role(kept[0]) != "user":
        kept.pop(0)                     # 保留窗口从 user 起头，别把半截轮次塞进上下文
    older = msgs[:len(msgs) - len(kept)]
    if not older or len(older) < _env_int("STREAM_CTX_MIN_DROP", 2):
        return msgs, "", meta

    n = len(older)
    fp = _fingerprint(older)
    sid = str(session_id or "_anon")
    path = os.path.join(_data_dir(data_dir), "ctx_summary.json")
    chars = _env_int("STREAM_CTX_SUMMARY_CHARS", 600)
    step = _env_int("STREAM_CTX_SUMMARY_STEP", 8)
    cooldown = _env_int("STREAM_CTX_COOLDOWN_SEC", 30)

    with _LOCK:
        ent = _load(path).get(sid)
    text, source, cached_n = "", "none", None
    if isinstance(ent, dict) and ent.get("text"):
        text = str(ent["text"])
        cached_n = ent.get("n")
        source = "cache" if ent.get("fp") == fp else "stale"
    if not text:
        text = _placeholder(n)

    need = not (isinstance(ent, dict) and ent.get("fp") == fp)
    if need and isinstance(ent, dict) and isinstance(ent.get("n"), int):
        if abs(n - ent["n"]) < max(1, step):
            need = False                # 步进不够：沿用旧摘要，别每轮都去打摘要模型
    if need and _should_spawn(sid, cooldown):
        try:
            _spawn(sid, older, fp, n, path, ask, chars, _env_int("STREAM_CTX_SRC_CHARS", 12000))
        except Exception:
            pass

    meta.update({"dropped": n, "source": source, "cached_n": cached_n,
                 "kept": len(kept), "len": len(text)})
    head = "【更早对话摘要（原 %d 条已折叠）】" % n if source in ("cache", "stale") else ""
    return kept, (head + "\n" + text) if head else text, meta
