# -*- coding: utf-8 -*-
"""AI 记忆模块：存储用户偏好条目 → 每次对话注入 system（供 stream_api 调用）

写入：用户消息含"记住/我是/我喜欢/别忘了"等 → 自动提取存入（去重，上限 50 条）
注入：entries 非空时作为 system 消息（"关于用户的信息"）
API：/api/memory/list|add|delete|update（memory_api.py）
"""
import hashlib
import json
import os
import re
import tempfile
import threading
import time

MEMORY_PATH = os.environ.get("QL_DATA_DIR", "/data") + "/memory.json"
MAX_ENTRIES = 50

# v4.0.x 第 4 项「记忆条目结构化」：状态 / 日期 / 来源会话。
#
# 🚨 为什么**旁挂 meta 表**而不是把 entries 从 [str] 换成 [dict]：
#   entries 是全仓最热的共享结构 —— App 三处（MemoryView / SettingsCore 计数 / HomeCards 提示卡）、
#   memory_api 三处响应、prompt_block() 注入、proactive_agent._memory_block() 全都按字符串读它。
#   一旦换结构，这些地方**全部静默变成空**（`as? [String]` 拿不到 → 记忆看起来"丢了"）。
#   而 proactive_agent.py:394 早就写着 `r.get("text","")` 在等 dict 形态 —— 说明这条路
#   曾经被规划过但没做完（一直返回空串，那一整块用户偏好对 proactive 一直是空的）。
#   旁挂 meta = 旧读者零感知（零回归），新字段按 text 查。
META_KEY = "meta"
# 状态取值（App 侧三选一）。默认 active = 生效中，会注入 prompt。
STATUS_ACTIVE = "active"      # 生效中
STATUS_PENDING = "pending"    # 待确认（仍注入，只是提醒用户复核）
STATUS_STALE = "stale"        # 已过时（仍注入，但 App 标灰、明确表示可能不再准确）
STATUSES = (STATUS_ACTIVE, STATUS_PENDING, STATUS_STALE)

# v3.0.6 review fix：记忆 JSON 高并发读写（每条流式消息 inject→add_entry），
# 加全局锁 + 原子写（tmp+os.replace+fsync），防丢条目/写一半损坏
_lock = threading.Lock()


def _read_doc():
    """读整个记忆文档 {"entries": [...], "meta": {...}}。

    v3.9.14：解析失败不再静默返回空——先把损坏文件改名留档。返回 (entries, meta, ok)。
    ok=False 表示「读到了但坏了」（已留档），调用方**不要**拿这个空列表去 _save 覆盖真文件。
    """
    try:
        with open(MEMORY_PATH, encoding="utf-8") as f:
            doc = json.load(f)
    except FileNotFoundError:
        return [], {}, True
    except Exception as e:
        try:
            bad = MEMORY_PATH + ".corrupt-" + time.strftime("%Y%m%d%H%M%S")
            os.replace(MEMORY_PATH, bad)
            print("[memory] 记忆文件解析失败，已留档为 %s：%s" % (bad, e), flush=True)
        except Exception:
            pass
        return [], {}, False
    if not isinstance(doc, dict):
        return [], {}, False
    entries = doc.get("entries", [])
    if not isinstance(entries, list):
        entries = []
    # meta 必须是 dict[str, dict]；任何别的形态（被手改坏 / 老版本遗留）一律当空，
    # 绝不因为 meta 读不出来就把 entries 也丢掉 —— 那才是真事故。
    meta = doc.get(META_KEY, {})
    if not isinstance(meta, dict):
        meta = {}
    clean = {}
    for k, v in meta.items():
        if isinstance(k, str) and isinstance(v, dict):
            clean[k] = v
    return [str(e) for e in entries], clean, True


def _load():
    """读记忆条目（只取正文，兼容全部旧调用方）。"""
    return _read_doc()[0]


def _default_meta(text, source="", session_id=""):
    return {
        "status": STATUS_ACTIVE,
        "created": int(time.time()),
        "updated": int(time.time()),
        "source": source,
        "sessionId": session_id,
    }


def _normalize_meta(m, text):
    """把一条 meta 补齐成完整形态（缺字段给默认值，非法状态归 active）。

    为什么要 normalize 而不是直接信任文件：这份 JSON 用户能编辑、也能被老版本写坏。
    App 侧拿到 status 去查表，值不在表里就会显示成一个空白胶囊（用户看着像 bug）。
    """
    out = _default_meta(text)
    if not isinstance(m, dict):
        return out
    st = m.get("status")
    if st in STATUSES:
        out["status"] = st
    for k in ("created", "updated"):
        v = m.get(k)
        if isinstance(v, (int, float)) and v > 0:
            out[k] = int(v)
    for k in ("source", "sessionId"):
        v = m.get(k)
        if isinstance(v, str):
            out[k] = v[:200]
    return out


def list_meta():
    """返回 [{text, status, created, updated, source, sessionId}, ...]（与 entries 同序）。

    旧文件里的条目**也**在这里出现，status 缺省 active —— 所以 App 一次都不用改
    就能显示全部条目，而不是只显示「改过的那几条」。
    """
    with _lock:
        entries, meta, _ = _read_doc()
        out = []
        for t in entries:
            m = _normalize_meta(meta.get(t), t)
            row = {"text": t}
            row.update(m)
            out.append(row)
        return out


def set_meta(text, status=None, source=None, session_id=None):
    """改一条记忆的状态 / 来源。text 不存在返回 False。

    只改 meta，**绝不碰 entries 正文**（这条最容易写成顺手 append 一遍）。
    """
    t = (text or "").strip()
    if not t:
        return False
    with _lock:
        entries, meta, ok = _read_doc()
        if not ok or t not in entries:
            return False
        m = _normalize_meta(meta.get(t), t)
        if status is not None:
            if status not in STATUSES:
                return False
            m["status"] = status
        if source is not None and isinstance(source, str):
            m["source"] = source[:200]
        if session_id is not None and isinstance(session_id, str):
            m["sessionId"] = session_id[:200]
        m["updated"] = int(time.time())
        meta[t] = m
        return _save(entries, meta)


def _save(entries, meta=None):
    """原子写记忆文件（v3.0.6 起就是 mkstemp+fsync+replace；v3.9.14 让失败可见）。

    meta 缺省时**沿用文件里现有的 meta**（重新读一次）—— 原来 add_entry/delete_entry
    只传 entries，若这里默认清空 meta，用户点一次「删除」就会把旁边所有条目的
    状态/来源/日期全抹掉。宁可多读一次文件（读在锁内、无并发风险），也不能静默丢数据。
    """
    tmp = None
    try:
        os.makedirs(os.path.dirname(MEMORY_PATH), exist_ok=True)
        if meta is None:
            meta = _read_doc()[1]
        keep = entries[-MAX_ENTRIES:]
        # 正文被 MAX_ENTRIES 截掉的条目，它的 meta 是孤儿键 → 顺手剪掉，
        # 否则 meta 表会单调增长（每次新增都留一条永远读不到的记录）。
        live = set(keep)
        meta = {k: v for k, v in meta.items() if k in live}
        # v3.0.6 review fix：tmp + write + flush + fsync + os.replace 原子落盘
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(MEMORY_PATH), suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            doc = {"entries": keep}
            if meta:
                doc[META_KEY] = meta
            json.dump(doc, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, MEMORY_PATH)
        return True
    except Exception as e:
        # v3.9.14：原来是静默 pass —— 磁盘满/权限问题导致写失败时，App 仍显示「已记住」。
        print("[memory] 记忆保存失败：%s" % e, flush=True)
        try:
            if tmp and os.path.exists(tmp):
                os.unlink(tmp)
        except Exception:
            pass
        return False


def list_entries():
    with _lock:
        return _load()


def add_entry(text, source="", session_id=""):
    """新增一条记忆。source/session_id 是第 4 项新加的来源标注（缺省不影响旧行为）。

    ⚠️ 正文去重命中时**不覆盖来源**：用户手动加过的条目不该被一次聊天里的重复句
    改写成「来自某会话」（那会把用户自己整理过的东西降级）。所以只在真正新增时写 meta。
    """
    t = text.strip()
    if not t or len(t) < 2:
        return False
    # v3.0.6 review fix：读-改-写全在锁内，防并发丢条目
    with _lock:
        entries, meta, ok = _read_doc()
        if not ok:
            return False
        if t not in entries:
            entries.append(t)
            meta[t] = _default_meta(t, source, session_id)
            # v3.9.14：如实返回落盘结果（原来无论 _save 成败都 return True）
            return _save(entries, meta)
        return False


def delete_entry(text):
    # v3.9.95 修：函数体曾被一次改动顶成裸 `return False`（8 空格缩进仍在，
    # 语法合法所以没被发现）→ App 点「删除」永远返回 False、条目删不掉。
    # v3.0.6 review fix：读-改-写全在锁内
    with _lock:
        entries, meta, ok = _read_doc()
        if not ok:
            return False
        if text in entries:
            entries.remove(text)
            # 🚨 第 4 项：正文删了，meta 里的孤儿键**必须**一起删。
            #   原来 _save 只按 entries 重建 JSON（当时也没有 meta），改成 dict 后
            #   留着键会让「同名条目重新加进来」继承上一世的状态/来源 —— 极难察觉。
            meta.pop(text, None)
            _save(entries, meta)
            return True
        return False


def update_entry(old, new):
    """就地改写一条记忆（v3.9.40 #19），保持它在列表里的位置不变。

    不做成 delete + add：add_entry 是 append，改完会跳到末尾；注入 system 时
    是 "；".join(entries)，条目顺序就是模型读到记忆的次序，不该被编辑打乱。
    """
    o = (old or "").strip()
    n = (new or "").strip()
    if not n or len(n) < 2:
        return False
    with _lock:
        entries, meta, ok = _read_doc()
        if not ok or o not in entries:
            return False
        i = entries.index(o)
        if n == o:
            return True
        if n in entries:
            entries.pop(i)
            meta.pop(o, None)
            return _save(entries, meta)
        entries[i] = n
        # 🚨 第 4 项：正文改了就**改名 meta 的键**并把 updated 推到现在。
        #   忘了搬 meta → 这条记忆的状态/来源/日期静默归零（用户看着像"结构化没生效"）。
        m = _normalize_meta(meta.pop(o, None), n)
        m["updated"] = int(time.time())
        meta[n] = m
        return _save(entries, meta)


# 记忆意图检测（记住/我是/我喜欢/别忘了…）
_REMEMBER = re.compile(
    r"(?:记住|请记住|别忘了|我是|我叫|我喜欢|我不喜欢|我经常|我习惯|我一直|以后)([^。！？!?，,；;\n]{2,60})")

# 排除词（命令式/临时指令，不误存）
_SKIP = ("你", "这个", "那个", "这里", "那里", "一下", "的话")


def _last_user(messages):
    for m in reversed(messages):
        if isinstance(m, dict) and m.get("role") == "user":
            return m.get("content", "")
    return ""


def check_and_save(user_text, session_id=""):
    """检测用户消息的记忆意图 → 提取句子存入；返回新存条目。

    v4.0.x 第 4 项：session_id 只用来给新条目**标注来源会话**（不传 = 不标注，
    与旧调用完全等价）。source 固定 "chat" —— 「记住…」这句话本身就来自聊天。
    """
    t = str(user_text)
    saved = []
    for m in _REMEMBER.finditer(t):
        phrase = m.group(1).strip()
        if phrase and len(phrase) >= 2 and not any(phrase.startswith(s) for s in _SKIP):
            if add_entry(phrase, source="chat", session_id=session_id):
                saved.append(phrase)
    return saved


def inject(messages):
    """先检测写入（最后一条 user），再注入记忆条目到 system。
    v3.0.28 review：避免每次流式请求都 _load()——check_and_save 内部已有锁保护读写，
    这里只读一次。"""
    try:
        check_and_save(_last_user(messages))
        with _lock:
            entries = _load()
        if not entries:
            return messages
        ctx = "关于用户的信息（回答时自然参考，不要逐条复述）：" + "；".join(entries)
        return [{"role": "system", "content": ctx}] + list(messages)
    except Exception:
        return messages


# v3.9.95：system 前缀注入（哈希门控）。
# 背景：memory.json 一直只有 App 侧 CRUD（memory_api）在写，stream_api 里 `import memory_store`
# 之后从无调用点 → 用户在「AI 记忆」页写的条目从未进过对话 prompt（写了没人读）。
# 门控做法（照 Kelivo 的思路，自己重写）：把条目序列化成**逐字稳定**的前缀，先算内容
# sha256 前 16 位签名，签名没变就直接复用上次的字符串——整段 system 因此逐字不变，
# 上游 prompt cache 才能命中；记忆一改，前缀才变一次。顺序必须是「先比签名再决定重建」，
# 反过来先写后比就永远检测不到变化。
# 线程安全：dict 赋值原子；文件读失败沿用上一次的块（绝不把「读不到」当成「没有记忆」）。
_prefix_cache = {"sig": None, "block": ""}


def prompt_block():
    """返回记忆 system 前缀（无条目时返回空串）。供 stream_api 各 system 组装点拼接。"""
    try:
        with open(MEMORY_PATH, "rb") as f:
            raw = f.read()
    except FileNotFoundError:
        raw = b""
    except Exception as e:
        print("[memory] 记忆注入读取失败：%s" % e, flush=True)
        return _prefix_cache["block"]
    sig = hashlib.sha256(raw).hexdigest()[:16]
    if sig == _prefix_cache["sig"]:
        return _prefix_cache["block"]
    try:
        entries = json.loads(raw.decode("utf-8")).get("entries", []) if raw else []
    except Exception as e:
        # 解析失败（写一半/外部截断）→ 沿用上一次的块，而不是当成「没有记忆」把前缀清空
        print("[memory] 记忆注入解析失败，沿用上次前缀：%s" % e, flush=True)
        return _prefix_cache["block"]
    items = [str(e).strip() for e in entries if str(e).strip()]
    block = ""
    if items:
        block = ("\n\n【用户长期记忆（App「AI 记忆」页维护的条目，回答时自然参考，"
                 "不要逐条复述）】" + "；".join(items))
    _prefix_cache["sig"] = sig
    _prefix_cache["block"] = block
    return block

