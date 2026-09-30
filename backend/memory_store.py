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

MEMORY_PATH = os.path.join(os.environ.get("QL_DATA_DIR", "/volume1/docker/hermes/微信文件/轻聊web/data"),"memory.json")
MAX_ENTRIES = 50

# v3.0.6 review fix：记忆 JSON 高并发读写（每条流式消息 inject→add_entry），
# 加全局锁 + 原子写（tmp+os.replace+fsync），防丢条目/写一半损坏
_lock = threading.Lock()


def _load():
    """读记忆条目。v3.9.14：解析失败不再静默返回空——先把损坏文件改名留档。

    原来 `except Exception: return []` 会把「文件损坏」表现成「用户没有记忆」，而调用方
    紧接着 `_save()` 就用这个空列表覆盖真文件 → 记忆永久丢失、日志里也查不到任何线索。
    """
    try:
        with open(MEMORY_PATH, encoding="utf-8") as f:
            return json.load(f).get("entries", [])
    except FileNotFoundError:
        return []
    except Exception as e:
        try:
            bad = MEMORY_PATH + ".corrupt-" + time.strftime("%Y%m%d%H%M%S")
            os.replace(MEMORY_PATH, bad)
            print("[memory] 记忆文件解析失败，已留档为 %s：%s" % (bad, e), flush=True)
        except Exception:
            pass
        return []


def _save(entries):
    """原子写记忆文件（v3.0.6 起就是 mkstemp+fsync+replace；v3.9.14 让失败可见）。"""
    tmp = None
    try:
        os.makedirs(os.path.dirname(MEMORY_PATH), exist_ok=True)
        # v3.0.6 review fix：tmp + write + flush + fsync + os.replace 原子落盘
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(MEMORY_PATH), suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"entries": entries[-MAX_ENTRIES:]}, f, ensure_ascii=False)
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


def add_entry(text):
    t = text.strip()
    if not t or len(t) < 2:
        return False
    # v3.0.6 review fix：读-改-写全在锁内，防并发丢条目
    with _lock:
        entries = _load()
        if t not in entries:
            entries.append(t)
            # v3.9.14：如实返回落盘结果（原来无论 _save 成败都 return True）
            return _save(entries)
        return False


def delete_entry(text):
    # v3.9.95 修：函数体曾被一次改动顶成裸 `return False`（8 空格缩进仍在，
    # 语法合法所以没被发现）→ App 点「删除」永远返回 False、条目删不掉。
    # v3.0.6 review fix：读-改-写全在锁内
    with _lock:
        entries = _load()
        if text in entries:
            entries.remove(text)
            _save(entries)
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
        entries = _load()
        if o not in entries:
            return False
        i = entries.index(o)
        if n == o:
            return True
        if n in entries:
            entries.pop(i)
            return _save(entries)
        entries[i] = n
        return _save(entries)


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


def check_and_save(user_text):
    """检测用户消息的记忆意图 → 提取句子存入；返回新存条目"""
    t = str(user_text)
    saved = []
    for m in _REMEMBER.finditer(t):
        phrase = m.group(1).strip()
        if phrase and len(phrase) >= 2 and not any(phrase.startswith(s) for s in _SKIP):
            if add_entry(phrase):
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

