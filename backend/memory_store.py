# -*- coding: utf-8 -*-
"""AI 记忆模块：存储用户偏好条目 → 每次对话注入 system（供 stream_api 调用）

写入：用户消息含"记住/我是/我喜欢/别忘了"等 → 自动提取存入（去重，上限 50 条）
注入：entries 非空时作为 system 消息（"关于用户的信息"）
API：/api/memory/list|add|delete（memory_api.py）
"""
import json
import os
DATA_DIR = os.environ.get("QL_DATA_DIR", "/data")
import re

MEMORY_PATH = os.path.join(DATA_DIR, "memory.json")
MAX_ENTRIES = 50


def _load():
    try:
        with open(MEMORY_PATH, encoding="utf-8") as f:
            return json.load(f).get("entries", [])
    except Exception:
        return []


def _save(entries):
    try:
        os.makedirs(os.path.dirname(MEMORY_PATH), exist_ok=True)
        with open(MEMORY_PATH, "w", encoding="utf-8") as f:
            json.dump({"entries": entries[-MAX_ENTRIES:]}, f, ensure_ascii=False)
    except Exception:
        pass


def list_entries():
    return _load()


def add_entry(text):
    t = text.strip()
    if not t or len(t) < 2:
        return False
    entries = _load()
    if t not in entries:
        entries.append(t)
        _save(entries)
        return True
    return False


def delete_entry(text):
    entries = _load()
    if text in entries:
        entries.remove(text)
        _save(entries)
        return True
    return False


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
    """先检测写入（最后一条 user），再注入记忆条目到 system"""
    try:
        check_and_save(_last_user(messages))
        entries = _load()
        if not entries:
            return messages
        ctx = "关于用户的信息（回答时自然参考，不要逐条复述）：" + "；".join(entries)
        return [{"role": "system", "content": ctx}] + list(messages)
    except Exception:
        return messages
