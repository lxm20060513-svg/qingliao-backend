# -*- coding: utf-8 -*-
"""Agent 规则记忆（v2.0.98）：用户声明"以后XX都用agent" → 存规则 → 下次同类请求直接 agent 回复。

写入：用户消息含「以后/下次/之后/记住 + XX + agent/工具/直接」→ 提取功能词存规则
匹配：新请求的最后一条用户消息命中任一规则子串 → 强制走 agent（stream_api._worker 调用）
管理：/api/agent/rules (list|add|delete)（agent_api.py）
"""
import json
import os
import re
import time
import uuid

DATA_DIR = os.environ.get("QL_DATA_DIR", os.environ.get("QL_DATA_DIR", "/data"))
RULES_PATH = os.path.join(DATA_DIR, "agent_rules.json")
MAX_RULES = 20

# 声明话术：以后/下次/之后/记住 ... XX ... agent/工具/直接/自动
# 前缀词放在非捕获组（不进 group(1)），提取出的 pattern 才是干净功能词（如 "查内存"）
_PATTERNS = [
    re.compile(r"(?:以后|下次|之后|往后|记住|记一下|从今天起)(.{1,30}?)(?:都|就|一律|直接|自动|用)?(?:用?agent|交给agent|用工具|直接)"),
    re.compile(r"把(.{1,30}?)(?:加|添加|放)(?:进|到)?agent"),
    re.compile(r"(.{1,30}?)(?:都|就|一律|直接)(?:用|走|交)?agent"),
]
# 提取后去掉语气词/标点，得到干净的功能词（如 "查磁盘"）
_CLEAN = re.compile(r"[，。！？、,.!?的了我你他它和与就都一律直接自动用交给以后下次之后记住]")


def _load():
    try:
        with open(RULES_PATH, encoding="utf-8") as f:
            return json.load(f).get("rules", [])
    except Exception:
        return []


def _save(rules):
    try:
        os.makedirs(os.path.dirname(RULES_PATH), exist_ok=True)
        # v3.0.28 review：原子写（tmp+fsync+os.replace），防并发写坏
        import tempfile
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(RULES_PATH), suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"rules": rules[-MAX_RULES:]}, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, RULES_PATH)
    except Exception:
        try:
            if os.path.exists(tmp):
                os.unlink(tmp)
        except Exception:
            pass


def list_rules():
    return _load()


def add_rule(pattern):
    p = pattern.strip()
    if not p or len(p) < 2 or len(p) > 40:
        return False, "规则内容太短或太长"
    rules = _load()
    for r in rules:
        if r.get("pattern") == p:
            return False, f"规则「{p}」已存在"
    rules.append({"id": uuid.uuid4().hex[:8], "pattern": p, "created": time.strftime("%m-%d %H:%M")})
    _save(rules)
    return True, f"已记住：以后「{p}」直接交给 Agent 处理"


def delete_rule(rid):
    rules = [r for r in _load() if r.get("id") != rid]
    _save(rules)
    return True, "规则已删除"


def extract_from_text(text):
    """从用户消息提取规则声明；命中返回 pattern，未命中返回 None"""
    t = (text or "").strip()
    if not t or "agent" not in t.lower():
        return None
    for pat in _PATTERNS:
        m = pat.search(t)
        if m:
            raw = m.group(1)
            # 去语气词，保留核心功能词
            clean = _CLEAN.sub("", raw).strip()
            if 2 <= len(clean) <= 12:
                return clean
    return None


def match(text):
    """任一规则是消息子串 → True（强制走 agent）"""
    t = (text or "")
    for r in _load():
        p = r.get("pattern", "")
        if p and p in t:
            return True
    return False


def rules_hint():
    """注入 agent system 提示：用户指定必须用 agent 的功能"""
    rules = [r["pattern"] for r in _load()]
    if not rules:
        return ""
    return "用户指定以下任务必须直接调用工具/Agent 处理，不要用普通对话回答：" + "、".join(rules)
