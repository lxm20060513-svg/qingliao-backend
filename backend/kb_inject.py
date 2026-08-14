# -*- coding: utf-8 -*-
"""知识库注入模块（@知识库 触发检索 → system 注入），供 stream_api 调用"""


def inject(messages):
    """最后一条 user 消息含 @知识库 → 检索结果注入 system prompt（简化版 RAG）"""
    try:
        import kb_api
        last_user = None
        for m in reversed(messages):
            if isinstance(m, dict) and m.get("role") == "user":
                last_user = m
                break
        if not last_user:
            return messages
        content = last_user.get("content", "")
        if isinstance(content, list):
            content = " ".join(str(b.get("text", "")) for b in content if isinstance(b, dict))
        if "@知识库" in str(content):
            q = str(content).replace("@知识库", "").strip()
            hits = kb_api._search(q)
            if hits:
                ctx = "\n\n".join("[%s] %s" % (h["doc"], h["text"]) for h in hits)
                injected = [{"role": "system",
                             "content": "以下是从知识库检索到的资料，请优先基于这些内容回答（可标注来源文档）：\n" + ctx}]
                return injected + list(messages)
    except Exception:
        pass
    return messages
