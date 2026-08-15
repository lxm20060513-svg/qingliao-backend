# -*- coding: utf-8 -*-
"""知识库注入模块（@知识库 触发检索 → system 注入）+ 聊天文件自动收录，供 stream_api 调用"""
import re


def auto_ingest(content):
    """聊天消息含 [文件:xxx.txt/.md] → 自动收录知识库（v2.0.96：文件即知识）"""
    try:
        import kb_api, files_api, os
        for m in re.finditer(r"\[文件:([^\[\]]+\.(?:txt|md))\]", content):
            fname = m.group(1).strip()
            src = os.path.join(files_api.UPLOAD_DIR, fname)
            if not os.path.exists(src):
                continue
            name = fname.rsplit(".", 1)[0]
            if not kb_api.NAME_RE.match(name):
                continue
            dest = os.path.join(kb_api.KB_DIR, name + ".txt")
            if os.path.exists(dest):
                continue  # 已收录
            with open(dest, "w", encoding="utf-8") as f:
                f.write(open(src, encoding="utf-8").read())
    except Exception:
        pass


def auto_ingest_all(messages):
    """遍历最近用户消息，自动收录文件"""
    for m in reversed(messages):
        if isinstance(m, dict) and m.get("role") == "user":
            content = m.get("content", "")
            if isinstance(content, list):
                content = " ".join(str(b.get("text", "")) for b in content if isinstance(b, dict))
            auto_ingest(str(content))


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
