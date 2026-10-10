# -*- coding: utf-8 -*-
"""聊天附件「正文按需注入」（v3.9.44，方案 1+3）

背景：App 过去把整份文件的文本（PDFKit / 直读，截 12000 字）拼进用户消息正文，于是
① 这份全文被存进聊天历史，之后每一轮都重复发给模型（token 每轮重付、上下文被挤爆）；
② Word/Excel/PPT 客户端不提取，AI 完全读不到内容。

改：App 只发引用标记「（已上传 NAS：doc=<服务器保存名>）」，原件留在上传目录；
正文由本模块在**组装 prompt 时**按需读取——最新 user 轮给全文（上限 STREAM_DOC_INLINE_MAX），
更早的轮只给节选（STREAM_DOC_OLDER_MAX）。跨轮记忆保住，成本从「每轮全文」压到「每轮一节选」。

解析复用 kb_api.extract_file：docx/xlsx/pptx/txt/csv 纯标准库零依赖；PDF 需 PyMuPDF
（fitz，见 requirements.txt），缺失时优雅降级成一行「读取失败」提示，绝不影响主流程。
"""
import hashlib
import os
import re

DEFAULT_NEWEST_MAX = 30000       # 最新 user 轮：单文件注入正文上限（字）
DEFAULT_OLDER_MAX = 800          # 更早的 user 轮：只给节选，防历史每轮重付全文
MAX_DOC_BYTES = 20 * 1024 * 1024  # 超过此大小不读原件（在请求线程里解析大文件会拖慢整条流）
MAX_DOCS_PER_MSG = 3             # 单条消息最多展开几个附件
CACHE_DIR = os.path.join(os.environ.get("QL_DATA_DIR", "/data"), "doc_cache")

# App 侧格式：[文件: xxx.pdf]（已上传 NAS：doc=xxx_1737.pdf）
DOC_REF_RE = re.compile(r"（已上传 NAS：doc=([^）\n]{1,160})）")


def _int_env(key, default):
    try:
        v = int(os.environ.get(key, "") or 0)
        return v if v > 0 else default
    except (TypeError, ValueError):
        return default


def _abs_path(doc):
    """把引用里的保存名解析成上传目录内的绝对路径。

    只接受**纯文件名**：客户端可控字符串，含路径分隔符/.. 一律拒（commonpath 再兜一层），
    防止把 prompt 注入变成任意文件读取。"""
    try:
        import upload_config_helper
        base = upload_config_helper.get_dir()
    except Exception:
        return None
    cleaned = (doc or "").strip().replace("\\", "/")
    name = os.path.basename(cleaned)
    if not name or name != cleaned or name.startswith("."):
        return None
    path = os.path.normpath(os.path.join(base, name))
    try:
        if os.path.commonpath([path, base]) != base:
            return None
    except ValueError:
        return None
    return path if os.path.isfile(path) else None


def _extract(path):
    """返回 (text, err)。带 sidecar 缓存：同一附件的多次追问不必反复解析 PDF。"""
    try:
        st = os.stat(path)
    except OSError:
        return "", "文件已不存在"
    if st.st_size > MAX_DOC_BYTES:
        return "", "文件过大（%d MB），未读取正文" % (st.st_size // (1024 * 1024))
    key = hashlib.sha1(("%s|%d|%d" % (path, int(st.st_mtime), st.st_size)).encode("utf-8"))
    cache_path = os.path.join(CACHE_DIR, key.hexdigest() + ".txt")
    try:
        with open(cache_path, encoding="utf-8") as f:
            cached = f.read()
        if cached:
            return cached, None
    except OSError:
        pass
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError as e:
        return "", "原件读取失败：%s" % str(e)[:60]
    try:
        import kb_api
        text, err = kb_api.extract_file(os.path.basename(path), data)
    except Exception as e:
        return "", "解析异常：%s" % str(e)[:80]
    if err:
        return "", err
    text = (text or "").strip()
    if not text:
        return "", "未提取到文字（扫描件/图片型 PDF 没有文字层）"
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        tmp = "%s.%d.tmp" % (cache_path, os.getpid())
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, cache_path)
    except OSError:
        pass                                  # 缓存只是加速，写失败不影响本次注入
    return text, None


def _block(doc, text, limit):
    n = len(text)
    if n > limit:
        head = "【附件 %s 正文（全文 %d 字，以下为前 %d 字）】" % (doc, n, limit)
        return "\n%s\n%s\n【正文结束】" % (head, text[:limit])
    return "\n【附件 %s 正文（全文 %d 字）】\n%s\n【正文结束】" % (doc, n, text)


def expand_text(text, newest=True, limit=None):
    """把一条 user 文本里的 doc= 引用换成正文；无引用则原样返回。"""
    if not text or "doc=" not in text:
        return text
    if limit is None:
        limit = _int_env("STREAM_DOC_INLINE_MAX", DEFAULT_NEWEST_MAX) if newest \
            else _int_env("STREAM_DOC_OLDER_MAX", DEFAULT_OLDER_MAX)
    budget = limit * MAX_DOCS_PER_MSG if newest else limit
    out, pos, done = [], 0, 0
    for m in DOC_REF_RE.finditer(text):
        out.append(text[pos:m.start()])
        pos = m.end()
        head, doc = m.group(0), m.group(1).strip()
        if done >= MAX_DOCS_PER_MSG or budget <= 0:
            out.append(head)
            continue
        done += 1
        path = _abs_path(doc)
        body, err = ("", "上传目录里找不到该文件") if not path else _extract(path)
        if err:
            # 旧轮只留引用：一行失败说明重复出现在每轮历史里毫无价值（新轮才需要告诉模型为什么读不到）
            out.append(head if not newest else head + "\n【附件 %s 读取失败：%s】" % (doc, err))
            continue
        take = min(budget, limit)
        budget -= take
        out.append(head + _block(doc, body, take))
    out.append(text[pos:])
    return "".join(out)


def _expand_content(content, newest):
    if isinstance(content, str):
        return expand_text(content, newest=newest)
    if isinstance(content, list):
        out, expanded = [], False
        for b in content:
            if (not expanded) and isinstance(b, dict) and isinstance(b.get("text"), str):
                nb = dict(b)
                nb["text"] = expand_text(nb["text"], newest=newest)
                out.append(nb)
                expanded = True
            else:
                out.append(b)
        return out
    return content


def expand_turns(msgs):
    """就地返回展开后的历史：只有最新一条 user 拿全文，更早的 user 轮只给节选。

    注意：只影响**发给模型**的内容，聊天库里存的消息（引用标记）不变——这正是本方案的目的。
    另外 _sanitize_history 只认 text / image_url 两种 block，所以正文一律拼在纯文本里，
    不要新增自定义 block 类型（会被当空消息整条丢弃）。"""
    try:
        if not msgs:
            return msgs
        last_user = -1
        for i in range(len(msgs) - 1, -1, -1):
            m = msgs[i]
            if isinstance(m, dict) and m.get("role") == "user":
                last_user = i
                break
        if last_user < 0:
            return msgs
        out = []
        for i, m in enumerate(msgs):
            if isinstance(m, dict) and m.get("role") == "user" \
                    and DOC_REF_RE.search(str(m.get("content") or "")):
                mm = dict(m)
                mm["content"] = _expand_content(mm.get("content"), newest=(i == last_user))
                out.append(mm)
            else:
                out.append(m)
        return out
    except Exception:
        return msgs
