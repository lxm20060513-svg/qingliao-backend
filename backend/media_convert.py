# -*- coding: utf-8 -*-
"""MEDIA 协议 → data URL 图片转换（v2.0.130 + 2026-08-17）

问题：Hermes Agent 回复图片时输出 MEDIA:/路径 协议（如 MEDIA:/app/data/random_image.jpg），
App 端只认 markdown 图片语法 ![alt](url)，导致图片显示成一行路径文本（用户实测反馈）。

方案：写入侧把 MEDIA: 路径转成 data:image base64 URL（App v2.0.128 已支持 data URL 本地解码），
零 App 改动、免鉴权、蜂窝环境最稳。只在非流式写入/全量读取处转换（流式增量轮询按 offset
推进，中途变长会错位——Agent 路径是一次性写入，安全）。

路径映射：Hermes 容器数据目录 → 宿主机数据目录（按 docker-compose 挂载配置）。
"""
import base64
import os
import re

# 容器路径前缀 → 宿主真实路径（按 docker-compose 挂载）
_PREFIX_MAP = [
    (os.environ.get("QL_DATA_DIR", "/data"), os.environ.get("QL_HERMES_DATA_DIR", "/data/hermes")),
    (os.environ.get("QL_HERMES_HOST_DIR", "/data/hermes_host"), os.environ.get("QL_HERMES_ROOT_DIR", "/data/hermes")),
]

# 图片扩展名 → MIME
_MIME = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".png": "image/png", ".gif": "image/gif",
    ".webp": "image/webp", ".bmp": "image/bmp",
}

# 源文件大小上限（base64 膨胀 4/3，防撑爆 MAX_CONTENT_LEN 200000）
_MAX_FILE = 100 * 1024
# v3.0.28 review：多张图片 base64 总量上限（防多图撑爆 MAX_CONTENT_LEN）
_MAX_TOTAL_B64 = 180 * 1024  # 180KB base64 ≈ 135KB 原图

_RE_MEDIA = re.compile(r"MEDIA:\s*(\S+)")

_MISSING_HINT = "（图片文件不存在或过大，无法内嵌显示）"


def _to_host_path(p):
    """容器路径 → 宿主路径（原样返回若无法映射）"""
    for pre, host in _PREFIX_MAP:
        if p.startswith(pre + "/") or p == pre:
            return host + p[len(pre):]
    return p


def _file_to_data_url(path):
    """读取图片文件 → data URL；失败/超限返回 None"""
    if not os.path.isfile(path):
        return None
    ext = os.path.splitext(path)[1].lower()
    mime = _MIME.get(ext)
    if not mime:
        return None
    try:
        if os.path.getsize(path) > _MAX_FILE:
            return None
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("ascii")
        return f"data:{mime};base64,{b64}"
    except OSError:
        return None


def convert_media_marks(text):
    """把文本中的 MEDIA:/路径 替换为 markdown 图片（data URL）。

    找不到/超限 → 保留原路径并加说明（避免无声丢失）。
    v3.0.28 review：追踪多图 base64 总量，超 _MAX_TOTAL_B64 后不再内嵌（防撑爆 MAX_CONTENT_LEN）。
    """
    if not text or "MEDIA:" not in text:
        return text
    total_b64 = 0  # 已内嵌的 base64 字节总量

    def _repl(m):
        nonlocal total_b64
        raw = m.group(1).strip()
        host_path = _to_host_path(raw)
        # 超总量上限 → 不内嵌，保留原路径
        if total_b64 >= _MAX_TOTAL_B64:
            return f"{raw} {_MISSING_HINT}"
        url = _file_to_data_url(host_path)
        if url:
            total_b64 += len(url)
            return f"![图片]({url})"
        return f"{raw} {_MISSING_HINT}"

    return _RE_MEDIA.sub(_repl, text)
