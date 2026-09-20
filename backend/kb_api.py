#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""知识库 API（简化版 RAG）：文档管理 + 关键词检索，供聊天注入上下文

接口：
  GET  /api/kb/list                    文档列表
  POST /api/kb/upload {name, content}  上传文档（App 提取的文本/txt/md）
  POST /api/kb/upload {name, filename, file_b64}
                                       上传原始文件（后端解析 docx/xlsx/pptx/csv/...）
  POST /api/kb/delete {name}           删除文档
  POST /api/kb/search {q, top_k}       检索相关段落
  GET  /api/kb/search?q=xxx            检索（GET 形态）

检索策略：文档按 ~800 字切块（带重叠），查询词拆分后按块内命中计数评分，返回 top N。
纯标准库，零依赖；聊天时由 stream_api 在消息含「@知识库」时注入检索结果。

v3.4.30 新增：/api/kb/upload 支持 file_b64 + filename，后端用标准库 zipfile+XML
解析 docx / xlsx / pptx（无需任何第三方依赖），txt/md/csv 直接解码。
"""
import base64
import io
import json
import os
import re
import zipfile
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler

KB_DIR = os.environ.get("QL_KB_DIR",
                        os.path.join(os.environ.get("QL_DATA_DIR", "/data"), "kb"))
CHUNK_SIZE = 800
CHUNK_OVERLAP = 100
MAX_DOC = 50
MAX_DOC_SIZE = 300000
MAX_UPLOAD_B64 = 14 * 1024 * 1024      # base64 载荷上限（≈10MB 原始文件）
MAX_XLSX_ROWS = 3000                   # 单个 sheet 最多解析行数
NAME_RE = re.compile(r"^[\w\u4e00-\u9fff._-]{1,80}$")


def _ensure_dir():
    os.makedirs(KB_DIR, exist_ok=True)


def _chunk(text):
    """段落感知切块（固定长度 + 重叠）"""
    paras = [p.strip() for p in re.split(r"\n+", text) if p.strip()]
    chunks, cur = [], ""
    for p in paras:
        if len(cur) + len(p) + 1 < CHUNK_SIZE:
            cur = cur + "\n" + p if cur else p
        else:
            if cur:
                chunks.append(cur)
            cur = p
    if cur:
        chunks.append(cur)
    result = []
    for c in chunks:
        if len(c) <= CHUNK_SIZE:
            result.append(c)
        else:
            for i in range(0, len(c), CHUNK_SIZE - CHUNK_OVERLAP):
                result.append(c[i:i + CHUNK_SIZE])
    return result


def _terms(q):
    """查询词拆分（空白/常见标点；2 字以上才算词）"""
    return [t for t in re.split(r"[\s,，。；;:：、.!?？()（）\"'“”‘’]+", q)
            if len(t) >= 2]


# ---------------------------------------------------------------- 文件解析
# 全部基于标准库：docx/xlsx/pptx 本质是 zip + XML，直接抽文本，零依赖。

def _tag(el):
    return el.tag.rsplit("}", 1)[-1]


def _decode_plain(data):
    for enc in ("utf-8", "utf-8-sig", "gb18030", "utf-16"):
        try:
            return data.decode(enc)
        except (UnicodeDecodeError, UnicodeError):
            continue
    return data.decode("utf-8", errors="ignore")


def _extract_docx(data):
    """Word：按段落抽文本（表格内的段落会自然成为独立行）"""
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            if "word/document.xml" not in z.namelist():
                return ""
            xml = z.read("word/document.xml")
    except (zipfile.BadZipFile, OSError):
        return ""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return ""
    lines = []
    for p in root.iter():
        if _tag(p) != "p":
            continue
        buf = []
        for node in p.iter():
            t = _tag(node)
            if t == "t" and node.text:
                buf.append(node.text)
            elif t == "tab":
                buf.append("\t")
            elif t == "br":
                buf.append("\n")
        s = "".join(buf).strip()
        if s:
            lines.append(s)
    return "\n".join(lines)


def _sheet_rows(xml, shared):
    """解析一个 worksheet：返回 ['行1\t行2', ...]（共享字符串按索引还原）"""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return []
    out = []
    for row in root.iter():
        if _tag(row) != "row":
            continue
        cells = []
        for c in row:
            if _tag(c) != "c":
                continue
            ctype = c.get("t") or ""
            val = None
            for child in c:
                ct = _tag(child)
                if ct == "v" and child.text is not None:
                    val = child.text
                elif ct == "is":            # 内联字符串
                    buf = [x.text for x in child.iter()
                           if _tag(x) == "t" and x.text]
                    val = "".join(buf)
            if val is None:
                continue
            if ctype == "s":
                try:
                    val = shared[int(val)]
                except (ValueError, IndexError):
                    val = ""
            cells.append(val.replace("\n", " ").replace("\t", " "))
        if any(x.strip() for x in cells):
            out.append("\t".join(cells))
            if len(out) >= MAX_XLSX_ROWS:
                out.append("...(表格过长，已截断)")
                break
    return out


def _extract_xlsx(data):
    """Excel：共享字符串 + 每个 sheet 输出 TSV"""
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            names = z.namelist()
            shared = []
            if "xl/sharedStrings.xml" in names:
                try:
                    sroot = ET.fromstring(z.read("xl/sharedStrings.xml"))
                except ET.ParseError:
                    sroot = None
                if sroot is not None:
                    for si in sroot:
                        if _tag(si) != "si":
                            continue
                        shared.append("".join(
                            x.text for x in si.iter()
                            if _tag(x) == "t" and x.text))
            sheets = sorted(n for n in names
                            if re.match(r"xl/worksheets/sheet\d+\.xml$", n))
            out = []
            for n in sheets:
                rows = _sheet_rows(z.read(n), shared)
                if not rows:
                    continue
                out.append("### " + n.split("/")[-1].replace(".xml", ""))
                out.extend(rows)
    except (zipfile.BadZipFile, OSError):
        return ""
    return "\n".join(out)


def _extract_pptx(data):
    """PPT：按页抽文本"""
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            slides = [n for n in z.namelist()
                      if re.match(r"ppt/slides/slide\d+\.xml$", n)]
            slides.sort(key=lambda s: int(re.findall(r"\d+", s)[-1]))
            out = []
            for i, n in enumerate(slides, 1):
                try:
                    root = ET.fromstring(z.read(n))
                except ET.ParseError:
                    continue
                buf = [el.text for el in root.iter()
                       if _tag(el) == "t" and el.text]
                txt = " ".join(buf).strip()
                if txt:
                    out.append("## 第%d页\n%s" % (i, txt))
    except (zipfile.BadZipFile, OSError):
        return ""
    return "\n\n".join(out)


def _extract_pdf(data):
    """PDF：需要 PyMuPDF（v3.9.44 起进 requirements）。没装时明确报错——
    App 侧已不再自己把全文拼进消息（改发 doc= 引用），所以这里失败就是真读不到。"""
    try:
        import fitz  # PyMuPDF
    except ImportError:
        return "", "服务器未安装 PDF 解析组件（PyMuPDF），无法读取该文件正文"
    try:
        doc = fitz.open(stream=data, filetype="pdf")
        pages = [p.get_text() for p in doc]
        doc.close()
    except Exception as e:                       # noqa: BLE001
        return "", "PDF 解析失败：%s" % str(e)[:80]
    return "\n".join(pages), None


TEXT_EXTS = {"txt", "md", "markdown", "csv", "tsv", "json", "log",
             "py", "swift", "html", "htm", "xml", "yaml", "yml"}


def extract_file(filename, data):
    """返回 (text, err)。err 非空 = 无法解析，text 为空。"""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext == "docx":
        return _extract_docx(data), None
    if ext in ("xlsx", "xlsm"):
        return _extract_xlsx(data), None
    if ext == "pptx":
        return _extract_pptx(data), None
    if ext == "pdf":
        return _extract_pdf(data)
    if ext in TEXT_EXTS:
        return _decode_plain(data), None
    if ext == "doc":
        return "", "旧版 .doc 不支持，请另存为 .docx 再上传"
    if ext == "xls":
        return "", "旧版 .xls 不支持，请另存为 .xlsx 再上传"
    if ext == "zip":
        return "", "压缩包不支持，请解压后上传单个文档"
    return "", "不支持的格式：.%s（支持 docx/xlsx/pptx/pdf/txt/md/csv）" % ext


# ---------------------------------------------------------------- 检索
def _search(q, top_k=4):
    """关键词命中评分检索"""
    terms = _terms(q)
    if not terms:
        return []
    results = []
    _ensure_dir()
    for fname in sorted(os.listdir(KB_DIR)):
        if not fname.endswith(".txt"):
            continue
        try:
            with open(os.path.join(KB_DIR, fname), encoding="utf-8") as f:
                text = f.read()
        except OSError:
            continue
        for i, c in enumerate(_chunk(text)):
            score = 0
            for t in terms:
                n = c.count(t)
                if n:
                    score += n * (2 if len(t) >= 4 else 1)
            if score > 0:
                results.append((score, fname, i, c))
    results.sort(key=lambda x: -x[0])
    return [{"doc": r[1], "chunk": r[2], "text": r[3][:500], "score": r[0]}
            for r in results[:top_k]]


def _list_docs():
    _ensure_dir()
    out = []
    for fname in sorted(os.listdir(KB_DIR)):
        if not fname.endswith(".txt"):
            continue
        p = os.path.join(KB_DIR, fname)
        try:
            out.append({"name": fname[:-4], "size": os.path.getsize(p),
                        "chunks": len(_chunk(open(p, encoding="utf-8").read()))})
        except OSError:
            continue
    return out


class KBHandler(BaseHTTPRequestHandler):
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Auth-Token")

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _check_auth(self):
        import auth_api
        return auth_api.check_auth(self.headers, "X-KB-Password",
                                   os.environ.get("QL_PASSWORD", ""))

    def _read_json(self):
        try:
            n = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return {}

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if parsed.path.startswith("/api/kb/list"):
            self._send(200, {"ok": True, "docs": _list_docs()})
            return
        if parsed.path.startswith("/api/kb/search"):
            q = params.get("q", [""])[0]
            top = int(params.get("top_k", ["4"])[0])
            self._send(200, {"ok": True, "hits": _search(q, top)})
            return
        self._send(404, {"error": "Not Found"})

    def do_POST(self):
        if not self._check_auth():
            self._send(401, {"error": "未授权"})
            return
        import urllib.parse
        parsed = urllib.parse.urlparse(self.path)
        body = self._read_json()
        _ensure_dir()
        if parsed.path.startswith("/api/kb/upload"):
            name = (body.get("name") or "").strip()
            content = body.get("content") or ""
            b64 = body.get("file_b64")
            # v3.4.30：原始文件上传（后端解析 docx/xlsx/pptx/csv/...）
            if b64:
                fname = (body.get("filename") or name or "").split("/")[-1]
                if not name:
                    name = os.path.splitext(fname)[0]
                try:
                    raw = base64.b64decode(b64)
                except Exception:                # noqa: BLE001
                    self._send(200, {"ok": False, "message": "文件数据解码失败"})
                    return
                if len(raw) > MAX_UPLOAD_B64:
                    self._send(200, {"ok": False, "message": "文件过大（限约 10MB）"})
                    return
                content, err = extract_file(fname, raw)
                if err:
                    self._send(200, {"ok": False, "message": err})
                    return
                if not content.strip():
                    self._send(200, {"ok": False,
                                     "message": "未提取到文字（可能是扫描件/空文档）"})
                    return
            if not NAME_RE.match(name):
                self._send(200, {"ok": False, "message": "名称不合法"})
                return
            if len(content) > MAX_DOC_SIZE:
                self._send(200, {"ok": False, "message": "文档超过 300KB 限制"})
                return
            if len(_list_docs()) >= MAX_DOC and not os.path.exists(
                    os.path.join(KB_DIR, name + ".txt")):
                self._send(200, {"ok": False, "message": "文档数量已达上限(50)"})
                return
            try:
                with open(os.path.join(KB_DIR, name + ".txt"), "w",
                          encoding="utf-8") as f:
                    f.write(content)
                self._send(200, {"ok": True, "message": "已保存",
                                 "chars": len(content),
                                 "chunks": len(_chunk(content))})
            except OSError as e:
                self._send(200, {"ok": False, "message": str(e)[:150]})
            return
        if parsed.path.startswith("/api/kb/delete"):
            name = (body.get("name") or "").strip()
            if not NAME_RE.match(name):
                self._send(200, {"ok": False, "message": "名称不合法"})
                return
            try:
                os.remove(os.path.join(KB_DIR, name + ".txt"))
                self._send(200, {"ok": True, "message": "已删除"})
            except OSError:
                self._send(200, {"ok": False, "message": "文档不存在"})
            return
        self._send(404, {"error": "Not Found"})

    def log_message(self, fmt, *args):
        pass
