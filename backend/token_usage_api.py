#!/usr/bin/env python3
"""轻聊 · token 用量聚合（v3.9.82 · 看板「模型用量」栏第二块）

数据源：Hermes 生产库 state.db 的 `sessions` 表（**只读**）。
  容器内可见路径 `/volume1/docker/hermes/hermes-data/state.db`
  （= 宿主 `/opt/data/state.db` = NAS 挂载 `/opt/hermes_host/hermes-data/state.db`，同一 inode）。

口径（与 App 卡片标签一致，均为**自然日/自然月**，非滚动 24 小时）：
  - 时间轴走 `started_at`（会话开始时刻），时区 CST(UTC+8) —— 宿主/容器可能是 UTC，
    所以这里**显式**按 +8 算零点，不能用 time.localtime（否则北京时间 0~8 点算到前一天）。
  - today  = started_at >= 今天 00:00 (+08)
  - month  = started_at >= 本月 1 日 00:00 (+08)
  - total  = input + output + cache_read + cache_write（缓存读占九成以上，用户看到的「M」是这个）
  - cache_write 恒为 0（现用 provider 不上报），保留字段以便前端不改。

App 侧解析（`qingliao/Core/Models.swift` → `TokenUsage.parse`）只认：
  {"ok": true, "today": {input, output, cache, total, sessions}, "month": {...}}
多给的键会被忽略，故本文件额外带 reasoning / cache_write / ts / tz 便于排查。
"""
import datetime
import json
import os
import sqlite3
import time

# 环境变量优先（便于换库/测试），否则按容器 → 宿主顺序探测
DB_CANDIDATES = [
    os.environ.get("QL_STATE_DB") or "",
    "/volume1/docker/hermes/hermes-data/state.db",
    "/opt/data/state.db",
    "/opt/hermes_host/hermes-data/state.db",
]
# 用户所在时区（北京 +8）。宿主/容器是 UTC，故不跟随系统 localtime。
TZ_OFFSET_HOURS = float(os.environ.get("QL_TZ_OFFSET") or 8)

_SQL = ("SELECT COUNT(*),"
        " COALESCE(SUM(input_tokens),0),"
        " COALESCE(SUM(output_tokens),0),"
        " COALESCE(SUM(cache_read_tokens),0),"
        " COALESCE(SUM(cache_write_tokens),0),"
        " COALESCE(SUM(reasoning_tokens),0)"
        " FROM sessions WHERE started_at >= ?")


def _db_path():
    for p in DB_CANDIDATES:
        if p and os.path.exists(p):
            return p
    return None


def _windows(now_ts=None):
    """→ (今日零点, 本月零点, tzinfo)，均按 TZ_OFFSET_HOURS 固定偏移计算。"""
    tz = datetime.timezone(datetime.timedelta(hours=TZ_OFFSET_HOURS))
    now = datetime.datetime.fromtimestamp(now_ts if now_ts is not None else time.time(), tz)
    day0 = datetime.datetime(now.year, now.month, now.day, tzinfo=tz).timestamp()
    mon0 = datetime.datetime(now.year, now.month, 1, tzinfo=tz).timestamp()
    return day0, mon0, tz


def _agg(con, since):
    n, inp, out, cr, cw, rt = con.execute(_SQL, (since,)).fetchone()
    n, inp, out, cr, cw, rt = (int(x or 0) for x in (n, inp, out, cr, cw, rt))
    return {
        "sessions": n,
        "input": inp,
        "output": out,
        "cache": cr,
        "cache_write": cw,
        "reasoning": rt,
        "total": inp + out + cr + cw,
    }


def collect_token_usage(now_ts=None):
    """返回给 `/api/nas/token-usage` 的 JSON（永不带异常，失败时 ok:false + error）。"""
    path = _db_path()
    if not path:
        return {"ok": False, "error": "state.db 未找到", "today": {}, "month": {}}
    day0, mon0, tz = _windows(now_ts)
    _rp = _reset_point()          # v3.9.85：重置起点后的才计入
    day0 = max(day0, _rp)
    mon0 = max(mon0, _rp)
    try:
        con = sqlite3.connect("file:%s?mode=ro" % path, uri=True, timeout=5)
        try:
            today = _agg(con, day0)
            month = _agg(con, mon0)
        finally:
            con.close()
    except Exception as e:                       # 库被占/表结构变化都不该让面板整块塌掉
        return {"ok": False, "error": "%s: %s" % (type(e).__name__, str(e)[:120]),
                "today": {}, "month": {}}
    return {
        "ok": True,
        "ts": int(now_ts if now_ts is not None else time.time()),
        "tz": "UTC%+g" % TZ_OFFSET_HOURS,
        "db": path,
        "today": today,
        "month": month,
    }



# ---- v3.9.85：用量重置（长按 token 卡触发）----
# 语义：记录「重置起点」epoch，聚合窗口起点取 max(自然窗口零点, 起点)。
# 不动 state.db 原始数据（Hermes 的账本），起点存本文件同目录 token_usage_reset.json。
_RESET_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "token_usage_reset.json")


def _reset_point():
    try:
        with open(_RESET_FILE, "r", encoding="utf-8") as f:
            return float(json.load(f).get("since", 0))
    except (OSError, ValueError):
        return 0.0


def reset_token_usage(now_ts=None):
    # 写入重置起点=现在。自然日/月窗口滚动后各自重新累积；起点前旧账不再计入。
    ts = float(now_ts if now_ts is not None else time.time())
    with open(_RESET_FILE, "w", encoding="utf-8") as f:
        json.dump({"since": int(ts)}, f, ensure_ascii=False)
    return {"ok": True, "since": int(ts)}


if __name__ == "__main__":
    import json
    print(json.dumps(collect_token_usage(), ensure_ascii=False, indent=2))
