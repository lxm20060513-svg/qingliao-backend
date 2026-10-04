# v4.0.7 · 长期目标（生活页「长期目标」栏目的服务端半边）
#
# 为什么并进 life_api 而不是新建 goals_api.py：
#   /api/life 前缀已在 unified_router.ROUTE_TABLE + nginx(16668) + webui_443 + ALLOWED_RELAY
#   四处都通。新开前缀要同步改四处，漏一处就 404/403（MEMORY 里的老坑）。
#   并进去 = 零接线改动、零 nginx reload。
#
# 闭环：AI 判定「我在筹备 XX」是长期目标 → 回「建目标卡」→ 用户确认
#       → POST /api/life/goal → 落 goals.json + 建每天跑的 cron job（早推进 + 晚复盘）
#       → 两段汇报经 hermes cron 的 deliver 落 App 任务中心 + 微信（投递不用我们管）
#       → cron 跑完 POST /api/life/goal/report 回写 lastReport，卡片显示进度。
#
# 🚨 两条硬约定：
# 1) goals.json 与 iOS 端待办/备忘同层（/api/files/pin_read|pin_write 是 iOS 的通道），
#    本模块只负责「建目标时建 job」+「cron 回写汇报」，不重复造 iOS 已在用的落点。
# 2) 删目标必须连 cron job 一起删，否则明天还会推一个用户已经删掉的目标。
#
# 仅标准库。

import hashlib
import json
import os
import re
import threading
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime

HERMES_API = "http://127.0.0.1:9123"
HERMES_KEY = os.environ.get("STREAM_HERMES_KEY") or os.environ.get("QL_AGENT_KEY") or ""

GOALS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "..", "data", "goals.json")
GOALS_PATH = os.path.normpath(GOALS_PATH)

# 写锁：iOS 端是 FIFO 串行写，服务端同样不能并发覆盖
_goals_lock = threading.Lock()
MAX_STEPS = 12          # 别让 AI 拆出 50 步，12 步足够覆盖一个季度

# v4.0.40：待办联动的真值落点（与 iOS 端 TodoStore 同一个文件，GoalTodoBridge 的标记口径）
TODOS_PATH = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                           "..", "data", "todos.json"))
_todos_lock = threading.Lock()

# v4.0.40：手动「现在开始推进」的单次执行上限（秒）。超时按 error 收尾，不留僵尸 running。
PUSH_NOW_TIMEOUT = 180

# ── v4.0.44 · 后台推进闭环（用户 7 条要求里的 3/4/5/6）────────────────────────
# ① 报告回写闭环：cron 跑完由 Hermes 侧 cron 桥（ql_task_push.py）带 cronJobID 回写，
#    卡片进度 / 步骤勾选 / 下一步开始时间自动更新（此前端点没人调，reports 永远空）。
# ② 每步完成：推一条 system 进固定会话「轻聊投递」+ 联动划掉该步的待办。
# ③ 需要确认：不再是纯文本推回原会话，改成可点选 / 可手输的 question 卡；
#    答案回写目标时间线 + 推投递 + 注入原会话（避免后台推进一直等不到回复）。
# ④ 进行中作业标题 / 详情带「第 k/N 步：xxx」。
QUESTION_TIMEOUT = 6 * 3600      # 问题卡等答案的上限（秒）
_ANSWER_DONE = set()             # 已处理过答案的 question id（幂等，防重复回写）
_WATCHERS = set()                # v4.0.44 审查修复：正在等答案的 question id（并发上限用）
_WATCH_MAX = 8                   # 等答案线程上限（每张卡一个 daemon，最多空转 QUESTION_TIMEOUT）
DEFAULT_ASK_OPTIONS = ["继续推进（现在就推进一次）", "我知道了，按计划来"]


def _goals_read():
    """返回 {id: goal}。文件不存在/坏掉/非列表 → 空 dict（前端当空态，不抛）。"""
    if not os.path.exists(GOALS_PATH):
        return {}
    try:
        with open(GOALS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return {}
        return {g["id"]: g for g in data
                if isinstance(g, dict) and g.get("id")}
    except Exception:
        return {}


def _goals_write(goals):
    items = sorted(goals.values(), key=lambda g: g.get("updatedAt", ""), reverse=True)
    d = os.path.dirname(GOALS_PATH)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = GOALS_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False)
    os.replace(tmp, GOALS_PATH)      # 原子替换：iOS 端不会读到半个文件


def _hermes(method, path, payload=None, timeout=12):
    """跟 hermes 9123 说话（容器内直连，绕开 9127 的 token 门）。"""
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(HERMES_API + path, data=data, headers={
        "Authorization": "Bearer %s" % HERMES_KEY,
        "Content-Type": "application/json",
    }, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _job_create(name, cron, prompt, deliver="origin"):
    # deliver 白名单校验，防存储型 XSS（与 cron_api.py 同口径）
    if deliver not in ("origin", "weixin", "local", "all"):
        deliver = "origin"
    return _hermes("POST", "/api/jobs", {
        "name": str(name)[:200],
        "prompt": str(prompt)[:4000],
        "schedule": str(cron)[:100],
        "enabled": True,
        "deliver": deliver,
    })


def _job_delete(job_id):
    if not job_id:
        return
    try:
        _hermes("DELETE", "/api/jobs/%s" % job_id)
    except Exception as e:
        print("[goals] delete job %s failed: %s" % (job_id, e))


def _job_id_of(result):
    """hermes 建 job 的返回体在不同版本里包在 job / 顶层，两种都认。"""
    if isinstance(result, dict):
        j = result.get("job") if isinstance(result.get("job"), dict) else result
        return j.get("id") or ""
    return ""


def _steps_digest(steps):
    return "\n".join("  [%s] %s" % ("x" if s.get("done") else " ", s.get("title", ""))
                     for s in steps)


def _goal_morning_prompt(goal):
    steps = goal.get("steps") or []
    done_n = sum(1 for s in steps if s.get("done"))
    nxt = next((s for s in steps if not s.get("done")), None)
    nxt_txt = nxt.get("title") if nxt else "（全部完成：确认收尾，或拆一个后续目标）"
    return (
        "你在帮用户推进一个长期目标（每天这个时段提醒一次）。\n\n"
        "目标：%s\n当前进度：%d/%d\n步骤清单：\n%s\n\n"
        "今天要推进的一步：%s\n\n"
        # v4.0.20（#11）：第一行必须是机器可解析的进度行 —— 任务中心靠它显示
        # 「后台自主推进到第几步」（此前只有正文，App 只能看到一段散文）
        "【硬性格式】回复**第一行**必须原样输出下面这一行，不要加任何前缀、不要换行：\n"
        "【目标推进 %d/%d】\n"
        "这一行之后才可以写正文。\n\n"
        "请用中文输出今天的推进提醒，三部分：\n"
        "1) 今天推进哪一步 —— 具体到动作，不要只说「继续努力」\n"
        "2) 需要用户本人做什么 —— 最多 1 件事，一句话说清（没有就写「无」）\n"
        "3) 一句务实提醒 —— 别灌鸡汤\n\n"
        "正文控制在 200 字以内。**正文之后**另起一段，严格按下面格式输出（供 App 回写卡片，\n"
        "这一段不要有任何多余文字）：\n"
        "##GOAL_REPORT##\n"
        "今天推进：<一句话>\n"
        "需要你做：<一句话>\n"
        "完成步骤：<你这一轮**确实已经做完**的步骤序号，按上面清单从 1 数（如 2 或 2,3）；\n"
        "  只是提醒用户去做、还没做完，或者没有 → 写 无>\n"
        "选项：<上面「需要你做」是要用户拍板的选择时，给 2~3 个短选项用 | 分隔；\n"
        "  否则写 无>\n"
        "##END##"
    ) % (goal.get("title", ""), done_n, len(steps),
         _steps_digest(steps) or "  （未拆步骤）", nxt_txt,
         min(done_n + 1, len(steps)) if steps else 0, len(steps))


def _goal_evening_prompt(goal):
    steps = goal.get("steps") or []
    done_n = sum(1 for s in steps if s.get("done"))
    return (
        "你在帮用户复盘一个长期目标的今天（每天这个时段复盘一次）。\n\n"
        "目标：%s\n当前进度：%d/%d\n步骤清单：\n%s\n\n"
        # v4.0.20（#11）：同早推进，第一行给机器可解析的进度
        "【硬性格式】回复**第一行**必须原样输出下面这一行，不要加任何前缀、不要换行：\n"
        "【目标推进 %d/%d】\n"
        "这一行之后才可以写正文。\n\n"
        "请用中文输出今晚复盘，三部分：\n"
        "1) 今天做了什么 —— 没推进就直说没推进，不要编\n"
        "2) 还剩多少 —— %d 步\n"
        "3) 明天计划推进哪一步\n\n"
        "正文控制在 200 字以内。**正文之后**另起一段，严格按下面格式输出（供 App 回写卡片）：\n"
        "##GOAL_REPORT##\n"
        "今日：<一句话>\n"
        "剩余：<已完成>/<总数>\n"
        "明日：<一句话>\n"
        "需要你做：<一句话；没有写 无>\n"
        "完成步骤：<今天**确实已经做完**的步骤序号，从 1 数（如 3 或 3,4）；没有 → 写 无>\n"
        "选项：<「需要你做」是要用户拍板的选择时给 2~3 个短选项用 | 分隔；否则写 无>\n"
        "##END##"
    ) % (goal.get("title", ""), done_n, len(steps),
         _steps_digest(steps) or "  （未拆步骤）",
         done_n, len(steps), len(steps) - done_n)


def _auto_split(title):
    """手工建目标的兜底拆解。

    这里**不调 LLM**（建目标的 HTTP 请求要秒回，调模型会卡 10~30 秒）。
    真正的智能拆解由 AI 在聊天里给（用户口径：AI 自己识别 → 回建目标卡 → 用户确认）。
    这个模板只保证「手工建的目标也有可推进的步骤」，不会让 cron 每天推空话。
    """
    return [
        "明确「%s」的完成标准（做成什么样算完成）" % title,
        "拆出关键里程碑与时间点",
        "推进第一个里程碑",
        "复盘并调整后续计划",
    ]


def goals_create(payload):
    """建目标 + 建 cron job。返回 (code, body)。"""
    title = str(payload.get("title") or "").strip()
    if not title:
        return 400, {"ok": False, "error": "缺少 title"}

    raw_steps = payload.get("steps") or []
    if not raw_steps:
        raw_steps = [{"id": uuid.uuid4().hex, "title": t} for t in _auto_split(title)]

    steps = []
    now = datetime.now().isoformat()
    for s in raw_steps[:MAX_STEPS]:
        st = (s.get("title", "") if isinstance(s, dict) else str(s)).strip()
        if not st:
            continue
        steps.append({
            "id": (s.get("id") if isinstance(s, dict) and s.get("id") else uuid.uuid4().hex),
            "title": st[:200],
            "todoLinked": bool(s.get("todoLinked")) if isinstance(s, dict) else False,
            "done": False,
            "doneAt": None,
            # v4.0.40（#5）：首步建目标即视为已开始（后续步骤由 goals_report 打戳）
            "startedAt": now if not steps else None,
        })

    morning_on = bool(payload.get("morningEnabled", True))
    evening_on = bool(payload.get("eveningEnabled", True))
    if not morning_on and not evening_on:
        morning_on = True        # 两段全关 = 建了目标永远不响；至少留早间一段
    mh = min(max(int(payload.get("morningHour", 9)), 0), 23)
    eh = min(max(int(payload.get("eveningHour", 21)), 0), 23)

    gid = str(payload.get("id") or uuid.uuid4().hex)
    stub = {"title": title, "steps": steps}
    job_ids, errs = [], []

    # 汇报投递口径：走 weixin —— 用户要的是「每天微信收到推进/复盘」。
    # 若他的 weixin 未接，hermes 会记失败，不会影响 App 卡片。
    if morning_on:
        try:
            job_ids.append(_job_id_of(_job_create(
                "目标·早推进·%s" % title[:50], "%d 9 * * *" % mh,
                _goal_morning_prompt(stub), deliver="weixin")))
        except Exception as e:
            errs.append("morning: %s" % e)
    if evening_on:
        try:
            job_ids.append(_job_id_of(_job_create(
                "目标·晚复盘·%s" % title[:50], "%d 21 * * *" % eh,
                _goal_evening_prompt(stub), deliver="weixin")))
        except Exception as e:
            errs.append("evening: %s" % e)

    job_ids = [j for j in job_ids if j]
    goal = {
        "id": gid, "title": title[:200], "steps": steps,
        "cronJobID": job_ids[0] if job_ids else "",
        "cronJobIDs": job_ids,
        "morningEnabled": morning_on, "eveningEnabled": evening_on,
        "morningHour": mh, "eveningHour": eh,
        "createdAt": now, "updatedAt": now,
        "lastReport": "", "lastPushedAt": None, "paused": False,
        # v4.0.40（#3）：建目标时那条会话 —— 后台推进遇到「需要你确认」推回这里，
        # 而不是落到 App 当前打开的会话（否则用户停在别的会话就永远看不到）
        "originSessionId": str(payload.get("sessionId") or payload.get("originSessionId") or "").strip(),
        # v4.0.20（#6）：后台推进留痕（倒序时间线），由 goals_report / Agent 追加。
        # lastReport 是覆盖写 —— 历史留不下，用户「不知道后台到底跑过几次」。
        "reports": [],
    }
    with _goals_lock:
        goals = _goals_read()
        goals[gid] = goal
        _goals_write(goals)

    # 建 job 失败**不回滚目标**：用户至少还能在卡片里看到这条，之后可重试。
    # 但要如实告诉调用方，否则 App 会显示「已开启每日推进」而其实没 job。
    if not job_ids:
        return 200, {"ok": False, "error": "cron job 创建失败：%s" % ("; ".join(errs) or "未知"),
                     "goal": goal}
    return 200, {"ok": True, "goal": goal, "warnings": errs}


def goals_report(payload):
    """cron 跑完后回写推进汇报。返回 (code, body)。"""
    gid = str(payload.get("goalId") or "")
    report = str(payload.get("report") or "").strip()
    if not gid or not report:
        return 400, {"ok": False, "error": "缺少 goalId / report"}
    warn = []       # v4.0.44 审查修复：锁外联动的失败如实上报（见函数尾，别静默吞）
    with _goals_lock:
        goals = _goals_read()
        g = goals.get(gid)
        if not g:
            return 404, {"ok": False, "error": "目标不存在"}
        now = datetime.now().isoformat()
        # v4.0.37（OpenMuse 借鉴②）：同一份汇报正文重复回写不再往时间线塞重复条目
        # （cron 重试、早晚两段跑出同样内容都会走到这里），但"上次汇报时间"照常刷新。
        # 用整串 md5 比对：lastReport 截断到 2000 字，直接比正文对长汇报会漏判。
        _sig = hashlib.md5(report.encode("utf-8")).hexdigest()
        _dup = (g.get("lastReportSig") == _sig)
        g["lastReportSig"] = _sig
        g["lastReport"] = report[:2000]
        g["lastPushedAt"] = now
        g["updatedAt"] = now
        # v4.0.20（#6）：推进留痕滚动保留最近 50 条（App 详情页倒序渲染时间线）
        reps = g.setdefault("reports", [])
        if not _dup:
            reps.insert(0, {"at": now, "text": report[:1000], "kind": "report",
                            "src": str(payload.get("source") or "manual")})
            del reps[50:]
        # 只认后端明确传来的 doneStepIds / doneSteps(1 起序号) —— 不从汇报正文里猜哪步做完了
        steps_l = g.get("steps") or []
        want_ids = set(str(x) for x in (payload.get("doneStepIds") or []))
        for _n in (payload.get("doneSteps") or [])[:MAX_STEPS]:
            try:
                _i = int(_n) - 1
            except Exception:
                continue
            if 0 <= _i < len(steps_l):
                want_ids.add(str(steps_l[_i].get("id") or ""))
        newly = []
        for _i, s in enumerate(steps_l):
            if str(s.get("id") or "") in want_ids and not s.get("done"):
                s["done"] = True
                s["doneAt"] = now
                # v4.0.40（#5）：被勾上的步骤若还没开始时间，用「本次推进时刻」兜底打戳
                s.setdefault("startedAt", s.get("startedAt") or now)
                newly.append((_i + 1, str(s.get("title") or "")))
        # v4.0.40（#5）：当前待推的那一步第一次被后台列为「今天推这一步」时打开始时间。
        # 判据用 startedAt 缺失（幂等）—— 已打过的不覆盖，用户看到的时间才是真实起点。
        for s in steps_l:
            if not s.get("done") and not s.get("startedAt"):
                s["startedAt"] = now
                break
        # v4.0.40（#4）：全部步骤完成 → 记 finishedAt（App 据此折叠 + 停 cron）
        finished = _apply_finish(g, now)
        _goals_write(goals)
    # v4.0.44（用户第②条要求）：每完成一步 → 划掉该步待办 + 推一条 system 进「轻聊投递」。
    # 放在锁外：_sync_todos_* 自己要拿 _goals_lock（非重入锁），锁里调必自死锁。
    if newly:
        # v4.0.44 审查修复：联动结果**必须进返回体**。此前两个返回值被丢弃、异常只 print 后返回 0/False，
        # 于是「待办没划 / 通知没发」也返回 ok=True → 桥推进游标 → **永不重试**，
        # 表现为「报告回写成功」其实什么都没做（要求④⑤静默失效）。这里如实报 warn。
        if _sync_todos_steps(gid, [t for _, t in newly]) == 0:
            warn.append("第%s步的待办没划掉（可能本来就没有该步待办）" % ",".join(str(i) for i, _ in newly))
        for _idx, _title in newly:
            if not _notify_step_done(gid, _idx, _title, total=len(steps_l)):
                warn.append("第%d步完成通知没发出" % _idx)
    if finished:
        _sync_todos_finish(gid)
    if warn:
        print("[goals] 回写告警 goal=%s: %s" % (gid[:8], "；".join(warn)), flush=True)
    return 200, {"ok": True, "finished": bool(finished),
                 "doneSteps": [i for i, _ in newly], "warn": warn}


def _apply_finish(g, now):
    """全部步骤完成时标 finishedAt；返回 True 表示**这次**才刚完成（用于触发待办联动）。

    幂等：已 finished 的目标重复回写不再联动（否则每次汇报都去改 todos.json）。
    """
    steps = g.get("steps") or []
    if not steps or not all(s.get("done") for s in steps):
        return False
    if g.get("finishedAt"):
        return False
    g["finishedAt"] = now
    g["stepsFinished"] = True
    return True


def goals_update(payload):
    """改目标（暂停/恢复、改时间、改标题）。暂停要同步 disable job。"""
    gid = str(payload.get("id") or "")
    if not gid:
        return 400, {"ok": False, "error": "缺少 id"}
    now = datetime.now().isoformat()
    with _goals_lock:
        goals = _goals_read()
        g = goals.get(gid)
        if not g:
            return 404, {"ok": False, "error": "目标不存在"}
        if "paused" in payload:
            g["paused"] = bool(payload["paused"])
        for k in ("title",):
            if payload.get(k):
                g[k] = str(payload[k])[:200]
        for k in ("morningEnabled", "eveningEnabled"):
            if k in payload:
                g[k] = bool(payload[k])
        for k in ("morningHour", "eveningHour"):
            if k in payload:
                g[k] = min(max(int(payload[k]), 0), 23)
        if "doneStepIds" in payload:
            want = set(payload.get("doneStepIds") or [])
            for s in g.get("steps", []):
                if s.get("id") in want and not s.get("done"):
                    s["done"] = True
                    s["doneAt"] = now
                elif s.get("id") not in want and s.get("done"):
                    s["done"] = False
                    s["doneAt"] = None
        finished = _apply_finish(g, now)
        if not finished:
            # 用户手动取消勾选 → 完成态要收回，否则 App 会一直折叠着它
            g["finishedAt"] = None
            g["stepsFinished"] = False
        g["updatedAt"] = now
        _goals_write(goals)
    if finished:
        _sync_todos_finish(gid)
    return 200, {"ok": True, "goal": g, "finished": bool(finished)}


def goals_delete(goal_id):
    """删目标 + 连 cron job 一起删（否则明天还会推一个已删目标）。"""
    goal_id = str(goal_id or "")
    if not goal_id:
        return 400, {"ok": False, "error": "缺少 id"}
    with _goals_lock:
        goals = _goals_read()
        g = goals.pop(goal_id, None)
        if g:
            _goals_write(goals)
    if g:
        for jid in (g.get("cronJobIDs") or ([g["cronJobID"]] if g.get("cronJobID") else [])):
            _job_delete(jid)
    return 200, {"ok": True, "deleted": bool(g)}


def goals_list():
    with _goals_lock:
        return sorted(_goals_read().values(),
                      key=lambda g: g.get("updatedAt", ""), reverse=True)


# ══════════════════════════════════════════════════════════════
# v4.0.40 · 手动推进（App 卡片「现在开始推进」胶囊）+ 会话归属 + 待办联动
#
# ① 「现在开始推进」：POST /api/life/goal/push_now {id}
#    复用早推进提示词在后台跑一次 → 解析 ##GOAL_REPORT## → 回写卡片 → 推送
#    同步在 /api/tasks/bg 登记一条作业 → 任务中心实时可见（用户第②条要求）
# ② 「需要你确认」：汇报里「需要你做」非空 → 除轻聊投递外再推回建目标时的原会话
#    （复用 inbox_api.push 的 session_id 字段，App 侧 v4.0.21 已有归属渲染）
# ③ 全部步骤完成：把 todos.json 里属于本目标的待办全部划掉（用户第④条要求）
# ④ 每个步骤的开始时间：后台第一次把它列为「今天推这一步」时打戳（用户第⑤条要求）
# ══════════════════════════════════════════════════════════════


def _agent_chat(prompt, timeout=120):
    """容器内直调 Hermes 跑一次 agent（绕开 9127 的 token 门）。

    走 /v1/chat/completions 的非流式形态：只要 final content，不做流式拆段。
    ⚠️ 上游超时会抛，调用方必须自己兜住（push_now 里按 error 收尾，不留僵尸 running）。
    """
    body = {"model": os.environ.get("QL_GOAL_PUSH_MODEL") or "default",
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "model_options": {"reasoning": {"enabled": False}}}
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        HERMES_API + "/v1/chat/completions", data=data,
        headers={"Authorization": "Bearer %s" % HERMES_KEY,
                 "Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = json.loads(resp.read().decode("utf-8", "replace"))
    # 上游不同版本的正文出口不一样，逐个认（chat/completions 只有 choices）
    try:
        return (raw.get("choices") or [{}])[0].get("message", {}).get("content") or ""
    except Exception:
        return ""


_REPORT_RE = re.compile(r"##GOAL_REPORT##(.*?)##END##", re.S)
_PROGRESS_RE = re.compile(r"【目标推进\s*(\d+)\s*/\s*(\d+)\s*】")


def _parse_push_output(text):
    """从模型输出里抠出 (正文, 回写段)。回写段缺失 → 正文原样、report 空（不编造）。"""
    m = _REPORT_RE.search(text or "")
    if not m:
        return (text or "").strip(), ""
    body = (text[:m.start()] + text[m.end():]).strip()
    lines = [ln.strip() for ln in m.group(1).strip().splitlines() if ln.strip()]
    report = "\n".join(lines).strip()
    return body, report


def _report_needs_user(report):
    """汇报里「需要你做」是否真的要用户回应。

    口径：空 / 无 / 无需 / 不用 / 不需要 / - / 0 等一律视为「不需要」，不算待确认 ——
    否则每天早上都弹一条「请确认」，用户会直接把它当噪声（宁可漏推不滥推）。
    """
    if not report:
        return ""
    for ln in report.splitlines():
        if not ln.startswith("需要你做"):
            continue
        val = ln.split("：", 1)[-1].split(":", 1)[-1].strip()
        val = val.strip("。．.！!，,、 ")
        if not val or val in ("无", "无需", "不用", "不需要", "无。", "-", "—", "0", "无额外"):
            return ""
        # v4.0.40：白名单是逐字枚举，「无需额外操作」「没有要你做的事」这类自然说法会漏网
        # 被当成真需要确认 → 每天早上都弹一条「请确认」，用户直接当噪声。
        # 口径改为：否定词开头（无/没/不用/不需要/不必）或整句含否定收尾，一律视为「不需要」。
        if re.match(r"^(无|没|不用|不需要|不必|不必再|暂时不)", val):
            return ""
        if re.search(r"(无需额外|没有需要|没有要你|不用回复|无需回复|不需要回复|无额外)", val):
            return ""
        return val
    return ""


def _push_to_origin(goal, text, task_type="agent"):
    """把需要用户回应的一条推回建目标时的原会话。

    老目标没有 originSessionId → 回落推「轻聊主动」固定会话（删不掉、可直接回复）。
    ⚠️ 只推需要确认的那条：普通推进正文仍走轻聊投递/微信，不重复刷屏。
    """
    if not text:
        return False
    sid = str(goal.get("originSessionId") or "").strip()
    fallback = not sid
    if fallback:
        sid = "qingliao_proactive"
    try:
        import inbox_api
        ok, _ = inbox_api.push(text, task_id="goal-%s-%d" % (goal.get("id", "")[:8], int(time.time())),
                               task_type=task_type, session_id=sid)
        print("[goals] 推原会话 goal=%s sid=%s%s ok=%s"
              % (goal.get("id", "")[:8], sid, "(回落主动会话)" if fallback else "", ok), flush=True)
        return bool(ok)
    except Exception as e:
        print("[goals] 推原会话失败 goal=%s: %s" % (goal.get("id", "")[:8], e), flush=True)
        return False


def _bg_register(job_id, title, detail=""):
    """登记后台作业到任务中心。拿不到 stream_api 就静默跳过（任务中心只是可见性，不该拖垮推进）。"""
    try:
        import stream_api
        return stream_api.bg_register(title, job_id=job_id, detail=detail)
    except Exception as e:
        print("[goals] bg_register 失败（忽略）: %s" % e, flush=True)
        return ""


def _bg_update(job_id, status=None, detail=None, result=None):
    try:
        import stream_api
        return stream_api.bg_update(job_id, status=status, detail=detail, result=result)
    except Exception:
        return False


def _run_push_now(gid):
    """后台线程：跑一次早推进 → 回写 goals.json → 推送。返回 (ok, 结果摘要)。"""
    with _goals_lock:
        g = _goals_read().get(gid)
        if not g:
            return False, "目标不存在"
    job_id = "goal-%s" % gid[:16]
    # v4.0.44（用户第④条）：进行中作业的标题/详情写清「正在推进第几步」，
    # 任务中心「⏳ 进行中」一眼能看出后台在干哪一步。
    step_txt = _current_step_text(g)
    _bg_register(job_id, "目标推进 · %s" % (g.get("title", "")[:36]), "正在推进 %s" % step_txt)
    started = time.time()
    try:
        text = _agent_chat(_goal_morning_prompt(g), timeout=PUSH_NOW_TIMEOUT)
    except Exception as e:
        msg = "推进失败：%s" % str(e)[:150]
        _bg_update(job_id, status="error", detail="失败", result=msg)
        return False, msg
    body, report = _parse_push_output(text)
    if not body and not report:
        msg = "推进失败：模型没有返回可解析内容"
        _bg_update(job_id, status="error", detail="失败", result=msg)
        return False, msg

    # 🚨 不要在 _goals_lock 里调 goals_report —— 它自己也拿同一把**非重入**锁，会自死锁。
    # goals_report 自己读-改-写 goals.json，所以额外字段必须在它之后再单独加锁写。
    # v4.0.44：把「完成步骤」一并回传 → 卡片勾选 + 单步待办联动 + 每步完成通知都在函数里闭环。
    goals_report({"goalId": gid, "report": report or body[:2000],
                  "doneSteps": _parse_done_steps(report), "source": "push_now"})
    now = datetime.now().isoformat()
    m = _PROGRESS_RE.search(text or "")
    with _goals_lock:
        goals = _goals_read()
        cur = goals.get(gid)
        if not cur:
            _goals_write(goals)
            msg = "推进中途目标被删除，结果丢弃"
            _bg_update(job_id, status="error", detail="已删除", result=msg)
            return False, msg
        cur["manualPushAt"] = now
        cur["updatedAt"] = now
        if m:
            cur["lastProgressText"] = "【目标推进 %s/%s】" % (m.group(1), m.group(2))
        _goals_write(goals)
        finished = bool(cur.get("finishedAt"))

    need = _report_needs_user(report)
    summary = body.strip()
    if need:
        summary = (summary + "\n\n需要你做：%s" % need).strip()
    # ① v4.0.44（用户第③条）：需要用户拍板 → 推**可点选/可手输的 question 卡**（原先是纯文本推原会话）
    if need:
        _ask_user(g, need, options=_parse_options(report))
    else:
        _push_to_origin(g, summary)
    if finished:
        # 目标在这次推进中被勾完全部步骤 → 明确告诉用户「已完成、待办已划掉」
        _push_to_origin(g, "✅ 目标「%s」全部步骤已完成，相关待办已自动划掉。"
                          % g.get("title", ""), task_type="agent")
    _bg_update(job_id, status="done", detail="%s 已完成推进" % step_txt, result=summary[:300])
    print("[goals] push_now 完成 goal=%s 用时=%.1fs 需要用户=%s"
          % (gid[:8], time.time() - started, bool(need)), flush=True)
    return True, summary


def goals_push_now(payload):
    """POST /api/life/goal/push_now —— App 卡片「现在开始推进」。

    立即返回（后台线程跑），任务中心随后可见进度。返回 (code, body)。
    """
    gid = str(payload.get("id") or "").strip()
    if not gid:
        return 400, {"ok": False, "error": "缺少 id"}
    with _goals_lock:
        g = _goals_read().get(gid)
    if not g:
        return 404, {"ok": False, "error": "目标不存在"}
    if (g.get("steps") or []) and all(s.get("done") for s in g["steps"]):
        return 200, {"ok": True, "skipped": True, "reason": "目标已完成，无需推进"}
    t = threading.Thread(target=_run_push_now, args=(gid,), daemon=True)
    t.start()
    return 200, {"ok": True, "started": True, "jobId": "goal-%s" % gid[:16]}


def _todos_read():
    if not os.path.exists(TODOS_PATH):
        return []
    try:
        with open(TODOS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _todos_write(items):
    d = os.path.dirname(TODOS_PATH)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = TODOS_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False)
    os.replace(tmp, TODOS_PATH)


def _sync_todos_finish(gid):
    """目标全部步骤完成 → 把待办里属于它的条目全部划掉（用户第④条要求）。

    匹配口径与 iOS 端 GoalTodoBridge 完全一致：标题前缀 `［目标·<目标标题>］`。
    🚨 整个函数 try/except：写用户数据失败绝不能让 goals.json 的回写跟着炸。
    🚨 只置 done/doneAt，不删条目 —— 用户要能看到「它曾经是个待办、现在做完了」。
    """
    try:
        with _goals_lock:
            g = _goals_read().get(gid)
        if not g:
            return 0
        marker = "［目标·%s］" % g.get("title", "")
        now = datetime.now().isoformat()
        n = 0
        with _todos_lock:
            items = _todos_read()
            changed = False
            for t in items:
                if not isinstance(t, dict):
                    continue
                if str(t.get("content") or "").startswith(marker) and not t.get("done"):
                    t["done"] = True
                    t["doneAt"] = now
                    t["updatedAt"] = now
                    n += 1
                    changed = True
            if changed:
                _todos_write(items)
        print("[goals] 目标完成联动待办 goal=%s 划掉 %d 条" % (gid[:8], n), flush=True)
        return n
    except Exception as e:
        print("[goals] 待办联动失败 goal=%s: %s" % (gid[:8], e), flush=True)
        return 0


# ══════════════════════════════════════════════════════════════
# v4.0.44 · 后台推进闭环（用户 7 条要求里的 3/4/5/6）
#
# ① cron 跑完的回写：Hermes 侧桥（ql_task_push.py）带 cronJobID 调 goals_report_from_cron
#    → 卡片进度/步骤勾选/下一步开始时间自动更新（此前 /api/life/goal/report 没人调，时间线永远空）
# ② 每完成一步：推一条 system 进「轻聊投递」+ 只划掉该步的待办（不是等全部完成才划）
# ③ 「需要你做」：推可点选/可手输的 question 卡 → 答案回写目标时间线 + 推投递 + 注入原会话
# ④ 「现在开始推进」的进行中作业标题/详情带「第 k/N 步：xxx」
# ══════════════════════════════════════════════════════════════

_RE_DONE_STEPS = re.compile(r"完成步骤\s*[:：]\s*(.+)")
_RE_OPTIONS = re.compile(r"选项\s*[:：]\s*(.+)")
_OPT_NONE = ("", "无", "无。", "-", "—", "0", "none", "无选项")


def _parse_done_steps(report):
    """从回写段里取「完成步骤：2」→ [2]（1 起序号）。缺失/写「无」→ []。

    只解析机器可读那一行；正文里出现「完成了第 2 步」这类自然语言一律不算（不猜）。
    """
    m = _RE_DONE_STEPS.search(report or "")
    if not m:
        return []
    out = []
    for x in re.findall(r"\d+", m.group(1)):
        try:
            n = int(x)
        except Exception:
            continue
        if 1 <= n <= MAX_STEPS and n not in out:
            out.append(n)
    return out


def _parse_options(report):
    """回写段里的「选项：A | B」→ ['A','B']（没有 → []，App 只显示输入框）。"""
    m = _RE_OPTIONS.search(report or "")
    if not m:
        return []
    raw = m.group(1).strip().strip("。. ")
    if raw.lower() in _OPT_NONE:
        return []
    return [o.strip() for o in raw.split("|") if o.strip()][:4]


def _current_step_text(goal):
    """当前待推进那一步的文案：`第 k/N 步：<标题>`（全完成/无步骤 → 收尾）。"""
    steps = goal.get("steps") or []
    if not steps:
        return "推进中"
    done_n = sum(1 for s in steps if s.get("done"))
    nxt = next((s for s in steps if not s.get("done")), None)
    if not nxt:
        return "全部 %d 步已完成" % len(steps)
    return "第 %d/%d 步：%s" % (done_n + 1, len(steps), str(nxt.get("title") or "")[:30])


def _notify_step_done(gid, idx, title, total=0):
    """v4.0.44（用户第②条）：每步完成推一条 system 进固定会话「轻聊投递」+ App 通知。

    task_id 用 `goalstep-<目标id8>-<序号>`：同一步重复完成时 App 侧去重，不会刷两条。
    """
    with _goals_lock:
        g = _goals_read().get(gid)
    gtitle = (g or {}).get("title", "") or ""
    txt = "✅ 第 %d/%d 步已完成：%s\n目标：%s" % (idx, total or idx, title or "（无标题）", gtitle)
    try:
        import inbox_api
        inbox_api.push(txt, task_id="goalstep-%s-%d" % (gid[:8], idx), task_type="system")
        print("[goals] 步骤完成通知 goal=%s 第%d步" % (gid[:8], idx), flush=True)
        return True
    except Exception as e:
        print("[goals] 步骤完成通知失败 goal=%s: %s" % (gid[:8], e), flush=True)
        return False


def _sync_todos_steps(gid, titles):
    """单步完成 → 只划掉该步的待办（v4.0.44，用户第④条）。

    匹配口径与 iOS 端 GoalTodoBridge.syncStepDone 完全一致：
    前缀 `［目标·<目标标题>］` + 内容里含该步骤标题。
    🚨 只置 done/doneAt，不删条目；整体 try/except，写待办失败绝不拖垮 goals.json 的回写。
    """
    titles = [str(t) for t in (titles or []) if t]
    if not titles:
        return 0
    try:
        with _goals_lock:
            g = _goals_read().get(gid)
        if not g:
            return 0
        marker = "［目标·%s］" % g.get("title", "")
        now = datetime.now().isoformat()
        n = 0
        with _todos_lock:
            items = _todos_read()
            changed = False
            for t in items:
                if not isinstance(t, dict) or t.get("done"):
                    continue
                c = str(t.get("content") or "")
                if not c.startswith(marker):
                    continue
                if any(ti in c for ti in titles):
                    t["done"] = True
                    t["doneAt"] = now
                    t["updatedAt"] = now
                    n += 1
                    changed = True
            if changed:
                _todos_write(items)
        if n:
            print("[goals] 单步完成联动待办 goal=%s 划掉 %d 条" % (gid[:8], n), flush=True)
        return n
    except Exception as e:
        print("[goals] 单步待办联动失败 goal=%s: %s" % (gid[:8], e), flush=True)
        return 0


def _inject_to_session(sid, text):
    """把一条消息追加进**已存在**的会话（用于把用户对问题卡的回答送回原会话）。

    🚨 append_fixed_message 会顺手改会话标题 → 必须先把现有标题读回来再传，否则把用户的
    聊天会话改名成占位标题；会话不存在就**不凭空造**（避免写出一条空壳会话）。
    """
    sid = str(sid or "").strip()
    if not sid or not text:
        return False
    try:
        import sessions_api
    except Exception:
        return False
    try:
        title = ""
        for s in sessions_api.load_sessions():
            if isinstance(s, dict) and s.get("id") == sid:
                title = str(s.get("title") or "")
                break
        if not title:
            print("[goals] 注入跳过：原会话不存在 sid=%s" % sid[:16], flush=True)
            return False
        ok = sessions_api.append_fixed_message(sid, title, text, task_type="system")
        print("[goals] 回答注入原会话 sid=%s ok=%s" % (sid[:16], ok), flush=True)
        return bool(ok)
    except Exception as e:
        print("[goals] 注入原会话失败 sid=%s: %s" % (sid[:16], e), flush=True)
        return False


def _ask_user(goal, need, options=None):
    """「需要你做」→ 可点选/可手输的 question 卡（v4.0.44，用户第③条要求）。

    · 落点：卡带 session_id = 建目标时的原会话 → App 里**同时**能在该会话气泡与任务中心作答
      （App v4.0.21 起支持 session_id 归属；老目标没有 originSessionId 时回落「轻聊主动」）。
    · 作答后由 _watch_answer 取回答案 → 回写目标时间线 + 推投递 + 注入原会话，
      后台推进不会「一直等不到回复」。
    """
    opts = [str(o) for o in (options or []) if str(o).strip()] or list(DEFAULT_ASK_OPTIONS)
    text = "⏳ 目标「%s」的推进需要你确认：\n%s" % (goal.get("title", ""), need)
    text += "\n选项：\n" + "\n".join("%d. %s" % (i, o) for i, o in enumerate(opts, 1))
    sid = str(goal.get("originSessionId") or "").strip() or "qingliao_proactive"
    mid = ""
    # v4.0.44 审查修复：task_id 必须**稳定**。原先带秒级时间戳（goalask-<gid>-<ts>），
    # 重推/重试时 id 每变一次，App 端按 source_task_id 去重就失效 → 重复出卡。
    # 身份 = 「目标 + 当前未完成步的序号」，同一步重推 id 不变。
    _steps = goal.get("steps") or []
    _cur = next((i for i, st in enumerate(_steps) if not st.get("done")), -1)
    qid = "goalask-%s-%d" % (goal.get("id", "")[:8], _cur + 1)
    try:
        import inbox_api
        ok, mid = inbox_api.push(text, task_id=qid,
                                 task_type="question", want_id=True, session_id=sid)
        if not ok:
            mid = ""
    except Exception as e:
        print("[goals] 问题卡推送失败 goal=%s: %s" % (goal.get("id", "")[:8], e), flush=True)
    print("[goals] 需要确认 → 问题卡 goal=%s sid=%s id=%s" % (goal.get("id", "")[:8], sid[:16], mid or "(失败)"),
          flush=True)
    if mid:
        # v4.0.44 审查修复：等答案的 daemon 线程加**并发上限**（原先每张卡起一个、最多空转 6h，
        # 叠加「卡答完不收尾」缺陷会长期堆积）。到上限就不再起线程——卡照常可作答、答案照常进队列，
        # 只是不再阻塞式等待（_watch_answer 的职责只是「顺手回写 + 注入原会话」，不是唯一路径）。
        if len(_WATCHERS) >= _WATCH_MAX:
            print("[goals] 问题卡等待线程已达上限 %d，本次不等待 id=%s" % (_WATCH_MAX, mid), flush=True)
        else:
            _WATCHERS.add(mid)
            threading.Thread(target=_watch_answer, args=(mid, goal.get("id", ""), sid), daemon=True).start()
    return mid


def _watch_answer(mid, gid, sid):
    """后台线程：等用户在 App 作答（最多 QUESTION_TIMEOUT）→ 落地答案 → **收尾这张卡**。"""
    try:
        import inbox_api
    except Exception:
        return
    try:
        deadline = time.time() + QUESTION_TIMEOUT
        while time.time() < deadline:
            try:
                found, ans = inbox_api.read_answer(mid)
            except Exception:
                found, ans = True, None
            if not found:
                print("[goals] 问题卡已收尾/被清 id=%s" % mid, flush=True)
                return
            if ans:
                if mid in _ANSWER_DONE:
                    return
                _ANSWER_DONE.add(mid)
                _apply_answer(gid, sid, ans)
                # 🚨 v4.0.44 审查修复：答完必须**立刻收尾**这张 question 卡。
                # 不收尾 → inbox_api 的 sending 超时重置（SENDING_TIMEOUT=60s）会把它重投回 pending，
                # 用户每 60 秒收到同一张已答卡，直到 24h STALE_TTL 才消散（生产已实证）。
                try:
                    inbox_api.mark_done(mid)
                    print("[goals] 问题卡已收尾 id=%s" % mid, flush=True)
                except Exception as e:
                    print("[goals] 问题卡收尾失败 id=%s: %s" % (mid, e), flush=True)
                return
            time.sleep(5)
        print("[goals] 问题卡超时未作答 id=%s（不阻塞，推进照常）" % mid, flush=True)
    finally:
        _WATCHERS.discard(mid)      # v4.0.44：腾出并发名额（上限见 _WATCH_MAX）


def _apply_answer(gid, sid, ans):
    """答案落地三件事：目标时间线留痕 / 推投递让用户看到闭环 / 注入原会话。

    命中「继续推进」类回答 → 立刻再推一次（用户第④条：等不及就现在开始）。
    """
    ans = str(ans or "").strip()[:400]
    if not ans:
        return
    now = datetime.now().isoformat()
    title = ""
    try:
        with _goals_lock:
            goals = _goals_read()
            g = goals.get(gid)
            if g:
                title = g.get("title", "") or ""
                reps = g.setdefault("reports", [])
                reps.insert(0, {"at": now, "text": "【你的回复】%s" % ans[:300],
                                "kind": "answer", "src": "ask"})
                del reps[50:]
                g["updatedAt"] = now
                _goals_write(goals)
    except Exception as e:
        print("[goals] 答案回写失败 goal=%s: %s" % (gid[:8], e), flush=True)
    line = "💬 你对目标「%s」的回复已收到：%s" % (title or gid[:8], ans[:200])
    try:
        import inbox_api
        inbox_api.push(line, task_id="goalans-%s-%d" % (gid[:8], int(time.time())), task_type="system")
    except Exception as e:
        print("[goals] 答案推投递失败: %s" % e, flush=True)
    if sid:
        _inject_to_session(sid, line)
    if any(k in ans for k in ("继续推进", "立即推进", "现在推进", "开始推进")):
        print("[goals] 用户点「继续推进」→ 立刻再推一次 goal=%s" % gid[:8], flush=True)
        threading.Thread(target=_run_push_now, args=(gid,), daemon=True).start()


def goals_report_from_cron(job_id, report, done_steps=None):
    """Hermes 侧 cron 桥的回写入口：按 **cronJobID 精确匹配**目标（不靠标题猜）。

    返回 (code, body)，与 goals_report 同形（桥据 body.ok / code 决定是否推进游标重试）。
    """
    job_id = str(job_id or "").strip()
    report = str(report or "").strip()
    if not job_id or not report:
        return 400, {"ok": False, "error": "缺少 jobId / report"}
    with _goals_lock:
        goals = _goals_read()
        hits = []
        for g in goals.values():
            ids = g.get("cronJobIDs") or ([g.get("cronJobID")] if g.get("cronJobID") else [])
            if job_id in [str(x) for x in ids]:
                hits.append(g)
    if not hits:
        # 不是目标 job（或目标已删）→ **跳过但不算失败**：桥据此正常推进游标，不会无限重试。
        print("[goals] cron 回写：job=%s 没有对应目标（跳过）" % job_id, flush=True)
        return 200, {"ok": True, "skipped": True, "reason": "没有目标挂在这个 cron job 上"}
    # 桥带了 doneSteps 就用桥的；没带 → 从回写段的「完成步骤：N」行兜底解析（双保险）
    done = [n for n in (done_steps or [])] or _parse_done_steps(report)
    # v4.0.44 审查修复：一个 job 挂多个目标时必须**全部回写**（此前命中即 break，
    # 只更新第一个 → 其余目标静默不更新 = 回写看着成功其实丢了一半）。
    code, body = 200, {"ok": True}
    for _h in hits:
        code, body = goals_report({"goalId": _h.get("id"), "report": report,
                                   "doneSteps": done, "source": "cron"})
    if len(hits) > 1:
        print("[goals] cron 回写：job=%s 命中 %d 个目标，已全部回写" % (job_id, len(hits)), flush=True)
        body["updated"] = len(hits)
    return code, body



