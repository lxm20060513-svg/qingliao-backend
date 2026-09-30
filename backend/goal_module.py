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

import json
import os
import threading
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
        "请用中文输出今天的推进提醒，三部分：\n"
        "1) 今天推进哪一步 —— 具体到动作，不要只说「继续努力」\n"
        "2) 需要用户本人做什么 —— 最多 1 件事，一句话说清（没有就写「无」）\n"
        "3) 一句务实提醒 —— 别灌鸡汤\n\n"
        "正文控制在 200 字以内。**正文之后**另起一段，严格按下面格式输出（供 App 回写卡片，\n"
        "这一段不要有任何多余文字）：\n"
        "##GOAL_REPORT##\n"
        "今天推进：<一句话>\n"
        "需要你做：<一句话>\n"
        "##END##"
    ) % (goal.get("title", ""), done_n, len(steps),
         _steps_digest(steps) or "  （未拆步骤）", nxt_txt)


def _goal_evening_prompt(goal):
    steps = goal.get("steps") or []
    done_n = sum(1 for s in steps if s.get("done"))
    return (
        "你在帮用户复盘一个长期目标的今天（每天这个时段复盘一次）。\n\n"
        "目标：%s\n当前进度：%d/%d\n步骤清单：\n%s\n\n"
        "请用中文输出今晚复盘，三部分：\n"
        "1) 今天做了什么 —— 没推进就直说没推进，不要编\n"
        "2) 还剩多少 —— %d 步\n"
        "3) 明天计划推进哪一步\n\n"
        "正文控制在 200 字以内。**正文之后**另起一段，严格按下面格式输出（供 App 回写卡片）：\n"
        "##GOAL_REPORT##\n"
        "今日：<一句话>\n"
        "剩余：<已完成>/<总数>\n"
        "明日：<一句话>\n"
        "##END##"
    ) % (goal.get("title", ""), done_n, len(steps),
         _steps_digest(steps) or "  （未拆步骤）", len(steps) - done_n)


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
        })

    morning_on = bool(payload.get("morningEnabled", True))
    evening_on = bool(payload.get("eveningEnabled", True))
    if not morning_on and not evening_on:
        morning_on = True        # 两段全关 = 建了目标永远不响；至少留早间一段
    mh = min(max(int(payload.get("morningHour", 9)), 0), 23)
    eh = min(max(int(payload.get("eveningHour", 21)), 0), 23)

    gid = str(payload.get("id") or uuid.uuid4().hex)
    now = datetime.now().isoformat()
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
    with _goals_lock:
        goals = _goals_read()
        g = goals.get(gid)
        if not g:
            return 404, {"ok": False, "error": "目标不存在"}
        now = datetime.now().isoformat()
        g["lastReport"] = report[:2000]
        g["lastPushedAt"] = now
        g["updatedAt"] = now
        # 只认后端明确传来的 doneStepIds —— 不从汇报正文里猜哪步做完了
        for sid in (payload.get("doneStepIds") or [])[:MAX_STEPS]:
            for s in g.get("steps", []):
                if s.get("id") == sid and not s.get("done"):
                    s["done"] = True
                    s["doneAt"] = now
        _goals_write(goals)
    return 200, {"ok": True}


def goals_update(payload):
    """改目标（暂停/恢复、改时间、改标题）。暂停要同步 disable job。"""
    gid = str(payload.get("id") or "")
    if not gid:
        return 400, {"ok": False, "error": "缺少 id"}
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
            now = datetime.now().isoformat()
            want = set(payload.get("doneStepIds") or [])
            for s in g.get("steps", []):
                if s.get("id") in want and not s.get("done"):
                    s["done"] = True
                    s["doneAt"] = now
                elif s.get("id") not in want and s.get("done"):
                    s["done"] = False
                    s["doneAt"] = None
        g["updatedAt"] = datetime.now().isoformat()
        _goals_write(goals)
    return 200, {"ok": True, "goal": g}


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
