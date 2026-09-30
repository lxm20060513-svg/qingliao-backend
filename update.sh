#!/usr/bin/env bash
# 轻聊后端一键更新脚本
#
#   ./update.sh          更新到最新并重启
#   ./update.sh --check  只检查有没有新版，不改任何东西
#   ./update.sh --version <tag>   更新到指定 tag（如 v4.0.13 配套后端）
#
# 设计原则：**绝不碰用户数据**
#   - .env  不动（访问密码 / token 全部在里面，重生成=用户登不上）
#   - data/ 不动，且升级前自动打包备份到 backups/
#   - 只替换 backend/ 代码，然后重建容器
set -euo pipefail
cd "$(dirname "$0")"

MODE="update"
TARGET=""
for a in "$@"; do
  case "$a" in
    --check)  MODE="check" ;;
    --version) MODE="tag" ;;
    -h|--help)
      sed -n '3,11p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *)
      if [ "$MODE" = "tag" ]; then
        TARGET="$a"
      else
        echo "❌ 未知参数：$a（用 --help 看用法）"
        exit 1
      fi
      ;;
  esac
done

say()  { echo "$@"; }
ok()   { echo "✅ $*"; }
warn() { echo "⚠️  $*"; }
die()  { echo "❌ $*"; exit 1; }

command -v git >/dev/null 2>&1 || die "未安装 git"
git rev-parse --git-dir >/dev/null 2>&1 || die "当前目录不是 git 仓库，请从 clone 下来的目录运行本脚本"
[ -f .env ] || die "缺少 .env —— 请先跑一次 ./install.sh 完成初始化"

# ── 当前版本 ───────────────────────────────────────────────
_cur_commit="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
_cur_date="$(git log -1 --format=%cd --date=short 2>/dev/null || echo unknown)"
say "当前版本：${_cur_commit}（${_cur_date}）"

# ── 拉远端 ─────────────────────────────────────────────────
git fetch --quiet origin 2>&1 | sed 's/^/  /' || warn "git fetch 失败（离线？）"
_remote="origin/main"
git rev-parse --verify --quiet "$_remote" >/dev/null || die "找不到 $_remote 分支"

if [ "$MODE" = "tag" ]; then
  [ -n "$TARGET" ] || die "--version 需要给 tag 名，如 ./update.sh --version v4.0.13"
  git rev-parse --verify --quiet "refs/tags/$TARGET" >/dev/null || die "远端没有 tag：$TARGET"
  _target_commit="$(git rev-parse "origin/$TARGET" 2>/dev/null || git rev-parse "$TARGET")"
  _target_date="$(git log -1 --format=%cd --date=short "$_target_commit")"
  say "目标版本：${TARGET} → ${_target_commit}（${_target_date}）"
else
  _target_commit="$(git rev-parse "$_remote")"
  _target_date="$(git log -1 --format=%cd --date=short "$_target_commit")"
  if [ "$_target_commit" = "$(git rev-parse HEAD)" ]; then
    ok "已是最新（$_remote 与本地一致）"
    exit 0
  fi
  say "最新版本：${_target_commit}（${_target_date}）"
fi

# ── 变更清单 ───────────────────────────────────────────────
_behind="$(git rev-list --count HEAD..$_target_commit)"
_ahead="$(git rev-list --count $_target_commit..HEAD)"
say ""
say "待更新 ${_behind} 个提交${_ahead:+（本地领先 ${_ahead} 个）}"
if [ "$_behind" -gt 0 ]; then
  say "─────────────────────────────────────────"
  git --no-pager log --oneline --no-decorate "HEAD..$_target_commit" | head -30 | sed 's/^/  /'
  [ "$_behind" -gt 30 ] && say "  … 还有 $((_behind - 30)) 个"
  say ""
  say "改动文件："
  git --no-pager diff --stat "HEAD..$_target_commit" -- backend/ | tail -1 | sed 's/^/  /'
fi

if [ "$MODE" = "check" ]; then
  say ""
  say "（--check 模式：未做任何修改。执行 ./update.sh 即可更新）"
  exit 0
fi

# ── 数据备份（保险：万一新代码有 bug，用户的会话/配置能回滚）────────
if [ -d data ]; then
  _bk="backups/data-$(date +%Y%m%d-%H%M%S).tar.gz"
  mkdir -p backups
  if tar czf "$_bk" data 2>/dev/null; then
    ok "数据已备份：$_bk（$(du -h "$_bk" | cut -f1)）"
  else
    warn "数据备份失败（data/ 可能为空），继续"
  fi
else
  warn "没有 data/ 目录，跳过备份"
fi

# ── 更新代码 ───────────────────────────────────────────────
# 只取远端代码；.env / data / backups / .gitignore 都不受影响
if [ "$MODE" = "tag" ]; then
  git checkout --quiet "$TARGET" 2>/dev/null || git fetch origin "refs/tags/$TARGET:refs/tags/$TARGET" && git checkout --quiet "$TARGET"
else
  # 本地有改动就 stash，别把用户的临时修改冲掉
  if ! git diff --quiet || ! git diff --cached --quiet; then
    warn "检测到本地未提交改动，先 stash 保存"
    git stash push -q -m "update.sh 自动 stash $(date +%F\ %T)" || die "stash 失败"
  fi
  git merge --ff-only "$_remote" || die "无法快进合并（本地有分叉提交）。请手动处理：git log --oneline --graph -10"
fi
ok "代码已更新到 $(git rev-parse --short HEAD)"

# ── 重建容器 ───────────────────────────────────────────────
command -v docker >/dev/null 2>&1 || die "未安装 docker"
docker compose version >/dev/null 2>&1 || die "未安装 docker compose 插件"

say ""
say "→ 重建并启动容器..."
docker compose up -d --build 2>&1 | tail -5 | sed 's/^/  /'

# ── 健康检查 ───────────────────────────────────────────────
say ""
say "→ 等待服务就绪..."
_ready=0
for i in $(seq 1 40); do
  if curl -s -m 2 "http://127.0.0.1:9127/" >/dev/null 2>&1; then _ready=1; break; fi
  sleep 2
done

if [ "$_ready" = "1" ]; then
  ok "服务已就绪（统一路由 9127 / 流式 9132）"
else
  warn "服务 80 秒内未就绪，查看日志：docker compose logs -f qingliao"
fi

# ── 错误日志提示 ───────────────────────────────────────────
if docker compose logs --since 2m qingliao 2>/dev/null | grep -qiE "traceback|error"; then
  warn "容器日志里有报错，排查：docker compose logs --tail=50 qingliao"
  warn "如需回滚代码：git checkout - && docker compose up -d --build"
fi

say ""
say "─────────────────────────────────────────"
say "✅ 更新完成"
say "   版本：$(git rev-parse --short HEAD)（$(git log -1 --format=%cd --date=short)）"
say "   查看日志：docker compose logs -f qingliao"
[ -d backups ] && say "   备份目录：backups/（回滚时可从这里恢复 data/）"
