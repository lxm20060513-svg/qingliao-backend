#!/usr/bin/env bash
# 轻聊后端一键安装脚本（Docker Compose）
set -e
cd "$(dirname "$0")"

echo "=============================="
echo " 轻聊后端 Qingliao Backend"
echo " 一键安装（Docker Compose）"
echo "=============================="

command -v docker >/dev/null 2>&1 || { echo "❌ 未安装 docker，请先安装"; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "❌ 未安装 docker compose 插件"; exit 1; }

# BE3：服务间 token 不再有公开默认值，缺失=对应接口拒绝服务，必须随机生成
_rand_hex() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 16
    else
        head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n'
    fi
}

if [ -f .env ] && grep -q "^QL_PASSWORD=" .env && ! grep -q "^QL_PASSWORD=changeme$" .env; then
    echo "✅ 已有 .env 配置，跳过初始化"
else
    read -r -p "设置访问密码（所有 API 鉴权用，直接回车=随机生成）: " PW
    PW="${PW:-$(_rand_hex)}"
    cat > .env <<EOF
QL_PASSWORD=${PW}
EOF
    echo "✅ .env 已生成"
fi

# BE3：老 .env 里没有这两个变量 → 补随机值并打印（需同步到 Hermes 插件/cron 配置）
for _v in QL_INBOX_TOKEN QL_PUSH_TOKEN; do
    if ! grep -q "^${_v}=." .env; then
        sed -i "/^${_v}=/d" .env 2>/dev/null || true
        _tv=$(_rand_hex)
        echo "${_v}=${_tv}" >> .env
        echo "→ 已生成 ${_v}=${_tv}（请同步到 Hermes 侧使用同一值）"
    fi
done

read -r -p "上游 LLM 端点（OpenAI 兼容，回车=host.docker.internal:9123）: " LLM
if [ -n "$LLM" ]; then
    grep -q "^QL_HERMES_URL=" .env || echo "QL_HERMES_URL=${LLM}" >> .env
    read -r -p "上游 LLM API Key（可空）: " KEY
    grep -q "^QL_HERMES_KEY=" .env || echo "QL_HERMES_KEY=${KEY}" >> .env
fi

mkdir -p data
echo "→ 构建并启动容器..."
docker compose up -d --build

echo "→ 等待服务就绪..."
for i in $(seq 1 30); do
    if curl -s -m 2 "http://127.0.0.1:9127/" >/dev/null 2>&1; then break; fi
    sleep 2
done

echo ""
docker compose ps
echo ""
echo "✅ 安装完成！"
echo "   统一路由: http://<本机IP>:9127/api/<模块>"
echo "   流式服务: http://<本机IP>:9132"
echo "   查看日志: docker compose logs -f qingliao"
