# nginx 入口配置（参考件·脱敏）

轻聊后端的对外入口由 **nginx + Lucky** 两层组成。新增后端 API 路径时，**漏改任一入口，该入口就 404**。

## 链路

```
App  ──https://<你的域名>:16666──▶  Lucky（独占监听 16666）
                                    │ 按【域名】整端口反代（无路径白名单）
                                    ▼
                          nginx :8080  hermes-webui.conf
                                    │  location /api/xxx（单列）→ 127.0.0.1:9127 ✅ 轻聊后端
                                    │  location /api/    （兜底）→ 127.0.0.1:9123 ⚠️ Hermes 网关
                                    ▼
                          nginx :16668 / :443  qingliao_http.conf / webui_443.conf
```

## 三份配置与入口的对应

| 文件 | 入口 | 说明 |
|---|---|---|
| `hermes-webui.conf` | **8080** | **App 真实入口**（经 Lucky 反代），最常漏 |
| `qingliao_http.conf` | 16668 | HTTP 直连入口 |
| `webui_443.conf` | 443 | HTTPS 入口 |

## ⚠️ 最容易踩的坑：9123 兜底

`hermes-webui.conf` 里有一条兜底：

```nginx
location /api/ { proxy_pass http://127.0.0.1:9123/; }   # 9123 = Hermes 网关，不是轻聊后端
```

`/api/xxx` 没在 9127 那组里**单列**时，会落到这条兜底被转去 9123；9123 上没有该路由 →
返回 **Go 风格 404**（`content-type: text/plain`，body 为 `404: Not Found`）。

**新增规则必须放在这条兜底之前**（nginx 按最长前缀优先匹配）。

### 怎么判断 404 出在哪一层

| 现象 | 含义 |
|---|---|
| `text/plain` + body `404: Not Found` | 没到轻聊后端（多半落到 9123 兜底 / Lucky 未匹配） |
| `application/json` + CORS 头 + `401`/`200` | 已到轻聊后端（401 只是缺 token，路径是通的） |

## 部署注意

- 本目录是**参考件**，不是可直接使用的部署件：证书路径、域名、端口需按本机实际情况修改。
- 修改前**备份** → `nginx -t` 校验 → `nginx -s reload` → **用真实外网入口复测**（本机可能无 IPv6 出口，测不出真实结果）。
- 新增 API 后建议同步一条轻量自检命令：

```bash
curl -sk -o /dev/null -w '%{http_code}\n' https://<你的域名>:<入口端口>/api/<新路径>
```

## 版本信息

`/api/version`（免鉴权）由 `update.sh` 自动维护：更新成功后把 `version/commit/built`
写入 `backend/QL_VERSION`（**注意不要用裸 `VERSION` 文件名**——`backend/` 下本来就有 mail 模块的
`VERSION` 文件，内容是 IMAP 客户端标识 `1.0.0`，读它会把后端版本误报成 1.0.0）。
