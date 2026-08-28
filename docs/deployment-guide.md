# Media Pipeline Fresh Deployment Guide

记录时间：2026-07-08

本文目标：在一台新 Linux 设备上，从零部署一套与当前服务器同构的环境。本文只写部署步骤和配置位置，不写任何真实 token、密码、API key 或 115 access token。

## 1. 部署目标

最终服务分层：

```text
OpenList
  - 挂载 115 Open 到 /115
  - 维护 115 Open access_token/refresh_token
  - 暴露 OpenList API 给 pipeline 和 MSG

Prowlarr
  - 管理 torrent indexer
  - 暴露 Torznab/Newznab API 给 pipeline

MediaStationGo + PostgreSQL
  - 添加 OpenList 云盘存储
  - 创建五个云盘媒体库 root
  - 负责扫描、入库、刮削和 Emby/Jellyfin 协议

media-pipeline
  - Telegram Bot 搜索、选择、提交 115 离线
  - Telegram Bot 接收 115 分享链接并转存到指定 115 目录
  - 等待 115 完成，触发 OpenList 刷新和 MSG scan/scrape
  - 执行清理、番号格式化和自动字幕匹配
  - 成人元数据、横竖图处理和 Emby/Jellyfin 兼容由 MSG 原生实现
```

## 2. 前置条件

宿主机要求：

```text
Linux x86_64 / arm64
Docker Engine
Docker Compose v2
Git
可访问 Telegram Bot API
可访问 115、OpenList、Prowlarr indexer、MediaStationGo 镜像源
```

建议端口：

```text
5244  OpenList，仅本机或反代
9696  Prowlarr，仅本机或反代
18080 MediaStationGo 原始服务，仅本机
15432 MediaStationGo PostgreSQL，仅本机
8191  FlareSolverr，可选，仅本机
```

当前 compose 默认都绑定 `127.0.0.1`。需要远程访问 UI 时，优先使用 SSH tunnel 或 HTTPS 反代，不建议直接暴露后台服务。

本地调试 SSH tunnel 示例：

```bash
ssh \
  -L 5244:127.0.0.1:5244 \
  -L 9696:127.0.0.1:9696 \
  -L 18080:127.0.0.1:18080 \
  root@YOUR_SERVER
```

## 3. 目录规划

统一使用下面的目录，避免后续 compose、volume、文档对不上：

```bash
mkdir -p /opt/OpenList /opt/Prowlarr /opt/MediaStationGo /opt/media-pipeline
mkdir -p /data/OpenList/data
mkdir -p /data/Prowlarr/config
mkdir -p /data/MediaStationGo/data /data/MediaStationGo/cache /data/MediaStationGo/media /data/MediaStationGo/downloads /data/MediaStationGo/postgres
mkdir -p /data/media-pipeline/bot /data/media-pipeline/subtitles
docker network create media_backend || true
```

## 4. 部署 OpenList

创建 `/opt/OpenList/docker-compose.yml`：

```yaml
services:
  openlist:
    image: openlistteam/openlist:latest
    container_name: openlist
    restart: unless-stopped
    environment:
      - TZ=Asia/Shanghai
      - UMASK=022
    volumes:
      - /data/OpenList/data:/opt/openlist/data
    ports:
      - "127.0.0.1:5244:5244"
    networks:
      - media_backend

networks:
  media_backend:
    external: true
    name: media_backend
```

启动：

```bash
cd /opt/OpenList
docker compose up -d
docker logs --tail=100 openlist
```

设置或查看管理员凭据：

```bash
docker exec -it openlist /opt/openlist/openlist admin random
docker exec -it openlist /opt/openlist/openlist admin set 'REPLACE_WITH_ADMIN_PASSWORD'
docker exec -it openlist /opt/openlist/openlist admin token
```

配置要求：

1. 登录 OpenList 管理后台。
2. 添加 `115 Open` 存储，挂载路径固定为 `/115`。
3. 完成 115 Open 授权，确认 OpenList 可正常列出 115 文件。
4. 创建一个用于媒体扫描的账号，例如 `media_scan`。该账号至少要能读取 `/115`，如果不想单独建账号，也可以先用 admin，但不建议长期这样做。

pipeline 使用两类 OpenList 凭据：

```text
OPENLIST_TOKEN
  - OpenList admin token
  - 用于 fs/list、fs/get、rename、meta hide 等 API

OPENLIST_MEDIA_SCAN_USERNAME / OPENLIST_MEDIA_SCAN_PASSWORD
  - OpenList 登录账号
  - Bot 以普通用户视角验证目录可见性和 Hide 结果时使用
```

## 5. 创建 115 目录并记录 cid

在 115 网页或 OpenList 中创建五个目录：

```text
/115/电影
/115/剧集
/115/动漫
/115/成人
/115/其他
```

在 115 网页打开每个目录，URL 中的 `cid=` 就是 pipeline 要填写的 folder id，例如：

```text
https://115.com/storage/allfiles?cid=1234567890&mode=wangpan
```

把五个 `cid` 填入 `/opt/media-pipeline/.env`：

```text
MEDIA_PIPELINE_MOVIE_FOLDER_ID=...
MEDIA_PIPELINE_TV_FOLDER_ID=...
MEDIA_PIPELINE_ANIME_FOLDER_ID=...
MEDIA_PIPELINE_ADULT_FOLDER_ID=...
MEDIA_PIPELINE_OTHER_FOLDER_ID=...
```

这些值是每个 115 账号/目录独有的。不要复用其他服务器文档里的 folder id。

## 6. 部署 Prowlarr

创建 `/opt/Prowlarr/docker-compose.yml`：

```yaml
services:
  prowlarr:
    image: lscr.io/linuxserver/prowlarr:latest
    container_name: prowlarr
    environment:
      - PUID=0
      - PGID=0
      - TZ=Asia/Shanghai
    volumes:
      - /data/Prowlarr/config:/config
    ports:
      - "127.0.0.1:9696:9696"
    restart: unless-stopped

  flaresolverr:
    image: 21hsmw/flaresolverr:nodriver
    container_name: flaresolverr
    environment:
      - DRIVER=nodriver
      - LANG=en_US.UTF-8
      - TZ=Asia/Shanghai
    ports:
      - "127.0.0.1:8191:8191"
    mem_limit: 512m
    memswap_limit: 1g
    restart: unless-stopped
```

启动：

```bash
cd /opt/Prowlarr
docker compose up -d
```

推荐 indexer 和 tag：

```text
media-general:
  Knaben
  LimeTorrents
  YTS
  The Pirate Bay

media-adult:
  sukebei.nyaa.si

media-anime:
  ACG.RIP
  Nyaa.si
  Mikan
  Bangumi Moe

media-fallback:
  MagnetDownload
  TorrentProject2
```

注意：

- Prowlarr API key 不需要手动复制到 `.env`。pipeline 会只读挂载 `/data/Prowlarr/config/config.xml` 并从中读取。
- 如果某些 public indexer 被 Cloudflare 拦截，先不要强行加进默认 profile。慢源应放到 fallback，避免拖慢 Bot 搜索。

## 7. 部署 MediaStationGo

创建 `/opt/MediaStationGo/docker-compose.yml`。以下是最小可复刻配置，`POSTGRES_PASSWORD` 和 DSN 中密码必须一致：

```yaml
services:
  mediastation-go:
    image: ghcr.io/shukebta/mediastation-go:latest
    restart: unless-stopped
    init: true
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "127.0.0.1:18080:8080"
    extra_hosts:
      - "host.docker.internal:host-gateway"
    networks:
      - default
      - media_backend
    volumes:
      - /data/MediaStationGo/data:/data
      - /data/MediaStationGo/cache:/cache
      - /data/MediaStationGo/media:/media
      - /data/MediaStationGo/downloads:/downloads
    environment:
      TZ: Asia/Shanghai
      PUID: "1000"
      PGID: "1000"
      MEDIASTATION_APP_HOST: 0.0.0.0
      MEDIASTATION_APP_PORT: 8080
      MEDIASTATION_APP_WEB_DIR: /app/web/dist
      MEDIASTATION_APP_DATA_DIR: /data
      MEDIASTATION_LOGGING_LEVEL: warn
      MEDIASTATION_LOGGING_FORMAT: console
      MEDIASTATION_LOGGING_OUTPUT_PATH: /data/logs
      MEDIASTATION_DATABASE_TYPE: postgres
      MEDIASTATION_DATABASE_DSN: postgres://mediastation:REPLACE_WITH_POSTGRES_PASSWORD@postgres:5432/mediastation?sslmode=disable
      MEDIASTATION_DATABASE_DB_PATH: /data/no-sqlite-migration.db
      MEDIASTATION_CACHE_CACHE_DIR: /cache
      MEDIASTATION_UPDATE_IMAGE: ghcr.io/shukebta/mediastation-go:latest
      MEDIASTATION_MEDIA_DIR: /data/MediaStationGo/media
      MEDIASTATION_MEDIA_CONTAINER_DIR: /media
      MEDIASTATION_DOWNLOAD_DIR: /data/MediaStationGo/downloads
      MEDIASTATION_DOWNLOAD_CONTAINER_DIR: /downloads
    healthcheck:
      test: ["CMD-SHELL", "busybox wget -qO- http://127.0.0.1:8080/api/health || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"

  postgres:
    image: postgres:16-alpine
    pull_policy: missing
    restart: unless-stopped
    ports:
      - "127.0.0.1:15432:5432"
    environment:
      POSTGRES_DB: mediastation
      POSTGRES_USER: mediastation
      POSTGRES_PASSWORD: REPLACE_WITH_POSTGRES_PASSWORD
      TZ: Asia/Shanghai
    volumes:
      - /data/MediaStationGo/postgres:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -h 127.0.0.1 -U mediastation -d mediastation"]
      interval: 10s
      timeout: 5s
      retries: 10
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"

networks:
  default:
  media_backend:
    external: true
    name: media_backend
```

启动：

```bash
cd /opt/MediaStationGo
docker compose up -d
curl -fsS http://127.0.0.1:18080/api/health
```

首次配置：

1. 登录 MediaStationGo 管理后台，并修改默认管理员密码。
2. 添加 OpenList 云盘存储，类型选择 OpenList。
3. OpenList 地址建议填 `http://openlist:5244`。如果没有加入 `media_backend`，可改用 `http://host.docker.internal:5244`。
4. 使用 OpenList 管理员或专用扫描账号完成云盘授权。
5. 创建五个媒体库和 root：

```text
电影     type=movie  root=/115/电影
剧集     type=tv     root=/115/剧集
动漫     type=anime  root=/115/动漫
成人     type=adult  root=/115/成人
其他媒体 type=movie  root=/115/其他
```

建议设置：

```text
scrape.auto_on_scan=false
cloud.auto_sync_enabled=false
cloud.boot_scan_enabled=false
```

当前链路由 Bot 显式触发 root scan 和单项 scrape，不依赖 MSG 全局自动扫描/刮削。

验证 MSG 媒体库 root：

```bash
DSN=$(docker exec mediastationgo-mediastation-go-1 printenv MEDIASTATION_DATABASE_DSN | tr -d '\r')
docker exec -e DSN="$DSN" mediastationgo-postgres-1 sh -lc \
  'psql "$DSN" -tAc "select l.name,l.id,l.type,r.id,r.path from libraries l join library_roots r on r.library_id=l.id and r.deleted_at is null where l.deleted_at is null order by l.name,l.type;"'
```

必须在 `.env` 中显式配置各分类的 `MEDIA_PIPELINE_*_MSG_LIBRARY_ID` 和 `MEDIA_PIPELINE_*_MSG_ROOT_ID`。Bot 只通过 MSG 管理员 API 工作，不再连接 MSG PostgreSQL，也不会自动猜测媒体库或 root。

## 8. 部署 media-pipeline

拉取代码：

```bash
cd /opt
git clone REPLACE_WITH_YOUR_GITHUB_REPO_URL media-pipeline
cd /opt/media-pipeline
```

创建 `.env`：

```bash
cp .env.example .env
sed -i "s/^MEDIA_PIPELINE_REVISION=.*/MEDIA_PIPELINE_REVISION=$(git rev-parse --short HEAD)/" .env
```

必须替换 `.env` 中所有 `REPLACE_WITH_*`。替换后检查：

```bash
grep -n 'REPLACE_WITH_' .env && echo 'ERROR: placeholders remain' && exit 1 || true
```

关键字段来源：

```text
OPENLIST_TOKEN
  docker exec -it openlist /opt/openlist/openlist admin token

OPENLIST_MEDIA_SCAN_USERNAME / OPENLIST_MEDIA_SCAN_PASSWORD
  OpenList 后台创建的扫描账号

TG_BOT_TOKEN
  Telegram BotFather 创建

TG_ALLOWED_USER_IDS
  允许使用 Bot 的 Telegram 用户 ID

MSG_ADMIN_PASSWORD
  MediaStationGo 管理员密码

MEDIA_PIPELINE_*_FOLDER_ID
  115 目录 cid

115 Cookie
  115 分享链接转存使用的网页 Cookie 由 MediaStationGo 管理后台维护：打开“外部存储 -> 115网盘”，填写 Cookie 或使用 115 App 扫码登录后保存。
  Bot 通过 MSG 管理员账号读取该配置，不再在 Bot state.db 或环境变量中维护第二份 Cookie；`/p115_cookie` 仅返回管理入口提示。
  磁链离线和目录读取仍复用 OpenList 维护的 115 Open access_token/refresh_token

PANSOU_ENABLED / PANSOU_URL
  可选网盘搜索补充，仅用于 Bot 搜索结果里的“网盘搜索”按钮
  启用后调用 PanSou /api/search，并只消费 115 分享结果；入库仍复用现有 115 分享转存链路
  常用值：PANSOU_ENABLED=1，PANSOU_URL=http://127.0.0.1:8888

PANSOU_TOKEN / PANSOU_TIMEOUT_SECONDS / PANSOU_CLOUD_TYPES / PANSOU_SOURCE_TYPE / PANSOU_PLUGINS
  PanSou 认证和搜索参数；未启用 PanSou 认证时 PANSOU_TOKEN 留空
  默认只查 115：PANSOU_CLOUD_TYPES=115
  PANSOU_PLUGINS 是每次搜索请求指定的插件列表；通常留空，让 PanSou 服务端使用已启用插件

PANSOU_IMAGE / PANSOU_PORT / PANSOU_CACHE_DIR / PANSOU_SERVER_CHANNELS / PANSOU_SERVER_ENABLED_PLUGINS
  PanSou 服务端容器配置；默认使用 ghcr.io/fish2018/pansou:latest
  服务只绑定 127.0.0.1:8888，缓存默认写入 /data/pansou/cache
  PANSOU_SERVER_CHANNELS 和 PANSOU_SERVER_ENABLED_PLUGINS 控制 PanSou 自身可用搜索源

MEDIA_PIPELINE_*_MSG_LIBRARY_ID / MSG_ROOT_ID
  必填，分别填写对应分类在 MSG 中的 library id 和 root id
```

### 8.1 MSG 内部 API

内部 API 默认关闭。启用时至少配置：

```text
INTERNAL_API_ENABLED=1
INTERNAL_API_TOKEN=REPLACE_WITH_A_LONG_RANDOM_TOKEN
INTERNAL_API_HOST=127.0.0.1
INTERNAL_API_PORT=8765
INTERNAL_API_WORKERS=3
INTERNAL_API_OWNER_WORKERS=2
INTERNAL_API_SEARCH_TTL_SECONDS=900
```

`INTERNAL_API_HOST=127.0.0.1` 仅允许宿主机访问。如果 MSG 位于 Docker bridge 网络，可绑定宿主机对应的 bridge 地址；无法固定 bridge 地址时也可绑定 `0.0.0.0`，但必须同时使用宿主机防火墙限制为 MSG 所在内网访问。Bearer token 不是公网暴露方案，不要把该端口直接开放到互联网。

`GET /health` 不要求认证，只返回 `{"status":"ok"}`，用于容器健康检查且不泄露配置。所有 `/v1/*` 路由必须发送：

```http
Authorization: Bearer <INTERNAL_API_TOKEN>
```

API 契约：

```text
POST /v1/search
  JSON: owner_id, query, category, source(default|pansou|bt4g), limit
  返回: session_id, expires_at, items, metadata, capabilities
  capabilities 明确给出 pansou、bt4g、llm_rerank 是否可用；MSG UI 不应自行猜测
  每个 item 带服务端 candidate_id；session/candidate 写入 BOT_STATE_DB，默认 15 分钟过期

POST /v1/imports
  Header: Idempotency-Key
  JSON: owner_id, search_session_id, candidate_id, category,
        library_id, root_id, root_openlist_path, provider, media_type,
        force_duplicate(可选，默认 false)
  不接受浏览器提供 download_uri；只使用已持久化的搜索候选
  root_openlist_path 必须与该 category 当前配置的 OpenList 路径一致，否则在查重和 115 请求前返回 409
  入队前按显式 library_id 查询 MSG：弱重复返回可强制的 409，只有 force_duplicate=true 才继续；强番号重复始终不可强制

GET /v1/imports/{id}?owner_id=<owner>
POST /v1/imports/{id}/cancel  JSON: {"owner_id":"..."}
POST /v1/imports/{id}/retry   JSON: {"owner_id":"..."}
```

任务状态为 `queued/running/completed/completed_with_warning/failed/canceled`。响应持续包含 `stage`、可读 `message`、`request/result/error`、`info_hash`、`msg_media_id` 和 `msg_media_title`。只有明确得到 `msg_media_id` 才会返回 `completed`；已入库但刮削、字幕或后续阶段失败时返回 `completed_with_warning`。

`owner_id` 仅用于 API 任务隔离。真正的用户授权、可见媒体库和 owner 字符串签发由 MSG 负责；pipeline 不使用 Telegram 用户任务表推断 MSG Web 用户所有权。

构建并启动：

```bash
cd /opt/media-pipeline
docker compose up -d pansou
docker compose build media-pipeline-bot
docker compose up -d media-pipeline-bot
```

## 9. 反代与第三方 Emby 客户端

Telegram Bot 本身不要求反代。Infuse、VidHub 和其他 Emby/Jellyfin 客户端直接访问 MSG 的 HTTPS 域名；Nginx/OpenResty 上游统一指向 `127.0.0.1:18080`，不再部署额外协议代理。

Bot 自动补齐的字幕由 MSG 原生字幕接口读取。MSG Compose 必须包含同一宿主机目录的只读挂载和缓存目录配置：

```yaml
services:
  mediastation-go:
    volumes:
      - /data/media-pipeline/subtitles:/subtitle-cache:ro
    environment:
      MEDIASTATION_SUBTITLE_CACHE_DIR: /subtitle-cache
```

文件夹封面、播放进度、客户端标题、搜索和字幕流均由 MSG 自身的 Emby/Jellyfin 路由负责。

## 10. 部署验证

基础服务：

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | egrep 'openlist|prowlarr|mediastation|media-pipeline'
curl -fsS http://127.0.0.1:5244/api/public/settings >/dev/null
curl -fsS http://127.0.0.1:9696 >/dev/null
curl -fsS http://127.0.0.1:18080/api/health
```

期望：

```text
media-pipeline-bot 容器为 Up，且没有持续重启
MSG /api/health 返回 200
Bot /version 能返回当前 version 和 revision
```

Bot 验证：

```text
/version
/help
发送一个普通关键词，确认能返回候选
发送一个成人番号，确认走成人 profile
发送一个动漫关键词，确认能补查动漫源
```

真实闭环验证建议先选小体积公开资源或自有内容：

```text
搜索 -> 选择资源 -> 选择分类 -> 提交 115 -> 等待完成 -> OpenList 可见 -> MSG 入库/刮削 -> Bot 状态 success
```

## 11. GitHub 上传前检查

上传个人仓库前执行：

```bash
cd /opt/media-pipeline
git status --short
git status --ignored --short | egrep '(\.env|backup|cache|db|sqlite|log|token|key|secret|password)' || true
git ls-files | grep -Ei '(^|/)(\.env|.*\.env|.*key.*|.*token.*|.*secret.*|.*passwd.*|.*password.*|.*sqlite.*|.*db$|.*\.db|.*\.pem|.*\.crt|.*\.key|temp_|backup|cache|data/)' || true
```

要求：

```text
.env、.env.bak*、backups/、数据库、缓存、日志只能出现在 ignored/untracked，不能出现在 tracked。
.env.example 可以 tracked，但只能包含占位符。
```

建议再跑一次专用 secret scanner：

```bash
gitleaks detect --source . --no-git --redact
gitleaks detect --source . --redact
```

## 12. 常见卡点

### 12.1 115 access_token 失效

pipeline 不主动刷新 115 refresh token。115 Open token 以 OpenList 为准。遇到 115 token 失效时：

1. 先在 OpenList UI 确认 `/115` 能正常刷新和列目录。
2. 在 Bot 中重新执行原任务或点击重试；Bot 会在识别到 access token 失效后对目标目录执行一次 OpenList 刷新并重试。

### 12.2 搜索结果少或超时

优先检查 Prowlarr：

```bash
curl -fsS http://127.0.0.1:9696/api/v1/health -H "X-Api-Key: $(sed -n 's#.*<ApiKey>\(.*\)</ApiKey>.*#\1#p' /data/Prowlarr/config/config.xml)"
```

然后检查 indexer tag 是否符合本文第 6 节。慢源放 fallback，不要放默认 profile。

### 12.3 MSG 入库失败

确认 Bot 容器内已显式注入五类 MSG library/root ID：

```bash
docker inspect media-pipeline-bot --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -E '^MEDIA_PIPELINE_(MOVIE|TV|ANIME|ADULT|OTHER)_MSG_(LIBRARY|ROOT)_ID='
```

如果报 `root not found`，在 MSG 管理页面核对媒体库 root 仍对应 `/115/电影`、`/115/剧集`、`/115/动漫`、`/115/成人`、`/115/其他`，然后修正 `.env` 中相应的 `MEDIA_PIPELINE_*_MSG_LIBRARY_ID` 和 `MEDIA_PIPELINE_*_MSG_ROOT_ID`。Pipeline 不再通过 PostgreSQL 自动发现或修补这些 ID。

### 12.4 第三方客户端没有字幕或文件夹封面

确认客户端连接的域名直接反代 MSG `127.0.0.1:18080`，并检查 MSG 是否挂载了 Bot 字幕缓存：

```bash
curl -fsS http://127.0.0.1:18080/api/health
docker inspect mediastationgo-mediastation-go-1 --format '{{range .Config.Env}}{{println .}}{{end}}' | grep MEDIASTATION_SUBTITLE_CACHE_DIR
docker inspect mediastationgo-mediastation-go-1 --format '{{range .Mounts}}{{println .Source "->" .Destination}}{{end}}' | grep subtitle-cache
docker logs --tail=100 mediastationgo-mediastation-go-1
```
