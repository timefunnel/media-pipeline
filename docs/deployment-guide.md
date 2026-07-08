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
  - 等待 115 完成，触发 OpenList 刷新和 MSG scan/scrape
  - 执行清理、番号格式化、成人图片修复、字幕匹配
  - subtitle proxy 为 Emby/Infuse 补外部字幕、文件夹封面、标题番号和播放兼容
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
18081 media-pipeline-subtitle-proxy，仅本机或反代给 Emby 客户端
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
  -L 18081:127.0.0.1:18081 \
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
  - subtitle proxy 和部分媒体扫描能力使用
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

正常情况下不需要把 `library_id/root_id` 手动填入 `.env`。pipeline 会通过 `MSG_DATABASE_DSN` 查询 MSG PostgreSQL，并按 `media_type + OpenList root path` 自动发现。只有同一 `media_type + root path` 出现多条匹配，或你需要强制指定某个 root 时，才手动覆盖 `MEDIA_PIPELINE_*_MSG_LIBRARY_ID` 和 `MEDIA_PIPELINE_*_MSG_ROOT_ID`。

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

MSG_DATABASE_DSN
  postgres://mediastation:<postgres password>@127.0.0.1:15432/mediastation?sslmode=disable

MEDIA_PIPELINE_*_FOLDER_ID
  115 目录 cid

MEDIA_PIPELINE_*_MSG_LIBRARY_ID / MSG_ROOT_ID
  通常留空，由 pipeline 根据 MSG_DATABASE_DSN 自动发现；仅在自动发现歧义时手动覆盖
```

构建并启动：

```bash
cd /opt/media-pipeline
docker compose build media-pipeline-bot media-pipeline-subtitle-proxy
docker compose up -d media-pipeline-bot media-pipeline-subtitle-proxy
```

## 9. 反代与第三方 Emby 客户端

如果只是 Telegram Bot 入库，反代不是必须。若使用 Infuse、VidHub 或其他 Emby/Jellyfin 客户端，应让客户端访问 `media-pipeline-subtitle-proxy`，而不是直接访问 MSG 原始端口。

本项目提供 Nginx 片段：

```text
ops/privdo-subtitle-proxy.conf
```

使用方式：

1. 在站点 server block 中 include 该片段。
2. 片段会把需要补丁的 Emby/Jellyfin 路径转发到 `127.0.0.1:18081`。
3. 普通 MSG 页面和未拦截路径仍可走 `127.0.0.1:18080`。

如果不配置该代理，外部字幕注入、文件夹封面补丁、成人标题番号前缀和部分播放兼容不会完整生效。

## 10. 部署验证

基础服务：

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | egrep 'openlist|prowlarr|mediastation|media-pipeline'
curl -fsS http://127.0.0.1:5244/api/public/settings >/dev/null
curl -fsS http://127.0.0.1:9696 >/dev/null
curl -fsS http://127.0.0.1:18080/api/health
curl -fsS http://127.0.0.1:18081/health
```

pipeline CLI：

```bash
cd /opt/media-pipeline
docker exec media-pipeline-bot python -m pipeline.cli folders
docker exec media-pipeline-bot python -m pipeline.cli verify-folders
docker exec media-pipeline-bot python -m pipeline.cli probe
docker exec media-pipeline-bot python -m pipeline.cli msg-login
```

期望：

```text
folders 输出为当前设备自己的 115 folder id
verify-folders 中 movie/tv/anime/adult/other 均 code=0
probe 能读取 OpenList 中的 115 Open storage_id 和 quota
msg-login 返回 {"authenticated": true}
subtitle proxy /health 返回 200
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
2. 再重跑：

```bash
docker exec media-pipeline-bot python -m pipeline.cli probe
```

### 12.2 搜索结果少或超时

优先检查 Prowlarr：

```bash
curl -fsS http://127.0.0.1:9696/api/v1/health -H "X-Api-Key: $(sed -n 's#.*<ApiKey>\(.*\)</ApiKey>.*#\1#p' /data/Prowlarr/config/config.xml)"
```

然后检查 indexer tag 是否符合本文第 6 节。慢源放 fallback，不要放默认 profile。

### 12.3 MSG 入库失败

先确认 pipeline 能自动发现当前设备的 MSG root：

```bash
docker exec media-pipeline-bot python -m pipeline.cli folders
```

再查 MSG 当前库是否存在唯一的 `media_type + OpenList root path` 匹配：

```bash
DSN=$(docker exec mediastationgo-mediastation-go-1 printenv MEDIASTATION_DATABASE_DSN | tr -d '\r')
docker exec -e DSN="$DSN" mediastationgo-postgres-1 sh -lc \
  'psql "$DSN" -tAc "select l.name,l.id,l.type,r.id,r.path from libraries l join library_roots r on r.library_id=l.id and r.deleted_at is null where l.deleted_at is null order by l.name,l.type;"'
```

如果报 `root not found`，优先检查 MSG 媒体库 root path 是否仍是 `/115/电影`、`/115/剧集`、`/115/动漫`、`/115/成人`、`/115/其他` 对应的 OpenList 路径。

如果报 `multiple MediaStationGo roots matched`，说明同一类型和路径存在重复 root。先清理 MSG 内重复媒体库；确实需要保留重复项时，再在 `.env` 中手动覆盖对应分类的 `MEDIA_PIPELINE_*_MSG_LIBRARY_ID` 和 `MEDIA_PIPELINE_*_MSG_ROOT_ID`。

### 12.4 第三方客户端没有字幕或文件夹封面

确认客户端连接的是 subtitle proxy 或其反代，而不是 MSG 原始端口：

```bash
curl -fsS http://127.0.0.1:18081/health
docker logs --tail=100 media-pipeline-subtitle-proxy
```
