# 独立弹幕服务

代码随现有 media-pipeline 仓库维护，但使用独立进程、配置、缓存卷、镜像和发布工作流。pipeline 不再提供弹幕 API；MSG 直接调用此服务，客户端仍使用原 MSG 弹幕地址。

## 匹配规则

- `DANMAKU_PROVIDERS` 是从高到低的查询顺序，例如 `tencent,iqiyi,youku,dandanplay`。不是同时搜索后排序。
- 每次先查本地缓存。当前源返回唯一、准确的作品/季/集后立即停止，后续源不搜索、不取弹幕，也不合并多源内容。
- 未命中或有歧义才尝试下一源。源错误记录在 attempts 中；如果最后仍无命中且存在源错误，返回 502，不伪装为没有弹幕。
- 已命中的源即使返回空弹幕或取弹幕失败，也不会换另一源，以免时间轴和关联悄悄改变。
- 腾讯、爱奇艺、优酷按识别后的标题/原名、季号、源站正片集号精确匹配；多季合辑再次检查分集作品名。优酷还要求 `stage` 与原始分集标题中的集号一致，不使用数组位置或 `seq` 猜集号，并校验视频 ID 与播放 URL 一致。电影要求标题和年份一致；未知季集、特别篇、综艺等不猜测匹配。
- 弹弹play/兼容聚合源保留 TMDB → 未得到唯一节目时仅一次精确关键词查询的策略，已知季号时额外校验季号。不读取文件、不计算 hash、不调用 115。

默认启用 `tencent,iqiyi,youku,dandanplay`，弹弹play 优先级最低且必须提供官方凭据。前三源唯一命中后不会调用弹弹play；配置中移除它则没有官方计次接口调用。已有部署若显式设置了 `DANMAKU_PROVIDERS`，升级时须将 `youku` 加到弹弹play之前。`aggregator` 需要兼容 `/api/v2/search/episodes` 和 `/api/v2/comment/{episodeId}` 的地址。

## 缓存与关联

原生源一次缓存整季分集列表，换到下一集也先在本地选择，不重复搜索。搜索缓存默认 1 天、整集原始弹幕缓存 7 天、空结果缓存 1 小时；同键并发由单飞锁保护。缓存不会包含网络错误或不完整分片结果。

腾讯、爱奇艺、优酷节目编号由源名和稳定播放 URL 派生为十进制字符串，URL 关联持久化；已有相同关联不重复写盘。重启后可直接使用已有节目编号，不使用第三方服务的临时自增编号。必须持久保存缓存卷，否则原生源已有编号需重新匹配。

原生适配器共用最多两个常驻 Node 桥接进程，复用模块和 HTTP 连接；单进程串行执行，不并行搜索多个源。同一请求内复用重复 GET。排队与处理共享原超时预算，进程超时、退出或协议错误会清理该进程并显式报错，下一次请求重新启动，不在本次隐藏重试。源错误也回收进程，防止遗留异步请求污染下一次调用。关闭服务时回收进程。优酷匿名 token 与签名仅存在于进程内，原始第三方日志不输出，返回错误脱敏；身份获取、分页或任一弹幕分片失败都不缓存为成功或空结果，也禁用第三方本地生成 cna 的兜底。

MSG 继续持久化媒体关联、用户偏移和本地导入原文。本次与 MSG 配套升级至 `priority_v2`：旧版（含 `priority_v1`）未命中/失败记录会按包含优酷的新策略重新尝试一次，新未命中记录 1 天内复用；新失败记录也抑制重复回源 1 天，但始终返回错误，不作为没有弹幕。已成功/手动关联不会因优先级配置变化自动改写；删除源之前应处理对应旧关联，否则明确报错，不静默换源。原生源目前仅支持 `ch_convert=0`；非零请求明确拒绝。

## 本地运行

需要 Python 3.12 和 Node.js 22 或以上；Python 侧只有标准库。复制 `danmaku-server/.env.example` 的值到运行环境，token 使用随机值，不提交实际 `.env`。

```powershell
cd danmaku-server
npm ci --ignore-scripts --legacy-peer-deps --no-audit --no-fund
cd ..
$env:PYTHONPATH = "$PWD/app"
$env:DANMAKU_SERVER_TOKEN = 'replace-with-a-random-token-at-least-16-characters'
$env:DANMAKU_CACHE_DIR = "$PWD/.cache/danmaku"
python -m danmaku.server
```

默认仅监听 `127.0.0.1:9322`。`GET /healthz` 公共健康检查；所有 `/v1/danmaku/*` 需要 `Authorization: Bearer <token>`。匹配请求：

```json
{"media_id":"example","target":{"title":"爱情公寓","original_title":"iPartment","tmdb_id":"68809","season":4,"episode":16,"year":2009}}
```

POST 路由：`/v1/danmaku/match`、`comment`、`parse`、`season/prewarm`；GET 预热任务：`/v1/danmaku/season/prewarm/{task_id}`。预热仍由用户显式发起，单 worker 串行执行。HTTP 错误区分 400、401、404、413、502、500。

MSG 使用独立配置，与 resource_import 开关无关：

```yaml
danmaku:
  enabled: true
  url: http://127.0.0.1:9322
  token: replace-with-the-same-random-token
  timeout_seconds: 120
```

也可使用 `MEDIASTATION_DANMAKU_ENABLED`、`MEDIASTATION_DANMAKU_URL`、`MEDIASTATION_DANMAKU_TOKEN`、`MEDIASTATION_DANMAKU_TIMEOUT_SECONDS`。MSG 若不使用 host 网络，地址需改为实际可达的容器服务地址，不能使用容器自身的 127.0.0.1。

## 镜像与验证

`.github/workflows/docker-publish.yml` 选择 `target=danmaku` 单独构建 linux/amd64，固定源码 revision/version，独立 BuildKit cache scope；默认 target 仍为 media-pipeline。正式部署按顶层生产工作流程先隔离验证，再使用同一 `image@sha256:...` 切换，不在服务器构建。

`compose.yml` 要求 `DANMAKU_IMAGE` 为已验证的不可变 digest，并读取同目录 `.env`，仅绑定宿主机 loopback。它不连接 pipeline、MSG 数据库、云盘或 Telegram。源站分片在当前匹配源内最多 2 个并发，失败不保存部分结果。

```text
python -m unittest discover -s tests -p "test_danmaku*.py" -q
cd danmaku-server && npm test
```

腾讯/爱奇艺/优酷适配器来自 [huangxd-/danmu_api](https://github.com/huangxd-/danmu_api/tree/afc8b8119f981492a5caee52f1e1ebf756bd0d41)，固定提交 `afc8b8119f981492a5caee52f1e1ebf756bd0d41`，通过 npm lockfile 固定传递依赖。没有修改第三方包；优酷在本项目通过子类收紧分页及分片错误检查。上游 LICENSE 文本为 AGPL-3.0（与其 package.json 的 ISC 标注不一致）；镜像保留其原 LICENSE，分发或开放服务时需按实际 LICENSE 核对源代码提供义务。本变更不改写 MSG 或 pipeline 仓库的整体许可。

## 优酷接入本地验证（2026-10-08）

- `python -m unittest discover -s tests -p "test_danmaku*.py" -q`：89 项通过；覆盖源优先级、整季缓存、失败不缓存、进程复用/并发上限/超时/退出/回收。
- `npm test`：12 项通过；覆盖优酷季集号冲突、URL 校验、完整分页、分片错误与日志脱敏。
- 配套 MSG：`go test ./internal/service ./internal/config ./internal/handler -run Danmaku -count=1` 通过，覆盖旧版未命中/失败迁移与成功关联保护。
- 真实优酷搜索“爱情公寓”识别到第一季 20 集。S01E01 严格匹配到官方标题“爱情公寓 第一季 01”及 `XMTE0OTk3MzEy`，仅启用优酷时首次本地匹配 2048 ms；紧接着 S01E02 与再次 S01E01 命中磁盘缓存，分别为 0.28/0.21 ms，不回源。此时间不含整集弹幕抓取、HTTP 传输或客户端渲染，也不代表腾讯/爱奇艺首次未命中耗时。
- 单独获取 S01E01 的 180–240 秒分片：4 次优酷相关 HTTP 请求，返回 `SUCCESS`、业务码 `1` 和 359 条弹幕；未抓取整集。
- 同机无网络的桥接开销对比：旧版每次启动 117–127 ms，常驻进程消息往返 0.45–0.66 ms。网络回源耗时仍由源站决定。
- 未部署、未验证生产或 SenPlayer 播放；未调用弹弹play/115，也未下载媒体文件。
