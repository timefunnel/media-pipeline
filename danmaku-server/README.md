# 独立弹幕服务

代码随现有 media-pipeline 仓库维护，但使用独立进程、配置、缓存卷、镜像和发布工作流。pipeline 不再提供弹幕 API；MSG 直接调用此服务，客户端仍使用原 MSG 弹幕地址。

## 匹配规则

- `DANMAKU_PROVIDERS` 是准确结果的选择优先级，例如 `tencent,iqiyi,youku`。原生源并行搜索，不能按最快返回的源抢占更高优先级的准确结果。
- 每次先检查本地缓存。缓存里已有高优先级唯一命中时，不启动更低源；需要回源的原生源同时搜索。按优先级取到唯一、准确的作品/季/集后立即停止等待，不取更低源弹幕、不合并内容。未启动的低优先级任务取消，已启动的搜索有超时限制，完成后保留整季缓存。
- 高优先级未命中或有歧义才采用低优先级结果。搜索耗时从相加变为重叠；优酷命中时仍须等腾讯/爱奇艺完成或明确失败，确保优先级确定。源错误记录在 attempts 或源搜索完成日志中；如果最后仍无命中且存在源错误，返回 502，不伪装为没有弹幕。
- 已命中的源即使返回空弹幕或取弹幕失败，也不会换另一源，以免时间轴和关联悄悄改变。
- 腾讯、爱奇艺、优酷按识别后的标题/原名、季号、源站正片集号精确匹配；多季合辑再次检查分集作品名。优酷还要求 `stage` 与原始分集标题中的集号一致，不使用数组位置或 `seq` 猜集号，并校验视频 ID 与播放 URL 一致。电影要求标题和年份一致；未知季集、特别篇、综艺等不猜测匹配。
- 动画类型精确接受 `动漫`、`2D动漫`、`3D动漫`，兼容腾讯适配器保留的维度标签；不接受任意含“动漫”的类型，不放宽标题、季集或电影/剧集边界。
- 优酷只拆分标题末尾明确标注的 `(别名：...)` / `（别名：...）`，保留标题内的标点、空格及数字。主标题和别名共享明确季号；季号冲突报错，不把不带季号的英文别名当成第一季。显示标题和分集校验使用清理别名尾注后的源站主标题，匹配别名不增加上游搜索。
- 弹弹play/兼容聚合源保留 TMDB → 未得到唯一节目时仅一次精确关键词查询的策略，已知季号时额外校验季号。不读取文件、不计算 hash、不调用 115。

默认仅启用 `tencent,iqiyi,youku`，`DANMAKU_DANDANPLAY_ENABLED=false`：弹弹play 不参与搜索或取弹幕，不要求凭据，不消耗官方次数。升级已有部署必须同步移除 `DANMAKU_PROVIDERS` 内的 `dandanplay`；旧配置与禁用开关冲突时启动明确报错，不静默忽略。旧弹弹play成功关联不自动改写，仍可读取已有且未过期的整集弹幕缓存；缓存缺失或过期则明确报禁用，不回源、不静默换源。以后重新启用必须同时设置开关为 `true`、加入源列表并提供凭据；建议放在原生源之后，仅原生源无法唯一命中时串行调用。`aggregator` 需要兼容 `/api/v2/search/episodes` 和 `/api/v2/comment/{episodeId}` 的地址。

## 缓存与关联

原生源一次缓存整季分集列表，换到下一集也先在本地选择，不重复搜索。搜索缓存默认 1 天、整集原始弹幕缓存 7 天、空结果缓存 1 小时；同键并发由单飞锁保护。缓存不会包含网络错误或不完整分片结果。

发布动画类型或显式别名解析修正时，应在新版切换后按已确认的作品/季清理旧空搜索缓存及其 MSG 自动未命中关联，避免旧结果继续抑制重试。不在旧版运行期间提前清理，不清空全部缓存，不删除成功/手动关联或已下载的整集弹幕。

腾讯、爱奇艺、优酷节目编号由源名和稳定播放 URL 派生为十进制字符串，URL 关联持久化；已有相同关联不重复写盘。重启后可直接使用已有节目编号，不使用第三方服务的临时自增编号。必须持久保存缓存卷，否则原生源已有编号需重新匹配。

原生适配器共用最多三个常驻 Node 桥接进程，复用模块和 HTTP 连接；不同源在不同进程并行，单进程串行，第三方全局变量互不串扰。同一请求内复用重复 GET。排队与处理共享原超时预算，进程超时、退出或协议错误会清理该进程并显式报错，下一次请求重新启动，不在本次隐藏重试。源错误也回收进程，防止遗留异步请求污染下一次调用。关闭服务时回收进程。优酷匿名 token 与签名仅存在于进程内，原始第三方日志不输出，返回错误脱敏；身份获取、分页或任一弹幕分片失败都不缓存为成功或空结果，也禁用第三方本地生成 cna 的兜底。

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

`compose.yml` 要求 `DANMAKU_IMAGE` 为已验证的不可变 digest，并读取同目录 `.env`，仅绑定宿主机 loopback。它不连接 pipeline、MSG 数据库、云盘或 Telegram。当前命中源的分片默认 6 个并发，`DANMAKU_SEGMENT_CONCURRENCY` 可设 1–8；请求数量不因并发调整增加，失败停止调度剩余分片并不保存部分结果。整集完成后才返回，SenPlayer 无需追加请求。最多三个桥接任务可同时执行，因此多媒体同时抓取最多 18 个分片请求（默认配置）；不是每个播放器各建一个无界池。

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

## 并行提速本地验证（2026-10-08）

- `python -m unittest discover -s tests -p "test_danmaku*.py" -q`：95 项通过；新增三个源同时启动、较慢高优先级胜出、命中后不等待低优先级、缓存优先及弹弹play禁用/仅缓存读取检查。`npm test`：12 项通过，分片并发 2/6/8 均符合设定，分片失败不返回部分结果。
- 同机一次真实对照，目标为《爱情公寓》S01E01，两个独立临时缓存目录：基线使用上一提交的串行匹配与 2 个分片并发，新版使用并行匹配与 6 个分片并发。二者均使用常驻 Node 和相同精确季集规则，分别取齐优酷 46 个分片；不在第二次复用第一次缓存。

| 服务端阶段 | 串行匹配 / 2 分片并发 | 并行匹配 / 6 分片并发 |
| --- | ---: | ---: |
| 首次准确匹配 | 2100 ms | 1076 ms |
| 首次取齐整集弹幕并处理 | 2842 ms | 1296 ms |
| 首次合计 | 4942 ms | 2372 ms |
| 再次匹配并读取整集缓存 | 39 ms | 40 ms |

- 首次合计约减少 52%；两次分别得到 16318/16323 条去重弹幕，按现有上限均匀采样下发 6000 条。弹幕来源和节目编号相同，少量数量差异来自实时上游变化。本次时间含本地服务处理，不含 MSG 转发、XML 编码、客户端网络或 SenPlayer 渲染；单次测量不保证所有作品或生产环境相同收益。
- 实际 106 次 HTTP 请求，全部 200：腾讯 2、爱奇艺 2、优酷搜索 2、优酷 OpenAPI 4、mmstat 2、优酷 MTOP 94；总请求预算 120、当前命中源最多 6 个分片并发。弹弹play/115 调用为零，未下载视频。临时缓存、计数和探针已清理。
- 本轮仅本地实现、测试和提交；生产配置须更新为 `DANMAKU_PROVIDERS=tencent,iqiyi,youku`，并保持 `DANMAKU_DANDANPLAY_ENABLED=false`，不能沿用旧源列表。

## 优酷显式别名修正验证（2026-10-08）

- `npm test`：17 项通过；覆盖真实标题格式、全角/半角别名尾注、英文逗号保留、主标题/别名共享季号、冲突拒绝及严格集号/URL 校验。`python -m unittest discover -s tests -p "test_danmaku*.py" -q`：95 项通过。
- 两次本地真实验证，共 4 次优酷 HTTP 请求，全部 200；不写持久缓存、不取弹幕、不调用弹弹play/115。`Love, Death & Robots` 已准确命中“爱，死亡和机器人 第一季”（2019），同时排除第三季。
- 第一季的优酷分集接口两次均返回 0 集，因此本次仍无法选择 S01E01 或取得弹幕。作品搜索命中不等于分集/弹幕可用；保留未命中结果，不使用数组顺序、`seq` 或作品卡集数生成节目编号。
- 本轮只完成本地修复与提交，未推送、未发布；旧空搜索缓存及对应自动未命中关联仅在新版切换后定向处理。
