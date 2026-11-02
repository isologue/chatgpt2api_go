# Go 图片生成核心原型

独立 Go + PostgreSQL 服务，默认端口 `9080`。它用于验证高并发生成调度和下载链路，当前还不是 Python 服务的完整替代品。

此目录位于原仓库的独立 Git worktree，使用空历史分支 `codex/go-image-core`。原目录 `G:\Demo\chatgpt2api` 继续使用 `main`。本服务不读取原数据库，不修改原 Compose，不接管原服务端口。

## 已实现

- Go 原生生成流程：首页、Sentinel、SHA3 PoW、会话准备、SSE、结果补查、图片地址解析和下载；不调用 Python 生成流程。动态挑战仅兼容已实现的指令，求解失败时与旧实现一致提交空值，由上游决定是否接受。
- 固定数量的执行 goroutine、有界等待队列；满载返回 HTTP `429`，并提供 `Retry-After: 1`。
- 内存账号优先队列，选号时不访问数据库；锁内不进行网络、日志或数据库 I/O。
- 账号评分、限流冷却、失效禁用、切号；`no_image_generated` 扣分，安全审核拒绝更新活性但不加减分。
- 按需账号校验：最近成功生成或远程校验任一在有效期内即可复用，但仍检查 AT 到期时间；同账号并发校验合并；RT 旋转时保存新 AT 和 RT。
- 独立下载并发与图片解码并发；下载排队和实际传输分别计时；下载重试耗尽后可切号。
- 图片流式写磁盘，临时文件完整解码通过后才发布；Base64 从文件流式编码给调用方。同源 estuary 内容接口复用账号会话，CDN 和跨源重定向不携带账号凭据。
- 同步与异步接口、任务持久化、分阶段耗时和轻量指标。

## 本地启动

```powershell
cd G:\Demo\chatgpt2api-go
Copy-Item .env.example .env
# 编辑 .env，替换数据库密码、API key 和独立管理 key。
docker compose up -d --build
docker compose ps
curl.exe http://127.0.0.1:9080/readyz
```

本次开发已生成一份本地 `.env`，无需覆盖。该文件已加入 Git 忽略。数据库密码建议使用随机字母数字，避免连接 URL 中的特殊字符需要转义。

默认绑定 `127.0.0.1`。需要外部访问时设置 `CORE_BIND_ADDRESS`，并把 `CORE_PUBLIC_URL` 改为调用方能访问的地址。没有配置代理时走直连；Windows Docker Desktop 访问现有本地 HTTP 代理可设置 `CORE_PROXY_URL=http://host.docker.internal:40080`。Compose 不包含新的 WARP 容器。

## 导入账号

支持原项目导出的 JSON 数组，也支持 `accounts` / `data` 包裹数组。每项至少包含 `access_token` 或 `token`；可包含 `refresh_token`、`client_id`、`email`、`device_id`、`user_agent`。AT 按指纹去重，不会把令牌写到控制台。

```powershell
# 必须先停止这一套 Go core，导入程序需要独占同一调度数据库。
docker compose stop core
docker compose run --rm --no-deps --entrypoint /usr/local/bin/import --volume "G:/path/accounts.json:/import/accounts.json:ro" core --file /import/accounts.json
docker compose up -d core
```

原型不会自动复制原服务的账号。请使用独立测试账号，避免两个服务共享同一 RT 后互相覆盖旋转结果。当前导入按 AT 去重，尚未实现同一身份多个 AT 的归并。

## 接口

接口需 `Authorization: Bearer <CORE_API_KEY>`。当前只支持 `gpt-image-2` 文生图，`n` 为 `1..4`，单次多图按顺序生成，不返回 `revised_prompt`。不支持图片编辑、聊天、搜索或调用方流式输出。

同步与异步都使用 `POST /v1/images/generations`：

```json
{"model":"gpt-image-2","prompt":"一只蓝色陶瓷花瓶，产品摄影","n":1,"response_format":"url","async":true}
```

异步创建响应：

```json
{"created_at":1792650048,"model":"gpt-image-2","status":"queued","task_id":"task_..."}
```

轮询 `GET /v1/images/tasks/{task_id}`，排队和进行中分别返回 `0%`、`1%`；目前上游没有可用的精确进度，不伪造连续百分比。成功响应：

```json
{"created_at":1792650048,"model":"gpt-image-2","progress":"100%","status":"succeeded","task_id":"task_...","data":{"created":1792650081,"data":[{"url":"http://localhost:9080/images/uuid.png"}]}}
```

失败响应：

```json
{"task_id":"task_...","status":"failed","error":{"message":"上游具体失败原因或本地明确的故障分类"}}
```

同步返回 `{"created":...,"data":[{"url":"..."}]}`。返回格式支持 `url` 和 `b64_json`，兼容 `base64`、`b64`、`base64_json`。系统默认值与是否接受用户指定格式分别由两个配置项控制。

图片 URL 使用随机 UUID，持有 URL 即可访问，不额外要求 API key。`/healthz` 只检测进程；`/readyz` 检查服务状态及数据库。`GET /v1/models` 返回当前支持的模型。

## 主要性能参数

所有配置在 `.env.example`，修改后执行 `docker compose up -d --force-recreate core`。

| 参数 | 默认值 | 作用 |
| --- | ---: | --- |
| `CORE_IMAGE_WORKERS` | 1000 | 同时执行的请求数 |
| `CORE_IMAGE_QUEUE_SIZE` | 1000 | 额外等待请求数；允许设为 0 |
| `CORE_ACCOUNT_SLOTS` | 1 | 每账号同时使用数 |
| `CORE_DOWNLOAD_CONCURRENCY` | 64 | 同时实际下载数；不会乘以生成并发 |
| `CORE_VALIDATION_CONCURRENCY` | 4 | 同时完整解码图片数，限制解码内存峰值 |
| `CORE_VERIFY_CONCURRENCY` | 50 | 按需校验账号并发；同账号校验合并 |
| `CORE_RESPONSE_CONCURRENCY` | 32 | 同时构造/发送结果响应数，尤其限制 Base64 回传 |
| `CORE_DB_CONNECTIONS` | 32 | 业务数据库连接数，另保留 1 条实例锁连接 |
| `CORE_SWITCH_RETRIES` | 2 | 失败后最多再换 2 个账号，合计最多 3 次尝试 |
| `CORE_DOWNLOAD_RETRIES` | 1 | 相同图片下载最多再试 1 次 |
| `CORE_AVAILABILITY_TTL` | 900s | 账号活性有效期 |
| `CORE_RESPONSE_FORMAT` | url | 默认图片返回格式 |
| `CORE_ALLOW_USER_RESPONSE_FORMAT` | true | 是否接受调用方指定返回格式 |

这些默认值是原型测试起点，不是已验证的生产容量承诺。实际同时进入上游的数量还受到可用账号槽位、账号校验、上游限流、CPU、连接数和带宽影响。将执行数设为 1000 不会让 64 个下载名额变成 1000 个；下载名额也不需要等于生成执行数。

超时使用 Go duration，例如 `80s`、`10s`：

| 参数 | 默认值 | 计时范围 |
| --- | ---: | --- |
| `CORE_TOTAL_TIMEOUT` | 120s | 从接纳任务起，排队、所有账号尝试、下载共用生成预算 |
| `CORE_SSE_TIMEOUT` | 80s | 单次上游生成流，包含建联 |
| `CORE_POLL_TIMEOUT` | 10s | 流结束/异常后缺少图片标识的补查预算 |
| `CORE_DOWNLOAD_TIMEOUT` | 15s | 取得下载名额后，单次传输和随后校验的预算 |
| `CORE_METADATA_TIMEOUT` | 30s | 单次元数据查询预算；按需校验另有最多两倍预算 |

单飞校验独立于某一个调用方存在，调用方取消会停止等待；服务停止会取消仍在执行的校验。结果及日志持久化使用独立的短超时，因此完整 HTTP 响应可能晚于生成总预算。原型未实现后台批量日志写入。

## 指标与数据

`GET /admin/metrics` 使用独立的 `CORE_ADMIN_KEY`：执行/排队/拒绝数、账号槽位、真实活跃 SSE、下载活跃/等待/字节数、各阶段直方图，以及 Go 堆和 goroutine 数。计数和直方图在重启后清零。阶段名称为中文，耗时单位毫秒。

PostgreSQL 仅使用 `core_accounts`、`core_tasks`、`core_requests`；图片保存在独立卷 `images`。任务保存文件引用，不存图片 Base64。请求日志保存切号账号、错误分类和阶段耗时，当前未提供原始请求/响应详情 UI。重启时将尚未完成的任务标记失败，不自动重发可能已被上游计费的请求。

当前只支持一个调度实例；同一个数据库上的第二个实例会因 PostgreSQL advisory lock 拒绝启动。不能直接扩为多个副本来叠加并发。账号仍在启动时完整加载；评分排序为堆索引。未移植热池后台维护、管理页、自动清理、任务幂等、多租户或完整账号额度刷新。长期运行需要另行设计保留策略。

## 验证与后续开发

```powershell
go test ./... -count=1
go vet ./...
# 在支持 CGO 的环境中：
go test -race ./... -count=1
```

普通测试不连接真实上游。模拟 200/500/1000 并发测试需显式设置 `CORE_RUN_LOAD_TESTS=1`，建议 Linux 容器运行。PostgreSQL 集成测试还需 `CORE_TEST_DATABASE_URL`，必须指向独立的测试库：测试会清空其中的 `core_*` 表，不能使用运行库。

`CORE_SMOKE_ACCOUNTS_FILE` 会显式启用一次真实上游生图测试并消耗额度；可搭配 `CORE_SMOKE_PROXY_URL`。不设置时自动跳过。2026-11-02 已完成一次真实上游生成、下载和完整解码，总耗时约 16.2 秒；同时通过 1000 并发模拟 HTTP 全链路测试。真实上游千并发仍未验证，详见 [验证记录](docs/validation.md)。
