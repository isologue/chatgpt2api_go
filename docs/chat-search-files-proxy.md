# 聊天、搜索、文件生成与代理分流

这是一版从旧 Python 项目移植到 Go 的功能实现。已通过本地模拟上游、真实本地 HTTP/CONNECT 代理及隔离 PostgreSQL 测试；2026-11-03 又在独立线上实例完成真实上游验收：聊天、多轮历史、三种文本接口及流式输出、搜索、PPTX/PSD 异步生成与下载、图片回归均通过。PPTX 校验为 5 页，PSD 基础结构解析为 2 个图层；不等于已经人工验收所有页面的可编辑性或图层视觉质量。**本轮线上直连，按用户要求未测试代理，也不是 1000 并发压测**。详情见 `chat-search-live-validation-2026-11-03.md`。

## 鉴权与接口

除公开文件下载外，都使用现有调用 Key：

```http
Authorization: Bearer <调用密钥>
Content-Type: application/json
```

| 接口 | 用途 |
| --- | --- |
| POST /v1/chat/completions | 文本多轮对话，支持 stream |
| POST /v1/responses | 基础文本 Responses 适配，支持 instructions、input、stream |
| POST /v1/search | 网页搜索，返回 answer 和 sources |
| POST /v1/messages | 基础文本 Messages 样式，可用 X-Api-Key；不是完整 Anthropic SDK 兼容实现 |
| POST /v1/ppt/generations | 创建 PPTX + 素材 ZIP 异步任务 |
| POST /v1/psd/generations | 创建 PSD + 素材 ZIP 异步任务，必需参考图 |
| GET /v1/editable-file-tasks/{id} | 查询当前 Key 创建的文件任务 |
| GET /v1/editable-file-tasks?ids=id1,id2 | 批量查询文件任务，最多 100 个 |
| GET /v1/editable-file-tasks?before=id | 按创建时间倒序分页，最多 100 条，next_before 是下一页游标 |
| GET /files/{name} | 随机 UUID 文件下载链接；持有链接即可下载，无需 Key |

账号槽位由图片、文本、文件共享。文本使用独立内存索引，不因图片额度耗尽而被挡住，不扣图片 quota、不增加图片成功计数，也不会污染监控的图片成功率与 P50/P95。文本/搜索首段输出前可按切号配置重试；**一旦向调用方输出，不再切号重放**。

### 文本对话

```json
{"model":"auto","messages":[{"role":"system","content":"请简洁作答"},{"role":"user","content":"介绍一下 Go"}],"stream":true}
```

角色支持 system、developer（转换为 system）、user、assistant。多轮历史由调用方完整传入。model 原样交给私有上游，实际可用性受账号权限限制；模型列表包含 auto、gpt-5-5、gpt-5-5-thinking 和原图片模型。文本请求体上限为 1 MiB。

stream=false 返回 chat.completion；stream=true 返回 SSE chat.completion.chunk，正常结束含 [DONE]，中途失败以错误事件结束，不伪造成功。Responses 返回 response/output_text 结构及相应 SSE 生命周期事件。请求头成功发出前可返回非 200 错误；流已经开始后的错误只能放在 SSE 中。

Responses 示例：

```json
{"model":"auto","instructions":"用中文回答","input":"什么是连接池？","stream":false}
```

边界：当前仅文本，不支持输入图片/音频、function tools、previous_response_id、后台 Responses、JSON schema 输出等。温度、token 上限、stop 等无法保证生效的参数返回 400，不静默假装支持。Messages 样式中的 usage=0 是兼容占位，不是真实 token 计量；max_tokens 目前同样返回 400，因此不能当作完整 Anthropic 客户端的直接替换。reasoning_effort 支持 low/medium/high/standard/max/extended，xhigh 归一化为 extended，auto/none 不指定，未知值返回 400。

### 网页搜索

```json
{"prompt":"查找 Go 官方最近发布信息","model":"gpt-5-5","stream":false}
```

发送到 /v1/search，返回结构示例（示例不是实际搜索结果）：

```json
{"conversation_id":"...","status":"finished_successfully","answer":"搜索回答","sources":[{"title":"来源标题","url":"https://example.com/","snippet":"可选摘录"}],"assistant_message_id":"..."}
```

/v1/chat/completions 或 /v1/responses 也可传 tools:[{"type":"web_search"}] 开启搜索；web_search_options 同样作为搜索开关，目前不实现其细项。来源根据上游可用 metadata 提取，仅接受 HTTP(S)，去重最多 100 个，缺失时不伪造来源。Sources 位于非流式响应和流式完成事件，不保证内联 citation 标注。搜索会在总预算内进行最终会话补查，补查另受 CORE_METADATA_TIMEOUT 约束。

### PPTX / PSD

```json
{"prompt":"做一份关于城市低碳出行的6页演示文稿","client_task_id":"my-ppt-001","base64_images":[]}
```

发送到 /v1/ppt/generations。PSD 使用 /v1/psd/generations，并提供非空 base64_images 数组（纯 Base64 或 data:image/...;base64,...）。输入图片数量/解码后大小沿用图片编辑限制和解码并发配置。文件任务使用账号中已识别的 Plus/Pro/Team/Enterprise/Business 付费信息；账号导入或 JWT 没有套餐信息时不会盲目当作付费账号。

创建返回 HTTP 200 和任务对象，状态 queued；查询状态为 queued、running、success 或 error，**不是图片任务的 processing/succeeded/failed 协议**。id 与 taskId 是同一任务 ID。成功结果包含：

```json
{"id":"...","taskId":"...","kind":"ppt","status":"success","result":{"conversation_id":"...","primary_url":"https://your-host/files/uuid.pptx","zip_url":"https://your-host/files/uuid.zip"}}
```

同 Key 下 client_task_id 相同且请求内容相同可复用任务（响应 Idempotency-Replayed:true），内容不同返回 409；这是文件接口的幂等字段，和图片 Idempotency-Key 独立。并发准入先于解析与幂等查找，满载时幂等重放也可能返回 429。没有该字段时每次创建新任务。任务所有权隔离，别的 Key 查询不到。文件 URL 跟随公开地址配置/当前请求地址，文件不走 ImageDAV。

生成要求上游实际返回主文件及 ZIP；只返回文字不算成功。下载时有单文件大小限制和 PPTX ZIP 结构/PSD 文件头等基础校验，**不等价于完整验证每页可编辑或每个 PSD 图层质量**。一旦上游已给出会话 ID，失败不切号重新创建长任务，避免重复生成。

任务状态写 PostgreSQL，输出文件位于图片数据卷的 .exports 目录；重启时未完成任务标记为 server_restarted，不自动重复上游长任务。终态写入会有限重试，数据库持续不可用时可能暂时保持 running，下一次启动恢复会将其标错。文件任务当前记录阶段/切号日志，但不记录原始创建/查询报文。**当前尚无导出文件自动清理，图片保留策略不会清理 .exports，请监控磁盘**。

## 可调整资源上限

管理页「设置 → 聊天与文件生成」，保存在线生效；环境变量是首次默认值。

| 参数 | 默认 | 影响/关联 |
| --- | --- | --- |
| CORE_TEXT_CONCURRENCY | 100 | 同时执行的文本/搜索请求，没有额外文本等待队列，满返回 429 text_concurrency_full |
| CORE_TEXT_TIMEOUT | 180s | 整次文本请求，包括校验、PoW、流、搜索补查、有限切号 |
| CORE_FILE_CONCURRENCY | 4 | 文件同时执行数；和图片不相乘，但共享账号槽位/网络/PoW/数据库 |
| CORE_FILE_QUEUE_SIZE | 16 | 额外等待任务数，支持 0；执行+等待满返回 429 file_queue_full |
| CORE_FILE_TIMEOUT | 10m | 从接纳开始的预算，含读入/排队/上传/生成/补查/下载 |
| CORE_MAX_FILE_BYTES | 268435456 | 每个输出文件大小上限，256 MiB |

另受调用 Key 并发、账号槽位、校验并发、下载并发、PoW 并发及下载/元数据阶段超时约束。大文件下载仍受 CORE_DOWNLOAD_TIMEOUT（默认 15s）限制，生成预算 10m 并不意味着下载也能用 10m；需按文件大小/带宽合理调整。

## 代理设置

位置：「设置 → 代理设置」。只保留连接方式和代理地址列表；动态提取、TTL、客户端缓存上限及上传/下载单独分流均已移除。

- **全部直连**：所有上游请求直连，不使用代理；可以保留列表，方便下次启用。
- **全部走代理**：账号校验、RT 续期、首页、PoW/Sentinel、聊天与生成流、补查、上传以及图片/文件下载统一使用配置的代理。
- 每行一个地址，支持 HTTP、HTTPS、SOCKS5、SOCKS5H，以及带用户名密码的 URL。缺少协议时补 HTTP；空行和重复项会清理。特殊字符需 URL 编码。
- 多代理按新上游会话轮询；同一会话固定所选代理地址。代理商若每连接轮换 IP，需在供应商侧配置粘性会话，本服务不能保证供应商的出口 IP 不变。
- 保存到 PostgreSQL 后，新会话使用新配置，在途会话不强制更换出口。已移除且无会话引用的客户端会清理，不另设 64 个出口上限。
- 此设置只控制本服务访问上游的连接，不代理管理页面、数据库和向调用方返回的数据。WebDAV 是独立存储模块，保持自身连接方式。

管理 API 仅管理员可用：

- GET /admin/proxy：读取配置和 revision；凭据仅在这个专用管理员入口回显。
- PUT /admin/proxy：提交 revision 和 config；旧版本返回 409。config 仅含 mode（direct/proxy）和 proxies（地址数组）。普通配置导出不含代理凭据。
- POST /admin/proxy/test：提交当前草稿 config，无须先保存，也不改配置。逐项返回 results（序号、脱敏地址、HTTP 状态、耗时、reachable、success、错误码与说明），直连模式只测一次直连。每项 10 秒预算，诊断并发 8，不限制真实生成并发。

测试仅访问固定上游首页，不使用账号、不上传下载、不调用生成功能。HTTP 403 表示收到响应但被上游拒绝，不能视为可正常生成。无真实代理凭据时，可用本地模拟上游和 HTTP/CONNECT 代理做回归，但不能替代实际代理验收。

CORE_PROXY 不设置时兼容旧 CORE_PROXY_URL。旧 mode=static 持久化数据会读取为单元素列表；旧动态提取模式不再支持，应先重新设置为直连或代理列表，不会静默转成直连。

可选环境变量：

```dotenv
CORE_PROXY='{"mode":"proxy","proxies":["http://user:password@proxy-host:1234","socks5://proxy-host:1080"]}'
```

代理连接故障不会静默回退直连，也不扣账号分或触发切号；上游明确的账号错误仍按原规则处理。管理接口建议使用 HTTPS 或可信内网。
