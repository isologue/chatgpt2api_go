# 2026-11-03 聊天、搜索与文件生成线上验收

## 部署范围

- 本地独立仓库：`G:\Demo\chatgpt2api-go`，分支 `codex/chat-search-proxy`。
- 镜像：`milodk/chatgpt2api-go:chat-search-20261103-4`。
- 镜像摘要：`sha256:1716ca4152afe6285901f02136663b42d6a5661d8692300c5d41b91b9f385a3c`。
- 新实例：`http://108.186.180.104:9082/admin/`，独立 Compose project `c2go-chatsearch`。
- 服务器目录：`/www/wwwroot/chatgpt2api_go_chatsearch`，Compose 文件 `compose.yml`。
- PostgreSQL 与 images 均使用独立持久卷；未覆盖 `latest`，未替换原服务。
- 最终比对部署前 baseline：原 Go/Python 应用及其 PostgreSQL 共四个容器的 ID、StartedAt 均未改变，全部仍 running；9080 与 9082 的 readyz 均为 200。
- 管理员密钥沿用原 Go 容器环境中的 `CORE_ADMIN_KEY`，管理员 Bearer 认证实测成功；本实例 API Key 独立随机生成，可以在管理页查看。密钥不写入此文档。
- 代理未配置、未执行线上代理测试；WebDAV 未配置。

## 账号副本

从旧 Python 服务 PostgreSQL 只读选择 1000 个状态正常、AT 剩余有效期超过 2 小时的账号：999 free + 1 Plus。

只复制 AT 和必要身份、套餐、指纹及来源元数据，**不复制 RT**，避免两个服务同时续期旋转 RT 干扰旧服务。未修改旧项目账号。导入通过服务器内存 stdin 完成，没有把账号导出文件下载到本地。

最终计数：账号 1000 / Plus 1 / 非空 RT 0 / enabled 1000。这是功能验收账号副本，不代表完成 1000 并发压测；副本 AT 到期后不具备 RT 自动续期能力。

## 最终镜像真实测试结果

| 功能 | 结果 | 耗时 |
| --- | --- | ---: |
| Chat Completions 普通文本 | HTTP 200，返回预期中文 | 3.774s |
| 多轮历史 | HTTP 200，仅返回“蓝鲸”，没有混入历史回答 | 3.713s |
| Chat Completions SSE | HTTP 200，6 个 data 事件，正确结束 `[DONE]` | 3.383s |
| Responses 普通文本 | HTTP 200，返回预期文本 | 4.301s |
| Messages 普通文本 | HTTP 200，返回预期文本 | 4.592s |
| Responses SSE | HTTP 200，包含 `response.completed` 终态 | 4.389s |
| Messages SSE | HTTP 200，包含 `message_stop` 终态 | 4.391s |
| 网页搜索 | HTTP 200，有回答及真实 Go 官方来源 URL | 13.637s |
| PPTX + 素材 ZIP | 从 queued/running 到 success，两份文件下载成功 | 252.468s |
| PSD + 素材 ZIP | 含参考图上传，从 queued/running 到 success | 95.623s |
| 原图片生成回归 | HTTP 200，本实例公开 URL 返回 PNG，下载 200 | 14.040s |

图片回归结果大小 1,115,596 bytes，PNG signature 正确；下载只在内存验证，没有额外落图片副本。以上耗时是单次样本，不是 P50/P95，也不是吞吐量数据。

文件测试使用一个 Plus 账号串行执行。PPTX 为 5 页，ZIP 容器 CRC 通过；PSD signature/version 正确，256×256、8 bit、3 channels、2 layers。素材 ZIP CRC 同样通过。未做人工版式、逐页可编辑性或 Photoshop 图层视觉质量验收。

### PPT 产物

- task：`filetask_ebf714f5-3f83-4c7b-85a8-cfd7ce2a58c8`
- PPTX：`http://108.186.180.104:9082/files/385647f4-97b1-42e3-abaa-151772c53a90.pptx`，212,919 bytes。
- ZIP：`http://108.186.180.104:9082/files/c0ffc4d9-a954-4a38-b486-09626881f060.zip`，375,542 bytes、13 entries。

### PSD 产物

- task：`filetask_a1d4b6df-bc64-408c-9724-aa90d791a44f`
- PSD：`http://108.186.180.104:9082/files/5359ec1d-5970-432a-abf5-8402d50d4df9.psd`，721,118 bytes。
- ZIP：`http://108.186.180.104:9082/files/5a5ce06e-7e7a-4075-a818-ed48d2d41c44.zip`，1,348 bytes、5 entries。

PPTX/PSD 返回 URL 均使用真实公网 IP + 9082，而不是 127.0.0.1。服务器内执行完整 GET 下载；另从本地对两个公网下载 URL 执行 HEAD 均为 200、正确 Content-Type 与 Content-Length。

## 附加验证

- 管理员认证后读取配置、账号列表、API Key 列表、监控接口均为 200。
- PPT 相同 `client_task_id` 与相同请求重放返回原 task，未重复创建生成任务。
- 文件任务列表、按 ID 批查与 missing_ids 返回正确。
- 无效 API Key 查询任务返回 401。
- 本实例下载超时设为 60s，适应较大文件；未改项目下载超时默认值。

## 真实验收中修复的问题

1. 多轮历史 assistant 消息被上游回显后重复返回：按请求消息 ID 排除输入历史。
2. 搜索/文件 prepare 触发 422：补齐 partial_query ID、正确 prepare state 及必要协议字段；无附件不发送 null MIME 列表。
3. 搜索 SSE 的 `delta_encoding: "v1"` 声明被误判为坏 JSON 消息：单独识别编码声明，保留其他坏事件的错误检查。
4. 原始文件/文本错误被“无其他可用付费账号”覆盖：没有下一账号时保留原始失败原因。

这些问题在先前验收镜像中实际出现，最终 `-4` 镜像已通过上述复测。搜索回答中仍可能包含上游 `<Link .../>` 展示标签；sources 含正常来源 URL。

## 本地验证和保留数据

- `go test ./...`：通过。
- `go vet ./...`：通过。
- 新增 live history / prepare / SSE 回归与文本 HTTP、原生搜索导出的定向 `go test -race`：通过。
- `git diff --check`：通过。
- 验收原始 JSON 记录在服务器新实例 `acceptance-4` 子目录；本地摘要在忽略的 `output/chat-search-live-20261103`。
- 验收脚本额外保存的 PPTX、PSD、ZIP 下载副本已删除；服务 images 持久卷 `.exports` 正式文件保留，公开 URL 仍可用。
- 本轮按用户要求仅构建推送 Docker 镜像与独立部署；没有提交或推送 Git 代码，没有合并分支。

## 2026-11-04 测试环境清理

用户确认后已删除本报告对应的 9082 测试实例、独立数据库/图片卷、服务器目录及测试镜像。以上测试结果为历史记录，PPT/PSD/ZIP 和图片 URL 已不可用。无凭据测试摘要与本报告保留在本地；9080 正式服务及旧 Python 服务未改动。清理详情见 `proxy-simple-live-validation-2026-11-03.md`。
