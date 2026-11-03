# 账号维护、迁移与备份恢复

此说明对应 Go 独立仓库，不操作 Python 项目的容器、工作树或数据库。

## 配置与活性

| 配置 | 默认 | 用途 |
| --- | --- | --- |
| `CORE_AVAILABILITY_TTL` | 900s | 生图选择账号后，最近成功校验或生图活性任一在有效期内即可复用；AT 临期检查始终保留 |
| `CORE_VERIFY_CONCURRENCY` | 50 | 每条通道独立的账号刷新并发 C |
| `CORE_HOT_POOL_SIZE` | 500 | 基础热池规模；不限制生图任务并发 |
| `CORE_HOT_IDLE` | 24h | 热池超出基础规模时，闲置账号退出时间 |
| `CORE_HOT_MAINTENANCE_INTERVAL` | 30m | 热池维护使用每账号最近活性判定到期 |
| `CORE_COLD_MAINTENANCE_INTERVAL` | 24h | 冷池低频维护使用每账号最近活性判定到期 |

设置页用秒显示时间；环境变量支持 Go duration（例如 `900s`、`30m`、`24h`）。数据库中保存的在线配置优先于环境变量，修改 `.env` 不覆盖已保存的相同配置。

通道分别是前台按需校验、手动校验、热池补充、热池维护、冷池维护。每条通道各 C 个名额，不共用一个总 C；同一个账号的并发校验合并为一个实际刷新，其他调用者等待相同结果，避免 RT 重复旋转。调用方等待受上下文预算控制；共享校验本身独立存在，最多使用三倍元数据超时。配置正整数可高于 1000，没有额外业务并发封顶；实际吞吐仍受数据库、连接、CPU 和上游约束。

使用冷账号会将其加入热池，热池随需求扩张；正在使用的账号不会被闲置回收。额度耗尽、失效及刷新失败的账号退出热池，后台补充基础规模。热池整理每 30 秒检查一次；刷新驱动使用有界在途窗口，最多保持各 C 个，不为整个号池创建 goroutine 或任务队列。成功校验、生图成功、安全审核拒绝更新活性；安全审核拒绝不加减分。明确未出图仍扣分。

维护按持久化的 `verified_at` / `success_at` 判断，而非按容器启动时间重新计时。刷新失败记录错误分类和重试时间，并指数退避（30 秒起，最多 3840 秒）；同一失败只计一次。管理停用优先，旧快照写回不会重新启用管理停用的账号。

上游 `/backend-api/me` 更新账号信息，`/backend-api/conversation/init` 的 `image_gen` 限额更新图片额度与恢复时间。额度未知不会伪造成 0；已知为 0 的账号停止选号，恢复时间到期后允许再次确认。已知额度按单张成功结果递减，它是本实例观测值，不能代替上游最终额度。账号仍在启动时完整加载到内存，生图选号沿用内存堆索引。

## 完整导入导出

控制台「账号池 → 完整导出」或管理员接口 `GET /admin/accounts/export` 返回完整 JSON 数组，包括 AT、RT、密码、设备信息、评分、额度、时间、未知扩展字段和 `linked_accounts`。普通列表和详情继续使用尾号或状态显示，完整凭据在下载中提供。

身份由 AT 的用户和工作区标识计算，不按邮箱归并，也不信任导入的 `identity_id`。可识别同身份的多 AT 归并成一个调度对象；其余完整记录作为 `linked_accounts` 保存，避免凭据丢失，不自动轮用归并记录。同 AT 重复导入跳过；同 ID 不同身份冲突会报错。运行中主记录不会因再次导入而被替换。启动旧数据归并每 256 条提交一次，可以中断重跑。归并不转移已经开始的生成任务，因为在启动调度前执行。

JSON 大整数用原始数字保留。旧格式不兼容字段保存在原始记录元数据中。导出服务端单次查询流式输出，不建立全量导出数组；页面直接保存 Blob，不再完整解析为 JavaScript 对象。流中途失败会终止传输。导入原项目账号前停止原服务对同一 RT 的使用。

账号导出只迁移账号；任务、调用记录、设置、图片和原始报文必须使用完整备份。

## 备份

需要 Docker Compose 和 PowerShell 7。备份期间短暂停止本项目 `core`，确保数据库与图片文件一致；PostgreSQL 保持运行。脚本默认使用根目录 `compose.yml` 和 `.env`，项目名默认 `chatgpt2api-go`，可显式覆盖。

```powershell
pwsh -File .\scripts\backup.ps1 -Destination G:\Backups\chatgpt2api-go-20261103 -ProjectName chatgpt2api-go -ComposeFile .\compose.yml -EnvFile .\.env
```

目标必须是新目录。包含 `database.dump`、`images.tar.gz`（含 `.request-logs`）、`environment.env`、`compose.yml`、`manifest.json`。清单记录长度及 SHA256；凭据保存在备份内，应按数据库备份管理。`backups/` 已被 Git 和 Docker build 忽略，其他目录也应放在仓库外。失败的半成品不能用于恢复。

## 恢复

先准备目标环境文件与 Compose，核对端口、目标 PostgreSQL 主版本和卷名。选择新项目名不会自动避开宿主端口冲突。恢复脚本使用指定的目标配置，不自动覆盖本地 `.env`；需要时从备份 `environment.env` 复制到目标后按部署调整。PostgreSQL dump 应恢复到相同或兼容的更新版本。

```powershell
pwsh -File .\scripts\restore.ps1 -Source G:\Backups\chatgpt2api-go-20261103 -ProjectName chatgpt2api-go-restored -ComposeFile .\compose.yml -EnvFile .\restore.env
```

脚本校验全部备份文件，启动 PostgreSQL，检查空数据库和空图片卷，停止目标 `core`，恢复数据库与文件，最后启动 `core`。默认拒绝非空目标。显式 `-ReplaceExisting` 会替换目标数据库和图片卷，应先备份目标；恢复失败保留 `core` 停止状态，不能直接宣称恢复成功。

验收顺序：`/readyz` → 管理页账号/日志/图片数 → 完整导出字段 → 原图与原始报文 → 设置与热池时间。重启会把未完成任务标为失败，不重发可能已经计费的生成。

## 测试与边界

常规测试、PostgreSQL 测试、竞态检测、模拟 200/500/1000 并发和持续负载测试均可在本地运行。`CORE_RUN_ACCOUNT_SOAK=1` 配合独立 `CORE_TEST_DATABASE_URL` 启用持续测试，`CORE_ACCOUNT_SOAK_SECONDS` 默认 120 秒，可延长。测试会清空测试库相关表，禁止连接业务数据库。

本轮不新增 webhook。仍为单调度实例；不能用多个副本共享数据库来叠加并发。模拟上游的成功率与耗时只验证本地调度、协议及持久化，不代表真实上游成功率、带宽或全天稳定性。
