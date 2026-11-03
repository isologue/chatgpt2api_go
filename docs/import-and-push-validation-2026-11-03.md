# 2026-11-03 CPA / Sub2API 导入与账号推送验收

所有代码变更位于独立仓库 G:\Demo\chatgpt2api-go，main 分支。本轮未提交、未推送 Git，未更新原有业务容器。使用专用 PostgreSQL 和 fixture 容器验收，未把样本导入业务号池。本记录补充此前的 validation-2026-11-03.md。

## 交付行为

- 支持 CPA 单对象、Sub2API accounts 包、data 包装、普通数组及 Go 完整导出。CPA 样本的 type=web 已兼容。
- 从顶层、credentials、extra 和 auth_config/authConfig 提取 AT、RT、client_id、邮箱和指纹，支持 clientId 别名。缺 RT 但有 AT 可导入；仅 RT 无 AT 明确失败，导入过程不联网交换令牌。
- 原始账号保存在 source_record，包级 exported_at、proxies 等元数据保存在 import_context，未知字段及大整数完整保存。源代理、并发、优先级及模型映射保存为元数据，不自动改变本服务代理和调度设置。
- 同身份账号归并，不覆盖主账号当前的 AT/RT、评分和管理停用状态；补充凭据与元数据保存为 linked_accounts，相同内容重放不重复追加。关联凭据不会自动替换主凭据，与 Aether 的覆盖更新语义不同。
- 默认上传上限 128 MiB，设置页或 CORE_IMPORT_MAX_MIB 可调整。浏览器直接上传文件，服务器先写权限 0600 的临时文件并验证完整 JSON，再按每批 100 条解析入库；上传读取预算 5 分钟。交付后关闭页面继续，结束删除临时文件。CLI 复用解析器。
- 同进程导入与推送互斥。JSON 格式错误不会部分入库；账号字段错误逐条计失败，最多记录前 100 条。入库中数据库故障可能保留已提交批次，需查看任务结果并重试。

## 推送配置与协议

在「账号池 → 账号推送」启用入口，配置 16～256 字符的独立推送密钥。密钥明文持久化并同时保存 SHA-256 鉴权摘要，管理员重新打开弹窗可查看当前密钥。升级前仅存摘要的旧密钥仍可鉴权，但需重新保存一次才能回显。外部程序选择 Sub2API 模式，地址填 Go 服务根地址，密钥填推送密钥。

- GET /api/v1/admin/accounts/data：鉴权探活，返回空账号数组，不导出本地账号。
- POST /api/v1/admin/accounts/data：Content-Type 为 application/json，接受 Sub 包或 data 包装。支持 X-API-Key 或 Authorization: Bearer，等待批次入库后响应。
- 返回 code/message/data；data 包含 job_id、account_created、account_reused、account_failed、errors，以及代理计数字段。account_updated 固定 0，归并通过 account_reused 表示。代理计数为 0，因为仅保存源代理元数据。
- errors 含 kind=account、从 0 开始的 index、message；没有错误时为 []。部分成功返回 200，全部账号失败 400；关闭 404、无效密钥 401、已有导入/推送 409、体积超限 413、媒体类型错误 415。
- GET/PUT /admin/account-push 使用管理员密钥及 revision。推送密钥不能访问管理或完整导出。配置和任务持久化到 PostgreSQL。

## 实际文件验收

源文件只读挂载到专用容器。通过实际 HTTP handler 导入，随后完整导出，对每条原始记录和包级元数据规范化后逐一比较 SHA-256，不访问上游。

| 样本 | 规模 | 入口 | 导入用时 | 结果 |
| --- | ---: | --- | ---: | --- |
| CPA 单账号 | 13,355 bytes / 1 条 | /admin/accounts/import | 0.02 秒 | 任务、字段均通过 |
| Sub2API 多账号 | 43,478,627 bytes / 3,416 条 | /api/v1/admin/accounts/data | 4.19 秒 | 计数、字段均通过 |

共 3,417 条原始记录全部比对通过，包含归并后保存的关联记录。Sub 中 1 条没有 RT，但有 AT，可以保存。

## 大号池本地基线

TestPostgresLargeAccountImport 使用每条约 700 bytes 的合成账号和 PostgreSQL 16，计时包括 HTTP 上传、校验和分批入库。heap 每 10 ms 采样，可能漏采瞬时峰值；GC 后值包含账号内存索引。

| 行数 | 输入 | 时间 | 吞吐 | GC 后 heap | 采样 heap 峰值 | 表与索引 | 选号/释放 P95 / 最大 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 6.62 MiB | 1.59 秒 | 6,305 条/秒 | 25.44 MiB | 44.89 MiB | 9.74 MiB | 3 / 94 μs |
| 100,000 | 66.44 MiB | 15.86 秒 | 6,304 条/秒 | 242.44 MiB | 430.69 MiB | 97.30 MiB | 4 / 273 μs |

合成记录远小于真实样本，不能据此承诺真实十万账号的内存。真实样本平均约 12.7 KB，同等十万条超过默认 128 MiB，需要分包或提高限制。当前仍全量加载内存，保留原始记录及包元数据也增加内存和存储，未实施紧凑索引/LRU。选号采样只是 Pool.Acquire/Release，不是真实生图压测。

## 页面与回归

- Playwright 验证 CPA 粘贴导入、后台进度、账号列表；推送启用保存、最近任务；重启 fixture 容器后配置及任务恢复；此前验收版本不回显密钥，后续明文读回变更见下方补充。浏览器没有 error/warning，仅有密码框不在 form 中的 DOM verbose 提示。
- HTTP 验证：轮换前旧密钥 401、新密钥 200；推送密钥访问导出 401；关闭入口 404。
- go test ./...：通过，core 6.592 秒。
- go vet ./...：通过。
- go test -race ./...：通过，core 9.109 秒。
- 实际文件专项验收：通过，core 9.215 秒。
- node --check internal/core/console/app.js：通过。
- git diff --check：通过，只有既有 CRLF 转 LF 提示。
- Docker 镜像 chatgpt2api-go:import-push-validation 构建通过，配置摘要 e6dc728580cf42a3e7b8e13c50eddf20925f59f5bc81575719caeed92243fdcf。未更新业务容器。

脱敏输出在 ignored output/import-validation/，页面截图在 output/playwright/。专用测试容器 c2go-import-ui 和 c2go-import-pg 已停止。删除凭据副本及移除测试容器的命令被自动审批以 blocked by policy 拒绝，未提供具体原因，因此专用数据库、匿名卷、网络和副本暂时保留；不通过其他工具绕过删除限制。

## 真实上游未通过项

样本 AT 均已过期。只用 CPA 一个账号，经本机现有 HTTP 代理尝试真实链路，首次同步 URL 场景在 auth.openai.com/oauth/token 的 RT 续期阶段返回上游 403，接口返回 502 / upstream_access_denied，尚未进入图片生成。后续同步 base64、异步 URL 未运行，不能宣称真实出图通过。

此次 AT/RT 均未变化。防止 RT 旋转丢失而建立的派生凭据文件已核对 AT/RT 未变化，但因清理命令被审批拒绝暂未删除，位置为 output/import-validation/refreshed-cpa-account.json（被 Git 忽略）；专用测试 PostgreSQL 也仍保留样本数据。原始文件未改动，未保留额外下载图片。TestProvidedAccountRealAPI 默认跳过，只有显式提供单账号文件及可持久化输出路径才运行；上游恢复后仍需补验真实三种响应场景。


## 推送密钥读回补充验收

按要求将推送密钥明文保存到 core_account_push.key_value，管理接口返回当前密钥，弹窗以文本输入框回填。原有 key_hash 继续用于鉴权；旧配置仅有摘要时保持有效，需重新保存一次才能回显。空输入保留原密钥，版本冲突保护继续生效。

- go test ./...：通过（独立 PostgreSQL，core 6.797 秒），覆盖保存、重新读取、留空保存、轮换、旧摘要兼容和管理权限隔离。
- go vet ./...、node --check internal/core/console/app.js、git diff --check：通过。
- 独立 fixture 容器 HTTP 验证保存、重新读取、容器重启后读回：通过；包含 HTML 特殊字符的密钥保持原值，管理响应 Cache-Control 为 no-store。未覆盖用户业务推送配置。
- 本地 compose.yml up -d --build：完成，core 与 PostgreSQL healthy；readyz 和管理页均为 200，已确认提供新版 app.js，运行镜像与本次构建一致。
- 本轮 fixture 容器与专用 PostgreSQL 已停止。未推送 Git，未操作线上。
