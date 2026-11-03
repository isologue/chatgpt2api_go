# 图片编辑、调用 Key 与幂等调用

## 图片编辑

`POST /v1/images/edits` 使用图片调用 Key 鉴权，接收 `multipart/form-data`。同步和异步共用路径，`async=true` 返回任务 ID，查询仍为 `GET /v1/images/tasks/{task_id}`。

支持字段：`prompt`（必填）、`model`、`n`、`size`、`quality`、`response_format`、`async`。上传文件字段为 `image`/`image[]` 和可选的 `mask`/`mask[]`。参考图支持 PNG、JPEG、WebP，遮罩使用 PNG。暂不接受编辑 JSON 图片 URL 或 Base64 输入；返回格式可选 URL 或 Base64，沿用系统默认和允许用户覆盖的设置。

```powershell
curl.exe http://127.0.0.1:9080/v1/images/edits -H "Authorization: Bearer <图片调用密钥>" -H "Idempotency-Key: edit-order-20261103-1" -F "prompt=把透明区域替换为一片花园" -F "image=@input.png" -F "mask=@mask.png" -F "response_format=url" -F "async=true"
```

遮罩沿用旧 Python 项目的处理方式：RGBA PNG 取透明通道，透明处允许编辑，不透明处保留；灰度/RGB PNG 取灰度值。多图按顺序配对，缺少的遮罩复用最后一张；遮罩尺寸不同时缩放到参考图尺寸，再合成为 PNG 上传。Go 使用 Catmull-Rom 缩放，旧版使用 Lanczos，边缘插值不会逐像素相同。上游生成仍受模型行为影响，不能保证保留区域逐像素不变。

`CORE_EDIT_MAX_MIB=32` 限制整个上传请求，超出返回 413；`CORE_EDIT_MAX_IMAGES=4` 限制参考图张数，与输出 `n`（1～4）独立。两项可在设置页在线调整。单张输入最多 6400 万像素，完整解码及遮罩合成共用 `CORE_VALIDATION_CONCURRENCY`；每次切号会用新账号重新上传。上传的元数据、内容 PUT 和确认步骤受请求预算及 `CORE_METADATA_TIMEOUT` 控制。

输入写私有临时目录；任务接纳后由任务管理生命周期，异步创建响应不会提前删除文件。完成、失败或准入拒绝后清理，重启先将未完成任务标为失败，再清理残留输入。原始 multipart 报文独立存磁盘，日志详情不尝试转成 JSON，支持下载真实原始请求；日志保留策略同时回收报文文件。

## Key 并发与统计

「设置 → 访问与密钥」中，每个调用 Key 可配置并发上限，0 表示不施加 Key 级上限。每个排队或正在执行的逻辑请求占一个名额，同步和异步相同，`n>1` 仍算一个请求。仍受全局任务执行数、队列容量、账号槽位、下载/解码等容量约束。

Key 名额不足返回 `429 api_key_concurrency_full`，全局执行加队列满返回 `429 image_queue_full`。异步响应返回后继续持有名额直到任务终态。调低上限不取消在途任务，轮换密钥保留稳定 ID 和在途计数；停用、删除不取消已接纳任务。重放同一幂等请求不占第二份任务名额。

日志列表可按 Key 筛选，摘要保存 Key ID 和调用时名称快照。Key 的「调用统计」可选今天、昨天、近 7 天和近 30 天，显示请求数、出图数、成功率、成功任务 P50/P95 与实时在途数。安全审核拒绝独立计数，不进入成功率分母。耗时从任务接纳到生成完成，包含排队。幂等重放不重复统计。

管理读取接口：`GET /admin/api-key-stats?api_key_id=<id>&period=today`；日志筛选：`GET /admin/requests?api_key_id=<id>`。统计查询使用 5 秒数据库预算，列表沿用索引游标分页，不加载所有日志正文。

## 持久化幂等

在生成或编辑请求头中传 `Idempotency-Key`（1～256 个可打印、无空白的 ASCII 字符）。作用域是「调用 Key 的稳定 ID + 接口路径 + 幂等键」。同作用域且请求相同，只接纳一次生成；重复异步请求返回同一任务 ID，重复同步请求等待并重放已持久化结果，响应头带 `Idempotency-Replayed: true`。原始调用日志和报文不会被重放覆盖。

同一作用域改传参数或图片/遮罩内容返回 `409 idempotency_conflict`。JSON 忽略字段顺序和空白；编辑忽略 multipart boundary 和表单排列顺序，但图片和遮罩各自的顺序、文件名、文件内容以及请求参数参与指纹。显式默认参数和省略参数可能视为不同请求，请重试时保持参数一致。

幂等映射和任务在同一事务持久化。第一次 HTTP 连接断开不会取消已持久化的幂等任务；服务重启把未完成任务标为 `server_restarted`，并返回原任务失败结果，不自动重新发图。要主动重试一次新的生成，请使用新的幂等键。

幂等保证限于记录和结果的保留期间：日志清理会删除超过保留期的终态任务及对应映射，此后再次使用原键可以创建新任务。图片清理会让已保留的结果文件失效，无法重放图片；请将重试窗口、日志保留时间和图片保留时间一起配置。当前部署通过数据库独占调度租约运行一个核心实例，幂等不是跨多个调度实例的分布式队列。

## 管理员密钥遗失恢复

先停止当前数据库对应的所有 core 服务，保留 PostgreSQL。恢复命令必须成功获取独占调度租约，在线运行时会拒绝。通过安全 stdin 管道提供新密钥，不把真实密钥写在命令参数或命令历史中。PowerShell 示例：

```powershell
docker compose -f .\compose.yml stop core
$adminResetSecret = Read-Host '新管理员密钥（16～256 字节）' -AsSecureString
$adminResetPlain = [System.Net.NetworkCredential]::new('', $adminResetSecret).Password
$adminResetPlain | docker compose -f .\compose.yml run --rm --no-deps -T --entrypoint /usr/local/bin/admin-reset core
Remove-Variable adminResetPlain, adminResetSecret
docker compose -f .\compose.yml up -d core
```

确认恢复命令成功后再启动 core；失败时先按错误提示处理。恢复会更新管理员密钥、撤销原会话，不修改图片调用 Key、账号或业务数据。直接在交互终端输入 stdin 可能回显，使用上面的安全输入方式。
