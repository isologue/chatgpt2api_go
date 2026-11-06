# 图片编辑、调用 Key 与幂等调用

## 图片编辑

`POST /v1/images/edits` 使用图片调用 Key 鉴权，接收 `multipart/form-data` 或 `application/json`。同步和异步共用路径，`async=true` 返回任务 ID，查询仍为 `GET /v1/images/tasks/{task_id}`。

支持字段：`prompt`（必填）、`model`、`n`、`size`、`quality`、`response_format`、`async`。上传文件字段为 `image`/`image[]` 和可选的 `mask`/`mask[]`。参考图支持 PNG、JPEG、WebP，遮罩使用 PNG。JSON 通过 `images: [{"image_url": "https://..."}]` 提供参考图，也兼容 `image` 数组和对象内的 `url` 别名；`images` 与 `image` 不可同时提供。可选 `mask` 使用相同对象数组格式。输入支持无需登录的 HTTP(S) 地址和 `data:image/png|jpeg|webp;base64,...`，Base64 可带标准填充或省略填充，也支持 CR/LF 换行；不接受本地路径、blob URL 或无前缀的裸 Base64；返回格式可选 URL 或 Base64，沿用系统默认和允许用户覆盖的设置。

```powershell
curl.exe http://127.0.0.1:9080/v1/images/edits -H "Authorization: Bearer <图片调用密钥>" -H "Idempotency-Key: edit-order-20261103-1" -F "prompt=把透明区域替换为一片花园" -F "image=@input.png" -F "mask=@mask.png" -F "response_format=url" -F "async=true"
```

JSON 图片编辑示例（请求头 `Content-Type: application/json`）：

```json
{
  "model": "gpt-image-2",
  "prompt": "参考图片1，将衣服换成黑色，保留人物和背景",
  "n": 1,
  "size": "1024x1024",
  "async": true,
  "images": [
    {"image_url": "https://images.example.com/reference.png"}
  ],
  "response_format": "url"
}
```

也可将 `image_url` 替换为 `data:image/jpeg;base64,/9j/4AAQ...`，`mask` 同样支持 Data URL（实际内容必须为 PNG）。HTTP 地址与 Data URL 可混用，顺序不变。服务端先下载或流式解码并校验参考图，再进入与 multipart 相同的上传和生图流程。JSON 的参考图/遮罩下载遵循系统直连/代理设置，共享 `CORE_DOWNLOAD_CONCURRENCY`；单张下载含排队受 `CORE_DOWNLOAD_TIMEOUT` 限制，所有参考图及遮罩合计受 `CORE_EDIT_MAX_MIB` 限制，过大返回 413。URL 需从部署服务器可访问；不会转发调用方密钥、Cookie 或账号 AT，最多跟随 5 次 HTTP(S) 重定向。下载失败直接返回 `image_input_download_failed`，不会因此选择账号或扣分。异步创建响应需等待参考图下载校验完成；任务接纳后继续持有本地输入直到终态。Data URL 在本地解码，不发起下载、不占用远程下载名额；实际图片格式根据内容识别，不信任声明的 MIME。输入大小限制与 JSON 请求体大小分别检查，Base64 编码约膨胀三分之一，整个 JSON 请求仍受 `CORE_EDIT_MAX_MIB` 限制。

模型名按系统模型映射验证，例如使用 `gpt-image-2.6` 前需在设置中启用对应的图片模型映射。`response_format` 由本地控制返回 URL 或 Base64，不决定 PNG/JPEG/WebP 编码。`output_format` 等额外参数会透传给实际会话上游，不在本地强制转码，上游是否支持和生效以实际返回为准。

### 额外参数透传

JSON 文生图、JSON 图生图和 multipart 图生图均接收额外参数（如 `negative_prompt`、`moderation`、`output_format`、`background`、`seed` 及未来参数），不再因未知的普通表单字段返回 `unsupported_form_field`，也不静默丢弃 JSON 额外参数。额外参数作为原名顶层字段发送到实际 ChatGPT 会话上游的 prepare 和生成请求；这不是标准 OpenAI 图片 API 的直接转发，接收不代表上游一定支持或执行，也不会关闭安全审核拒绝识别。

- JSON 额外参数保留字符串、数值、布尔、对象、数组与 `null` 类型，包括大整数精度。
- multipart 普通文本参数保留字符串；带 `Content-Type: application/json` 的额外 part 保留 JSON 类型。重名额外字段按顺序组成数组；已知核心字段重复仍会报错，避免歧义。
- `prompt/model/n/size/quality/response_format/async/stream` 继续走现有映射、校验及本地处理；图片和遮罩继续走上传流程，不直接把 Base64 或本地路径塞入会话信封。`upstream_model` 不能绕过模型映射。
- 系统构造的会话协议字段（例如 `messages`、`partial_query`、`model`、`action`、会话 ID、附件元数据）不会被额外参数覆盖；请求体参数不会变成鉴权请求头、代理或服务配置。
- 请求大小、单个表单字段长度及文件格式等限制保持不变；任意二进制文件字段并不因此开放，文件仍只接受 `image`/`image[]`、`mask`/`mask[]`。
- 原始请求日志仍保留调用方报文。额外参数进入任务存储和幂等指纹，修改额外参数后不能用旧幂等键错误重放旧结果。

遮罩沿用旧 Python 项目的处理方式：RGBA PNG 取透明通道，透明处允许编辑，不透明处保留；灰度/RGB PNG 取灰度值。多图按顺序配对，缺少的遮罩复用最后一张；遮罩尺寸不同时缩放到参考图尺寸，再合成为 PNG 上传。Go 使用 Catmull-Rom 缩放，旧版使用 Lanczos，边缘插值不会逐像素相同。上游生成仍受模型行为影响，不能保证保留区域逐像素不变。

`CORE_EDIT_MAX_MIB=32` 限制整个上传请求，超出返回 413；`CORE_EDIT_MAX_IMAGES=4` 限制参考图张数，与输出 `n`（1～4）独立。两项可在设置页在线调整。单张输入最多 6400 万像素，完整解码及遮罩合成共用 `CORE_VALIDATION_CONCURRENCY`；每次切号会用新账号重新上传。上传的元数据、内容 PUT 和确认步骤受请求预算及 `CORE_METADATA_TIMEOUT` 控制。

输入写私有临时目录；任务接纳后由任务管理生命周期，异步创建响应不会提前删除文件。完成、失败或准入拒绝后清理，重启先将未完成任务标为失败，再清理残留输入。原始 JSON 请求按字节保留在调用报文中；原始 multipart 报文独立存磁盘，日志详情不尝试转成 JSON，支持下载真实原始请求；日志保留策略同时回收报文文件。

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
