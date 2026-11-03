# 图片编辑与调用控制验收记录

日期：2026-11-03（北京时间）

工作目录：G:\Demo\chatgpt2api-go。旧 Python 项目仅作只读对照。

## 已交付

- 调用日志保存 Key ID 与调用时名称快照，支持 Key 筛选；独立统计请求数、成功率、成功图片数和成功任务 P50/P95。
- Key 并发限额支持 0 不额外限制，统计排队和执行任务，异步直到终态归还；轮换不丢失在途计数。
- admin-reset 本地恢复管理员密钥，要求停止核心服务，清除旧会话，保留账号及调用 Key。
- PostgreSQL 持久化 Idempotency-Key，作用域包含调用 Key 和接口路径；参数变化返回 409；断连不取消已接纳任务，重启不重复提交上游。
- /v1/images/edits multipart 上传参考图与可选 PNG mask，支持同步和异步；输入生命周期、原始报文磁盘记录及日志回收均已接入。

## mask 行为

对照旧项目 services/protocol/openai_v1_image_edit.py：RGBA/带透明通道 PNG 取 alpha，其他灰度/RGB PNG 取灰度，合成到参考图后上传。透明表示可编辑，不透明表示保留。多图按顺序配对，遮罩不足复用最后一张；尺寸不同进行缩放。Go 使用 Catmull-Rom，旧版 Lanczos，边缘不保证逐像素一致。

## 自动化验证

- Docker golang:1.27.3-alpine 中 go test ./...、go vet ./... 通过。最后一次完整普通测试 internal/core 8.973s。
- 完整 go test -race ./... 与 go vet ./... 已通过一轮（internal/core 19.725s）；随后补充 Key CRUD/轮换测试及日志回收断言，普通全套再次通过。
- node --check internal/core/console/app.js 通过，git diff --check 无空白错误。
- PNG alpha/灰度/RGB 像素合成断言，模拟上游文件申请、签名 PUT、上传确认、附件元数据、同步与异步生成、终态输入清理验证通过。
- PostgreSQL 集成覆盖 24 个并发重复请求只生成一次、409 冲突、断连续跑、失败状态重放、重启终态、Key 过滤与统计、原始 multipart 字节还原与日志文件回收、管理员恢复租约和数据保留。

## 浏览器验收

使用独立 imagecore_controls_ui 测试数据库及虚构 Key，模拟上游不可连接，未使用业务账号发起生成。

- 修复并验收 Key 统计和编辑按钮的多元素选择器绑定。
- 新增 Key，设置并发 3，保存后重新显示的值为 3，列表显示 0 / 3。
- 统计弹窗今天、昨天、近 7 天、近 30 天均返回 200，展示 P50/P95。
- 日志按 Key 筛选实际请求带 api_key_id，返回 200。
- 页面刷新保持管理员登录。
- 结果与存储中显示编辑请求大小默认 32 MiB、参考图张数默认 4。
- 接口说明展示 edits、mask、Idempotency-Key 与 Key 并发含义。
- 浏览器日志未见运行异常；仅有密码表单建议补用户名的浏览器提示。

截图位于 output/playwright/key-statistics.png 和 output/playwright/log-key-filter.png（本地忽略产物）。

## 本地部署

执行 docker compose -f .\compose.yml up -d --build，core 与 postgres 健康，GET /readyz 返回 {"ready":true}。

管理页：http://127.0.0.1:9080/admin/

镜像：chatgpt2api-go:prototype，配置 ID sha256:667e34ec3d3c90b52ab5bd0911b01ee683dcb800edadf089caf033a9cd82adde。

## 验证边界

本轮未做真实上游图片编辑质量验收，模拟上游测试只能证明附件与合成图片确实进入协议链路。模型对保留区域的实际处理需后续真实请求验证。幂等记录随日志终态任务清理过期，已清理图片不能重放可访问结果。未进行 Git 提交、远程推送或线上部署。
