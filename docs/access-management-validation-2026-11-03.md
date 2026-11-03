# 管理员会话与调用密钥验收（2026-11-03）

工作目录：G:\Demo\chatgpt2api-go。仅更新本地 Go 项目，未提交或推送 Git，未操作线上服务。

## 实现

- 管理员登录改为 HttpOnly、SameSite=Strict Cookie。浏览器保存 400 天，每次读取会话续期；服务端没有空闲登出计时。清除浏览器 Cookie 后需重新登录。
- 单管理员单会话：新登录替换旧会话，旧浏览器下一次管理请求返回 401；同一浏览器的多个标签共享 Cookie。
- 会话摘要、管理员密钥哈希和图片调用密钥存入 PostgreSQL，容器重启后仍保留。
- 设置 > 访问与密钥：修改管理员密钥，新增、查看、修改、启用、停用和删除图片调用密钥。
- 修改管理员密钥需要验证当前密钥，成功后更新当前浏览器会话；旧管理员密钥立即失效。
- 图片接口使用不可变内存快照鉴权，不为每个图片请求查询数据库。
- CORE_ADMIN_KEY 和 CORE_API_KEY 仅首次初始化访问配置；后续数据库配置优先，删除默认调用密钥后重启不会重建。
- 调用密钥共享号池和任务访问范围，不提供租户隔离或独立计费。

## 验证

- Docker Go 1.27.3 全量 go test ./...、go vet ./... 通过。
- 启用 CGO、安装 build-base 后，go test -race ./... 和 go vet ./... 通过，core race 检查耗时 13.324 秒。
- 并发测试期间发现并修正测试自身的共享变量读写，实际鉴权快照无需改动。
- node --check internal/core/console/app.js 通过。
- git diff --check 通过，仅有已有 CRLF 提示。
- Playwright 使用独立空数据库 imagecore_access_ui、容器 c2go-access-ui 和虚构密钥验证，未修改业务管理员密钥或调用密钥。
- 实际浏览器验证登录、刷新自动恢复、管理员改密钥后保持当前会话、错误当前密钥不登出、另一浏览器登录使原会话失效、主动退出后刷新仍要求登录。
- 实际页面验证调用密钥自动生成、名称与密钥修改、停用、启用、删除；通过 GET /v1/models 确认鉴权分别立即变为 200 或 401。
- 重启测试容器后原浏览器 GET /admin/session 返回 200，已删除调用密钥仍不存在，数据库管理员新密钥覆盖旧环境值。
- 600px 宽度无横向溢出；桌面和窄屏截图保存在忽略目录 output/playwright/。
- 浏览器未发现 JavaScript 运行异常；400/401 为错误密码、未登录及会话替换场景的预期响应。

## 本地部署

已执行 docker compose -f .\compose.yml up -d --build。

- 管理地址：http://127.0.0.1:9080/admin/
- GET /readyz：200。
- core、postgres：healthy。
- 业务容器运行镜像与 chatgpt2api-go:prototype 标签一致：sha256:b76c342a242241ba50c762ba29a6885fc32c0b45b0e994d7be7052195ae29c8e。

测试结束关闭浏览器并停止本轮测试容器及专用 PostgreSQL，业务容器保持运行。