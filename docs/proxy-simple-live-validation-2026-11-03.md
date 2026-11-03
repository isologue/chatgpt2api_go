# 2026-11-03 简化代理线上验收

## 部署

- 仓库：`G:\Demo\chatgpt2api-go`，分支 `codex/chat-search-proxy`。
- 镜像：`milodk/chatgpt2api-go:proxy-simple-20261103-1`。
- Registry digest：`sha256:3a50209202386105e1eba6ee56b473ce97beeec12a4e97121576e69aa82908a0`。
- 复用独立测试实例：`http://108.186.180.104:9082/admin/`。
- 服务器目录：`/www/wwwroot/chatgpt2api_go_chatsearch`，Compose project `c2go-chatsearch`。
- 仅更新测试实例 core；未覆盖 latest，未修改 9080 正式实例、旧 Python 服务或其数据库。
- 更新前备份 Compose、环境文件及测试实例 PostgreSQL dump：`/www/wwwroot/chatgpt2api_go_chatsearch/backups/proxy-simple-20261103-152456`。备份目录 0700，环境文件及 dump 为 0600。
- 更新前后比对受保护容器的 ID / StartedAt，正式服务及测试实例 PostgreSQL 均未变化。9080 / 9082 公网 readyz 均 HTTP 200。

## 已保存的代理配置

连接方式为 proxy，保存用户提供的四个代理，保持原顺序。输入的 host:port:user:password 转换为 HTTP 代理 URL；端口 443 实测支持该方式。本文不记录代理密码、账号 AT 或管理密钥。

为明确区分每个代理，真实链路验收期间临时逐个启用单条代理，完成后恢复完整四条列表。配置已写入 PostgreSQL；重启测试实例后逐项比对配置，仍为 proxy + 四条地址。

应用内连通测试固定访问上游首页，无账号、无生成；四项均 HTTP 200：

| 代理序号 | 首页耗时 | 聊天耗时 | 生图耗时 | 返回格式 |
| --- | ---: | ---: | ---: | --- |
| 1 | 824 ms | 8.037 s | 15.453 s | url |
| 2 | 777 ms | 5.076 s | 16.514 s | url |
| 3 | 1173 ms | 5.954 s | 16.645 s | url |
| 4 | 792 ms | 21.724 s | 22.420 s | b64_json |

## 真实上游结果

- 四个代理各执行一次 Chat Completions 与一次 gpt-image-2 生图，8/8 HTTP 200。聊天返回各自指定的 PROXY_OK 标识。
- 三个 url 结果均正确使用公网 IP + 9082；从服务端 GET 均 HTTP 200、PNG 签名正确。
- 第四个代理返回 b64_json，严格 Base64 解码后为 PNG，1,700,358 bytes。
- 第一代理额外执行 multipart 参考图上传、编辑生成、结果下载，HTTP 200，19.089 s，结果 PNG 1,374,024 bytes。
- 合计 9/9 真实业务请求 HTTP 200。这是单次功能验收，不是并发压测，不代表长期成功率或 P95。
- 另用服务器 curl 通过同一组代理访问固定上游 `/cdn-cgi/trace`，四条均 HTTP 200，观察到四个不同出口，loc 均为 US。这是独立出口诊断，不是应用内逐阶段抓包，也不能证明供应商长期保持同一 IP。
- 未执行新的 PPT/PSD 大文件任务；其下载路由此前有本地模拟回归，本次真实验收覆盖图片与编辑链路。

## 数据与记录

- 线上无凭据测试摘要：`/www/wwwroot/chatgpt2api_go_chatsearch/proxy-acceptance-20261103/` 下的 `connectivity.json`、`real-chain.json`、`final.json`。
- 本地无凭据摘要：`G:\Demo\chatgpt2api-go\output\proxy-live-20261103`。未下载账号数据或结果图片副本到本地；图片/base64 的验证在服务器内存完成。
- 服务正常生成的图片和调用日志保留在测试实例的数据卷/数据库，未删除其正常业务记录。
- 含用户输入代理密码的临时本地配置脚本已删除；运行中的代理配置按用户要求保留在专用管理员配置与数据库中。
- 未提交或推送 Git 代码；本次仅构建、推送自定义 Docker tag、部署独立测试实例及验收。

## 2026-11-04 测试环境清理

用户确认功能后，已删除服务器 `c2go-chatsearch` 独立测试实例：两个容器、独立 PostgreSQL/图片卷、默认网络及 `/www/wwwroot/chatgpt2api_go_chatsearch` 整个目录（含账号副本、代理配置、图片、日志、测试备份和验收原始记录）。本文中的测试结果和产物 URL 是历史验收记录，9082 已停止提供服务，产物 URL 不再可用。

删除服务器上四个测试镜像 tag：`proxy-simple-20261103-1`、`chat-search-20261103-4`、`chat-search-20261103-3`、`chat-search-20261103-1`。Docker Hub 镜像和本地镜像未删除。22 个非测试容器 ID、启动时间和运行状态比对均未变化，9080 正式服务 readyz 仍为 HTTP 200。

代码验收后合入 main；本次清理没有将正式服务升级为该版本。无凭据验收摘要保留在本地 output 及此文档。
