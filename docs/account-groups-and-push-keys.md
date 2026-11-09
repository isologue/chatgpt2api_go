# 账号分组与多个推送 Key

## 页面使用

账号池列表上方新增「账号分组」选择器与「管理分组」。一个账号只属于一个分组；「全部账号」是全池视图，「未分组」是不可删除、不可改名的内置分组。

- 管理分组：新增、改名、修改备注、删除。
- 列表每行显示分组，点击「调整分组」移动单个账号；选中账号后可批量移动。
- 「选择全部筛选结果」由服务端计算匹配数量，按分组、搜索、状态筛选执行，不向浏览器下发全部 ID。
- 切换分组、搜索或状态时清空选择。批量移动、删除以及刷新额度/AT沿用后台进度与通知。
- 顶部运行概览仍为全池数据，不受分组筛选影响；列表与操作使用当前分组。
- 完整导出遵守当前分组、搜索和状态，仍包含全部凭据，同时增加 group_id、group_name、group_description。
- 导入可以明确选择目标分组（默认当前分组或未分组），也可以选择「按导出文件中的分组恢复」。已有同名分组复用，不覆盖其名称与备注。

## 账号推送

在「账号推送」新增多条推送 Key，每条配置独立名称、启停状态、Key 和目标分组。多个 Key 可对应同一分组。

接收路径与 Sub2API/CPA 兼容格式不变：

```text
GET  /api/v1/admin/accounts/data   测试连接
POST /api/v1/admin/accounts/data   推送账号
X-API-Key: <push key>
# 也支持 Authorization: Bearer <push key>
```

目标分组只由认证 Key 决定，请求体的 group_id / group_name 不能覆盖。认证后每批（100 条）提交前复核 Key：停用、删除、换 Key 或重新绑定分组会停止后续批次，已完成批次保留。修改名称不会中断任务。

重复账号继续沿用现有身份/AT指纹去重与安全归并：保留正在使用的凭据、评分、停用标记，新凭据与元数据保存在关联记录中。

- 同组：复用/归并账号。
- 原账号未分组：可归入 Key 对应的分组。
- 原账号已有其他分组：保留原分组，增加 group_conflicts，不计为账号导入失败。
- 修改 Key 绑定不迁移历史账号；历史归属使用管理员移动操作调整。

推送响应增加 data.group_id 和 data.group_conflicts，保留 account_created、account_reused、account_failed、job_id、errors 等原字段。account_reused 仍包含安全归并，不冒充替换了在用凭据的 account_updated。

Key 明文保存在数据库并在管理员页面显示；鉴权使用哈希索引查找。Key 不出现在账号列表和运行日志，不具有导出、管理或生图权限。

## 删除和中断恢复

删除分组必须明确选择账号迁移目标；先分批移出账号，再删除空分组，不删除账号。仍有推送 Key 绑定时（包括停用的 Key），必须先解绑或删除 Key。

分组开始删除后标记为 deleting，禁止新导入、推送或移动进入；已有账号仍照常参与原来的生图调度。任务失败/重启后已移动的数据保留；管理分组中的「继续删除」可以重新选择目标并继续。任务进度存入现有 core_admin_jobs。

## 数据库与性能

- core_account_groups：分组名称、备注、状态、修订号。
- core_accounts.group_id：独立外键列，默认 ungrouped；(group_id,id) 索引及游标分页。
- core_account_push_keys：新增推送 Key 配置；旧 core_account_push 保留为 default 条目，旧 Key 无需重新配置。
- 分组数量统计按需执行，进程内缓存 5 秒，不挂在监控刷新热路径。
- 分组移动/删除每批 200 个账号提交，导入每批 100 个账号提交，不使用全池长事务。
- 分组字段由管理员操作直接更新；生成、校验等旧账号快照写回不会覆盖分组。
- 不修改生图选号/评分/冷热池逻辑，不给生图热路径新增数据库查询。
- 数据库完整备份覆盖所有表；只导出账号不包含推送 Key，推送配置迁移应使用数据库备份。
- 升级迁移会新增外键列和索引，首次部署大号池建议低峰执行；不会触发远程账号校验。

## 管理接口

```text
GET/POST   /admin/account-groups
PUT/DELETE /admin/account-groups/{id}
POST       /admin/accounts/move-group
GET        /admin/accounts/count?group_id=...&q=...&status=...
GET/POST   /admin/account-push/keys
PUT/DELETE /admin/account-push/keys/{id}
```

修改/删除配置携带 revision，过期返回 409。旧 GET/PUT /admin/account-push 继续兼容默认 Key。

账号列表与导出增加 group_id 查询参数；导入使用 POST /admin/accounts/import?group_id=...；刷新额度、刷新 AT、批量删除的 all_matching 请求增加 group_id。显式 ids 仍操作指定账号。选择范围为任务执行期间的数据库筛选结果，并非创建时冻结的快照；并发新增/删除/移动可能改变实际处理量，进度以已处理数量为准。

## 本地验收

使用独立 PostgreSQL 测试容器与虚构账号，不连接真实上游或生产数据库：

- 分组增改、名称冲突、修订号冲突、内置分组保护。
- 多 Key 推送归组、防请求体覆盖、跨组冲突、未分组自动归组。
- 旧推送配置兼容、Key 明文读回、管理权限隔离。
- 每批重验 Key 绑定与启停；改名不影响在途导入。
- 分组分页、计数、搜索、完整导出与分组恢复。
- 分组删除保留账号、旧账号快照保存不回滚分组。
- 刷新额度/AT遵守分组范围。
- 520 条账号跨多个批次移动，无跨组误操作。
- 浏览器验证新增分组、列表分组、全选筛选结果、批量移动完成、多 Key 页面。
