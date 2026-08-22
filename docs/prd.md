# 远端存储工作台产品需求

本文记录当前产品范围与边界；实现状态以源码、`README.md` 和
[`architecture-guide.md`](architecture-guide.md) 为准。

## 1. 产品定义

rstore 是一个 local-first 的远端存储工作台。用户可以注册多个 S3-compatible
连接，把 Bucket 加入 Workspace，建立本地 Catalog，并通过 CLI 完成浏览、搜索、
标签、上传、下载、复制、移动、删除和链接生成。

文件始终保留在用户自己的存储中。rstore 不依赖厂商中心服务器，也不向 Bucket
写入内部索引目录。

## 2. 产品入口

CLI 是当前完整的自动化入口。实现上先通过 CLI 验证底层能力，功能稳定后再实现桌面端；
这是研发顺序，不表示 CLI 的功能或产品优先级高于桌面端。未来的 GPUI Desktop 与 CLI
共用同一个 Application Core，不复制业务逻辑，也不直接访问 SQLite 或 Provider SDK。

当前桌面入口只负责 Bootstrap、Application API 和 JobNotification 的最小接入，
完整页面属于后续实现。

## 3. 领域范围

### 3.1 Registry

- Connection 保存名称、S3 API endpoint、public/CDN delivery base 的默认值、region、addressing style、TLS policy 和凭据字段。
- Bucket 保存连接归属、名称、读写策略、索引策略、并发配置和冲突默认值。
- LinkProfile 保存桶级 public/CDN delivery base 覆盖、presign 权限和默认 TTL。
- 链接 base 的生效顺序为：桶 CDN 覆盖 > 连接 CDN 默认 > 桶 public 覆盖 > 连接
  public 默认 > 原生 S3 endpoint；rstore 不按 provider 猜测地址。
- Credential 值直接存储在 Connection 的 SQLite 配置行中；内存中使用
  `Secret<T>` 保护，不能进入导出配置、通用 API JSON 或日志。连接详情只显示脱敏预览。

### 3.2 Catalog

- Workspace 是 Bucket 集合、Tag namespace 和查询范围。
- IgnoreRule 以 Bucket 根目录为基准，精确忽略目录子树。
- RemoteObject 使用 `(connection, bucket, path)` 唯一定位。
- IndexRun 记录扫描状态、分页 checkpoint、对象数量和错误。
- Catalog 支持状态过滤、Workspace membership、标签关联、排序、keyset 分页和
  FTS 路径预筛选。
- Refresh/Rebuild 只影响本地 Catalog，不删除远端对象。

### 3.3 Transfer

- Job 和 JobItem 持久化每个传输项的状态、进度和错误。
- Upload 支持小文件和 Multipart。
- Download 支持 Range、断点、source identity sidecar 和原子完成。
- Copy 支持同 Provider server-side copy、跨连接 relay 和递归展开。
- Move 先验证目标，再删除源对象。
- Delete 支持批量、递归、dry-run、精确确认、Bucket policy 和 tombstone。
- Ctrl+C 保存可观察的中断状态；前台传输重新运行原命令时可利用 checkpoint 继续，`task`
  命令不跨进程控制另一个正在运行的传输。

### 3.4 AccessDiagnostics

- Connection test 检查凭据是否已存储，执行连接探测和可选写探针。
- Capability probe 检查 list、multipart、copy 和 presign。
- `doctor` 汇总连接健康、Adapter、Job、Outbox 和 Repair 状态。
- 不支持的能力返回明确的 `CapabilityUnsupported` 错误。

## 4. CLI 命令

```text
rstore connection add|list|show|edit|test|remove
rstore bucket discover|add|list|show|edit|remove
rstore bucket ignore add|list|remove
rstore bucket link show|set
rstore workspace create|list|show|edit|remove
rstore workspace bucket add|remove|list
rstore remote ls|stat
rstore index start|status|stats|pause|resume|cancel|refresh|rebuild
rstore files list|search|show
rstore tag create|list|edit|remove|merge|add|detach
rstore upload
rstore download
rstore cp
rstore mv
rstore rm
rstore task list|show|pause|resume|retry|cancel
rstore url
rstore catalog gc
rstore config show|export|import
rstore doctor
rstore version
```

远端短引用先扩展为 canonical 字符串，再由 Application 解析为 `RemoteUri`；只有需要
稳定 Catalog / Outbox 身份的流程才构造 `RemoteLocator`。短引用遵循以下规则：

- `bucket/` 或 `connection/bucket/` 表示目录引用。
- `bucket/key` 表示一个精确对象。
- 递归操作需要目录引用或显式 `--recursive`。
- `-` 从 stdin 读取一行一个引用。

## 5. Application API

Application 暴露以下上下文 API：

```text
RegistryApi
CatalogApi
TransferApi
AccessApi
DiagnosticsApi
JobApi
ConfigApi
```

API 接收 typed Command/Query 以及部分 Service 输入类型，返回值既包含 Domain 类型，也
包含 Application-facing 的 Page、View、Report 和 Outcome，例如 `FilesPage`、`JobView`、
`TransferReport` 和 `RepairItem`。CLI 和 Desktop 不依赖
Repository、SQLite、SQLx、AWS SDK 或具体 Service 实现。

## 6. 一致性

SQLite 写入由单个有界 Writer Actor 串行执行，读查询使用只读连接池。WAL、foreign
keys、busy timeout 和事务 helper 保证本地事务边界；`UnitOfWork` 当前只覆盖
`OutboxOnly`、`RemoteMutation`、`RepairOnly` 三类操作。

远端和 SQLite 不共享跨系统事务，远端变更使用以下流程：

```text
Job intent
→ remote operation
→ RemoteMutationReceipt + Outbox fact
→ Catalog follow-up
→ Repair item on failure
```

Outbox 事件具备 event_id 幂等、aggregate 顺序、退避重试、失败记录和 Repair 状态。
当前入口没有常驻 Outbox 消费循环或投影 Handler；进度事件只属于运行时，不写入 Domain Event。

## 7. 安全

- Secret 值通过 `Secret<T>` 管理，与 Connection 配置一同存储在 SQLite；不进入导出配置、
  通用 API JSON 或日志。CLI 的 `connection show` 只显示 AK/SK 的首尾字符，中间使用星号。
- TLS 默认开启。
- `--tls-verify=false` 当前只保存设置，尚未让 S3 适配器使用自定义证书校验器。
- 日志、JSON、错误上下文和诊断输出不得泄露 Secret 或签名 URL。
- 删除、覆盖、递归操作必须经过对应的 Bucket policy 和 Capability 检查。
- 非交互环境中的破坏性命令必须显式 `--yes`。

## 8. 输出和错误

人类输出写 stdout，进度和日志写 stderr。JSON 使用统一 envelope：

```json
{
  "command": "files.list",
  "ok": true,
  "data": {},
  "warnings": [],
  "errors": [],
  "meta": {"generated_at": "2026-08-13T18:20:31Z"}
}
```

核心错误内部包含稳定 ErrorCode、可重试属性、来源和结构化上下文；当前 CLI JSON 对外
输出 code、message、retryable。退出码由 CLI 统一映射，
批量部分失败使用专用退出码，中断使用 130。

## 9. 可观测性和恢复

- Job 创建后立即持久化。
- 进程启动时将无法继续确认的运行任务标记为 interrupted。
- Multipart checkpoint 记录 source size、mtime 和已完成 part。
- 远端成功而 Catalog follow-up 失败时，保留 receipt 和 repair item。
- `doctor`、`task list` 和 `task show` 展示待处理 Repair。

## 10. 非目标

- 完整 GPUI 页面和交互。
- Event Sourcing。
- 微服务拆分。
- 多设备同步。
- WebDAV、SFTP 等额外协议。
- 自动后台 Agent。
- 跨 Provider 通用回收站。
- 向远端 Bucket 写入 rstore 私有元数据。
