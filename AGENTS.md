# AGENTS.md — rstore working agreement

本文件是当前代码库的协作规则。架构说明见
`docs/architecture-guide.md`、`CONTEXT.md` 和 `README.md`。

## 版本阶段与 API 稳定性

- 当前版本尚未对外发布，仍处于内部开发阶段。
- 所有 API 均未定型，不承诺向后兼容。
- 在未另行声明前，允许为实现正确架构、修复问题或推进开发而进行必要的破坏性变更；无需仅为保持现有 API 兼容而增加迁移层或兼容适配，除非任务明确要求。

## 开发原则

1. **先实现 CLI，再实现桌面端。** 非 UI 能力按 Domain → Application →
   Persistence/Adapter → 核心测试 → CLI 命令与 JSON → 桌面接入的顺序实现；CLI
   是便于测试底层能力的验证入口，这里的先后表示研发顺序，不表示功能优先级。
2. **一个 Core、两个入口。** CLI 与桌面端调用同一套
   `rstore_application`，不得复制业务逻辑，也不得在入口层直接访问 SQLite
   或 AWS SDK。
3. **依赖方向固定。**
   - `domain` 不依赖 GPUI、AWS SDK 或 SQLite。
   - `application`、`catalog`、`transfer`、`storage-api`、`persistence`、
     `platform`、`cli` 不依赖 GPUI。
   - `storage-s3` 不依赖 GPUI。
   - `desktop` 不直接依赖 AWS SDK 或 SQLite。
   - `persistence` 不依赖 `transfer`；Multipart 端口位于 `storage-api`。
4. **边界不允许 Panic。** 外部输入错误映射为 `domain::Error`，CLI 再映射为
   退出码。内部数据损坏可以让程序显式失败，但不得静默修正。

## 错误、ID 与 Secret

`rstore_domain::Error` 携带 `code`、`message`、`retryable`、`context` 和
`source`。使用 `Error::not_found`、`Error::invalid_input` 等构造函数；底层
错误使用 `.with_source(...)` 保留原因；上下文不得包含 Secret。

使用 `rstore_domain::id` 中的 Typed ID（`ConnectionId`、`BucketId` 等），不
用裸 `String` 或 `Uuid` 替代业务 ID。凭据直接存储在 Connection 的 SQLite 行中，
内存值使用 `rstore_domain::Secret<T>`，不得进入导出 JSON 或日志。

## Persistence

- SQLite 开启 WAL、foreign keys 和 busy timeout。
- Schema 由 `crates/persistence/migrations` 中的嵌入式 SQL 建立，迁移在事务
  中执行。
- 写操作经过有界 `SqliteWriter`，读操作使用只读连接池。
- `UnitOfWork` 当前仅承载 `RemoteMutation` 一类本地操作：远端 mutation 的
  receipt 与 Outbox 事件在一次事务中提交。没有生产者的操作变体不进枚举
  （`OutboxOnly`/`RepairOnly` 已删，待常驻消费循环落地时按需引入）。不要把
  它泛化为所有跨 Repository 写入的统一事务。
- 时间以 Unix seconds 存储，以 RFC 3339 输出。

## CLI

- 结果写 stdout，日志和进度写 stderr。
- JSON 使用统一 envelope；输出不包含入口层无法保证的持久化 Row。
- 破坏性命令在非交互环境必须要求 `--yes`。
- 新命令同时加入 `crates/cli/src/args.rs` 和 `Command::path()`。
- CLI 只解析输入、组装 Command/Query、调用 Application API、渲染 API 返回值
  和统一 envelope；不得渲染 persistence Row。

## 当前架构

- Domain 划分为 Registry、Catalog、Transfer、AccessDiagnostics 四个上下文。
- Bootstrap 是唯一的 Composition Root，负责具体数据库、Storage、Outbox 和通知通道的组装。
- Application 对外暴露上下文 API；返回值可为 Domain 类型或 Application-facing
  View/Page/Report/Outcome，不暴露 SQLite、AWS SDK 或具体 Service 实现。
- Domain Event 记录业务事实；进度和桌面通知属于运行时消息。
- Outbox 支持幂等、重试、失败记录和 Repair 查询。
- Outbox 的消费入口是启动时的一次性排水：Bootstrap 在取得写锁后把积压事件
  交付给 RemoteVerifyHandler（远端复验，缺失入 repair，瞬时错误留队重试）。
  常驻消费循环待长驻进程（桌面端）出现后再引入。
- 不引入 Event Sourcing、微服务或完整桌面页面。

## 测试与交付

优先使用 `FakeStorage`、`TempDb` 和 `TestClock`。Repository、Storage、Catalog
和 Outbox 的契约必须有测试；CLI 输出使用 `insta` 快照。

提交前运行：

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
