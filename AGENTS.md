# AGENTS.md — rstore working agreement

本文件是当前代码库的协作规则。架构说明见
`docs/architecture-guide.md`、`CONTEXT.md` 和 `README.md`。

## 版本阶段与 API 稳定性

- 当前版本尚未对外发布，仍处于内部开发阶段。
- 所有 API 均未定型，不承诺向后兼容。
- 在未另行声明前，允许为实现正确架构、修复问题或推进开发而进行必要的破坏性变更；无需仅为保持现有 API 兼容而增加迁移层或兼容适配，除非任务明确要求。

## 开发原则

1. **先实现 CLI，再实现桌面端。** 非 UI 能力按 Domain → Engine（用例与
   Store）→ Storage 适配 → 核心测试 → CLI 命令与 JSON → 桌面接入的顺序实现；
   CLI 是便于测试底层能力的验证入口，这里的先后表示研发顺序，不表示功能优先级。
2. **一个 Engine、两个入口。** CLI 与桌面端调用同一套 `rstore_engine`，不得
   复制业务逻辑，也不得在入口层直接访问 SQLite 或 AWS SDK。
3. **依赖方向固定。** Workspace 只有 `domain`、`storage`、`engine`、`cli` 四个
   库 crate 加两个入口。
   - `domain` 不依赖 GPUI、AWS SDK 或 SQLite。
   - `storage` 不依赖 GPUI 或 SQLite；AWS SDK 只出现在 `storage`。
   - `engine`、`cli` 不依赖 GPUI。`engine` 的 `db`、`transfer`、`catalog`
     模块是私有的，入口层只能经由 API 组访问。
   - `desktop` 不直接依赖 AWS SDK 或 SQLite。
4. **边界不允许 Panic。** 外部输入错误映射为 `domain::Error`，CLI 再映射为
   退出码。内部数据损坏可以让程序显式失败，但不得静默修正。

## 错误、ID 与 Secret

`rstore_domain::Error` 携带 `code`、`message`、`retryable`、`context` 和
`source`。使用 `Error::not_found`、`Error::invalid_input` 等构造函数；底层
错误使用 `.with_source(...)` 保留原因；上下文不得包含 Secret。

使用 `rstore_domain::id` 中的 Typed ID（`ConnectionId`、`BucketId` 等），不
用裸 `String` 或 `Uuid` 替代业务 ID。凭据直接存储在 Connection 的 SQLite 行中，
内存值使用 `rstore_domain::Secret<T>`，不得进入导出 JSON 或日志。

## Store（SQLite）

- SQLite 是应用的数据文件格式，不是可替换的适配器：用例直接调用 `engine::db`
  的仓储，不为只有一个实现的仓储再定义 trait。
- SQLite 开启 WAL、foreign keys 和 busy timeout。多个进程可以同时打开同一个
  数据目录，不使用进程锁；超过 busy timeout 映射为可重试的 `DatabaseBusy`。
- Schema 由 `crates/engine/migrations` 中的嵌入式 SQL 建立，迁移在事务中执行。
- 写操作经过单连接写池（`WriteGate`），多语句写入在 `BEGIN IMMEDIATE` 事务中
  执行；读操作使用只读连接池。
- Catalog 是缓存：保存已索引 bucket 的全部对象，以及任何带标签的对象；远端
  provider 是唯一事实来源。远端变更成功后经 `WriteThrough` 尽力更新缓存，缓存
  写失败只作为条目说明，不得让远端操作失败；不一致由 listing 或 index refresh
  修正。不引入 receipt / outbox / repair 一类"强一致投影"机制。
- 标签按 `(bucket, path)` 关联，不依赖索引。任何自动清理（GC、rebuild、关闭
  索引）都不得删除用户标签；只有 rstore 自己删除或移动对象时才迁移或清除标签。
- 时间以 Unix seconds 存储，以 RFC 3339 输出。

## CLI

- 结果写 stdout，日志和进度写 stderr。
- JSON 使用统一 envelope；输出不包含入口层无法保证的持久化 Row。
- 破坏性命令在非交互环境必须要求 `--yes`。
- 新命令加入 `crates/cli/src/args.rs`；envelope 的 `command` 标识由 clap 命令树
  自动推出。
- CLI 只解析输入、组装 Command/Query、调用 Engine API、渲染 API 返回值和统一
  envelope；不得渲染 Store Row。

## 当前架构

- Domain 划分为 Registry、Catalog、Transfer、AccessDiagnostics 四个上下文。
- `engine::Runtime` 是唯一的组装入口：打开数据库、选择具体 Storage 实现、执行
  启动恢复。
- Engine 对外暴露上下文 API 组；返回值可为 Domain 类型或 Application-facing
  View/Page/Report/Outcome，不暴露 SQLite 或 AWS SDK。服务是具体类型；只有确实
  存在多个实现的接缝（`Storage`、`SettleHook` 等）才使用 trait。
- 运行中的任务持有租约并由心跳续租；启动恢复只中断租约已过期的任务。取消：
  本进程的任务立即取消，其他进程的任务写入取消请求，由其心跳拾取。
- `EngineEvent`（经 `api.events()` 订阅）是尽力而为的运行时消息：任务开始、
  进度、结束，catalog 与标签变化。它不是持久事实，不重放；订阅方据此重新查询。
- 索引按 bucket 显式开启，默认关闭；`index run <bucket>` 开启并扫描。`find`
  对标签查询是完整的，对名称/属性搜索只覆盖已索引 bucket，并列出未索引的 bucket。
- 不引入 Event Sourcing、微服务或完整桌面页面。

## 测试与交付

开发 agent 不得自行新增、修改或编写测试代码；实现与测试必须由不同的 agent 或人工分别负责。

测试代码由独立验证方维护。Store、Storage、Catalog 缓存规则和任务租约的契约应由独立验证方覆盖；CLI 输出使用快照。

提交前运行：

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
