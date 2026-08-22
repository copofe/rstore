# CONTEXT.md — 领域术语与边界速查

完整架构说明见 `docs/architecture-guide.md`；协作规则见 `AGENTS.md`。本文件只收录
评审与开发对话中实际使用、且需要唯一定义的术语。按字母序排列。

## 组合与启动

- **组合根（Composition Root）** — `crates/bootstrap`。唯一允许选择具体 SQLite、
  Storage 适配器、Outbox 与通知通道的层；生产代码中只有它打开 `Database`。
- **组装点（assemble）** — `rstore_persistence::assemble(db, storage_factory,
  clock) → ApplicationDependencies`。把一个已打开的 `Database` 与可选 Storage
  工厂、时钟装配成 Application 依赖图的唯一函数；依赖组结构与端口别名词汇表
  位于 `rstore_application_ports::deps`。生产启动（bootstrap）与测试夹具
  （application::test_util）都必须经过它，不得各自转写依赖列表。
- **写门（WriteGate）** — `rstore_persistence::tx::WriteGate`。写连接池与
  有界写者（SqliteWriter）的不可拆分配对，`Database::gate()` 是唯一发放处。
  repo 的多语句写只能经 `gate.write_unit(label, work)`（permit 与事务一体
  获取，重入死锁在构造上不可能）；单语句写经 `gate.permit()`。repo 构造器
  只收 `(gate, read_pool)`——不存在能绕开共享写门、自建 writer 的单池构造
  器；`tx::atomically` 随之私有，事务只从写门进入。
- **启动编排（Runtime::open_locked）** — `bootstrap::Runtime::open_locked(&AppDirs)`。
  进程启动的完整序列：创建数据目录 → 获取跨进程写锁（DATA-008）→ 打开数据库 →
  迁移 → 恢复 processing 状态的 Outbox 事件 → `Application::initialize`（把遗留
  非终态 Job 标记为 interrupted）。锁 guard 由返回的 `Runtime` 持有，进程退出即释放。
- **构造/恢复分离（from_dependencies / initialize）** — `Application::from_dependencies`
  只组装服务、保持确定性；`Application::initialize` 执行启动恢复规则。两者分离是
  有意设计：测试可以只构造不恢复。
- **进程写锁（AppLock）** — `crates/platform` 提供的跨进程互斥锁（DATA-008）。
  platform 不依赖 domain；`LockError → domain::Error` 的映射只发生在 bootstrap。

## 核心域（速查，详见 architecture-guide §1）

- **Registry / Catalog / Transfer / AccessDiagnostics** — 四个领域上下文。
- **Connection** — 一个 S3 兼容端点的配置 + 凭据（凭据存于 Connection 行，
  内存中为 `Secret<T>`）。
- **Bucket** — Connection 下的远端桶，携带 `read_only` / `allow_delete` 守卫位。
- **RemoteObject** — Catalog 中一个远端对象的本地投影（含 tombstone 生命周期）。
- **Job** — 一次批处理运行的持久化意图与状态机（Pending → Running → …）。
- **Outbox / Repair** — 远端变更事实的可靠落库与可查询的跟进失败。
- **RemoteMutationReceipt** — 远端操作成功的事实凭据，与 Outbox fact 同事务提交。
- **Storage / StorageFactory** — Provider 无关的存储端口与工厂端口（storage-api）。
- **Multipart 会话（MultipartSession）** — storage-api 的会话端口：
  `Storage::create_multipart` / `adopt_multipart` 返回会话句柄，part 上传
  （`upload_part`）、服务端段拷贝（`copy_part`，默认 CapabilityUnsupported，
  S3 以 UploadPartCopy 覆写）、`complete` 与 `abort` 都在会话上。三种失败
  处置随类型走且只有一份：`complete` 从不 abort（留待续传）、`abort` 严格
  （失败即错误）、`abandon(self)` warn-only 消费。重试装饰器返回包一层
  RetryingSession，段级重试语义与平铺时代一致。
