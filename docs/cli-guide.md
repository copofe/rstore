# rstore CLI 使用教程与参考

rstore 是一个本地优先的 S3 兼容存储工作台：连接你自己的存储（MinIO、Cloudflare R2、Supabase Storage、阿里云 OSS 等），把桶索引成本地目录，并提供上传下载、复制移动、搜索打标、链接生成等日常操作。

本文的命令树以 `crates/cli/src/args.rs` 为准；CLI 是当前完整的自动化入口，桌面端目前只有
Bootstrap、Application API 和通知订阅的最小 Runtime 接入。这里的先实现 CLI、后实现桌面端
是研发顺序，不表示产品或功能优先级。

本文分两部分：前半是**教程**（按使用顺序），后半是**命令参考**。所有命令均以 `rstore <命令>` 调用；`rstore <命令> --help` 永远有最权威的参数说明。

---

## 教程

### 0. 约定

- **短格式**（推荐）：所有接受远端位置的命令都可直接写 `<桶>/[前缀/]<key>`，如 `media/img/logo.png`；桶根就是 `media/` 或裸 `media`。桶名在多个连接间有歧义时写 `<连接>/<桶>/…`（如 `nas/media/img/`）。
- **引用形式**：连接可用名字或 `conn_…` id，桶可用 `bkt_…` id、`<连接>/<名字>` 或裸名，任务可用 `job_…` id 或列表里的序号（`1` = 最新）。
- **输出约定**：结果走 stdout；进度和日志走 stderr。加 `--json` 得到稳定的 JSON envelope（脚本用）。
- **退出码**：`0` 成功；部分失败的批量操作 `13`；本地磁盘读写错误 `14`；Ctrl+C 中断 `130`；桶或适配器不支持预签名 `11`。其余错误码见 JSON envelope 的 `errors[].code`。
- **破坏性操作**（`rm`、`bucket remove`、`connection remove` 等）：交互模式会逐次确认；脚本/管道里必须加 `--yes`。

### 1. 连接存储

```sh
# 注册连接（密钥推荐直接给，写入 Connection 配置；或交互提示输入）
rstore connection add bucket_name \
  --endpoint http://localhost:9000 \
  --region auto \
  --access-key AK... --secret-key SK...

# 验证：凭证解析、鉴权、ListBuckets 三步检查
rstore connection test bucket_name

# 常用变体
rstore connection list --filter prod        # 按名字/endpoint 模糊过滤
rstore connection show bucket_name             # 看配置（永远不显示密钥）
rstore connection show bucket_name --capabilities   # 实测 list/分片/复制/预签名四项能力
rstore connection edit bucket_name --region us-east-1
rstore connection remove bucket_name --yes     # 只删本地注册；密钥一并清除；远端不动
```

**endpoint 细节**：

- 带路径的 endpoint（如 Supabase 的 `https://<ref>.supabase.co/storage/v1/s3`）结尾的 `/` 会被自动补齐，但建议写全。
- 部分网关要求 **path-style 寻址**：`connection add ... --addressing-style path`。症状是「连接测试通过、桶操作 404」。
- 云厂商对象存储用默认 `auto` 即可。

**凭证的两种来源**（按优先级）：

1. 环境变量，按 secret-ref 命名，支持多连接各用各的密钥：
   ```sh
   export RSTORE_SECRET_CONN_MY_MINIO__ACCESS_KEY='AK...'   # 双下划线
   export RSTORE_SECRET_CONN_MY_MINIO__SECRET_KEY='SK...'
   ```
2. Connection 配置表（`connection add --access-key` 时写入本地 SQLite）。

两者都没有时 `connection test` 会明确报缺凭证。

`connection add` 时不给密钥也允许（比如打算纯用环境变量的 CI 场景），只会有个警告。

### 2. 注册桶

rstore 只操作**显式注册过**的桶——这是防止指错桶的保护：

```sh
# 自动发现：列出远端可见的桶
rstore bucket discover bucket_name

# 注册（默认会探测桶可读性，失败只是警告不阻断）
rstore bucket add bucket_name media

# 注册时的本地策略
rstore bucket add bucket_name archive --read-only                  # 拒绝一切写入（上传/复制入站被挡）
rstore bucket add bucket_name trash --allow-delete=false         # 禁止删除
rstore bucket add bucket_name tmp --index-enabled=false        # 不参与索引
rstore bucket add bucket_name cold --conflict skip              # 该桶默认冲突策略

# 查看/调整
rstore bucket list
rstore bucket show bucket_name/media
rstore bucket edit bucket_name/media --read-only=false
rstore bucket edit bucket_name/media --concurrency 8    # 该桶传输并发覆盖（0 = 清除）
rstore bucket remove bucket_name/media --yes   # 只删本地注册
```

**忽略目录**（索引时跳过，不进本地目录）：

```sh
rstore bucket ignore add bucket_name/media .trash
rstore bucket ignore list bucket_name/media
rstore bucket ignore remove bucket_name/media .trash
```

### 3. 直接操作远端（无需任何索引）

```sh
# 列目录（实时远端，分页）
rstore remote ls media/
rstore remote ls media/img/ --limit 100
rstore remote ls media/ --cursor <上一页给的token>   # 翻页
rstore remote ls media/ --update-catalog            # 顺手写进本地目录

# 看单个对象
rstore remote stat media/img/logo.png
```

### 4. 上传 / 下载

```sh
# 上传：目标是 桶/[前缀/]，源文件名落在前缀下
rstore upload ./report.pdf media/docs/
rstore upload ./report.pdf media/      # 桶根（裸 media 等价）
rstore upload ./src media/backup/ -r   # 递归，保持目录结构
rstore upload a.txt b.txt media/       # 多文件
rstore upload - media/ < files.txt     # 批量：stdin 一行一个本地路径

# 连接限定形式（桶名有歧义时使用 <连接>/<桶>/）
rstore upload ./report.pdf bucket_name/media/docs/

# 下载：远端源同规则（短格式或连接限定形式），目标本地路径
rstore download media/img/logo.png ./out.png
rstore download media/img/ ./img-backup -r   # 递归镜像
rstore download - ./downloads < refs.txt     # 批量：stdin 一行一个远端引用

# 冲突策略（上传/下载/cp/mv 通用）
--conflict skip       # 跳过已存在
--conflict overwrite  # 覆盖
--conflict rename     # 自动改名（name-<随机8位>.ext）
--conflict ask        # 显式交互询问
# 省略 --conflict 时：上传/cp/mv 用目标桶 `bucket add --conflict` 存的默认，
# 没有存默认（或 download 写本地盘）时按 ask 处理
--yes                 # 非交互时把 ask 解析为 overwrite（否则报错）

# 其他
--dry-run             # 逐条预览（"would upload …"/"would download …"），不传输
--no-progress         # 关掉 stderr 进度条
```

大文件自动走分片上传、支持断点续传；**Ctrl+C 随时安全**——进度持久化为 `interrupted` 状态，重跑同一条命令即从 checkpoint 续传。

### 5. 复制 / 移动 / 删除

```sh
# 同连接走服务端复制；跨连接自动中继（恒定内存，大对象走分片）
rstore cp media/a.txt backup/a.txt
rstore cp media/a.txt backup/dir/     # 目录式目标，保留文件名
rstore mv media/old.bin media/archive/

# 递归前缀
rstore cp media/docs/ backup/docs/ -r
rstore mv media/old/ media/arc/ -r --keep-tags   # 标签跟着走

# 批量：stdin（短格式或连接限定形式均可）
printf '%s\n' media/a.txt media/b.txt | rstore cp - backup/

# 删除（有精确计数的确认；删除后本地目录记 tombstone；rm 也是持久化 job，Ctrl+C 可中断）
rstore rm media/tmp/junk.bin
rstore rm media/tmp/ --recursive --yes       # 递归，非交互
rstore rm media/tmp/ --recursive --dry-run   # 先看会删什么
```

`mv` 的安全顺序：**复制 → 校验目标大小 → 才删源**。校验不过源文件保留。

### 6. 索引与本地目录（可选增强层）

索引把桶扫描进本地 SQLite 目录，是 `files` 搜索和 `tag` 的数据基础：

```sh
# 按桶索引（注意：参数是桶，不是 workspace）
rstore index start bucket_name/media
rstore index status            # 各桶进度/checkpoint
rstore index pause bucket_name/media   # 标记暂停（前台正在跑的扫描用 Ctrl+C）
rstore index resume bucket_name/media  # 从 checkpoint 继续
rstore index cancel bucket_name/media  # 取消，已完成的部分保留

# 维护
rstore index refresh bucket_name/media   # 对账：远端已删的行标记 stale
rstore index rebuild bucket_name/media --yes   # 清空重扫（标签定义保留）；会先确认
rstore index stats                    # 目录规模概览
rstore catalog gc --dry-run           # 清理过期删除记录（默认保留 30 天）
```

索引是**桶级**操作；workspace 只是检索视图——见下一节。不建 workspace 也能用 `remote ls --update-catalog` 喂数据，只是没有 `files`/`tag` 入口。

### 7. Workspace：圈定检索范围

workspace = 一组桶的命名视图。`files` 和 `tag` 都以它为作用域：

```sh
rstore workspace create proj-a --description "A 项目的资产"
rstore workspace bucket add proj-a bucket_name/media
rstore workspace bucket add proj-a bucket_name/backup
rstore workspace bucket list proj-a

rstore workspace list
rstore workspace show proj-a
rstore workspace remove proj-a --yes   # 桶和远端数据不受影响
```

### 8. 搜索与打标

```sh
# 混合文件列表：跨成员桶，SQL 级过滤 + 游标分页
rstore files list proj-a
rstore files list proj-a --bucket bucket_name/media --path docs/
rstore files list proj-a --sort size:desc --limit 100
rstore files list proj-a --after <cursor>          # 下一页
rstore files list proj-a --untagged                # 只看没标签的

# 名称搜索（trigram 索引，大目录快）
rstore files search proj-a logo

# 单对象详情（含标签与新鲜度）
rstore files show media/img/logo.png --workspace proj-a

# 标签
rstore tag create proj-a important --color ff0000
rstore tag add proj-a media/img/logo.png --tag important
rstore tag add proj-a - --tag draft < uris.txt     # 批量
rstore tag add proj-a <uri> --tag a --tag b --replace   # 整组替换
rstore tag detach proj-a <uri> --tag draft         # 省略 --tag = 全部摘掉
rstore tag list proj-a                             # 含每个标签的对象计数
rstore tag merge proj-a old-name new-name          # 合并，源标签消失
rstore tag remove proj-a draft --yes
```

标签是 workspace 级的：同一个标签体系在 `proj-a` 和 `proj-b` 里互不影响。

### 9. 链接生成

```sh
rstore url media/img/logo.png                # 公开 URL（按桶配置的 base）
rstore url media/img/logo.png --format markdown
rstore url <uri> --format s3        # s3://bucket/key 形式
rstore url <uri> --format path      # 桶内相对路径
rstore url <uri> --format html --json   # 批量、机器可读
rstore url - --format json < uris.txt    # stdin 批量（# 开头是注释）

# 预签名（限时访问；最长 7 天）
rstore url media/report.pdf --signed --ttl 1h
rstore url media/report.pdf --signed           # 省略 --ttl 用桶的默认 TTL
```

`url` 逐条独立成败：一条引用失败不影响其余（人类输出一行一条链接，可直接管道），部分失败退出 13。

**链接档案**（base 与权限都在桶上配，`bucket show` 也会显示这五项）：

```sh
rstore bucket link show bucket_name/media
rstore bucket link set bucket_name/media --cdn-base https://cdn.example.com \
  --allow-presigned --default-ttl 15m
rstore bucket link set bucket_name/media --public-base ""   # 空串清除该 base
# 同样五个 flag 也能直接跟在 bucket edit 后面 —— 两条入口、同一条路径
```

公开 URL 的 base 优先级：桶的 CDN base > 桶的 public base > 连接 endpoint。预签名需要桶和适配器支持，不支持时报 `capability_unsupported`（退出码 11）。

### 10. 任务管理

每次 upload/download/cp/mv/rm 都是一个持久化 job：

```sh
rstore task list                 # 新任务在前；有待修复项时附计数
rstore task show 1               # 最新那个；或 job_xxx id
rstore task show 1 --errors      # 带失败明细
rstore task retry 1              # 失败/部分失败的 job 标记重跑
rstore task cancel 3
rstore task pause 2 / rstore task resume 2     # 持久化状态转移；前台跑着的任务用 Ctrl+C
```

想真正续传/重跑，重发原命令即可——upload/download 会从 checkpoint 继续；跨进程暂停另一个 rstore 里正在跑的任务尚未支持。

### 11. 诊断、配置与迁移

```sh
rstore doctor          # 数据库/适配器、连接数（未测计数）、凭证来源、待处理 outbox/repair、中断任务
rstore config show     # 数据/配置/缓存/日志目录和 db 路径

# 机器迁移/备份注册表（永远不含密钥）
rstore config export > bundle.json
rstore config export --out bundle.json
rstore config import bundle.json --dry-run    # 预览
rstore config import bundle.json              # 重名跳过，绝不覆盖
```

迁移后密钥要重新录入（`connection add --access-key ...` 或环境变量）——密钥不随 bundle 走是设计行为。

### 12. 脚本化速查

```sh
rstore ... --json | jq .data            # 结构化输出在 envelope 的 data 字段
some-cmd | rstore rm -                  # stdin 批量 URI（# 开头是注释）
some-cmd | rstore url - --format json   # url 同样吃 stdin 批量
rstore ... --quiet                      # 只留结果
rstore ... 2>/dev/null                  # 只看 stdout 结果、丢掉 stderr 诊断
set -e; rstore upload ...               # 部分失败退出码 13，脚本可感知
```

---

## 命令参考

全局选项（所有命令可用）：`--config <PATH>` 指定数据根目录；`--log-level <LEVEL>`（默认 `warn`）；`--json`；`-q/--quiet`。

| 命令 | 作用 | 关键参数 |
|---|---|---|
| `connection add <NAME>` | 注册连接 | `--endpoint` `--region` `--addressing-style {auto,path,virtual-host}` `--access-key` `--secret-key` `--secret-ref` `--tls-verify` |
| `connection list` | 列连接 | `--filter <子串>`（名字/endpoint，不分大小写） |
| `connection show <REF>` | 连接详情 | 不显示密钥；`--capabilities` 实测 list/分片/复制/预签名 |
| `connection test <REF>` | 三步探测 | `--write-test`（含写探针） |
| `connection edit <REF>` | 改可变字段 | `--name` `--endpoint` `--region` `--addressing-style` `--secret-ref`（+重新给密钥） |
| `connection remove <REF>` | 删除注册 | `--yes`；连带删除该 Connection 配置中的凭证 |
| `bucket discover <CONN>` | 列远端可见桶 | |
| `bucket add <CONN> <NAME>` | 注册桶 | `--display-name` `--read-only` `--allow-delete` `--index-enabled` `--conflict` `--skip-verify` |
| `bucket list` | 已注册桶 | |
| `bucket show <REF>` / `bucket edit <REF>` | 查看/调整本地设置 | show 含链接档案；edit：`--display-name` `--read-only` `--allow-delete` `--index-enabled` `--conflict` `--concurrency` + 链接档案五 flag |
| `bucket remove <REF>` | 删本地注册 | `--yes` |
| `bucket link show <REF>` / `bucket link set <REF>` | 链接档案 | `--public-base` `--cdn-base` `--allow-public-url` `--allow-presigned` `--default-ttl`；只改给出的项，空串清除 base |
| `bucket ignore add/list/remove <REF> <DIR>` | 索引忽略目录 | |
| `workspace create <NAME>` | 建工作区 | `--description` |
| `workspace list/show/edit/remove` | 管理 | remove 需 `--yes` |
| `workspace bucket add/remove <WS> <REF>` | 成员管理 | |
| `workspace bucket list <WS>` | 成员列表 | |
| `remote ls <URI>` | 实时列目录 | URI 支持短格式；`--cursor` `--limit` `--update-catalog` |
| `remote stat <URI>` | 对象元数据 | |
| `index start <BUCKET>` | 扫描入目录 | 断点续扫；参数是**桶引用** |
| `index status/stats` | 状态/规模 | |
| `index pause/resume/cancel <BUCKET>` | 扫描控制 | 参数是**桶引用** |
| `index refresh <BUCKET>` | 对账标 stale | |
| `index rebuild <BUCKET>` | 重扫 | 标签定义保留；破坏性确认，非交互需 `--yes` |
| `files list <WS>` | 混合列表 | `--path` `--bucket`（可重复）`--sort <name|size|modified|type|bucket|connection>:<asc|desc>` `--limit` `--after` `--untagged` |
| `files search <WS> <PATTERN>` | 名称搜索 | `--sort` `--limit` |
| `files show <URI>` | 对象详情 | URI 支持短格式；`--workspace` |
| `tag create <WS> <NAME>` | 建标签 | `--color` `--description` |
| `tag list/edit/remove` | 管理 | remove 需 `--yes` |
| `tag merge <WS> <SRC> <DST>` | 合并 | 源标签移除，关联去重迁移 |
| `tag add <WS> <URIS.../- >` | 打标 | URI 支持短格式；`--tag`（可重复）`--replace`；`-` 读 stdin |
| `tag detach <WS> <URIS.../- >` | 摘标 | URI 支持短格式；`--tag`；省略 = 全部 |
| `upload <SOURCES...> <TARGET>` | 上传 | 目标支持短格式 `桶/[前缀/]`；`-r` `--conflict` `--dry-run` `--yes` `--no-progress`；`-` 读 stdin |
| `download <SOURCES...> <TARGET>` | 下载 | 远端源支持短格式；多 URI 批量，一 URI 一子任务；其余同 upload |
| `cp <SOURCES.../-> <TARGET>` | 复制 | 源和目标均支持短格式；`--conflict` `--dry-run` `--yes` `-r` `--keep-tags` |
| `mv <SOURCES.../-> <TARGET>` | 移动 | 同上；删源前校验目标 |
| `rm <URIS.../->` | 删除 | URI 支持短格式；`--recursive` `--dry-run` `--yes` |
| `task list/show/pause/resume/retry/cancel` | 任务管理 | show 支持 `--errors`；引用支持序号 |
| `url <URIS.../->` | 链接 | URI 支持短格式、`-` 读 stdin；逐条独立成败；`--format path/s3/url/markdown/html/json` `--signed` `--ttl` |
| `catalog gc` | 清删除记录 | `--dry-run` `--retention 30m/1h/1d/2w/秒`（上限 365d） |
| `config show/export/import` | 本地配置 | export `--out`；import `--dry-run`，重名跳过 |
| `doctor` | 体检 | |
| `version` | 版本 | |

### 数据存放位置

| 内容 | 位置 |
|---|---|
| 连接/桶/workspace/标签定义/索引/任务 | `catalog.db`（SQLite，WAL），见 `rstore config show` |
| 凭证 | Connection 配置表（SQLite），或环境变量覆盖 |
| 日志 | `<config>/log/`（滚动） |

### 常见问题

- **连接测试通过，桶操作 404** → endpoint 带路径时确认写全；部分网关（Supabase）需要 `--addressing-style path`。
- **`bucket add` 警告 not readable 但注册成功** → 探测失败不阻断，先用 `remote ls` 看真实错误；`--skip-verify` 可跳过探测。
- **非交互下删除/确认报错** → 破坏性操作加 `--yes`（含 `index rebuild`）；`ask` 冲突策略加 `--yes`（视为 overwrite）或换 `--conflict`。
- **自签证书 / `--tls-verify=false`** → 该开关目前只存储设置、尚未接入校验器；证书仍按系统默认校验。
- **Ctrl+C 之后** → 任务存为 interrupted，重跑原命令即续传；`task list` 可查。
- **换机器** → `config export/import` 迁注册表，密钥重新录入。
