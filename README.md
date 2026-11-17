# 水质检测变更日志 → SQLite 同步系统

市级水质实验室把检测系统的变更日志（changelog）同步到供内部查询的本地 SQLite 库。
采样编号可能经历**初测、撤回、复测**，同一批复测结果必须**整体可见**；上游传输顺序受
传输队列影响，新旧事务交错、提交标记可能早于行变更到达、分页可能与上一页重叠。

本仓库从零搭建，包含三个互相独立的部分：

| 组件 | 位置 | 作用 |
| --- | --- | --- |
| 本地源模拟服务 | `src/source/` | 读取自带夹具，通过 HTTP 提供 changelog，支持重叠分页、重复投递、交错事务、HTTP 503 |
| 同步服务 | `src/sync/` | 拉取日志、持久化、分批准备、短事务发布，对外提供只读查询 API，全部状态落在 SQLite |
| 自动化测试 | `tests/` | 真实 SQLite 文件 + 真实子进程重启的集成测试，以及独立参考回放器核对结果 |

零运行时第三方依赖：使用 Node.js 内置的 `node:sqlite`（WAL 模式）与全局 `fetch`；
仅在开发期依赖 `typescript` / `tsx`（见 `package-lock.json`）。要求 **Node >= 22.5**。

---

## 快速开始

```bash
npm install        # 仅安装开发期依赖（tsx / typescript）
npm run dev        # 一条命令同时启动源服务(8080)与同步服务(8081)
```

另开终端：

```bash
curl -X POST http://127.0.0.1:8081/sync/start     # 开始后台同步
curl http://127.0.0.1:8081/status                 # 查看同步状态
curl "http://127.0.0.1:8081/readings?limit=5"    # 分页列表（按 recordId 排序）
curl http://127.0.0.1:8081/readings/R-0003        # 单条；删除或未可见返回 404
curl http://127.0.0.1:8081/diagnostics            # 流水线诊断
curl -X POST http://127.0.0.1:8081/sync/pause     # 当前持久化批次结束后停止拉取
```

运行测试（安装依赖时间不计入；当前约 50–55 秒，预算 80 秒）：

```bash
npm test
```

不需要浏览器，也不需要任何外部数据库服务。

---

## 目录结构

```
src/
  shared/protocol.ts      线上协议与 API 的 TypeScript 类型
  fixtures/fixtures.ts    确定性夹具（含 2 万行大事务），不访问外网
  source/server.ts        源模拟器 HTTP 服务（重叠/重复/503）
  source/main.ts          源进程入口
  sync/store.ts           SQLite 持久化层：接收、准备、发布、物理迁移、读取
  sync/engine.ts          拉取循环、退避重试、暂停语义、故障注入、结构化日志
  sync/server.ts          同步服务 HTTP API
  sync/main.ts            同步进程入口
tests/
  helpers.ts              子进程/真实 SQLite 文件/端口/轮询工具
  reference-replayer.ts   独立参考回放器（不与同步器共用任何合并实现）
  store-unit.test.ts      真实 SQLite 文件上的迁移、乱序、批次、持久性测试
  happy-path.test.ts      全量回放 + 参考核对 + 分页/404/重启幂等
  fault-recovery.test.ts  四个故障点的真实崩溃-重启链、可见性、暂停、503 重试
scripts/dev.js            npm run dev 的双进程启动器（带前缀日志）
```

---

## 日志协议（含完整 JSON 示例）

所有 `offset` 与 `txVersion` 都是**十进制字符串**。程序内部用 `bigint`、SQLite 中用
`TEXT` 存储，因此日志从 `9007199254740990`（`Number.MAX_SAFE_INTEGER` 附近）开始编号也能
精确保存，不会被 JS number 截断。

### 1. 分页信封：`GET /log?after=<位置>&limit=<数量>`

首次请求 `after=0`。正常页按 offset 升序返回，响应包含 `events`、`nextOffset`、`hasMore`。
每个事件带 `_offset`，即它在源端的真实传输位置；重叠页会重复较早的事件，但 `_offset`
不变，接收端据此在 offset 主键上去重。

```json
{
  "events": [
    {
      "kind": "row",
      "txId": "TX-0002",
      "txVersion": "7000000000000002",
      "index": 0,
      "schemaVersion": 1,
      "op": "upsert",
      "recordId": "R-0001",
      "after": {
        "recordId": "R-0001",
        "stationId": "ST-01",
        "sampledAt": "2026-09-01T10:00:00Z",
        "nitrate_mg_l": 4.2
      },
      "_offset": "9007199254740990"
    },
    {
      "kind": "commit",
      "txId": "TX-0002",
      "txVersion": "7000000000000002",
      "eventCount": 1,
      "_offset": "9007199254740991"
    }
  ],
  "nextOffset": "9007199254740991",
  "hasMore": true
}
```

重叠分页示例（`SOURCE_OVERLAP=1` 时每 5 页有一页在开头重复最多 2 个已送达事件）：

```json
{
  "events": [
    { "kind": "row", "txId": "TX-0001", "index": 1, "...": "较早的事件被重复",
      "_offset": "9007199254740993" },
    { "kind": "row", "txId": "TX-0003", "index": 0, "...": "本页新事件",
      "_offset": "9007199254740995" }
  ],
  "nextOffset": "9007199254740995",
  "hasMore": true
}
```

### 2. 行事件（`kind=row`）

| 字段 | 含义 |
| --- | --- |
| `txId` / `txVersion` | 事务标识与源端分配的唯一**提交序号**（十进制字符串），新旧以此为准 |
| `index` | 从 0 开始的事务内操作顺序 |
| `schemaVersion` | 行镜像使用的 schema 版本（1 或 2） |
| `op` | `upsert`（携带完整行镜像 `after`）或 `delete`（**不**携带 `after`） |
| `recordId` | 采样编号，业务主键 |

浓度 `nitrate_mg_l`（v1）/ `nitrateMgL`（v2）允许为 `0`（有效测量值）或 `null`
（本次未得到有效浓度），二者语义不同。

删除事件：

```json
{
  "kind": "row",
  "txId": "TX-0004",
  "txVersion": "7000000000000004",
  "index": 1,
  "schemaVersion": 1,
  "op": "delete",
  "recordId": "R-0002",
  "_offset": "9007199254740999"
}
```

同一事务内先撤回再复测同一个采样编号（T3）：

```json
{ "kind": "row", "txId": "TX-0003", "index": 0, "op": "upsert",
  "recordId": "R-0003", "after": { "nitrate_mg_l": 9.9, "...": "初测" } }
{ "kind": "row", "txId": "TX-0003", "index": 1, "op": "delete", "recordId": "R-0003" }
{ "kind": "row", "txId": "TX-0003", "index": 2, "op": "upsert",
  "recordId": "R-0003", "after": { "nitrate_mg_l": 3.3, "...": "复测" } }
```

### 3. 提交事件（`kind=commit`）

声明该事务应当包含多少个行事件。**只有同时收到提交事件和全部行事件**，事务才具备
发布条件。提交标记可能早于行事件到达（夹具 T6），相同行/提交事件也可能在不同 offset
重复出现，接收端均幂等处理。

```json
{
  "kind": "commit",
  "txId": "TX-0006",
  "txVersion": "7000000000000006",
  "eventCount": 1,
  "_offset": "9007199254741001"
}
```

### 4. Schema 事件（固定的 v1 → v2 演进）

v2 把 `nitrate_mg_l` 改名为 `nitrateMgL`，并新增**非空**字段 `methodCode`；v1 记录对应
`methodCode="legacy"`。schema 事件可能晚于引用 v2 的行事件到达（T8），v1 积压事件也可能
在它之后到达（T9）。本系统只实现这一次固定演进，不提供通用迁移语言。

```json
{
  "kind": "schema",
  "schemaVersion": 2,
  "changes": [
    { "type": "renameColumn", "table": "readings", "from": "nitrate_mg_l", "to": "nitrateMgL" },
    { "type": "addColumn", "table": "readings", "column": "methodCode",
      "ddlType": "TEXT", "nullable": false, "defaultForExisting": "legacy" }
  ],
  "_offset": "9007199254741500"
}
```

v2 行镜像使用新字段名并自带 `methodCode`：

```json
{
  "kind": "row",
  "txId": "TX-0008",
  "txVersion": "7000000000000008",
  "index": 0,
  "schemaVersion": 2,
  "op": "upsert",
  "recordId": "R-0007",
  "after": {
    "recordId": "R-0007",
    "stationId": "ST-07",
    "sampledAt": "2026-09-05T08:00:00Z",
    "nitrateMgL": 1.4,
    "methodCode": "ion-chroma"
  }
}
```

---

## 持久化与发布模型（为什么不会丢、不会重复、不会泄露中间态）

全部状态都在一个 WAL 模式的 SQLite 文件里，共五张核心表：

| 表 | 内容 |
| --- | --- |
| `meta` | 接收位置 `received_offset`、当前 schema 版本、重试次数、最近错误 |
| `raw_events` | 收到的每个事件，`offset_val` 为主键，天然对重复/重叠投递去重 |
| `tx_state` | 每个事务一行：`open → prepared → published`，记录期望行数、已收行数、已暂存行数 |
| `tx_rows` | 事务内行事件（`tx_id+idx` 主键），带逐行 `staged` 水位标记 |
| `record_versions` | **暂存区**：准备后的行变更，未发布前业务查询完全不可见 |
| `published_txns` | 已发布事务（短事务只往这里插入一行） |

关键设计：

1. **接收位置与事件在同一个 SQLite 事务里提交。** `persistPage` 先写 `raw_events`、
   分组到 `tx_rows`/`tx_state`，最后才推进 `received_offset`，整体 `BEGIN IMMEDIATE …
   COMMIT`。崩溃发生在接收边界时，要么整页都在、要么整页都不在，不会出现“位置已前进
   但事件丢失”，重复投递也不会产生重复业务效果。
2. **准备（staging）对查询不可见。** 行变更先进入 `record_versions`，该表只有与
   `published_txns` join 后才会被读取。准备过程中业务查询看到的始终是上一个已发布版本。
3. **每批最多 200 行。** `prepareBatch` 用 `tx_rows.staged` 逐行水位推进，单批
   `LIMIT 200`；2 万行事务被拆成 100 个批次，跨批次、跨进程重启都能续做。同一事务内对
   同一 `recordId` 的多次操作会折叠到一个 `(record_id, tx_version)` 版本行，因此进度以
   逐行水位而非版本行数判断。
4. **发布是短事务，且发布时不再逐行复制。** 准备阶段已经把最终版本行写好，发布只执行
   一条 `INSERT INTO published_txns` + 状态翻转，毫秒级完成。
5. **读取按 `txVersion` 取每个编号的最新可见版本。** 旧事务晚送达也不会覆盖更新的源端
   状态；最新版本若是删除墓碑（`op='delete'`），该编号从列表消失、单条查询返回 404。
   事务可见性是整体的：查询只能看到发布前或发布后的结果，绝不会看到一个事务只生效一半。
6. **接收进度可以领先发布进度。** 尚未齐全（如永久缺行的 T7）或 schema 尚不能解释的事务
   都完整保存在库里，持续计入待补齐/待处理状态，不阻塞其它事务发布。

### 物理 schema 迁移（不是只改响应字段名）

`applyDueSchemaEvents` 从 `raw_events` 按 offset 顺序读取所有 schema 声明，首次见到 v2 时
对**物理表结构**执行：

```sql
ALTER TABLE record_versions RENAME COLUMN nitrate_mg_l TO nitrateMgL;
ALTER TABLE record_versions ADD COLUMN method_code TEXT;   -- 删除墓碑允许 NULL
UPDATE tx_state SET schema_version=2 WHERE schema_version=1;
```

之后到达的 v1 积压行在暂存时映射为 `methodCode='legacy'`；已经发布的 v1 行重新读取时也
以 v2 形态（`nitrateMgL` + `legacy`）返回。重新打开一个已迁移的数据库时，暂存语句按盘上
schema 版本编译，不会因旧列名不存在而崩溃。

### 重启恢复

同步进程不保存任何内存中的关键状态。重启后：

- 从 `meta.received_offset` 继续拉取（重叠/重复事件由主键忽略）；
- 重新应用尚未生效的 schema 声明（幂等）；
- 扫描 `open` 事务，依据 `tx_rows.staged` 从未暂存的行继续分批准备；
- 已 `prepared` 的事务直接重新发布，已 `published` 的不会重复（`insert or ignore`）。

因此即使退出发生在接收、分批准备、发布提交的任意边界，恢复都**不依赖源端恰好再送来一条
新事件**——集成测试里大事务故障发生时全部日志已持久化，最终收敛过程源端无需再发任何东西。

---

## 同步服务 API

| 方法与路径 | 行为 |
| --- | --- |
| `POST /sync/start` | 后台开始拉取；重复调用返回 `started:false`，不会建立第二个循环 |
| `POST /sync/pause` | 等当前持久化批次/准备/发布步骤结束后停止；能立即打断退避等待；之后可再次 start |
| `GET /status` | 见下方计数口径 |
| `GET /readings?limit=&after=` | 按 `recordId` 升序的 keyset 分页，响应带当前对外 `schemaVersion` 与 `nextRecordId` |
| `GET /readings/:recordId` | 返回 `{schemaVersion, item}`；删除后或未发布 → 404 |
| `GET /diagnostics` | 每个事务的状态、暂存行数、已发布行数等结构化诊断 |

### `/status` 计数口径

```json
{
  "receivedOffset": "9007199254761020",
  "schemaVersion": 2,
  "openTransactions": 1,
  "preparedTransactions": 0,
  "publishedTransactions": 9,
  "retryCount": 3,
  "lastError": null,
  "running": true
}
```

| 字段 | 精确口径 |
| --- | --- |
| `receivedOffset` | `meta.received_offset`：已与事件在同一事务内持久化的最新传输位置（十进制字符串） |
| `schemaVersion` | 已在 SQLite 物理结构上应用、且对外读取使用的 schema 版本 |
| `openTransactions` | `tx_state.status='open'` 的事务数：仍缺行、缺提交标记，或引用了尚未解释的 schema |
| `preparedTransactions` | `status='prepared'`：全部行已暂存到 `record_versions` 但尚未发布 |
| `publishedTransactions` | `published_txns` 的行数：已对外可见的事务数（重启后不重复计数） |
| `retryCount` | 累计遇到 HTTP 503 / 网络错误并进入退避重试的次数 |
| `lastError` | 最近一次拉取错误文本；下一次成功持久化后清空为 `null` |
| `running` | 拉取循环是否处于运行中（pause 完成后为 `false`） |

夹具里 T7 永久缺少一个行事件，所以稳定状态下 `openTransactions` 恒为 **1**，其余 9 个事务
全部发布。

---

## 结构化日志与可观测性

同步进程对每个关键步骤输出一行 JSON（stdout），可直接观察批次规模与发布过程：

```json
{"svc":"sync","event":"page-persisted","rows":1000,"nextOffset":"9007199254741989","hasMore":true}
{"svc":"sync","event":"prepare-batch","txId":"TX-0010","rows":200,"batchCap":200}
{"svc":"sync","event":"tx-prepared","txId":"TX-0001","rows":2,"batchCap":200}
{"svc":"sync","event":"schema-applied","schemaVersion":2}
{"svc":"sync","event":"tx-published","txId":"TX-0010","via":"short-commit"}
{"svc":"sync","level":"warn","event":"fetch-retry","retryInMs":25,"error":"source 503: ..."}
{"svc":"sync","level":"fatal","event":"fault-exit","fault":"mid_prepare_big_tx","txId":"TX-0010"}
```

准备批次之间会让出事件循环（生产环境仅一个 `setImmediate`，测试可用
`SYNC_PREPARE_DELAY_MS` 拉长窗口），因此大事务准备期间读接口始终即时响应。

---

## 内存边界

- 源端每次只取一页（默认 `SYNC_PAGE_LIMIT=1000`），同步端同一时刻只持有这一页；
- 准备单批最多 200 行，2 万行事务被拆成 100 批，逐批提交、逐批让出事件循环；
- 没有任何代码路径把整份日志或整个大事务一次性装入内存。

---

## 源模拟器能力（确定性，无外网）

由环境变量控制，夹具内置在 `src/fixtures/fixtures.ts`：

| 环境变量 | 效果 |
| --- | --- |
| `SOURCE_OVERLAP=1` | 每 5 个前向页有一页在开头重复最多 2 个已送达事件（重叠分页） |
| `SOURCE_FAIL_EVERY=K` | 每 K 个不同的前向页请求返回一次 HTTP 503（之后重试即成功） |
| `SOURCE_PORT` | 监听端口（默认 8080） |

夹具覆盖：普通新增/修改、撤回后复测（T3）、新旧事务倒序到达（T2 先于 T1）、事务交错
（T4 被 T5 拦腰截断）、同一事务内重复操作一个编号（T5）、提交标记提前（T6）、schema 前后
交错（T8/T9）、一个始终缺行的事务（T7）、2 万行大事务（T10，被其它流量中途打断），以及在
新 offset 上的重复行/重复提交投递。

---

## 故障注入与测试矩阵

同步进程通过 `SYNC_FAULT` 配置**一次性**故障点，触发后进程以退出码 **42** 硬退出：

| 故障点 | 触发时机 |
| --- | --- |
| `after_receive_persist` | 第 2 个接收页持久化之后 |
| `mid_prepare_big_tx` | 大事务第一个 200 行准备批次落盘之后 |
| `before_publish_commit` | 第一个就绪事务发布提交之前 |
| `after_publish_commit` | 第一个发布短事务提交之后 |

`tests/fault-recovery.test.ts` 用**同一个 SQLite 文件**串行启动四个带不同故障的进程，每个
都真实崩溃、下一个进程在同一文件上恢复，最后一个无故障进程收敛到最终状态，并与独立参考
回放器逐条核对。另含：

- **准备期间持续查询**：大事务准备过程中高频轮询列表，证明 BIG 记录数量只能是 0 或
  19999，绝不会出现部分可见的中间态，且读接口全程可响应；
- **暂停/继续/重启**：`/sync/start` 幂等、暂停后接收位置不再前进、重启后可完成；
- **503 退避重试**：退避可被暂停立即打断，恢复后仍正确收敛。

### 独立参考回放器

`tests/reference-replayer.ts` **不 import 同步器的任何合并/准备/发布代码**，而是从夹具事件
重新独立推导规则（事务齐全才可见、按 index 应用、重复投递忽略、按 txVersion 取最新、v1
积压映射 legacy），作为乱序、重复与迁移结果的独立裁决者。所有集成测试的最终记录都与它
比对。

### 测试输出怎么看

`npm test` 的用例命名直接体现四类验收点：

- `FAULT chain ...` / `... invisible while preparing` —— **故障恢复**与大事务可见性；
- `MIGRATION ...` —— **版本迁移**（物理列改名、legacy 回填、v1 积压后置处理）；
- `HAPPY: full replay ...` —— **大事务可见性**、乱序/重复/删除/404 与参考核对；
- `DURABILITY` / `ORDERING` / `BATCHING` / `RETRY` / `PAUSE` —— 持久化、新旧判定、200 行
  批次、退避与暂停语义。

---

## 同步进程环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SYNC_PORT` | 8081 | HTTP 端口 |
| `SOURCE_URL` | http://127.0.0.1:8080 | 源模拟器地址 |
| `SYNC_DB` | data/sync.db | SQLite 文件路径（父目录自动创建） |
| `SYNC_PAGE_LIMIT` | 1000 | 每页拉取事件数 |
| `SYNC_FAULT` | 未设置 | 一次性故障点（见上表） |
| `SYNC_AUTOSTART` | 未设置 | 设为 `1` 时进程启动即开始同步 |
| `SYNC_BACKOFF_MS` / `SYNC_MAX_BACKOFF_MS` | 100 / 2000 | 503 退避起点与上限（指数翻倍） |
| `SYNC_PREPARE_DELAY_MS` | 0 | 每个准备批次后的额外让出时间（测试用） |
