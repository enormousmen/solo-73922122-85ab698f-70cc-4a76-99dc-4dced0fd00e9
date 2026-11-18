# 共享诊断 SQL 服务（FastAPI + PostgreSQL）

值班数据库工程师共用的诊断执行服务：提交至多 10 条可信 SQL，在**固定复用的两条
PostgreSQL 连接**之一上按序自动提交执行；支持逐句结果查询、取消未结束的单；
每单结束后清理会话状态（SET、临时表、会话级咨询锁等），清理失败则淘汰连接并补建。

## 架构

```
                ┌────────────── FastAPI (uvicorn, 单进程) ──────────────┐
  POST /jobs ──▶│  校验(sqlguard) → JobStore(内存注册表) → 共享队列       │
  GET  /jobs/*  │                                                        │
  POST ../cancel│  cancel: 置标志 + 202 即时受理 ──▶ CancelSender 线程    │
  GET  /pool    │       (可配置发送停顿) ──▶ conn.cancel()               │
                │                                                        │
                │  Worker 线程 #1 ── 连接 #1 (autocommit, 长连接)         │
                │  Worker 线程 #2 ── 连接 #2 (autocommit, 长连接)         │
                └────────────────────────┬───────────────────────────────┘
                                         ▼
                                   PostgreSQL
```

- **固定两条连接**：两个 worker 线程各自持有一条长连接（`autocommit=True`），
  从同一个队列取单。连接在启动时建立并反复复用，**绝不每单重连**；唯一的重建
  路径是"清理失败/连接损坏 → 淘汰旧连接 → 原地补建"（`generation` 递增，
  池子仍是两条）。
- **排队**：两条连接都忙时，新单在内存队列等待（默认上限 1000，超出返回 429）。
- **互不阻塞**：每条连接一个独立 worker；慢 SQL、慢清理只占用本连接的 worker，
  另一连接的执行、提交以及所有状态查询（读内存注册表）都不受影响。

## 快速开始

```bash
docker compose up --build
# API: http://localhost:8000  文档: http://localhost:8000/docs
```

本地开发（需自有 PostgreSQL）：

```bash
pip install -r requirements.txt
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diag \
  uvicorn app.main:app --port 8000
```

演示脚本（覆盖：顺序执行、会话清理验证、取消、慢取消发送/慢清理钩子、迟到取消）：

```bash
./scripts/demo.sh            # BASE=http://localhost:8000 可覆盖
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/jobs` | 提交 1–10 条 SQL（202，返回 `job_id`） |
| GET | `/jobs` | 最近的单（摘要列表） |
| GET | `/jobs/{id}` | 单详情 + 逐句结果（状态/列/行/rowcount/耗时/错误） |
| POST | `/jobs/{id}/cancel` | 取消未结束的单（202 受理；已结束返回 409） |
| POST | `/maintenance/partitions/backfill` | 补建缺失的日 RANGE 分区（整批事务，见下） |
| GET | `/pool` | 两条连接的状态（idle/busy/cleaning/reconnecting、代数、清理失败数） |
| GET | `/health` | 存活检查 |
| GET | `/docs` | Swagger UI |

## 日分区补建（`POST /maintenance/partitions/backfill`）

按日入库的 RANGE 分区表在缺少对应日期的子分区时写入会失败。该端点为值班
工程师一次性补建一个日期范围内缺失的**日分区**。

### 适用范围（不满足即 422，且不执行任何 DDL）

- 父表为**单层、单列、键类型为 `date` 的 RANGE 分区**；
- **没有 DEFAULT 分区**；
- LIST/HASH 分区、非 `date` 键、多列键、普通表、二级分区、自身是分区的表一律拒绝；
- 日期范围闭区间，**最多 7 天**（`PARTITION_MAX_DAYS` 可调），`end_date` 不得早于 `start_date`。

### 语义

- **沿用**：已存在且边界恰好为 `[当日, 次日)` 的分区直接复用，响应里记为 `reused`；
- **新建**：没有任何分区覆盖的日期，创建名为 `<表名>_YYYYMMDD` 的日分区，记为 `created`；
- **整批冲突**：某天落入边界**不是** `[当日, 次日)` 的跨日旧分区时，整批返回 409，
  保持原状、不建任何分区；
- **全有或全无**：整批在**一个事务**内完成；任一天冲突或创建失败，整批回滚，不留半成品；
- **可重试且可区分**：重试时在同一把锁内重新读目录，已存在的（含本次之前建的）一律
  报 `reused`，本次新建的才报 `created`；
- **立即可用**：提交成功后这些日期即可正常入库。

### 并发与隔离

- 每个请求使用**独立的短连接**（`autocommit=False` 的单事务），**绝不占用**两条诊断
  长连接；两条诊断连接的执行与取消不受维护操作影响（`/pool` 始终只有两条）。
- 事务内先 `LOCK TABLE ... IN SHARE UPDATE EXCLUSIVE`，而
  `CREATE TABLE ... PARTITION OF` 需要 AccessExclusiveLock，两者互斥；因此与其他补建
  请求或手工 DDL 交错时，同一天**不可能建出两个分区**。
- 等锁受 `lock_timeout`（默认 10s）保护，超时返回 409 提示稍后重试，不留任何改动。

### 请求/响应示例

```bash
curl -XPOST localhost:8000/maintenance/partitions/backfill \
  -H 'content-type: application/json' -d '{
    "schema_name": "app",
    "table_name": "events",
    "start_date": "2026-11-18",
    "end_date": "2026-11-21"
  }'
```

```json
{
  "schema_name": "app", "table_name": "events",
  "range": {"start": "2026-11-18", "end": "2026-11-21"},
  "created": [
    {"date": "2026-11-18", "partition": "events_20261118",
     "bounds": {"from": "2026-11-18", "to": "2026-11-19"}}
  ],
  "reused": [{"date": "2026-11-20", "partition": "events_20261120"}],
  "created_count": 1, "reused_count": 1
}
```

标识符经 psycopg `Identifier` 转义、日期字面量经 `Literal` 转义；schema/表名仅接受
不含 `.`、引号、NUL 的普通标识符。

### 提交

```bash
curl -XPOST localhost:8000/jobs -H 'content-type: application/json' -d '{
  "statements": ["SET work_mem = '"'"'64MB'"'"'", "SELECT count(*) FROM pg_stat_activity"],
  "options": {"cleanup_delay_ms": 0, "break_connection_after": false}
}'
```

- 每条元素必须是**一条**语句（`;` 分隔的多句会被 422 拒绝；字符串/美元引号/
  注释里的 `;` 不算）。
- **拒绝显式事务控制**：`BEGIN/START/COMMIT/ROLLBACK/ABORT/END/SAVEPOINT/
  RELEASE`、`SET TRANSACTION`、`SET SESSION CHARACTERISTICS`、
  `PREPARE TRANSACTION`（含注释伪装的写法），返回 422。每条语句各自自动提交。
- 单语句错误 → 该句记 `error`，后续句记 `skipped`，单状态 `failed`。

### 取消语义

- **及时受理**：`POST /jobs/{id}/cancel` 立即置标志并返回 202；真正的取消发送
  由独立线程完成（可用 `{"delay_ms": N}` 模拟慢取消管道），不阻塞受理。
- **受理后**：正在执行的语句被 `conn.cancel()` 中断（PostgreSQL 标准取消），
  后续语句全部 `skipped`；**此前已提交的语句效果保留**（自动提交，无回滚）。
  极端时序下已发往服务器的语句可能自然完成，属正常现象。
- **排队中的单**：直接标记 `cancelled`，不会执行。
- **迟到取消**：单已结束时取消返回 409（无操作）；发送线程在发送前按连接校验
  "该单仍是此连接当前单且未结束"，因此**迟到的取消绝不会误停同一连接上的下一单**。
- **连接归还条件**：SQL 停止 + 取消发送完成 + 会话清理完成，三者齐备后连接才
  回到空闲（可用延迟钩子直接观测）。

### 会话清理（每单结束后，成功/失败/取消都执行）

`RESET ALL`、`RESET ROLE`、`RESET SESSION AUTHORIZATION`、`DEALLOCATE ALL`、
`DISCARD SEQUENCES`、删除本会话全部临时表/视图/序列、
`pg_advisory_unlock_all()`。清理失败 → 关闭并淘汰该连接，原地补建新连接
（`/pool` 可见 `generation` 与 `cleanups_failed` 递增）。

## 测试钩子（可控停顿/故障注入）

| 钩子 | 位置 | 作用 |
|---|---|---|
| `options.cleanup_delay_ms` | 提交单时 | 清理阶段停顿 N 毫秒（模拟慢清理） |
| `{"delay_ms": N}` | 取消请求体 | 受理后延迟 N 毫秒再向 PG 发送取消（模拟慢取消发送） |
| `options.break_connection_after` | 提交单时 | 单结束后断开本连接（模拟连接故障），触发淘汰+补建 |
| `CANCEL_SEND_DELAY_MS` / `CLEANUP_DELAY_MS` | 环境变量 | 上述两个延迟的全局默认值 |

## 自测

```bash
pip install -r requirements-dev.txt
pytest tests/test_sqlguard.py        # 纯单元测试（SQL 守卫），无需数据库
# 集成测试需要运行中的服务与数据库：
API_BASE=http://localhost:8000 TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/diag \
  pytest tests/test_integration.py tests/test_partitions.py
```

集成测试覆盖：按序自动提交与逐句结果、行数截断、失败即停、参数校验、
会话状态不泄漏（SET/临时表/咨询锁/预备语句，含失败单）、取消运行中/排队中的单、
迟到取消（409 与延迟发送不误伤下一单）、慢 SQL/慢清理不阻塞另一连接、
清理失败淘汰并补建连接（钩子注入 + `pg_terminate_backend` 真实杀连接两种）。

分区补建测试（`tests/test_partitions.py`）覆盖：缺失新建与精确日分区沿用、
单日/恰好 7 天范围、成功后立即入库、重试区分 `created`/`reused`、跨日旧分区整批 409
且保持原状、DEFAULT/LIST/非 date 键/多列/普通表/二级分区等形状拒绝、超过 7 天与
日期非法等请求校验、批次中途失败整批回滚不留分区、独立连接（两条诊断连接忙时仍可用）、
同表并发每天只建一个分区、手工 DDL 交错不重复。

> 注：PostgreSQL 的语句取消依赖向后端进程投递 SIGINT。在禁止进程间信号的
> 沙箱里，"中断正在执行的语句"无法生效（`pg_terminate_backend` 同样无效），
> 相关测试用例设计为在两种环境下都通过：沙箱内验证协作式停止（语句间停止），
> Docker/正常 Linux 下同一代码路径会真正中断执行中的语句。

## 设计说明与限制

- 面向**可信 SQL**（值班 DBA）：守卫只强制执行契约（单句、禁显式事务），
  不是安全边界；`CALL`/`DO` 体内的事务控制无法静态识别，属约定范围。
- 任务注册表在内存中（重启即失），保留最近 1000 单。
- 结果行数上限 `ROW_LIMIT`（默认 200，超出截断并标记 `truncated`）。
- 单进程单实例：连接池在进程内，勿以多 worker 方式启动 uvicorn。
- 连接使用 `prepare_threshold=None`，避免 psycopg 的客户端预备语句缓存与
  清理中的 `DEALLOCATE ALL` 冲突。
- 分区补建的维护连接是每请求新建、用完即关的短连接，与诊断连接池完全独立；
  阻塞的 psycopg 调用经 `anyio.to_thread` 在线程中执行，不阻塞事件循环。

> 锁与信号环境说明：分区补建依赖 PostgreSQL 的锁等待唤醒与 `lock_timeout`
> （后者通过向后端进程投递信号中断等待）。在禁止进程间信号/IPC 的沙箱里，
> 多个会话同时阻塞等待同一父表锁时可能无法被及时唤醒。`tests/test_partitions.py`
> 的强锁串行用例会先探测该能力，缺失时自动跳过（无竞争的并发、原子性、形状校验等
> 用例不受影响）；在 Docker/正常 Linux 下该用例完整执行。锁互斥本身
> （SUE 与 CREATE PARTITION 的 AccessExclusiveLock 冲突）是 PostgreSQL 的确定行为。
