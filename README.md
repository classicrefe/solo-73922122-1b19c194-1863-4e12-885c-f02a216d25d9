# LIMS 生化仪接口接收服务（HL7 v2.5.1 ORU^R01 / MLLP）

单进程接收服务：FastAPI + SQLite，MLLP TCP 端口只接收单台生化仪的 ASCII
HL7 v2.5.1 `ORU^R01`。每条消息一个 OBR、1～20 个不重复的数值 OBX；
样本号取 OBR-3.1，项目取 OBX-3.1，值取 OBX-5，状态取 OBX-11。

## 快速启动（Docker Compose）

```bash
docker compose up --build
```

| 入口 | 地址 |
|---|---|
| 演示 / 交互式 API 文档 | http://localhost:8000/docs （恢复接口在 **recovery** 分组） |
| 按样本查询当前结果 | http://localhost:8000/api/v1/samples/{样本号} |
| 按 MSH-10 查询消息与原回执 | http://localhost:8000/api/v1/messages/{MSH-10} |
| 样本列表 | http://localhost:8000/api/v1/samples |
| 健康检查 | http://localhost:8000/health |
| 仪器 MLLP 端口 | `localhost:2575`（VT…FS+CR，每帧一个 ACK） |

数据保存在命名卷 `lis-data` 的 `/data/lis.db`（WAL、`synchronous=FULL`），
容器重启后去重记录与结果都保留。（无 Docker 时用 `./run_local.sh` 启动同一
服务，见下文“本地开发与测试”。）

### 在 /docs 中演示

`POST /api/v1/demo/messages` 不是直接写库，而是把消息通过**真实 MLLP
socket** 发送到本服务，可配置：

* `fragments`：把整帧拆成 N 次 TCP write（演示分片）；
* `delay_ms`：每次 write 之间停顿；
* `drop_ack`：第一次发送后丢弃回执并立即重发完全相同的字节（演示
  "提交后丢回执重发"），响应里会有两条 ACK，可直接比对是否逐字节相同。

示例消息（换行可用 `\n`，服务会规范成 CR）：

```
MSH|^~\&|ANALYSER|LAB|LIS|HOSP|20261112090000||ORU^R01|DEMO-001|P|2.5.1
OBR|1|S01|S-800^LAB|||||||||||||DR
OBX|1|NM|GLU^x||5.0|mmol/L|||||F
OBX|2|NM|K^y||4.1|mmol/L|||||F
```

然后 `GET /api/v1/samples/S-800` 查看当前结果；把 `DEMO-001` 原样重发（或
`drop_ack=true`）会返回同一条已持久化的 AA，且结果不会被二次应用。

### 并发同 MSH-10

`POST /api/v1/demo/concurrent`（见 `/docs` 的 demo 分组）会用 N 条独立
连接同时发送同一条消息；也可用本地模拟器：

```bash
docker compose exec lis python -m app.simulator --host 127.0.0.1 --port 2575
```

模拟器依次演示例行：分片（40 段）、粘包（一连接连发 3 条）、丢 ACK 重发、
8 连接并发同 ID、F/C 修正、含非法项的整条拒绝。也可只跑某一个：

```bash
docker compose exec lis python -m app.simulator --host 127.0.0.1 --port 2575 \
    --scenario concurrent --scenario dropped-ack
```

## 接收库恢复（误写数据后回档再重采）

仪器联调误写数据后，维护人员可上传本服务用 SQLite backup 生成的备份把
接收库整体恢复到备份时刻，再让仪器重新采集。恢复过程不需要重启进程，原
有接口全部保留。

### 恢复接口（均在 `/docs` 的 recovery 分组）

| 方法/路径 | 说明 |
|---|---|
| `GET /api/v1/admin/recovery/backup` | 用 SQLite 在线 backup API 下载当前库的一致性备份（单文件，无 WAL 旁挂文件） |
| `POST /api/v1/admin/recovery/backups` | 上传备份到暂存目录并**离线校验**，上限 16 MiB；不合格返回 413/422 且不影响当前服务 |
| `POST /api/v1/admin/recovery/commit?backup_id=` | 用已校验的暂存备份原子替换在线库 |
| `GET /api/v1/admin/recovery/status` | 查询恢复阶段、在途作业、上次结果 |
| `DELETE /api/v1/admin/recovery/backups/{backup_id}` | 放弃并删除暂存备份 |

上传校验内容：SQLite 文件头、`PRAGMA integrity_check` 必须为 `ok`、只允许
存在 `messages` 与 `results` 两张用户表且列定义与服务一致（名称/类型/
NOT NULL/主键位置），并要求存在 `idx_results_sample` 索引。校验以
`immutable` 只读方式打开，不会在上传文件旁生成 WAL，也不触碰在线库。

### 切换时的保证

1. 暂停接受**新 MLLP 连接**（监听 socket 关闭、端口保持占用），切换完成
   后在同一端口重新监听；
2. 数据查询接口（samples / messages / demo 文本）在切换期间返回 **503**，
   `/health` 与恢复状态接口仍可用；
3. 关闭全部**旧 MLLP 连接**，未到 FS 的残帧随连接丢弃，绝不写库；
4. 等已经进入入库或查询的后台作业**真正结束**（作业栅栏跟踪到线程完成，
   即使发起请求的 HTTP/MLLP 连接已断开）；
5. 在存储写锁内整体替换数据库主文件与 WAL/SHM：结果、报文、原回执一起
   回到备份时刻；候选备份自带的旧 WAL 会被删除，绝不混入恢复库；
6. 替换后重新打开并再做一次 `integrity_check`；任何一步失败都把旧库移回
   并继续用旧库服务，不会停留在维护状态；
7. 同期只允许一次恢复，并发提交得到 409；提交任务与 HTTP 请求脱钩，请求
   断连后切换或回退仍会执行完毕，可轮询 status 跟踪；
8. 恢复后同一 MSH-10 重发按**备份中的记录**处理：原文一致回放备份里持久
   化的原 ACK，原文不同仍回 AE；恢复库就是普通的在线库，重启后保留。

### 一键演示

容器内置纯标准库演示脚本，覆盖"可上传备份"与"慢入库时恢复"两条路径：

```bash
docker compose up --build
# 终端 1：备份 → 误写 → 上传 → 恢复 → 同 ID 重放 → 非法上传被拒
docker compose exec lis python -m app.recovery_demo \
    --base-url http://127.0.0.1:8000 --mllp-host 127.0.0.1 --mllp-port 2575
```

慢入库演示（写事务故意持有 3 秒，验证切换会等在途作业结束、旧任务迟到
数据不写入恢复库）：

```bash
docker compose --profile demo up --build
docker compose exec lis-slow-demo python -m app.recovery_demo \
    --scenario slow --base-url http://127.0.0.1:8000 \
    --mllp-host 127.0.0.1 --mllp-port 2575
```

也可用 curl 手工执行：

```bash
curl -s http://localhost:8000/api/v1/admin/recovery/backup -o lis-backup.db
curl -s -X POST http://localhost:8000/api/v1/admin/recovery/backups \
    -F "file=@lis-backup.db;type=application/vnd.sqlite3"
# 取响应里的 backup_id 后：
curl -s -X POST "http://localhost:8000/api/v1/admin/recovery/commit?backup_id=<id>"
curl -s http://localhost:8000/api/v1/admin/recovery/status
```

## 行为约定

### 接收与 ACK

* 每连接独立帧状态机，支持 TCP 分片、粘包、多帧连发、并发连接；
  VT…FS 之间为一条消息，帧外字节（如多余 CR）忽略。非 ASCII、NUL、
  空帧、超长（>256 KiB）帧回 AE。
* 解析/校验失败**整条拒绝**并回 AE，不写任何结果与消息记录。校验包括：
  MSH 必须首段且 `^~\&`；MSH-9 必须 `ORU^R01`；MSH-12 必须 `2.5.1`；
  MSH-10 必须 1–64 字符不含分隔符；OBR 恰好一条且 OBR-3.1 非空；1–20 个
  OBX；OBX-1 正整数；OBX-2 为 NM/SN 或空；OBX-3.1 非空且消息内唯一；
  OBX-5 必须有值且为数值（支持正负号/小数/科学计数）；OBX-11 必须 F 或 C；
  未知段、第二条 MSH/OBR 均拒绝。
* ACK：`MSH|^~\&|…|ACK^R01^ACK|…`，`MSA|AA|原 MSH-10|…`；AE 时附
  `ERR|段^错误码^HL70357||E`。仅当消息与回执在**同一事务提交成功之后**才
  发送 AA（`synchronous=FULL`，提交即落盘）。

### F / C 语义（与结果同事务）

* `F`：样本下**新增**项目；项目已存在则整条 AE（HL7 205）。
* `C`：**更正**样本下已有项目；项目不存在则整条 AE（HL7 207）。
* 一条消息内任一项目不满足上述规则，整条回滚、AE，其它项目也不会写入。

### MSH-10 去重与并发

* 已接纳的 MSH-10：
  * 原文逐字节相同（CR/LF 规范后）→ 返回当时持久化的**原 ACK**（MSA-2
    相同），不再应用；
  * 原文不同 → AE（HL7 205），不改动已有结果。
* 唯一性由 SQLite 主键 + `BEGIN IMMEDIATE` 写事务保证：同 ID 并发到达时
  只有一个事务应用，其余读到已提交记录并回放同一条 ACK；进程重启后同样
  成立（记录已持久化）。
* 提交前中断（进程崩溃/连接断开）由事务回滚保证不留部分结果；仪器重发后
  正常应用。未收到 FS 的不完整帧不触发任何写入。

### HTTP API

| 方法/路径 | 说明 |
|---|---|
| `GET /api/v1/samples/{sample}` | 当前结果（项目、值、单位、状态、来源 MSH-10、更新时间）及该样本已接纳消息列表；切换期间 503 |
| `GET /api/v1/samples?limit=` | 最近更新的样本及项目数；切换期间 503 |
| `GET /api/v1/messages/{msh10}` | 消息原文、原文摘要、ACK 代码、持久化的 ACK 原文；切换期间 503 |
| `POST /api/v1/demo/messages` | 经真实 MLLP 发送（分片 / 丢回执重发） |
| `POST /api/v1/demo/concurrent` | N 连接并发发送同一条消息 |
| `GET /api/v1/demo/samples/{sample}` | 纯文本结果摘要 |
| `GET /api/v1/admin/recovery/backup` | 下载在线一致性备份（SQLite backup API） |
| `POST /api/v1/admin/recovery/backups` | 上传并校验备份（≤16 MiB），暂存不换库 |
| `POST /api/v1/admin/recovery/commit` | 用暂存备份原子恢复在线库 |
| `GET /api/v1/admin/recovery/status` | 恢复状态与在途作业 |
| `DELETE /api/v1/admin/recovery/backups/{id}` | 删除暂存备份 |

## 本地开发与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest                      # 78 项：解析、拒绝、F/C、并发、重启、分片/粘包、HTTP、demo、恢复（23 项）
./run_local.sh              # http://localhost:8000/docs，MLLP 2575，库在 ./data/lis.db
python -m app.simulator --host 127.0.0.1 --port 2575
python -m app.recovery_demo # 备份恢复端到端演示（也可用 --scenario slow）
```

环境变量：`LIS_DB_PATH`（默认 `/data/lis.db`）、`LIS_MLLP_HOST`（默认
`0.0.0.0`）、`LIS_MLLP_PORT`（默认 `2575`）、`LIS_SLOW_APPLY_DELAY`
（演示用，秒，默认 0；>0 时每个写事务在提交前额外持有写锁该时长）。

## 设计取舍

* **单进程 + 单写者**：接收进程只有一个 asyncio 循环，写操作经
  `asyncio.to_thread` 交给 SQLite 单写事务，配合进程内锁与
  `busy_timeout` 串行化；不引入外部队列，保证"提交后才回 AA"。
* **回执持久化**：把生成的 ACK 字节与结果一起存库，重发时原样返回，
  避免重新生成 ACK 导致时间戳/MSH-10 不同而破坏"同 ID 同回执"。
* **指纹用规范化原文**：CR/LF 差异不算"异文"，其余任何字节差异都算。
* **数值校验从严**：OBX-2 非 NM/SN、OBX-5 非数值都整条拒绝，避免 LIS
  里出现无法计算的结果。
* **恢复两阶段、单写者换库**：上传阶段只写暂存目录并以只读 immutable
  方式校验，与在线库完全隔离；提交阶段先暂停监听、关旧连接、经作业栅栏
  排空在途入库/查询，再在 SQLite 写锁内用同文件系统 `os.replace` 整体换
  库，失败即移回旧库。所有连接均为短连接，换库时没有任何打开的 SQLite
  句柄，旧 WAL 在换库前折叠删除，候选备份的旁挂 WAL 也一律删除。
* **断连不中断恢复**：提交任务通过 `asyncio.create_task` 与 HTTP 请求
  脱钩，客户端断连后切换/回退仍执行完毕，`finally` 保证监听恢复与维护
  状态清除；同一时刻用异步锁保证只允许一次恢复。
