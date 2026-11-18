# replication-log-shipper

Python 数据库**物理复制日志投递**核心库。主库按 WAL segment 向副本投递
WAL 记录，副本按字节偏移确认；库负责把“段内偏移 ACK”翻译成严格的
**同步提交点（safe LSN）**，并保证：

- 只有**完整事务**（收到 COMMIT）在**要求数量的同步副本**持久化后，
  safe LSN 才会推进；
- safe LSN 单调递增，永远不会越过仍在进行中的事务；
- 来自**旧 timeline / 旧 epoch（旧 term）的延迟 ACK 被隔离**，
  不会被错误计入切换后的新主；
- 正确处理半包、重复 ACK、投递缺口、事务跨段、slot 重启与主备切换。

仅使用 Python 标准库，无第三方依赖。

## 目录结构

```
replication_log_shipper/
├── lsn.py       # LSN 整数表示、16 MiB segment 几何与段文件名
├── records.py   # WalRecord、事务边界跟踪器 TransactionTracker、WalWriter
├── protocol.py  # 长度前缀帧、CRC32、半包解码器 StreamDecoder、ACK 编解码
├── slot.py      # ReplicationSlot / SlotStore、WalStore / MemoryWalStore
├── primary.py   # PrimaryNode：投递、ACK 仲裁、safe LSN、WAL 回收、提升
└── replica.py   # ReplicaReceiver：缺口保护、持久化 ACK、提升为新主
tests/           # unittest 测试（50 个）
```

## 核心概念

### LSN、segment 与 term

- **LSN**：WAL 字节流中的整数字节偏移，内部用非负 `int` 表示，
  也支持 `0/16B3748` 风格字符串（`parse_lsn` / `format_lsn`）。
- **WAL segment**：固定 `SEGMENT_SIZE = 16 MiB`，段文件名采用
  `TTTTTTTTNNNNNNNNNNNNNNNN`（timeline + 段号），与 PostgreSQL 习惯一致。
- **term = `(timeline_id, epoch)`**：每次主备切换都会提升 timeline 与
  epoch。所有帧和 ACK 都携带 term，用于区分新旧主任期。

### 记录与事务边界

`WalRecord` 有四种类型：`BEGIN` / `DATA` / `COMMIT` / `ABORT`。
记录的 `lsn` 与 `end_lsn` 相邻，无缝铺满字节流；一个事务可以横跨任意
数量的 segment。`TransactionTracker` 只把收到匹配 `COMMIT` 的事务视为
完整事务，`ABORT` 与仅有 `BEGIN/DATA` 的在途事务不计入可提交前缀。

## 复制协议

### 线格式

每条消息是自描述的长度前缀帧：

```
magic(1)=0x57 | type(1) | payload_len(4, 大端) | payload | crc32(4, 大端)
```

CRC32 覆盖其之前的所有字节（magic/type/length/payload）。消息类型：

- `MSG_FRAME`（主 → 副本）：timeline、epoch、单调 `seqno`、记录的
  `lsn/end_lsn/type/xid` 与变长 payload；
- `MSG_ACK`（副本 → 主）：timeline、epoch、`replica_id`、
  `durable_lsn`、ACK 自身的单调 `seqno`。

### 半包与粘包

TCP 等流式传输会任意切分字节。`StreamDecoder.feed(chunk)` 增量缓冲：

- 头部或载荷不完整时保留残片，等待下次 `feed`（半包）；
- 一次收到多帧时一次性全部解码（粘包）；
- magic 错误、未知类型、CRC 不匹配抛 `ProtocolError`；
- 重复帧可被幂等重复解码，上层按 LSN/seqno 去重。

### 投递与缺口

- 主库从副本对应 slot 的 `restart_lsn` 开始，按 LSN 升序、无缝投递；
- 副本只接受 `lsn == expected_lsn` 的帧，乱序/跳跃返回 `gap`，
  要求主库从 slot 边界重发，从而保证确认的是**无缺口持久化前缀**；
- 已持久化区间内的重发帧返回 `duplicate`。

### ACK、去重与隔离

主库 `receive_ack` 对每个 ACK 做如下判定：

1. `unknown_replica`：未注册副本 → 隔离；
2. `stale_term` / `future_term`：term 与当前主不一致 → 隔离，
   **旧 timeline 或旧 epoch 的延迟 ACK 永不修改当前任期状态**；
3. `duplicate`：ACK `seqno` 不新于已应用序号 → 幂等忽略；
4. `overclaim`：确认了从未投递或本地不存在的 LSN → 隔离；
5. 否则 `applied`：推进副本 `durable_lsn` 与 slot `restart_lsn`
   （二者都只单调前进），并重算 safe LSN。

所有被隔离的 ACK 保留在 `PrimaryNode.quarantine` 中可审计。

## Durability 假设与 safe LSN 规则

`safe_lsn`（同步提交点）的推进规则：

1. 取同步副本集合中**最好的 `required_syncs` 个**副本的 `durable_lsn`
   的最小值（多数派/仲裁视角）；异步副本不参与；
2. 该值再与本地 `flush_lsn` 取最小，确认点不能超过真实存在的 WAL；
3. 在该上界内，取**最长完整已提交事务前缀**的 COMMIT 结束 LSN；
   在途事务（含跨段事务未收到 COMMIT 的部分）会阻断前缀；
4. 结果只在大于当前值时更新——**safe LSN 单调不减，且绝不越过
   未完整持久化的事务**。段内偏移 ACK 不会让提交点越过事务边界。

持久化语义假设：

- 主库 `WalStore.append_frame` 后 `flush + fsync`，帧落盘才算本地
  `flush_lsn` 推进；
- 副本在 `append_frame`（同样 fsync）成功后才更新 `durable_lsn`，
  即 ACK 的含义是“已落盘、崩溃可恢复”，不是仅收到；
- slot 状态与接收器状态通过临时文件 + 原子 `os.replace` 持久化，
  崩溃后从最后一致状态恢复；
- WAL 回收 (`recycle_wal`) 同时受**所有 slot 的最小 restart_lsn** 与
  safe LSN 约束，避免回收仍需重发的段；
- 被回收而 slot 仍需要的段在投递时抛 `WALUnavailableError`。

## 主备切换（failover）

- 副本提升 (`ReplicaReceiver.promote`) 以无缺口持久化前缀为新主的
  WAL，重放记录重建事务状态；恢复出的 safe LSN 停在最后一个完整
  COMMIT，未完成尾部事务保持 open，不推进提交点；
- 新任期 timeline/epoch 递增，保留的 WAL 重新打标签到新 term 后
  继续从 slot 投递；
- 所有副本的当前任期进度（`shipped/durable/last_ack_seqno`）重置，
  因此旧任期 ACK 必然落入 `stale_term` 隔离区，不可能污染新主仲裁；
- 旧主自身提升 (`PrimaryNode.promote`) 行为一致，并把 slot
  `restart_lsn` 钳制到分歧点以内。

## 快速上手

```python
from replication_log_shipper import PrimaryNode, ReplicaReceiver, WalWriter
from replication_log_shipper.records import RECORD_BEGIN, RECORD_DATA, RECORD_COMMIT
from replication_log_shipper.slot import MemoryWalStore

primary = PrimaryNode("primary", required_syncs=1, wal_store=MemoryWalStore())
writer = WalWriter()
primary.add_replica("s1", "slot-s1")

begin = primary.append(writer.write(RECORD_BEGIN, xid=1))
primary.append(writer.write(RECORD_DATA, xid=1, payload=b"row"))
commit = primary.append(writer.write(RECORD_COMMIT, xid=1))

replica = ReplicaReceiver("s1", MemoryWalStore())
for frame in primary.frames_for("s1"):
    replica.handle_frame(frame)

print(primary.receive_ack(replica.build_ack()).safe_lsn)  # == commit.end_lsn
```

## 运行测试

```bash
python -m unittest
```

测试覆盖：LSN/segment 几何与跨段事务、半包/粘包/CRC、分页限流、重复与越界 ACK、
同步副本集合变化、副本滞后与缺口、slot 与 WAL 的磁盘持久化/重启、
旧 term ACK 隔离，以及切主后提交点不越过未完整持久化事务的端到端场景。
