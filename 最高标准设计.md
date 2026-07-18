# kvspace-rdma 最高标准设计

> **范围**：本文档仅覆盖 **server**。SDK/client 在独立仓库中设计。
>
> **接口契约**：与 `kvlang/internal/kvspace/kvspace.go` 完全一致的 12 个方法；
> server 是这个契约的 RDMA 传输实现，不增减语义。

---

## 零、一句话定位

**kvspace-rdma 是一个单机 RDMA KV 存储服务器**：
- 对外暴露 `KVSpace` 接口语义（Get/Set/Del/List/Watch/Notify/Link/Unlink）
- 以 InfiniBand / RoCE 作为唯一传输层（libibverbs，RC 队列对）
- 目标：P99 GET latency < 5 μs，SET < 8 μs，单核吞吐 > 5 Mops/s（4-byte key/value）

---

## 一、行业对标（必须超过或持平）

| 系统 | 年份 | 核心贡献 | kvspace-rdma 借鉴点 |
|------|------|---------|-------------------|
| **Pilaf** (ATC'13) | 2013 | 客户端单边 RDMA READ 直接查 cuckoo hash，GET 零 server CPU | 服务端内存布局对 one-sided 友好的 cuckoo hash |
| **HERD** (SIGCOMM'14) | 2014 | RDMA WRITE 发送请求，UD 组播回复，揭示 QP 数量瓶颈 | 用 WRITE 发送请求（绕过 RECV 配额），控制 QP 数量 |
| **FaRM** (NSDI'14) | 2014 | one-sided RDMA READ + distributed tx；循环缓冲区轮询 | 无锁环形 RX 缓冲区，CQ 忙轮询代替中断 |
| **DrTM** (SOSP'15) | 2015 | HTM + RDMA 组合事务，latency < 1 μs | 热路径无锁（HTM 或 lock-free CAS） |
| **Clover** (ATC'20) | 2020 | 解耦元数据与数据，cold-tier 直接 RDMA | Link/List 元数据路径独立，不影响 Get/Set 热路径 |
| **MICA** (NSDI'14) | 2014 | DPDK 每核独立分片，消除跨核通信 | 多核分片：key hash 决定 owner core，零跨核通信 |

**与以上系统的关键差异**：
1. 本系统实现完整的 `Watch`（BLPOP 语义）和 `Notify`（LPUSH 语义），这是上述系统均未覆盖的 pub/sub 语义。
2. 支持 `Link`/`Unlink`（路径软链接），元数据操作均为 two-sided（server CPU 参与）。

---

## 二、接口语义契约

```
Get(key)             → (value, error)         — 单 key 精确查找
Gets(keys...)        → ([]string, error)       — 多 key，缺失位置为 ""
Set(key, value)      → error                  — 写入并维护目录索引
Sets(map)            → error                  — 原子批量写入（best-effort atomicity）
Del(keys...)         → error                  — 精确删除（含索引清理）
DelR(prefix)         → error                  — 递归删除；prefix 是链接则只删链接本身
List(prefix)         → ([]string, error)       — 列出直接子项名（一级，不递归）
Watch(key, timeout)  → (string, error)         — 阻塞等待，BLPOP 语义
Notify(key, value)   → error                  — 推送，LPUSH 语义
Link(target, link)   → error                  — 软链接：link → target
Unlink(link)         → error                  — 删除链接本身，不影响 target
DisConn()            → error                  — 客户端主动断开
```

---

## 三、传输层设计

### 3.1 QP 类型选择

| 类型 | RTT | 可靠性 | QP 数量 | 选择 |
|------|-----|-------|---------|------|
| RC (Reliable Connected) | 最低 | 硬件保证 | O(clients) | ✅ **首选** |
| UC | 低 | 不可靠 | O(clients) | ❌ 需软件重传 |
| UD | 低 | 不可靠 | O(1) | ❌ 1200-byte MTU 限制 |

**结论**：使用 RC。QP 数量 = 并发连接数（预期 ≤ 10k），在现代 HCA（ConnectX-6）上可接受。

### 3.2 操作类型分配

| 操作 | RDMA 原语 | Server CPU | 理由 |
|------|----------|----------|------|
| `Get` (未来 v2) | one-sided READ (client-issued) | 0（zero server CPU） | Pilaf 方案：client 直接 READ cuckoo hash |
| `Get` (v1) | two-sided SEND/RECV | 低 | 正确性优先，v2 优化 |
| `Set` | RDMA WRITE（请求）+ SEND（应答）| 低 | HERD 方案：WRITE 绕过 RECV 配额 |
| `Del/DelR` | two-sided | 中 | 需索引清理，CPU 不可避免 |
| `List` | two-sided | 中 | 目录遍历 |
| `Watch` | two-sided + server-push WRITE | 中 | server 主动 WRITE 到 client MR 唤醒 |
| `Notify` | two-sided SEND | 低 | 写入 queue 后 server-push |
| `Link/Unlink` | two-sided | 中 | 元数据 |

### 3.3 RDMA 操作路径（Set 示例，HERD 风格）

```
Client                                Server
  │                                      │
  │──RDMA WRITE req → server RX buf ────▶│  (零 server CPU：DMA 直接入内存)
  │                                      │  server CQ polling 检测新请求
  │                                      │  执行 Set，更新 hash + 目录索引
  │◀── RDMA SEND resp ───────────────────│  (server 发送应答)
  │                                      │
```

### 3.4 Watch/Notify：server-push 设计

```
Watch 请求到达 server
  │
  ├─ key 已有值（queue 非空）→ 立即 RDMA WRITE 结果到 client MR → SEND 唤醒信号
  │
  └─ key 无值 → 注册 watcher{client_qp, mr_addr, rkey, timeout}
                │
                Notify(key, val) 触发
                  │
                  ├─ RDMA WRITE val 到 watcher.mr_addr（zero-copy 写入 client 内存）
                  └─ SEND 唤醒信号（1 byte，触发 client CQ）

超时路径：server 定时器触发，WRITE 空值 + 错误码
```

---

## 四、服务端内存布局

### 4.1 KV 存储：Cuckoo Hash（对 one-sided READ 友好）

```
┌──────────────────────────────────────────────────────┐
│  MR（Memory Region，Hugepage 2MB 对齐）                 │
│                                                      │
│  ┌─────────────────────────────────────────────────┐ │
│  │  Hash Table（cuckoo，2 个桶数组 T1 / T2）         │ │
│  │  每个 Slot = 64 bytes（1 cache line）             │ │
│  │  ┌──────────────────────────────────────────┐   │ │
│  │  │ version:u32 | keylen:u16 | vallen:u16    │   │ │
│  │  │ key[24]                                   │   │ │
│  │  │ val[24] (inline) or ptr_to_heap:u64       │   │ │
│  │  └──────────────────────────────────────────┘   │ │
│  └─────────────────────────────────────────────────┘ │
│                                                      │
│  ┌─────────────────────────────────────────────────┐ │
│  │  Value Heap（大 value：> 24 bytes）               │ │
│  │  Slab allocator（8/16/32/64/128/256/512/1024…）  │ │
│  └─────────────────────────────────────────────────┘ │
│                                                      │
│  ┌─────────────────────────────────────────────────┐ │
│  │  Directory Index（路径 → 子项集合）               │ │
│  │  独立 hash table，key = prefix string            │ │
│  │  val = sorted array of child names              │ │
│  └─────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────┘
```

**设计原则**：
- 每个 Slot 恰好 64 bytes（一个 cache line），允许 client 用单次 one-sided RDMA READ 读取
- Version 字段（MFENCE 写，seqlock 语义）：client one-sided READ 时可检测写冲突，重试
- Inline value（≤ 24 bytes）：slot 内直接存储，GET 一次 READ 完成，零额外 RTT
- 大 value：slot 内存指针；client one-sided 需两次 READ（v2）；v1 server 直接返回

### 4.2 Watch Queue：每 key 一条无锁 MPSC 链表

```c
struct watch_queue {
    std::atomic<uint64_t>  head;        // 最新值（LPUSH 写，BLPOP 读）
    std::atomic<watcher*>  waiters;     // 等待 client 链表（server push 后摘除）
};
```

- `Notify(key, val)` = MPSC 原子 push + 遍历 waiters → 对每个 watcher RDMA WRITE
- `Watch(key, timeout)` = 尝试 pop → 注册 watcher → 等待 CQ completion

---

## 五、并发模型

### 5.1 Per-core Shard（MICA 方案）

```
Core 0           Core 1           Core 2           Core 3
  │                │                │                │
  ▼                ▼                ▼                ▼
Shard[0]        Shard[1]         Shard[2]         Shard[3]
(hash[0,4,8…])  (hash[1,5,9…])  (hash[2,6,10…])  (hash[3,7,11…])
  │                │                │                │
  ▼                ▼                ▼                ▼
CQ poller       CQ poller        CQ poller        CQ poller
（忙轮询，不中断）  （忙轮询）         （忙轮询）         （忙轮询）
```

- `key → shard` = `MurmurHash3(key) % num_cores`（在 client 请求头中携带，server 不需再 hash 路由）
- 每个 shard 独立的 hash table 分区，**零跨核通信**，零 lock contention
- `List`/`DelR`（跨 shard 操作）：通过 core-0 协调，批量 CAS

### 5.2 CQ 轮询策略

```c
// 热路径：忙轮询（< 10 μs latency 必须）
while (true) {
    int n = ibv_poll_cq(cq, BATCH_SIZE, wcs);
    if (n > 0) process_completions(wcs, n);
    // n == 0：不休眠，继续轮询（100% core 占用换 μs 级 latency）
}
```

- 生产模式：每核专用轮询线程，`sched_setaffinity` 绑核，`mlockall` 锁内存
- 调试模式：允许中断驱动（`ibv_req_notify_cq` + `epoll`），降低 CPU 占用，latency 升至 ~50 μs

### 5.3 RX Buffer 管理（FaRM 风格循环缓冲区）

```
Server RX MR（每 shard 独立）
┌─────────────────────────────────────────────────┐
│ buf[0] │ buf[1] │ buf[2] │ … │ buf[N-1] │ buf[0] │
│  4096B │  4096B │  4096B │   │   4096B   │ ← WRITE│
└─────────────────────────────────────────────────┘
         ↑ head（server 消费位置）
```

- Client 用 RDMA WRITE 写入 `server_rx_buf + (seq % N) * BUF_SIZE`（FaRM 循环缓冲区）
- Server CQ polling 检测完成，`head` 指针前移；ring-buffer 满则 back-pressure client

---

## 六、Wire Protocol（帧格式）

所有消息均为**定长 header + 变长 payload**，header 恰好 32 bytes：

```
┌────────────────────────────────────────────────────────────────┐
│ magic:u16=0x4B56 | version:u8=1 | op:u8 | seq:u32             │ 8B
│ key_len:u16      | val_len:u32  | flags:u16                    │ 8B
│ reserved[16]                                                   │ 16B
├────────────────────────────────────────────────────────────────┤
│ key_data[key_len]                                              │
│ val_data[val_len]   (Set/Notify 才有)                           │
└────────────────────────────────────────────────────────────────┘

op 编码：
  0x01 Get     0x02 Gets    0x03 Set     0x04 Sets
  0x05 Del     0x06 DelR    0x07 List
  0x08 Watch   0x09 Notify
  0x0A Link    0x0B Unlink  0x0C DisConn
  0x81 Resp_OK 0x82 Resp_Err 0x83 Resp_Push（Watch server-push）

flags（Request）：
  bit0 = TAIL_CALL（DisConn 时置位）
  bit1 = BATCH_MORE（Sets/Gets 流水线时，批次未结束）

flags（Response）：
  bit0 = HAS_MORE（Gets/List 多值时分片）
  bit1 = TIMEOUT（Watch 超时）
```

**为什么不用 FlatBuffers / Protobuf**：
- header 固定 32 bytes，server CQ completion 后无需任何 decode 即可识别 op 和 key 位置
- payload 直接在 RDMA MR 中，decoder 直接 cast，零拷贝
- FlatBuffers 的 vtable 开销（~12 bytes/字段）在 < 64 byte 消息中不可接受

---

## 七、文件职责与依赖层次

```
kvspace-rdma/
├── 最高标准设计.md         ← 本文档
│
├── include/
│   └── kvspace_server.h    公开 C API（仅用于测试和基准）
│
├── src/
│   ├── main.cc             进程入口：CLI 参数 → Server::Start()
│   │
│   ├── rdma/               RDMA 基础设施层（不感知 KV 语义）
│   │   ├── context.cc/h    ibv_context / PD / MR 生命周期
│   │   ├── qp.cc/h         RC QP 创建 / 连接握手（CM 或 out-of-band exchange）
│   │   ├── cq_poller.cc/h  CQ 忙轮询线程，每核一个，回调驱动
│   │   └── mr_pool.cc/h    Hugepage MR 分配器（slab: 32B/64B/128B/256B/1KB/4KB）
│   │
│   ├── store/              KV 存储层（不感知 RDMA，可单独单元测试）
│   │   ├── cuckoo.cc/h     Cuckoo hash table（lock-free get，seqlock set）
│   │   ├── slab.cc/h       Value heap slab allocator
│   │   ├── dir_index.cc/h  Directory index（List / Set / Del 索引维护）
│   │   ├── watch_queue.cc/h Watch/Notify 队列（MPSC + watcher 链表）
│   │   └── symlink.cc/h    Link/Unlink（元数据软链接解析）
│   │
│   ├── proto/              协议层（帧 encode/decode，无 I/O）
│   │   ├── frame.h         Request/Response struct，cast-safe（static_assert sizes）
│   │   └── op_codes.h      OpCode enum
│   │
│   └── server/             核心逻辑层（RDMA + Store 组合）
│       ├── server.cc/h     Server 主类：初始化 / 分片 / 连接管理
│       ├── shard.cc/h      per-core shard：CQ → dispatch → store → reply
│       ├── conn_accept.cc/h CM 事件处理 / QP 握手（独立线程）
│       └── handlers.cc/h   每个 op 的处理函数（一函数一 op，无 switch 嵌套）
│
├── bench/                  基准测试（YCSB workload A/B/C/D/F）
│   ├── ycsb_bench.cc
│   └── latency_cdf.cc
│
├── test/                   单元 + 集成测试
│   ├── store_test.cc       cuckoo / slab / dir_index 纯内存测试（无需 RDMA 硬件）
│   ├── proto_test.cc       帧编解码测试
│   └── integration_test.cc 需要 rxe（软 RDMA）或真实 HCA
│
└── CMakeLists.txt
```

**单向依赖（严格）**：
```
main.cc → server/ → rdma/
                 → store/
                 → proto/
store/ → （无内部依赖，只用 STL + 锁原语）
rdma/  → （只用 libibverbs + 系统调用）
proto/ → （只用 stdint.h）
```

- `store/` 层**禁止** include 任何 rdma/ 头文件
- `rdma/` 层**禁止** include 任何 store/ 头文件
- `server/handlers.cc` 是唯一允许同时 include rdma/ 和 store/ 的位置

---

## 八、性能目标与衡量标准

| 指标 | 目标 | 测量条件 |
|------|------|---------|
| GET P50 latency | < 2 μs | 4-byte key，inline value，1 client，100% read |
| GET P99 latency | < 5 μs | 同上 |
| SET P99 latency | < 8 μs | 4-byte key/value，1 client |
| Watch latency（Notify→Watch唤醒） | < 10 μs | 1 watcher，local rack |
| 单核 GET 吞吐 | > 5 Mops/s | 4 core，100% YCSB-C（read-only） |
| 单核 Mixed 吞吐 | > 2 Mops/s | YCSB-A（50% read, 50% update） |
| 内存利用率 | > 70%（有效数据/已分配内存）| cuckoo load factor ≥ 0.7 |
| 连接扩展 | 1000 并发 clients，latency 不超过 1.5× | RC QP，ConnectX-6 |

以上数字参考 HERD（SIGCOMM'14, 10 Mops/s GET on IB FDR）进行设定，目标持平或超越。

---

## 九、禁止项（不得妥协）

| 编号 | 禁止 | 理由 |
|------|------|------|
| P1 | 热路径 malloc/free | 每次 `malloc` ≈ 100ns，直接破坏 μs 级 latency；一律用 slab/pool |
| P2 | 热路径 mutex lock | 用 lock-free CAS（`std::atomic`）或 seqlock；mutex 引入 kernel syscall |
| P3 | 热路径 `ibv_req_notify_cq`（中断模式） | 中断路径 latency > 50 μs；一律忙轮询 |
| P4 | `std::string` 在 MR 管理的内存中 | `std::string` 内部 malloc；MR 中的 buffer 用 `char*` + 长度字段 |
| P5 | store/ 层感知 RDMA 地址 / rkey | 破坏分层；store 只操作 void*，不知道 MR |
| P6 | 跨 shard 的同步调用 | 死锁风险；跨 shard 用消息传递（ring buffer），不用 shared lock |
| P7 | 函数名超过 50 字符，或文件超过 500 行 | 可维护性；超限必须拆分 |
| P8 | 注释用英文 + 中文混合在同一行 | 统一：接口注释英文（doxygen），实现注释中文 |
| P9 | 在 `handlers.cc` 之外调用 ibv_post_send | 所有 RDMA 发送必须经过 `shard::reply()`，便于 tracing 和 backpressure |
| P10 | 忽略 ibv_post_send / ibv_poll_cq 的返回值 | 每个 RDMA 调用结果必须检查，错误路径必须有 metric 计数 |

---

## 十、可观测性（第一天必须有）

- **Latency histogram**：每个 op 的 P50/P99/P999，用 HdrHistogram（lock-free）
- **Throughput counter**：每核每秒 ops，原子计数，1s 聚合
- **RDMA error counter**：`IBV_WC_REM_INV_REQ_ERR` 等每类错误独立计数
- **Watch queue depth**：每 key 的 watcher 数量，异常 > 1000 时告警
- **Slab utilization**：每个 slab class 的使用率，< 30% 触发告警
- 输出格式：`/metrics` HTTP endpoint（port 9090），Prometheus text format

---

## 十一、构建与依赖

```cmake
# 最小依赖原则：只引入必须的库
find_package(libibverbs REQUIRED)   # RDMA 原语
find_package(rdmacm REQUIRED)       # CM 握手（可选，可用 out-of-band）

# 禁止引入：protobuf / grpc / boost / folly / abseil
# 理由：以上库均有不可接受的热路径分配开销，或引入不必要的模板实例化
#
# 允许引入（header-only 或极小）：
#   HdrHistogram_c     — latency 直方图
#   {fmt}              — 日志格式化（zero-allocation for fixed strings）
#   googletest         — 测试（仅 test/ 目录）
```

C++ 标准：**C++20**（`std::atomic`、`std::span`、`std::jthread`、concepts 均可用）

---

## 十二、演进路线

| 阶段 | 目标 | 里程碑 |
|------|------|-------|
| **v0** | 跑通 | two-sided SEND/RECV 实现全部 12 个方法，单线程，无 shard |
| **v1** | 达标 | per-core shard，忙轮询，slab allocator，P99 GET < 5 μs |
| **v2** | 卓越 | client one-sided READ for GET（Pilaf 方案），seqlock version，P99 GET < 2 μs |
| **v3** | 生产 | 持久化（NVMe SSD，io_uring），Prometheus metrics，连接重建，graceful shutdown |

**当前阶段：v0 起点**。每次提交必须保持所有 `test/store_test.cc` 通过（不依赖 RDMA 硬件）。
