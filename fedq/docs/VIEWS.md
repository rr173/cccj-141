# 跨源读取视图：协议与生命周期说明

本文档说明第二部分新增的版本化数据源协议、读取视图生命周期与持久化保证。
代码入口：`fedq/versions.py`（历史与选择）、`fedq/version_server.py`（版本源 HTTP）、
`fedq/viewstore.py`（持久化）、`fedq/coordinator.py`（协调者与冻结客户端）、
`fedq/enumcheck.py`（独立穷举核对）。

## 1. 概念与不变量

- **逻辑源 / 实例**：配置以逻辑源名（A/B/C…）映射 HTTP 地址；每个地址背后是一个有
  持久 `instance_id` 的独立实例。同地址换了 `instance_id` 即视为不同的源。
- **提交历史**：提交号从 1 连续递增，第 0 版是初始表。一笔提交是不可分割的读取单位，
  可同时改多张表（upsert/delete），并带 `requires` 跨源依赖。
- **开放前缀**：源只承诺 `earliest..head` 之间的状态可直接读取。head 随发布前移；
  earliest 随淘汰前移。依赖元数据 1..head 始终保留，与数据状态是否淘汰无关。
- **选择向量 F**：每个源一个本地提交位置。合法性 = 盒约束 `L≤F≤H` + 全部已纳入提交的
  requires 满足。选择结果是合法集合的**分量最大元**。
- **闭合不向下封闭**：缩小某源分量可能让别的已纳入提交失去前置，因此“随便给个安全的
  旧向量”是错的；必须求唯一的最大不动点。环不拒绝——互相要求的提交一起纳入即闭合。

## 2. 选择算法与可解释性

`select_vector` 从 H 出发做最小不动点：反复扫描已纳入提交的 requires，一旦
`requires[t]=d > F[t]`，就把触发源退到该提交前一个位置 `k-1`，记录一条 trace，直到
稳定。稳定点自身满足全部已纳入依赖，且任何合法 G 在每次退回后仍有 `G≤F`，故它就是
分量最大元。跌破 L 抛 `NoClosedVector`。`retreat_summary` 按源汇总退回原因。

`enumcheck.py` 是**独立实现**：笛卡尔积枚举盒内所有向量、逐个独立判闭合、再在合法集
里找最大元，与生产代码无任何共享函数，用于小规模核对。

## 3. 版本源 HTTP 协议

所有请求/响应为 JSON。读路径三类：

1. `GET /metadata`
   `{source, instance_id, dialect, head, earliest,
     tables:{t:{columns,key}}, commits:[{seq, requires, changes, note}]}`
2. `POST /snapshot/open` `{at, view_id}`
   → `{snapshot_id, at, instance_id, tables:{t:{columns,types,rows_total,pages}}}`
   对 at 加一份引用计数保护；at 不可读（已淘汰且无保护）→ 409。
3. `POST /snapshot/page` `{snapshot_id, table, page}`
   → `{snapshot_id, table, at, view_id, instance_id, columns, types, rows, page, next_page}`
   快照未知/页码非法 → 409；注入故障 → 500。
4. `POST /snapshot/release` `{snapshot_id}` 释放一份保护。

管理接口（`/admin/*`）只为可复做演示存在，不属于查询协议。每条请求都写 JSONL 日志，
带 `source / instance_id / at / view_id / snapshot_id / table / page`，可与视图账本核对。

## 4. 打开流程（协调者）

```
for s in sources: meta[s] = GET /metadata        # 记住 instance_id、H、L
F, trace = select_vector(...)                     # 无合法向量则拒绝，不产生视图
view_id = begin_build()                           # 目录里先落 failed 占位
for s in sources:                                 # 顺序冻结，任一失败即整体回滚
    snap = snapshot/open(at=F[s], view_id)
    校验 snap.instance_id == 观察到的 instance_id
    for table, page: 拉取并写入 <view_id>.frozen.db，逐页记 http_reads
    stage_protection(...)                         # 暂存源端保护，供失败清理
commit_view()                                     # 全部成功才原子置 open
释放各源源端保护（保留责任转移给本地冻结副本）
```

失败路径：不自动释放保护，置 `failed` 并保留 `build_staging`；`view-cleanup` 按暂存记录
逐源 release、删除半成品冻结库与目录行，只针对该 view_id。

## 5. 持久化布局

```
<workdir>/
  views.db                     # 目录库：views / view_sources / http_reads /
                               #         query_runs / build_staging
  views/<view_id>.frozen.db    # 每视图一个冻结库：frozen_rows(tbl,page,rowidx,jrow)
  runs/<view_id>/<query>/      # 每次查询的本地 SQLite 执行库
```

行以 JSON 文本存储，int/text/bool/null 的精确类型不经过 SQLite 类型亲和而失真
（bool 仍是 bool，大整数不截断）。协调进程重启后，`view-open` 不再需要重做，
`view-run` 直接打开已有冻结库；目录库里的源身份/版本/字段/读取依据即为查询依据。

## 6. 查询、关闭与并发

- `view-run`/`run --view-id` 用冻结库构造只支持 scan/project 的 `FrozenClient`，过滤、
  聚合、排序、limit、连接全部在协调者本地按统一语义执行（方言差异不再影响答案）。
- 关闭：状态置 `closed` 并立即 unlink 冻结库文件。新查询在 `begin_query` 被拒
  （`ViewClosed`）；已开始的查询持有自己的 sqlite 连接，POSIX 下 unlink 后仍可读完。
- 两视图引用相同源版本：各自有独立冻结库与独立生命周期，关闭一个不触碰另一个。
- 目录库的写操作在锁内串行；查询读取冻结库互不干扰。

## 7. 验收场景对照（`fedq/demo_views.py`）

| 场景 | 断言要点 |
|---|---|
| 手算环 H=(3,3,2) | 生产选择与独立穷举都得 F=(1,1,1)，并读到三源第 1 版真实行 |
| A.earliest=2 | 拒绝打开，只留 failed 记录且可清理 |
| 无交叉依赖、H 不同 | F=H，全部上界保留 |
| A2↔B2 互相要求 | 环不拒绝，F 含两笔提交 |
| 打开后发布（跨表同笔 upsert+delete） | 老视图明细/连接不变，新视图见新版本 |
| 翻页中途发布 | on_batch 回调里推进 head，老视图冻结结果不变 |
| 协调者重启 | 新进程凭原 view_id 查询成功 |
| 淘汰旧状态 | 源端拒绝对旧版本开新快照，老视图仍从本地冻结读出 |
| 第三源失败 | 无 open 视图，源保护被持有，cleanup 后归零且不碰其他视图 |
| 实例替换 | 固定身份的打开被拒，旧视图身份与数据不变 |
| 查询中关闭 | 在跑查询完成，之后新查询被拒 |
| 两视图共享版本 | 关一个，另一个结果一致且可用 |
