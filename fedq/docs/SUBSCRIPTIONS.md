# 持续查询订阅：协议、增量状态与生命周期说明

本文档说明第四部分新增的**持续查询订阅（continuous query subscription）**：
登记一次查询后，先在明确位置 `p` 得到初始结果，随后按源的每笔提交增量维护
会变化的关系结果，用户可逐笔取走结果的净增删量更新下游报表。

代码入口：

| 模块 | 职责 |
|---|---|
| `fedq/version_server.py` | 版本源新增按位置取完整提交的 HTTP 变更入口 `/changes/*` |
| `fedq/sub_scenarios.py` | 单源订单/配送小夹具、查询构造、运行时规模数据 |
| `fedq/subscription.py` | 增量维护引擎：本地关系状态、连接索引、支持计数、分组状态、变更台账（SQLite） |
| `fedq/sub_service.py` | 订阅服务：登记/推进/读取/分页/状态/停止/重建/崩溃恢复 |
| `fedq/sub_oracle.py` | **独立**核对器：从完整表重算预期袋与差分，不调用任何增量代码 |
| `fedq/demo_sub.py` | 端到端可复做演示（含真实终止进程、断页、断档、换脸、规模访问诊断） |
| `tests/test_sub.py` | 20 个单元/端到端测试 |

## 1. 它是什么、不是什么

- **是**：真正维护一个会随提交变化的关系结果。每笔提交的 upsert/delete 先转成
  袋重数差分，再沿查询树自底向上传播，只重算受影响的连接键/分组。
- **不是**：定时重跑查询、比较两份完整文件、保存旧读取视图、解释结果血缘。
  普通联查（`run`）、读取批（`view-*`）、血缘（`lineage-*`）入口保持原样可用。
- 只接**一个**已有版本化 HTTP 源（可查它的多张表）。任何已开放提交带非空
  `requires` 的源在登记时即被明确拒绝；配置含多个源同样拒绝。

支持的查询节点：`source / filter / project / join(inner|left 等值) / distinct /
aggregate`。聚合（`count(*)`、`count(字段)`、整数 `sum`）只允许在树的最外层；
`sort/limit` 不支持，登记时报错，不静默忽略。其他算子可任意组合，包括同一张表
在不同输入位置出现（自连接）。

结果是**袋**：业务值相同的行带重数 `mult`；对外给完整有类型业务行 + 重数，
不延续单次执行的 `row_ref`。所有维护表使用 `m_` 前缀内部名，业务字段名不被占用。

## 2. 版本源变更 HTTP 协议（新增）

在既有 `/metadata`、`/snapshot/*` 之外新增三个路由，全部为 JSON：

1. `POST /changes/open` `{after, subscription_id}`
   → `{change_id, after, head, instance_id, pages}`
   打开覆盖 `after+1..head` 的变更会话。`after < earliest`（历史断档）或
   `after > head` → `409`。
2. `POST /changes/page` `{change_id, page}`
   → `{change_id, page, next_page, seq, head, instance_id, requires, note,
      changes, commit_complete, session_complete}`
   每页只承载一笔提交的一部分；同一提交可跨多页，`commit_complete` 仅在该提交
   最后一页为真。订阅者收齐同一 `seq` 的全部页才可见该提交——**一笔提交不会被
   当成多个可见状态**。
3. `POST /changes/release` `{change_id}` 释放会话。

`changes` 内是真实的 upsert/delete 与提交号；注入故障走既有
`POST /admin/fail {route:"/changes/page"}`，返回 500。

## 3. 初始化：明确位置 p，初始化期间不丢不重

`sub-register` 时取一次 `/metadata`，把当时的 `head` 作为明确位置 `p`：

1. 在 `p` 打开一次快照（`/snapshot/open` + 分页 `/snapshot/page`），读真实表状态；
2. 每个 `source` 节点各写一份 `m_src_<nid>`（自连接的两个输入位置是独立副本）；
3. 自底向上一次性建立各节点本地状态与当前结果袋；
4. 在同一 SQLite 事务内写入位置 `p`、结果与 `p` 的完成边界（无差分）。

快照在初始化结束即释放。**初始化后不再请求完整源表，也不重新下载全部历史**；
之后推进只走 `/changes/*`。源在初始化期间继续发布不影响：位置 `p` 之前的状态由
快照固定，`p` 之后的提交留待后续变更入口，既不丢也不重复。

## 4. 增量状态与传播策略（每节点维护什么）

全部中间关系、索引、历史增量落在订阅私有 SQLite 库
`subscriptions/<sub_id>.state.db`，不把完整基础表、连接结果或所有分组常驻 Python。

| 节点 | 本地状态 | 一笔提交只碰什么 |
|---|---|---|
| source | `m_src_<nid>(pk,jrow)` 主键→当前完整行 | 变更主键（旧行 −1/新行 +1） |
| filter/project | `m_bag_<nid>(jrow,mult)` 输出袋 | 子节点差分涉及的行 |
| join | 输出袋 + 两侧连接键索引 `m_join_<nid>(side,jkey,jrow,mult)` | 差分涉及的**连接键**，不相关键一行不读 |
| distinct | 输出袋 + 每值输入支持数 `m_distinct_<nid>(jrow,support)` | 支持数变化的值；仍有其他支持则不撤回 |
| aggregate（仅根） | 输出袋 + 分组成员索引 `m_aggidx_<nid>` + 存活组 `m_aggstate_<nid>` | 差分落到的**分组**（允许重算一个热点大组） |

### 4.1 join 的键级重算与 left 撤回

对每个受影响连接键：先在连接索引上应用两侧差分，再把索引临时回退算出**旧**输出、
用新索引算出**新**输出，两者按业务行求净差。null 连接键永不匹配。

- left 连接右侧**最后一个**匹配被删除：旧匹配行负数撤回，左行的 null 扩展行正数补回；
- **第一个**匹配进入时反向：null 扩展行撤回、匹配行出现。
- inner 连接无匹配则该左行不产生任何输出。
- 同笔提交两边都变、连接键迁移、自连接：因为对每个受影响键都基于“完整提交后”的
  索引求新袋、基于回退索引求旧袋，交叉项天然只计一次，得到的是整笔净差。

### 4.2 distinct 的支持计数

distinct 维护每个去重值的输入重数 `support`。一次 delete 只要还有别的输入支持
（`support>0`），该值就留在结果里；只有最后一个支持消失（`support→0`）才撤回。

### 4.3 聚合的三态与空集

每个分组在成员索引上重算，明确区分：

- **贡献和恰好为 0**：有非 null 输入、正负抵消，`sum=0`（不是 null）；
- **还有行但全部为 null**：`sum=null`、`count(字段)=0`、`count(*)` 仍计行数；
- **组已没有输入**：分组聚合撤回该组行；全局（无 group by）聚合永远保留单行，
  空集时 `count(*)=0`、`count=0`、`sum=null`。

整数 `sum` 用 Python 任意精度整数计算并以十进制文本存储，保持精确，不绕浮点。

### 4.4 一笔提交的原子落定

每笔提交在**一个 SQLite 事务**内完成：源状态更新 → 自底向上传播 → 结果袋更新 →
变更台账写入 → 位置推进。`PRAGMA synchronous=FULL` 保证进程被真正终止后，
要么整笔可见（新位置 + 完整新变更），要么整体回滚（旧位置 + 旧完整结果），
不会有半笔推进，再次推进不重复追加。

## 5. 变更台账与稳定分页

表 `m_changes(id AUTOINCREMENT, seq, kind, jrow, delta, ord)`：

- `kind=delta`：某业务行在提交 `seq` 的净重数变化（新重数−旧重数，0 不写）；
  更新表现为旧行负数、新行正数；
- `kind=boundary`：每个处理过的提交（包括无结果变化的提交）都有一条完成边界。

`sub-changes --sub-id S --after-seq K [--after-id ID] --page-size N` 用
`(after_seq, after_id)` 键集游标分页：同游标重取内容相同；`next_cursor` 续取；
读到某 `seq` 的 boundary 即知道该提交已读完。台账保留到用户显式停止，
不需要自动消费确认，也不做任何远端写入。

## 6. 状态机与失败语义

订阅状态（`subscriptions.db` 的 `state`）：

- `building`：登记中，初始化尚未共同落定；进程死后 `sub-recover` 重做初始化；
- `active`：初始化已落定，可推进/读取；
- `needs_rebuild`：历史断档（必要变更已淘汰）或源 `instance_id` 变更。
  **保留最后完整结果与变更台账供核对**，拒绝推进，给 `sub-rebuild-hint`，
  不会悄悄跳到最新或混接新源；需要用同一查询重新 `sub-register`；
- `stopped`：用户显式停止，拒绝推进但读取/分页/状态仍可用；不影响别的订阅。

失败边界：

- HTTP 失败可重试；一笔提交（哪怕跨多页）没取全，位置绝不推进；
- 变更会话/页面的 `instance_id` 与登记时不符 → `needs_rebuild`；
- `earliest > position+1`（下一笔已不可读）→ `needs_rebuild`；
- 新进程可用原 `sub_id` 继续（状态全部在磁盘）。

## 7. CLI 与库 API

| CLI | 说明 |
|---|---|
| `sub-register --config --query --workdir` | 登记并初始化（位置=当前 head） |
| `sub-advance --sub-id [--to N]` | 推进到指定已开放提交号（缺省到 head） |
| `sub-result --sub-id` | 当前完整结果（业务行 + 重数） |
| `sub-changes --sub-id --after-seq K [--after-id] [--page-size]` | 分页读净变化与边界 |
| `sub-status [--sub-id]` | 单个状态或全部订阅列表 |
| `sub-diag --sub-id` | 访问诊断：触达主键、重算连接键/分组、读写行数、HTTP 请求数 |
| `sub-stop --sub-id` | 停止（拒绝推进，保留结果） |
| `sub-rebuild-hint --sub-id` | needs_rebuild 的原因与最后结果摘要 |
| `sub-recover` | 重启后回收 building 占位 |
| `sub-fixtures --out-dir` | 导出小夹具（单源历史 + 配置 + 查询） |
| `sub-demo --workdir [--scale-keys]` | 一键端到端验收 |

库 API 对应 `SubscriptionService(workdir)` 的
`register / advance / read_result / read_changes / status / list_subscriptions /
stop / rebuild_hint / diagnostics / recover_building`。

## 8. 访问诊断（证明没有全量重扫）

`sub-diag` 的每提交诊断含：`src_pks`（触达的源主键）、各节点 `join_keys`
（重算的连接键）、`groups_recomputed`（重算分组数）、`rows_read/rows_written`，
以及累计计数。规模演示生成 N 个互不相关键后只改一个键：触达源主键=1、重算连接键=1、
节点读取行数为与 N 无关的小常数，且推进期 HTTP 只走 `/changes/*`、不发快照请求。

## 9. 独立核对器（验收答案不来自生产代码）

`fedq/sub_oracle.py` 自己从提交历史重放每个位置的完整表，再调用既有的独立参照
解释器 `fedq/reference.py`（与执行器、增量引擎都不共享求值代码）在完整表上跑原查询，
相邻位置的 `Counter` 相减得净变化。演示与测试都逐笔把增量结果/差分与该核对器对照。

## 10. 复做

```sh
sh scripts/demo_sub.sh /tmp/fedq-sub-demo 2000     # 一键全部验收场景
sh scripts/manual_sub.sh /tmp/fedq-sub-manual      # 分进程真实 HTTP + CLI 全流程
python3 -m unittest tests.test_sub -v              # 20 个订阅测试
python3 -m unittest discover -s tests              # 全部 92 个测试
```
