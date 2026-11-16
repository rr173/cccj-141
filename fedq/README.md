# fedq — 联邦查询执行器

把两个供应商 HTTP 数据源（A、B）临时联查。两个源的文本/空值语义不同，
**用户结果统一遵循本项目的统一语义，远端差异不作为数据真相**。
执行器接收有类型的查询树，按每个连接器声明的能力决定远端可以提前做什么，
剩余语义在本地（SQLite 落盘 + Python）完成。

第二部分新增**跨源读取批（读取视图）**：各供应商独立发布、版本先后不一时，
先打开一个依赖闭合、版本固定的读取视图取得 `view_id`，再把多份查询交给同一视图
执行，直到显式关闭。它回答的是“多个独立 HTTP 源此刻究竟能同时读到哪些版本”，
而不是再加一种筛选或改写答案。

第三部分新增**结果数据血缘**：在视图上做一次可留存的带血缘执行，从某一条结果
追到它实际使用的输入出现（row_ref 逐层展开），或拿源行身份反查本次哪些结果
用到了它；证据自包含，视图关闭后仍可导航。

仅依赖 Python 3.11+ 标准库。

## 快速复做

```sh
sh scripts/demo.sh            # 第一部分端到端：内嵌两个源，全部演示查询+失败边界
sh scripts/demo_views.sh      # 第二部分：三个独立版本源的读取视图全部验收场景
sh scripts/demo_lineage.sh    # 第三部分：带血缘执行全部验收场景+关视图后新进程导航
sh scripts/manual_views.sh    # 分进程三源 + 打开/查询/推进/重启/淘汰/关闭全流程
sh scripts/manual_lineage.sh  # 分进程两源 + 带血缘执行/展开/反查/关闭后导航
python3 -m unittest discover -s tests -v   # 72 个测试（31 基础 + 19 视图 + 22 血缘）
```

第一部分手动流程：

```sh
python3 -m fedq serve --source A --port 8011 --log A.http.jsonl &
python3 -m fedq serve --source B --port 8012 --log B.http.jsonl &
python3 -m fedq queries --out-dir queries/          # 导出演示查询 JSON
python3 -m fedq plan --query queries/q1_A文本等值候选超集.json
python3 -m fedq run  --query queries/q1_A文本等值候选超集.json --out r1.jsonl --print
```

## 统一语义（`fedq/semantics.py`）

- 类型：`int64`（精确）、`text`、`bool`、`null`。**bool 不混作 int**（静态类型检查拒绝混比）。
- 文本比较：**Unicode 码点顺序，区分大小写，不去尾空格**。
- 三值逻辑：`true/false/unknown`；过滤**只有 true 通过**；`eq/lt` 遇 null 为 `unknown`。
- 聚合：`sum` 任意精度整数（可超 int64），全 null 为 null；`count(*)` 空集为 0；
  `count(字段)` 跳过 null；left 连接补 null 的行按所选 `count(*)`/`count(字段)` 语义计数。
- `order` 必须由用户给出唯一决胜字段（校验唯一集合），null 顺序显式 `nulls_first`；
  `limit` 只能作用于全序 sort 之上。

## 查询树（第一版不接受任意 SQL 文本、任意函数、相关子查询）

JSON 节点：`source / filter / project / join(inner|left 等值) / aggregate(分组 count、整数 sum)
/ distinct / sort / limit`。表达式：`eq / lt / is_null / and / or / not`，
操作数为字段或带类型字面量（int 字面量按字符串传输，任意精度安全）。
校验：类型解析、连接键类型一致、sort 全序、limit 下必须是 sort、不支持 HAVING/嵌套聚合。

## 连接器能力与两源方言（`fedq/caps.py`、`fedq/datasets.py`）

每个连接器公开：表与字段类型、唯一键、`text_eq`、`text_order`、`null_compare`、
过滤算子集、投影、聚合（`count`/`sum_int`）、排序/limit、
`max_page_bytes`（公开的单条响应大小上限）、每查询稳定读取令牌。

| 规则 | A | B |
|---|---|---|
| 文本等值 | 先折 ASCII A-Z→a-z、去尾部 ASCII 空格再比较 | 与统一语义相同 |
| 文本排序 | 规整后文本序（**非**统一序） | UTF-8 字节序（合法 Unicode 下与码点序相容 → 统一序） |
| null 普通比较 | unknown（三值） | false（二值），NOT 在二值结果上执行 |

模拟端按这些公开规则**真实执行**（`fedq/server.py` 独立实现，不与执行器共享求值代码）。

## HTTP 协议

- `GET /capabilities` → 能力声明。
- `POST /query {"plan": {...}}` → 建立查询会话（快照结果），返回第 0 页与稳定 `token`。
- `POST /query {"token": t, "page": k}` → 取第 k 页；**固定令牌下页次可重试、数据不变**，
  无需另做游标恢复账本。令牌失效/页码非法 → `409`；计划超出声明能力 → `400`。
- 远端子计划只含声明过的操作（scan/filter/project/aggregate/sort/limit），
  **不允许发送任意 Python 谓词**；谓词 JSON 只有 `eq/lt/is_null/and/or/not`。
- 单条响应 ≤ `max_page_bytes`；行值按声明类型传输（int 为 JSON 数字，实现需支持任意精度）。
- 服务端写 HTTP 日志（JSONL）：实际收到的计划、返回行数、传输字段；
  执行器报告各源实际请求数/读取行数/字节/字段。

## 计划规则（`fedq/planner.py`，每条下推都带能力依据与语义前提）

远端谓词两种角色：

- **exact（等价过滤）**：远端通过集 == 统一通过集。整数/bool 的 `eq/lt` 在 A、B 皆等价
  （null 在 unknown/false 下同样不通过）；B 的文本 `eq` 等价；B 的文本 `lt` 按 UTF-8 字节，
  合法 Unicode 下与码点序相容 → 等价（不能仅因编码不同就断言不安全）。
- **superset（候选超集过滤）**：远端通过集 ⊇ 统一通过集，**必须留下残余条件**本地精查。
  A 的文本 `eq`（规整后比较，统一为真的行远端必为真）。

组合规则（不能随便分发只在单原子上安全的近似）：

- `AND`：按合取项分别判定；超集项与不可下推项留作残余。
- `OR`：任一分支不可下推则整体本地；分支超集之并仍是超集 → 整体候选，残余为整个 OR。
- `NOT`：对超集近似取反是**子集**（可能漏行）→ 禁止；B 把 null 折叠为 false 再取反，
  null 行远端为真、统一为 unknown → 远端是超集，只能候选下推并留残余。
- A 的文本 `lt` 与码点序既非超集也非子集 → 不下推。

聚合/limit 安全：

- 有残余（或非等价过滤）时，聚合与 limit **一律不下推**——在残余过滤前执行会改变答案；
  绝不在远端先截前 N 行再本地筛选凑 N。
- sort 不增删行、本地残余过滤保序，故排序键为统一序时 sort 可精确下推（即便有残余）。
- 源内聚合（过滤等价、无连接、连接器声明了 count/sum_int）真实下推，远端只回聚合行。
- 投影列裁剪始终下推（外加行 id：分页去重与行身份需要）。
- left 连接右侧的过滤：连接前（ON 语义，输入过滤）与连接后（WHERE）是不同语义，
  规划器按查询树原样执行，绝不跨连接移动谓词。
- 连接器能力不足（如无聚合能力）时，相应算子留本地，受支持查询仍正确执行。

## 本地执行（`fedq/executor.py`）

- 远端行流入 SQLite 临时库（落盘），连接/distinct/排序/limit 在 SQLite 完成，
  **不把全部联查结果常驻 Python**；SQLite 的三值逻辑与 BINARY 文本序与统一语义一致。
- 分组聚合在 Python 流式完成（sum 任意精度），结果回注 SQLite；
  sum 列以十进制文本存储 + `NUMTEXT` 排序规则，大整数精确且排序正确。
- 输出 JSONL：首行 `{"status":"incomplete"}`，成功结束才追加 `{"status":"ok",...}`；
  中途失败写 `{"status":"error",...}`。**部分流永远没有成功结束标记**，
  用户可以保存它，但它不能被标成完整查询（`read_result()["complete"]` 为 False）。

## 失败边界（明确失败，不自动恢复）

- 令牌失效/页码非法（409）→ `TokenError`
- 计划超能力（400）→ `PlanRejected`
- 坏字段类型（返回值与公开类型不符）→ `TypeViolation`
- 分页重复行 id → `DuplicateId`
- 中途 HTTP/连接错误（同页重试数次后仍失败）→ `SourceError`

## 演示查询（`fedq/demo.py`，全部与独立参照解释器对照）

1. **q1** A 有 `X`、`x`、`X␣` 三行：统一 `name='X'` 只取第一行；A 实际先回 3 候选（统计可见），本地精筛。
2. **q2** B 有 `name=null` 行：`NOT(name='X')` 不能选它（远端超集含它，本地残余排除）。
3. **q3** `amount>5 OR NOT(name='X')` + 全序 + limit 2：远端排序首行 id=2 在本地被过滤，
   limit 仍取正确后继 id=5、id=3（limit 未下推）。
4. **q4a/q4b** left 连接右侧条件分别在连接前（ON）与连接后（WHERE）：手写两种不同预期；
   含重复连接键、全 null 聚合（sum=null）、大整数（2⁶²+2⁶²=2⁶³ 超 int64 精确）、count 语义。
5. **q5** distinct：`X`/`x`/`X␣` 是三个不同值。
6. **q6a** 选择性整数过滤真实下推：HTTP 只传 1 行、2 个字段；**q6b** 聚合下推只回聚合行；
   **q6c** 空集 count=0、sum=null。
7. 失败边界：坏令牌、中途 HTTP 500、坏字段类型、分页重复 id。
8. 能力退化：连接器无聚合能力时结果仍正确。

## 独立参照解释器（`fedq/reference.py`）

纯 Python、袋语义（保留重数）、自己的三值逻辑实现，与执行器不共享求值代码；
只跑小数据，用于对照完整结果（测试中对全部演示查询逐行比对）。

## 跨源读取视图（第二部分，`fedq/versions.py`、`version_server.py`、`viewstore.py`、`coordinator.py`）

### 要解决的问题

供应商各自发布数据且常有先后：B 的一笔提交已经引用 A 的新版本，A 的查询副本却还没
开放到那个位置。直接把两边“最新”拼起来会得到**缺少前置数据**的关系。读取视图先确定
一组互相满足依赖、分量最大的版本向量 F，再把各源恰好截至 F 的真实状态固定下来。

### 版本源模型（真实发布与历史读取）

- 配置以**逻辑源名 → HTTP 地址**组织，至少三个独立实例，复用 A/B 方言。
- 每个源有持久 `instance_id`、从 1 连续递增的本地提交号、第 0 版初始表。
- 一笔提交含**同源多表**的 upsert/delete 和 `requires` 映射；`{"A":3}` 表示包含这笔
  提交的读取批也必须包含 A 的第 3 笔提交。同行 id 的更新/删除按提交顺序生效，
  **一笔提交不可拆开读取**。
- 源只开放历史的一段前缀 `head`，发布时 head 向前推进；`earliest` 是当前仍可读的最早
  状态。1..head 每笔提交的依赖始终保留在元数据里，即使数据状态已被淘汰。
- 校验：依赖源名、非负整数位置、连续提交号、字段类型（int/text/bool）都被检查；
  **bool 不能冒充位置整数或整数字段值**。不涉及 DDL 或跨源写事务。

### 打开视图与最大闭合向量

打开时对每个源取一次 `/metadata`，记住当次观察到的上界 `H[s]=head`、下界
`L[s]=earliest` 与实例身份；之后新发布的提交不并入本次打开。选择向量 F 满足
`L[s] ≤ F[s] ≤ H[s]`，且对每个源 s 的所有提交 `k ≤ F[s]`，每项 `requires[t]=d`
都满足 `F[t] ≥ d`。

- 求的是所有合法向量中**按分量比较的最大者**（最小不动点），不是任意一个安全但过旧
  的向量；不同源的提交号只是各自本地序号，从不被当作全局时间。
- 依赖允许互相要求：环本身不构成拒绝理由，互相要求的提交只要彼此都在向量内即闭合。
- 若不动点跌破某源 L，则**明确拒绝打开**。
- 记录选择结果与逐条“退回”解释（哪个源的哪笔依赖让该源从观察上界退回），便于解释
  数据为何尚不可用。

手算三源例子 `H=(A:3,B:3,C:2)`、L 全 0，依赖 `A2→B3、B2→C3、C2→A2`：

| 步骤 | 结果 |
|---|---|
| A 纳入到 3，其第 2 笔要 B:3，而 B≤3 时 B 第 2 笔又要 C:3（C 只到 2）→ A 退到 1 | A=1 |
| B 同理第 2 笔要 C:3 不可得 → B 退到 1 | B=1 |
| C 第 2 笔要 A:2，但 A 已退到 1 → C 退到 1 | C=1 |
| 稳定后所有已纳入提交（各源第 1 笔，无额外依赖）都满足 | **F=(1,1,1)** |

`A.earliest=2` 后 F[A]=1 低于下界 → 无合法向量，拒绝打开。

### 持久化与读取保证（不是只保存版本号）

打开成功前，协调者逐源把**恰好截至 F[s] 的完整表状态冻结到本视图的 SQLite 库**
（按页拉取、批量 INSERT，不把整批源表常驻 Python 内存）。视图的源身份、版本、字段
声明与每一页 HTTP 读取依据写入目录库 `views.db`；协调进程重启后仅凭磁盘即可凭原
`view_id` 继续查询。

- 源继续发布、删除行或淘汰无保护的旧状态，都**不改变**已打开视图的结果（查询走本地
  冻结库，统计里 `requests=0`）。
- 建立期任一源读取失败、或同地址 `instance_id` 已更换，都不会登记为可用视图：已取得
  的源端保护记在 `build_staging`，由显式 `view-cleanup` 统一回收，清理不碰其他视图。
- 源端另有真实读取保护（引用计数）：版本被快照持有期间 earliest 推进不能淘汰它；
  多视图引用同一版本时关闭其一不影响其他。冻结完成后协调者释放源端保护，保留责任
  完全转移到本地冻结副本。

### 版本源 HTTP 协议

| 方法/路径 | 作用 |
|---|---|
| `GET /metadata` | 实例身份、head/earliest、表声明、1..head 每笔提交依赖与改动 |
| `POST /snapshot/open` `{at, view_id}` | 对版本 at 建立读取保护，返回分页清单 |
| `POST /snapshot/page` `{snapshot_id, table, page}` | 返回该版本某表的一页真实行 |
| `POST /snapshot/release` `{snapshot_id}` | 释放一次保护（引用计数） |
| `POST /admin/publish` `{head}` | 开放前缀向前推进（演示管理接口） |
| `POST /admin/earliest` `{earliest}` | 淘汰旧状态（受保护版本钉住） |
| `POST /admin/replace-instance` `{instance_id}` | 同地址换实例，旧快照作废 |
| `POST /admin/fail` `{route}` | 注入下一次请求失败 |
| `GET /admin/state` | head/earliest/保护计数/快照/实例身份 |

### 协调者入口（库 API 与 CLI）

| 生命周期 | CLI | 说明 |
|---|---|---|
| 打开 | `view-open --config urls.json --workdir D` | 返回 `view_id`、F、H/L、退回解释 |
| 查看 | `view-show` / `view-list` | F、源身份、字段、每页 HTTP 读取依据、查询账本 |
| 查询 | `view-run --view-id ... --query q.json`，或 `run --view-id ...` | 复用既有类型树与统一值语义，绑定同一 F |
| 关闭 | `view-close --view-id ...` | 拒绝新查询；在跑查询允许完成，回收冻结副本 |
| 失败清理 | `view-cleanup --view-id ...` | 仅处理 failed 建立遗留，释放源保护、删半成品 |
| 独立核对 | `enumcheck --config ...` | 小规模枚举所有候选向量求最大元，**不与生产选择共享代码** |
| 版本源 | `serve-history --history A.json --port ...` | 分进程启动一个版本实例 |
| 场景生成 | `gen-scenarios --out-dir ...` | 导出三源历史与协调配置 |

同一视图内的不同查询可读不同表、选不同列、不同过滤/连接/分页，但全部绑定同一 F；
结果与源 HTTP 日志可按 `instance_id / at / view_id / snapshot_id / page` 逐项核对。

## 结果数据血缘（第三部分，`fedq/lineage.py`、`lineage_ref.py`、`demo_lineage.py`）

对账看到一笔联查汇总不对时，需要知道它**到底用了哪些原始行**。在已打开视图上做
一次**带血缘执行**：执行用户提交的查询树本身，把结果与可展开的证据一起落本地，
之后按 `row_ref` 逐层展开推导、或拿源行身份反查本次哪些结果用到了它。
这是结果数据的血缘——不是重新解释下推计划，也不是“改一行会怎样”的反事实分析。

- **袋语义**：同值结果不是同一条记录。投影后同值的两行是两个出现、inner 连接的
  每个匹配对是一个出现，各有 `row_ref`（`run_id:occ_id`，仅保证已完成 run 内稳定）。
- **源行身份** = 逻辑源 + 实例 + 表 + 读取版本 + 主键 + 查询树输入位置；
  同一张表出现两次时按位置区分。血缘信息不混作业务列参与过滤/分组/去重。
- **三类推导**：连接左右依据共同成立（all）；distinct 的多个输入是**备选**推导
  （any，不是多份业务记录）；聚合把每个输入出现**逐份计入**（agg，边上带
  counted/null_skipped/sum 精确值）。left 无匹配是 null_fill 标记，不虚构右源行。
- 证据落 SQLite，子推导按出现共享，**不枚举备选分支的组合**；结果与证据一起完整
  才标 ok，中途失败留下可识别的 error 目录，不冒充成功；视图关闭、源停止后，
  另一个进程凭证据库继续导航，不再请求源。

```sh
sh scripts/demo_lineage.sh      # 端到端验收 + 关视图后新进程 CLI 导航
sh scripts/manual_lineage.sh    # 分进程两源：打开→带血缘执行→展开→反查→关闭→导航
python3 -m fedq lineage-run --view-id V --query q.json --workdir W
python3 -m fedq lineage-rows/expand/lookup/runs/show ...   # 详见 docs/LINEAGE.md
```

详细模型、存储布局、分页约定与失败目录识别见 `docs/LINEAGE.md`。

## 已知限制（第一版）

- 不接受任意 SQL 文本、任意函数、相关子查询；连接为单等值键 inner/left。
- 过滤不能位于聚合之上（无 HAVING）；不嵌套聚合；不连续两级 sort。
- 远端单条响应有公开大小上限；客户端按行 id 去重检测需要结果携带 id 列。
- 聚合/limit 不下推跨连接；distinct 总是本地。
