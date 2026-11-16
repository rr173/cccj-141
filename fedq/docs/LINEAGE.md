# 结果数据血缘：带血缘执行、可展开证据与按源行反查

本文档说明第三部分新增的结果数据血缘功能。代码入口：
`fedq/lineage.py`（带血缘执行引擎 + 证据库 + 导航服务）、
`fedq/lineage_ref.py`（独立小样本参照程序，不调用生产血缘代码）、
`fedq/demo_lineage.py`（验收演示与夹具）、`tests/test_lineage.py`。

## 1. 定位与边界

对账人员看到一笔联查汇总不对时，需要知道它**到底用了哪些原始行**。
本功能在已有视图（`view_id`）上做一次**带血缘执行**：执行用户提交的
（已有类型的）查询树本身，把结果和可展开的证据一起落到本地，之后可以：

- 读取结果：每行带稳定 `row_ref`；
- 按 `row_ref` 逐层展开推导，直到源行身份；
- 拿一个源行身份反查：本次哪些**最终结果**用到了它。

它**是**结果数据的血缘；它**不是**：

- 重新解释下推计划（血缘挂在查询树算子的输入/输出出现上，与远端下推无关）；
- 反事实影响分析（不回答“修改一行后答案会怎样”）；
- “为什么没有匹配”的全库否定证明（left 连接无匹配时只展示左侧依据与
  补 null 标记，不枚举右侧全库来证明没有匹配）。

## 2. 出现（occurrence）与袋语义

执行按**袋语义**保留重数：同值结果不一定是同一条记录。

- 投影后两条输入即使值完全相同，未经过 distinct 也保留两个结果出现，
  各有自己的 `row_ref`；
- inner 连接的每个匹配对是一个出现，不能用值或源 id 集合代替；
- `row_ref = <run_id>:<occ_id>`，只保证**已完成 run 内**稳定
  （结果翻页、展开、反查一直指向原出现），不同执行不产生相同编号。

**源行身份** = 逻辑源 + 实例(`instance_id`) + 表 + 读取版本(`at`，即视图 F
分量) + 主键 + **查询树输入位置**(`path`)。同一张表在查询中出现两次时
（如自连接），`path`（如 `/i/l/i` 与 `/i/r/i`）区分它分别走了哪个输入位置。

血缘列（`occ_id`/`seq`/`row_ref`）**不是业务列**：证据库的业务值表只含业务列，
过滤/分组/去重只作用于业务列；按 `row_ref` 过滤会在校验阶段被当作未知列拒绝。
普通执行入口（`run`/`view-run`）不受影响。

## 3. 三类推导关系

边挂在“输出现 → 输入出现”上，只有三种：

| 关系 | 含义 | 产生算子 |
|---|---|---|
| `all` | **共同成立**：所有依据必须同时存在 | join 的左/右依据；filter/project/sort/limit 的唯一输入 |
| `any` | **备选推导**：任一输入出现都足以推出该结果 | distinct 合并的多个输入出现 |
| `agg` | **逐份计入**：每个成员出现都实际计入同一汇总 | aggregate 的组成员 |

- **连接**：一行结果需要左右依据共同成立。left 连接无匹配时，右侧是一条
  `null_fill` **标记边**（没有 `to_occ`），绝不虚构一个右源 null 主键的行；
  真实右行的字段为 null（有 `row_ref`，可继续展开到源行）与完全没有右行
  （标记）严格区分。
- **distinct**：相同业务行的多个输入出现合成一个结果，这些输入是**备选**
  的推导，不是多份业务记录。
- **聚合**：把实际输入出现逐份计入同一个汇总。每条成员边带逐份贡献：
  - `count(*)`：每个成员都是 `{"kind":"counted"}`；
  - `count(字段)`/`sum(字段)`：非 null 的出现 `counted`，null 的出现
    `null_skipped`（因 null 未计入）；
  - `sum` 边上保留每个非 null 贡献的**精确值**（十进制文本，任意精度）；
    0 或正负抵消不会导致来源消失；不先把相同源行去重再算——连接可能让
    同一行贡献多次。
- 分组键说明本组的输入成员；**全局空集聚合**也有一条可解释结果
  （count=0、sum=null、`empty_input=true`、成员 0）；**空的分组聚合**没有结果行。
- 过滤、排序、limit 只让实际保留下来的出现成为最终结果：被淘汰的兄弟行
  从一开始就不产生输出现，追溯与反查都不会混入它们。

用户可以**逐层展开**这些关系（每次调用只返回直接推导的一层），而不是只拿到
一个去重后的源行清单。

## 4. 存储：共享子推导，不枚举组合

证据全部落 SQLite（`<workdir>/lineage/<run_id>/evidence.db`）：

```
meta       run 元数据（状态、查询、列、根节点、行数、结果校验和、源坐标）
nodes      查询树节点（path、算子、细节、业务列）
occs       出现登记（occ_id、节点、输出序号）
n<node>    每个节点的业务值表（只含业务列；sum 以 NUMTEXT 文本存，任意精度）
edges      推导边（from_occ、slot、kind∈{all,any,agg}、to_occ、aux）
src_rows   源行身份（逻辑源/实例/表/版本/主键/输入位置）
occ_src    扫描出现 → 源行身份
usage      反查表（src_id, root_occ）：递归 CTE 在库内计算的可达对
agg_summ   每组每聚合的口径（计入数、null 跳过数、结果值）
```

- distinct 的备选边用**集合式** `INSERT...SELECT` 写入：每个输入出现一条边，
  子推导按出现共享（DAG）。两个 distinct（备选 2 与 3）再连接时，连接出现
  只有左右 2 条边——**不展开备选分支的 2×3 组合**（演示 L8 清点：28 出现、
  25 边、6 反查对）。
- 连接结果、distinct 备选、聚合成员都不常驻 Python：执行逐批流式落盘；
  聚合只把每组的小状态留在内存（与既有执行器一致），成员边直接批量入库。
- `usage` 只记录“源行 → 最终结果出现”的可达对（线性于真实依赖），
  不是推导路径的笛卡尔展开。

## 5. 运行状态与失败目录

```
<workdir>/lineage/
  runs.db                    目录库：每次 run 的 state（building|ok|error）
  <run_id>/run.json          自描述：查询、列、源坐标、状态、错误、结果校验和
  <run_id>/evidence.db       证据库（见上）
  <run_id>/INCOMPLETE.txt    仅失败 run 存在：失败原因
```

- 结果与证据**一起完整**后才标成 `ok`（可追溯成功）：先落齐证据库并写入
  校验和，再更新 run.json，最后更新目录库。
- 执行或证据写入中途失败：run 标为 `error`，目录里留下 `run.json(state=error)`
  与 `INCOMPLETE.txt`；`lineage-runs` 可见。**识别失败目录**：目录库状态非 ok、
  run.json 的 state 非 ok、或存在 INCOMPLETE.txt，三者任一即为不完整；
  导航接口（rows/expand/lookup）对非 ok 的 run 一律拒绝，不给出看似完整的血缘。
  不自动续算；重跑就是新起一个 run。
- 已完成 run 的查询、结果和源坐标可核对：`lineage-show` 重算结果
  SHA-256 并与留存值比对（`verified`）。
- 证据库自包含：原视图关闭、源进程消失后，另一个进程仅凭
  `<workdir>/lineage/` 即可展开与反查，不再请求源、不重跑原查询。

## 6. API 与命令

Python API（`fedq.lineage.LineageService`，导航不需要视图）：

```python
svc = LineageService(workdir)                 # 与协调者同一 workdir
# 开始带血缘执行（需要仍打开的视图；通常经 Coordinator.lineage_run 调用）
info = coord.lineage_run(view_id, qjson, name="q1")     # -> {"run_id", ...}
svc.rows(run_id, page=0, page_size=100)       # 结果分页，每项带 row_ref
svc.expand(row_ref, page=0, page_size=100)    # 展开一层；备选/成员分页
svc.lookup(run_id, "A", "accounts", [1],      # 按源行身份反查（分页）
           path=None, page=0, page_size=100)
svc.list_runs(); svc.show(run_id)             # 目录与核对
```

命令（`--workdir` 为协调者目录；导航类命令在视图关闭后仍可用）：

```sh
python3 -m fedq lineage-run    --view-id V --query q.json --workdir W --name q1
python3 -m fedq lineage-rows   --run-id R --workdir W --page 0 --page-size 20
python3 -m fedq lineage-expand --row-ref R:16 --workdir W --page 0 --page-size 20
python3 -m fedq lineage-lookup --run-id R --source A --table accounts \
       --pk 1 [--path /i/l/i] --workdir W --page 0 --page-size 20
python3 -m fedq lineage-runs   --workdir W
python3 -m fedq lineage-show   --run-id R --workdir W
python3 -m fedq lineage-demo   --workdir /tmp/fedq-lineage-demo
python3 -m fedq lineage-fixtures --out-dir cfg   # 导出夹具供分进程复做
```

**分页约定**：页大小由调用方指定（1..10000），页码从 0 起；结果按 `seq`、
备选/成员按边插入序、反查按 `(root_occ, path)` 排序——已完成 run 的证据
不可变，因此翻页不遗漏、不重复。展开只返回直接子引用（`row_ref`），
调用方据此分步导航，不会把全部后代塞进一页。

## 7. 主例：袋重数与备选来源的差别

数据（夹具）：`A.accounts` 有一笔 `id=1, amount=10` 的账目；
`B.entries` 有两行 `amount=10`（id=1、id=2）。

- **直接连接后汇总**（L1：`join ⋈ amount → sum(amount), count(*)`）：
  结果 `[20, 2]`。A 的同一行参与两次，产生两个匹配对 `(a1×x)`、`(a1×y)`；
  展开 sum 看到 2 个成员，各带一份 `"sum": "10"` 的贡献；两个成员共享
  同一个左侧出现（子推导共享），右侧分别追到 B 的两条不同源行。
- **先投影成金额并 distinct 再汇总**（L2：`project(amount) → distinct → sum`）：
  结果 `[10, 1]`。这个 10 只有一份贡献（不是 20），但它来自的 distinct
  结果保留 **2 个备选依据**——仍是原来的两个匹配对（不是只随便留一个）。
  沿任一备选逐层展开都能回到对应的匹配对与两侧源行。

## 8. 覆盖范围与限制

- 覆盖现有查询树的全部算子组合：过滤、投影、inner/left 等值连接、
  distinct、`count(*)`/`count(字段)`/整数 `sum`（任意精度）、全序 sort、
  limit。不新增任意 SQL 或函数语言。
- 源行身份中的读取版本来自视图（冻结时各源固定在 F 分量）；血缘执行
  读取的是视图冻结库，与源方言无关，统一语义仍是唯一裁判。
- 稳定性只承诺：已完成 run 内 `row_ref` 与后续翻页一直指向原出现；
  不同执行产生不同编号。
- 反查回答“本次哪些最终结果用到了该源行”，不回答反事实问题，
  也不证明“为什么没有匹配”。
