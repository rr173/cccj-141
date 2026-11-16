"""跨源读取视图的端到端验收演示（所有数据运行时生成）。

覆盖任务书全部验收点：
0) 先手算三源环例子 H=(3,3,2)、L=0 → F=(1,1,1)，并实际读到三源第 1 版的行；
   A 的 earliest 改为 2 必须拒绝打开；
1) 无交叉依赖且 H 分量不同 → 全部上界保留；
2) 两个互相要求的已开放提交（环）一起纳入；
3) 独立穷举程序枚举所有候选向量核对最大向量（不与生产选择共享代码）；
4) 打开期间推进源版本（含跨表同笔 upsert/delete）：老 view 明细与连接始终对应原 F，
   新视图看到新版本；查询翻页期间再推进一次；
5) 协调者重启后凭原 view_id 继续查询；再淘汰旧状态，证明本地冻结确实起作用；
6) 第三源建立失败 → 不登记可用视图，遗留保护有清理入口且不碰其他视图；
7) 同地址实例被替换 → 打开被拒，旧视图结果不变；
8) 查询进行中关闭：在跑查询完成，关闭后拒绝新查询；
9) 两个视图共享同一源版本，只关闭一个不破坏另一个；
并保存真实 HTTP 请求日志与数据结果。
"""

from __future__ import annotations

import json
import os
import shutil
import threading

from . import scenarios as SC
from .coordinator import (Coordinator, CoordinatorError, VersionClient)
from .enumcheck import enumerate_greatest
from .version_server import make_version_server
from .versions import NoClosedVector, select_vector
from .viewstore import ViewClosed


def _check(cond, msg):
    if not cond:
        raise AssertionError("断言失败: " + msg)


def _rows(coord, view_id, qname, out_dir, qjson, on_batch=None):
    out = os.path.join(out_dir, f"{view_id}.{qname}.jsonl")
    plan, result, stats = coord.run(view_id, qjson, out, name=qname,
                                    on_batch=on_batch)
    _check(result["complete"], f"{qname} 必须完整结束")
    return result["rows"], plan, stats


# ---------------------------------------------------------------- 服务编排

class Fleet:
    def __init__(self, workdir, page_bytes=220):
        self.workdir = workdir
        self.page_bytes = page_bytes
        self.servers = {}
        self.hists = {}
        self.urls = {}

    def start(self, histories):
        for name, hist in histories.items():
            self._start_one(name, hist)

    def _start_one(self, name, hist):
        log = os.path.join(self.workdir, f"{name}.http.jsonl")
        httpd = make_version_server(hist, log, max_page_bytes=self.page_bytes)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        self.servers[name] = httpd
        self.hists[name] = hist
        self.urls[name] = f"http://127.0.0.1:{httpd.server_address[1]}"

    def client(self, name):
        return VersionClient(name, self.urls[name])

    def admin(self, name, path, payload=None):
        return self.client(name).admin(path, payload)

    def stop(self, name=None):
        if name is None:
            targets = list(self.servers.values())
            self.servers.clear()
        elif name in self.servers:
            targets = [self.servers.pop(name)]
        else:
            targets = []
        for httpd in targets:
            httpd.shutdown()
            httpd.server_close()

    def replace_at_same_address(self, name, new_hist):
        """停掉旧实例、在同一端口用新 instance_id 重启。"""
        port = self.servers[name].server_address[1]
        self.servers.pop(name).shutdown()
        from http.server import ThreadingHTTPServer
        from .version_server import make_version_handler, VersionSource
        log = os.path.join(self.workdir, f"{name}.http.jsonl")
        src = VersionSource(new_hist, log, self.page_bytes)
        httpd = ThreadingHTTPServer(("127.0.0.1", port), make_version_handler(src))
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        self.servers[name] = httpd
        self.hists[name] = new_hist


# ---------------------------------------------------------------- 主流程

def run(workdir, out=print):
    if os.path.exists(workdir):
        shutil.rmtree(workdir)
    os.makedirs(workdir, exist_ok=True)
    out_dir = os.path.join(workdir, "results")
    os.makedirs(out_dir, exist_ok=True)

    fleet = Fleet(workdir)
    fleet.start(SC.ring_histories())
    coord = Coordinator(os.path.join(workdir, "coord"))
    cfg = SC.coordinator_config(fleet.urls)
    qs = {name: q for name, q, _ in SC.view_queries()}
    try:
        _section0_handcompute(coord, fleet, cfg, qs, out_dir, out)
        _section_earliest_reject(workdir, fleet, out)
        _section_independent_and_cycle(workdir, out)
        old_view = _section_advance(coord, fleet, cfg, qs, out_dir, out)
        _section_restart_evict(workdir, cfg, qs, old_view, fleet, out_dir, out)
        _section_third_source_failure(workdir, fleet, out)
        _section_instance_replace(coord, fleet, cfg, qs, out_dir, out)
        _section_close_during_query(coord, cfg, qs, fleet, out_dir, out)
        _section_shared_versions(coord, cfg, qs, fleet, out_dir, out)
        out("\n================ 全部跨源视图验收场景通过 ================")
        out(f"产物目录: {workdir}（HTTP 日志 *.http.jsonl；查询结果 results/；"
            f"视图目录库 coord/views.db）")
    finally:
        fleet.stop()


def _section0_handcompute(coord, fleet, cfg, qs, out_dir, out):
    out("\n[0] 手算三源环：H=(A:3,B:3,C:2)，L 全 0，依赖 A2→B3, B2→C3, C2→A2")
    metas = {n: fleet.client(n).metadata() for n in fleet.urls}
    H = {n: m["head"] for n, m in metas.items()}
    L = {n: m["earliest"] for n, m in metas.items()}
    _check(H == {"A": 3, "B": 3, "C": 2} and set(L.values()) == {0}, "观察盒正确")
    # 用服务端真实历史做穷举核对（枚举器独立实现，不与生产选择共享代码）
    hists = fleet.hists
    F, trace = select_vector(hists, H, L)
    out("  生产选择 F = " + json.dumps(F, ensure_ascii=False))
    greatest, feasible, total = enumerate_greatest(hists, sorted(hists), H, L)
    out(f"  独立穷举：候选 {total} 个，合法 {len(feasible)} 个，最大向量 = "
        + json.dumps(greatest, ensure_ascii=False))
    _check(F == greatest == {"A": 1, "B": 1, "C": 1}, "手算 F=(1,1,1)")

    res = coord.open(cfg, note="手算环例子")
    vid = res["view_id"]
    out(f"  打开视图 {vid}，F={json.dumps(res['F'], ensure_ascii=False)}")
    out("  退回解释: " + json.dumps(res["retreat"], ensure_ascii=False))
    # 实际读到三个源第 1 版的行（核对状态重放，而非只看版本号）
    rows_a, _, _ = _rows(coord, vid, "q1_A_items过滤排序", out_dir,
                         qs["q1_A_items过滤排序"])
    _check(rows_a == [[1, "a1", 11, True], [2, "b0", 20, False],
                      [3, "c1", 30, True]], f"A 第1版 items 行 {rows_a}")
    rows_c, _, _ = _rows(coord, vid, "q3_C_parts扫描", out_dir,
                         qs["q3_C_parts扫描"])
    _check(rows_c == [[1, "p1", 101], [2, "p2", 200]], f"C 第1版 parts 行 {rows_c}")
    rows_b, _, _ = _rows(coord, vid, "q4_B_stock聚合", out_dir,
                         qs["q4_B_stock聚合"])
    _check(rows_b == [[2, 15]], f"B 第1版 stock 聚合 {rows_b}")
    desc = coord.describe(vid)
    out("  HTTP 读取依据（节选）: " + json.dumps(
        [{"source": r["source"], "instance_id": r["instance_id"], "at": r["at"],
          "table": r["table"], "page": r["page"], "rows": r["rows"]}
         for r in desc["http_reads"]], ensure_ascii=False))
    _check(all(r["at"] == 1 for r in desc["http_reads"]), "每页读取都绑定 F=1")
    return vid


def _section_earliest_reject(workdir, fleet, out):
    out("\n[0b] A 的 earliest 改为 2（F[A]=1 的旧状态被淘汰）→ 必须拒绝打开")
    fleet.admin("A", "/admin/earliest", {"earliest": 2})
    coord = Coordinator(os.path.join(workdir, "coord-earliest"))
    before = set(v["view_id"] for v in coord.list_views())
    try:
        coord.open(SC.coordinator_config(fleet.urls), note="应被拒绝")
        raise AssertionError("earliest=2 时必须拒绝打开")
    except NoClosedVector as e:
        out("  已拒绝 ✓: " + str(e))
    # 选择阶段即拒绝：begin_build 尚未发生，目录里不应有任何视图记录
    _check(coord.list_views() == [], "选择阶段拒绝时不产生任何视图记录")
    fleet.admin("A", "/admin/earliest", {"earliest": 0})  # 还原以便后续场景


def _section_independent_and_cycle(workdir, out):
    out("\n[1] 无交叉依赖、H 分量不同 (3,2,1) → 最大向量保留全部上界")
    ind = SC.independent_histories()
    H = {n: h.head for n, h in ind.items()}
    L = {n: h.earliest for n, h in ind.items()}
    g, feasible, total = enumerate_greatest(ind, sorted(ind), H, L)
    out(f"  候选 {total}、合法 {len(feasible)}、最大向量 {json.dumps(g)}")
    _check(g == {"A": 3, "B": 2, "C": 1}, "无依赖时 F=H")

    out("\n[2] A:2 与 B:2 互相要求（环）→ 环不拒绝，两笔一起纳入 F=(2,2,1)")
    cyc = SC.cycle_histories()
    H = {n: h.head for n, h in cyc.items()}
    L = {n: h.earliest for n, h in cyc.items()}
    Fp, _ = select_vector(cyc, H, L)
    g, feasible, total = enumerate_greatest(cyc, sorted(cyc), H, L)
    out(f"  生产 F={json.dumps(Fp)}；穷举最大={json.dumps(g)}（合法 {len(feasible)}/"
        f"{total}）")
    _check(Fp == g == {"A": 2, "B": 2, "C": 1}, "相互要求可共同纳入")


def _section_advance(coord, fleet, cfg, qs, out_dir, out):
    out("\n[3] 打开后推进源版本：老视图始终对应原 F，新视图看到新版本")
    old = coord.open(cfg, note="推进前基线")
    old_id = old["view_id"]
    base_rows, _, _ = _rows(coord, old_id, "q1_A_items过滤排序", out_dir,
                            qs["q1_A_items过滤排序"])
    base_join, _, _ = _rows(coord, old_id, "q2_A_B连接", out_dir,
                            qs["q2_A_B连接"])
    out(f"  老视图 {old_id} F={json.dumps(old['F'])}；A.items 行数 {len(base_rows)}")

    # 推进 A 到第 4 笔（同笔跨表 upsert id=1 + delete id=5），B、C 也推进
    fleet.admin("A", "/admin/publish", {"head": 4})
    fleet.admin("B", "/admin/publish", {"head": 4})
    fleet.admin("C", "/admin/publish", {"head": 3})
    out("  已发布 A→4（同笔改 id=1、删 id=5）、B→4、C→3")

    # 老视图结果不变
    base_rows2, _, _ = _rows(coord, old_id, "q1_A_items过滤排序", out_dir,
                             qs["q1_A_items过滤排序"])
    base_join2, _, _ = _rows(coord, old_id, "q2_A_B连接", out_dir,
                             qs["q2_A_B连接"])
    _check(base_rows2 == base_rows, "推进后老视图表明细不变")
    _check(base_join2 == base_join, "推进后老视图连接结果不变")
    out("  老视图明细与连接结果仍对应原 F ✓")

    # 新视图看到新版本：全量可闭合（A4/B4 无依赖，C3 满足 B2 的 C:3）
    fresh = coord.open(cfg, note="推进后新视图")
    _check(fresh["F"] == {"A": 4, "B": 4, "C": 3},
           f"新视图 F 应为 (4,4,3)，得到 {fresh['F']}")
    new_rows, _, _ = _rows(coord, fresh["view_id"], "q1_A_items过滤排序",
                           out_dir, qs["q1_A_items过滤排序"])
    # A 第4笔后：id=1 改为 a4/99，id=5 被删；id=2 仍是 b2/22
    id1 = [r for r in new_rows if r[0] == 1]
    _check(id1 == [[1, "a4", 99, False]], f"A v4 的 id=1 {id1}")
    _check(all(r[0] != 5 for r in new_rows), "A v4 已删除 id=5")
    out(f"  新视图 {fresh['view_id']} F={json.dumps(fresh['F'])}；"
        f"id=1 已更新为 {id1}，id=5 已删除 ✓")

    # 翻页期间再推进（on_batch 回调在读取中途发布 A→5、B→5）
    def advance_midway(source, table, n):
        if not hasattr(advance_midway, "fired"):
            advance_midway.fired = set()
        if source == "C" and table == "parts" and "x" not in advance_midway.fired:
            advance_midway.fired.add("x")
            fleet.admin("A", "/admin/publish", {"head": 5})
            fleet.admin("B", "/admin/publish", {"head": 5})
            out("  · 翻页读取中途发布 A→5、B→5")
    mid_rows, _, _ = _rows(coord, old_id, "q3_C_parts扫描", out_dir,
                           qs["q3_C_parts扫描"], on_batch=advance_midway)
    _check(mid_rows == [[1, "p1", 101], [2, "p2", 200]],
           "翻页中途发布不改变老视图冻结结果")
    out("  翻页期间发布后，老视图仍返回冻结的 C 第1版 ✓")
    return old_id


def _section_restart_evict(workdir, cfg, qs, old_view, fleet, out_dir, out):
    out("\n[4] 协调者重启后重用原视图，再淘汰旧状态核对本地冻结")
    # 新的协调进程（新对象、新连接），仅凭磁盘目录库与冻结库工作
    coord2 = Coordinator(os.path.join(workdir, "coord"))
    desc = coord2.describe(old_view)
    _check(desc["state"] == "open", "重启后视图仍为 open")
    rows, _, _ = _rows(coord2, old_view, "q1_A_items过滤排序", out_dir,
                       qs["q1_A_items过滤排序"])
    out(f"  重启后原 view_id {old_view} 可查询，返回 {len(rows)} 行 ✓")

    # 淘汰旧状态：把三源 earliest 推到各自当前 head 之上的历史深处
    fleet.admin("A", "/admin/earliest", {"earliest": 4})
    fleet.admin("B", "/admin/earliest", {"earliest": 4})
    fleet.admin("C", "/admin/earliest", {"earliest": 3})
    state = {n: fleet.admin(n, "/admin/state") for n in fleet.urls}
    out("  淘汰后源端状态: " + json.dumps(
        {n: {"earliest": s["earliest"], "head": s["head"]}
         for n, s in state.items()}, ensure_ascii=False))
    # 旧版本在源端已不可直接读取
    try:
        fleet.client("A").open_snapshot(1, "probe")
        raise AssertionError("A 版本1 已淘汰，源端应拒绝新快照")
    except CoordinatorError:
        out("  源端对已淘汰的 A:1 拒绝新快照 ✓")
    # 但老视图的本地冻结副本仍能完整查询（不依赖源）
    rows2, _, _ = _rows(coord2, old_view, "q1_A_items过滤排序", out_dir,
                        qs["q1_A_items过滤排序"])
    join2, _, _ = _rows(coord2, old_view, "q2_A_B连接", out_dir,
                        qs["q2_A_B连接"])
    _check(len(rows2) > 0 and all(r[0] in (1, 2, 3) for r in rows2),
           "淘汰后老视图冻结明细仍可读")
    out(f"  旧状态淘汰后，老视图仍从本地冻结库读出明细 {len(rows2)} 行、"
        f"连接 {len(join2)} 行 ✓（本地冻结接管保留责任）")
    # 还原 earliest，避免影响后续场景
    for n in fleet.urls:
        fleet.admin(n, "/admin/earliest", {"earliest": 0})


def _section_third_source_failure(workdir, fleet, out):
    out("\n[5] 第三源建立失败：不登记可用视图，遗留保护可清理且不碰其他视图")
    # 让 C 在冻结翻页时注入 500（open 按 A→B→C 顺序，page 路由只在 C 自己冻结时触发）
    fleet.admin("C", "/admin/fail", {"route": "/snapshot/page"})
    coord = Coordinator(os.path.join(workdir, "coord-fail"))
    try:
        coord.open(SC.coordinator_config(fleet.urls), note="C 应失败")
        raise AssertionError("C 注入失败后必须整体打开失败")
    except CoordinatorError as e:
        out("  打开被拒 ✓: " + str(e))
    failed = [v for v in coord.list_views() if v["state"] == "failed"]
    opens = [v for v in coord.list_views() if v["state"] == "open"]
    _check(len(failed) == 1 and not opens, "失败后只有 failed、没有 open 视图")
    vid = failed[0]["view_id"]
    # 失败前 A、B 的源端读取保护确实被持有（C 在取数时失败，未形成完整输入）
    held_before = {n: fleet.admin(n, "/admin/state")["protected"]
                   for n in ("A", "B")}
    out("  失败后源端仍持有保护: " + json.dumps(held_before, ensure_ascii=False))
    _check(any(held_before.values()), "失败建立确实在源端留下了临时保护")
    info = coord.cleanup_failed(vid)
    out(f"  清理失败建立遗留 {vid}: {info}")
    _check(info["released_snapshots"], "清理入口释放了源端临时保护")
    held_after = {n: fleet.admin(n, "/admin/state")["protected"]
                  for n in ("A", "B")}
    _check(not any(held_after.values()), "清理后源端临时保护全部释放")
    out("  清理后 A/B 保护计数归零 ✓")
    _check(all(v["view_id"] != vid for v in coord.list_views()), "清理后记录消失")
    # 证明清理只针对本次失败：另一个协调目录里已存在的 open 视图不受影响
    main_coord = Coordinator(os.path.join(workdir, "coord"))
    still_open = [v for v in main_coord.list_views() if v["state"] == "open"]
    _check(len(still_open) >= 2, "其他协调目录的 open 视图未被清理波及")
    out(f"  主协调目录仍有 {len(still_open)} 个 open 视图，清理未波及 ✓")


def _section_instance_replace(coord, fleet, cfg, qs, out_dir, out):
    out("\n[6] 同地址实例被替换：新建打开被拒，旧视图结果不变")
    before = coord.open(cfg, note="替换前视图")
    base_rows, _, _ = _rows(coord, before["view_id"], "q1_A_items过滤排序",
                            out_dir, qs["q1_A_items过滤排序"])
    desc_before = coord.describe(before["view_id"])
    old_inst = {s["source"]: s["instance_id"] for s in desc_before["sources"]}
    # 再次打开时按已知实例身份固定（协调者记住各地址应有的 instance_id）
    pinned_cfg = {"sources": {n: {"url": u, "expect_instance": old_inst[n]}
                              for n, u in fleet.urls.items()}}
    # 用新 instance_id 在 C 的同一端口重启
    new_c = SC.ring_histories()["C"]
    new_c.instance_id = "inst-C-REPLACED"
    fleet.replace_at_same_address("C", new_c)
    try:
        coord.open(pinned_cfg, note="实例已替换应失败")
        raise AssertionError("实例身份变化后必须拒绝打开")
    except CoordinatorError as e:
        out("  替换后打开被拒 ✓: " + str(e))
    # 旧视图（本地冻结）查询结果与记住的实例身份都不变
    rows_after, _, _ = _rows(coord, before["view_id"], "q1_A_items过滤排序",
                             out_dir, qs["q1_A_items过滤排序"])
    desc_after = coord.describe(before["view_id"])
    new_inst = {s["source"]: s["instance_id"] for s in desc_after["sources"]}
    _check(old_inst == new_inst, "旧视图记住的实例身份不被替换影响")
    _check(rows_after == base_rows, "实例替换后旧视图冻结结果不变")
    out("  旧视图记住的源实例身份与冻结数据保持不变 ✓")
    # 还原 C 实例，保证后续场景可用
    fleet.replace_at_same_address("C", SC.ring_histories()["C"])


def _section_close_during_query(coord, cfg, qs, fleet, out_dir, out):
    out("\n[7] 查询进行中关闭：在跑查询允许完成，关闭后拒绝新查询")
    vid = coord.open(cfg, note="关闭时序视图")["view_id"]
    fired = {"n": 0}

    def close_midway(source, table, n):
        fired["n"] += 1
        if fired["n"] == 1:
            info = coord.close(vid)
            out(f"  · 首批读取后立即关闭视图：{info}")

    # 进行中的查询（已开始）必须能跑完
    rows, _, _ = _rows(coord, vid, "q3_C_parts扫描", out_dir,
                       qs["q3_C_parts扫描"], on_batch=close_midway)
    _check(rows == [[1, "p1", 101], [2, "p2", 200]], "关闭时在跑的查询完整结束")
    out("  关闭时已在跑的查询完整返回 ✓")
    # 关闭后新查询被拒
    try:
        coord.run(vid, qs["q1_A_items过滤排序"],
                  os.path.join(out_dir, "should-fail.jsonl"), name="after-close")
        raise AssertionError("关闭后必须拒绝新查询")
    except ViewClosed as e:
        out("  关闭后新查询被拒 ✓: " + str(e))


def _section_shared_versions(coord, cfg, qs, fleet, out_dir, out):
    out("\n[8] 两视图共享同一源版本：只关闭一个，另一个仍可用")
    v1 = coord.open(cfg, note="共享视图1")
    v2 = coord.open(cfg, note="共享视图2")
    _check(v1["F"] == v2["F"], "两视图选择了相同 F")
    r1, _, _ = _rows(coord, v1["view_id"], "q1_A_items过滤排序", out_dir,
                     qs["q1_A_items过滤排序"])
    info = coord.close(v1["view_id"])
    out(f"  关闭 {v1['view_id']}: {info}")
    # v2 仍可查询
    r2, _, _ = _rows(coord, v2["view_id"], "q1_A_items过滤排序", out_dir,
                     qs["q1_A_items过滤排序"])
    _check(r1 == r2, "关闭共享版本的一个视图不破坏另一个")
    out(f"  另一视图 {v2['view_id']} 结果一致（{len(r2)} 行）✓")
    # v1 再查被拒
    try:
        coord.run(v1["view_id"], qs["q1_A_items过滤排序"],
                  os.path.join(out_dir, "shared-fail.jsonl"), name="closed-v1")
        raise AssertionError("已关闭视图必须拒绝查询")
    except ViewClosed:
        out("  已关闭的 v1 拒绝新查询 ✓")
