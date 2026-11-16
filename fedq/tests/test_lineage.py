"""结果数据血缘的单元与端到端测试。

运行：python -m unittest tests.test_lineage -v
覆盖：袋重数与备选来源、left 补null与真实null、全null/空集聚合、正负抵消、
重复投影、同表两次引用、共享子推导（不枚举组合）、大组多页、反查多页、
过滤/limit 淘汰不混入、失败 run 状态、关视图后导航、独立参照三方对照。
"""

import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fedq import lineage_ref
from fedq.coordinator import Coordinator
from fedq.demo_lineage import (SCALE_N, _check, check_run_vs_reference,
                               fetch_all_pages, fetch_all_rows,
                               fixture_histories, lineage_queries, ref_data,
                               walk_to)
from fedq.demo_views import Fleet
from fedq.lineage import (LineageError, LineageNotFound, LineageService,
                          LineageStateError)


class LineageBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workdir = tempfile.mkdtemp(prefix="fedq-lineage-test-")
        cls.hists = fixture_histories()
        cls.data, cls.meta = ref_data(cls.hists)
        cls.fleet = Fleet(cls.workdir)
        cls.fleet.start(cls.hists)
        cls.coord = Coordinator(os.path.join(cls.workdir, "coord"))
        cls.service = LineageService(cls.coord.workdir)
        opened = cls.coord.open(
            {"sources": {n: {"url": u} for n, u in cls.fleet.urls.items()}},
            note="test")
        cls.view_id = opened["view_id"]
        cls.queries = dict((name, q) for name, q, _ in lineage_queries())
        cls.runs = {}
        for name, qjson, _ in lineage_queries():
            cls.runs[name] = cls.coord.lineage_run(
                cls.view_id, qjson, name=name)["run_id"]

    @classmethod
    def tearDownClass(cls):
        cls.fleet.stop()
        shutil.rmtree(cls.workdir, ignore_errors=True)

    def run_id(self, prefix):
        return self.runs[next(n for n in self.runs if n.startswith(prefix))]


class TestMainExample(LineageBase):
    def test_L1_join_sum_bag(self):
        rid = self.run_id("L1_")
        rows = fetch_all_rows(self.service, rid)
        self.assertEqual([it["values"] for it in rows], [[20, 2]])
        ex = self.service.expand(rows[0]["row_ref"], 0, 10)
        members = ex["derivation"]["members"]["items"]
        self.assertEqual(len(members), 2)
        # 两个匹配对共享同一个左侧出现（A 的行参与两次，不是两行）
        left_refs = []
        for m in members:
            jx = self.service.expand(m["row_ref"])
            edges = {e["slot"]: e for e in jx["derivation"]["edges"]}
            left_refs.append(edges["left"]["row_ref"])
        self.assertEqual(left_refs[0], left_refs[1])
        # sum 的逐份贡献：两份 10
        self.assertEqual([m["contribs"]["s"] for m in members],
                         [{"kind": "sum", "value": "10"},
                          {"kind": "sum", "value": "10"}])

    def test_L2_distinct_alternatives(self):
        rid = self.run_id("L2_")
        rows = fetch_all_rows(self.service, rid)
        self.assertEqual([it["values"] for it in rows], [[10, 1]])
        ex = self.service.expand(rows[0]["row_ref"], 0, 10)
        mem = ex["derivation"]["members"]["items"]
        self.assertEqual(len(mem), 1)          # 不是 20：只有一份 10
        dx = self.service.expand(mem[0]["row_ref"], 0, 10)
        self.assertEqual(dx["derivation"]["alternatives"]["total"], 2)
        # 两个备选都能逐层追到扫描叶子
        for alt in dx["derivation"]["alternatives"]["items"]:
            ex = self.service.expand(alt["row_ref"])
            while ex["derivation"]["kind"] != "scan":
                d = ex["derivation"]
                nxt = (d["input"] if d["kind"] == "pass"
                       else {e["slot"]: e for e in d["edges"]}["left"])
                ex = self.service.expand(nxt["row_ref"])
            self.assertEqual(ex["derivation"]["source_row"]["source"], "A")


class TestSmallCases(LineageBase):
    def test_left_join_null_fill_vs_real_null(self):
        rid = self.run_id("L3_")
        rows = fetch_all_rows(self.service, rid)
        self.assertEqual(len(rows), 4)
        # a3 无匹配：右侧 null_fill 标记，无 row_ref、无虚构主键
        ex = walk_to(self.service, rows[3]["row_ref"], "join")
        right = {e["slot"]: e for e in ex["derivation"]["edges"]}["right"]
        self.assertEqual(right.get("marker"), "null_fill")
        self.assertNotIn("row_ref", right)
        # a1×y：右侧是真实出现（qty 为 null 的真实行）
        ex2 = walk_to(self.service, rows[1]["row_ref"], "join")
        right2 = {e["slot"]: e for e in ex2["derivation"]["edges"]}["right"]
        self.assertIn("row_ref", right2)
        self.assertEqual(right2["values"], [2, "y", 10, None])

    def test_left_join_aggregate_null_count(self):
        rid = self.run_id("L3b_")
        rows = fetch_all_rows(self.service, rid)
        self.assertEqual([it["values"] for it in rows], [[4, 1, 70]])
        ex = self.service.expand(rows[0]["row_ref"], 0, 10)
        aggs = {a["out"]: a for a in ex["derivation"]["aggregates"]}
        self.assertEqual((aggs["cq"]["counted"], aggs["cq"]["null_skipped"]),
                         (1, 3))

    def test_all_null_and_empty_aggregates(self):
        rows = fetch_all_rows(self.service, self.run_id("L4a_"))
        self.assertEqual([it["values"] for it in rows], [[2, 0, None]])
        rows = fetch_all_rows(self.service, self.run_id("L4b_"))
        self.assertEqual([it["values"] for it in rows], [[0, 0, None]])
        ex = self.service.expand(rows[0]["row_ref"], 0, 10)
        self.assertTrue(ex["derivation"]["empty_input"])
        self.assertEqual(ex["derivation"]["members"]["total"], 0)
        rows = fetch_all_rows(self.service, self.run_id("L4c_"))
        self.assertEqual(rows, [])                    # 空分组聚合没有结果
        rows = fetch_all_rows(self.service, self.run_id("L4d_"))
        self.assertEqual([it["values"] for it in rows],
                         [[1, 1, None], [2, 1, None]])
        ex = self.service.expand(rows[0]["row_ref"], 0, 10)
        self.assertEqual(ex["derivation"]["group_key"], {"id": 1})

    def test_cancellation_keeps_contributions(self):
        rid = self.run_id("L5_")
        rows = fetch_all_rows(self.service, rid)
        self.assertEqual([it["values"] for it in rows], [[0, 4]])
        ex = self.service.expand(rows[0]["row_ref"], 0, 10)
        vals = [m["contribs"]["s"]["value"]
                for m in ex["derivation"]["members"]["items"]]
        self.assertEqual(vals, ["7", "-3", "-4", "0"])

    def test_duplicate_projection_occurrences(self):
        rid = self.run_id("L6_")
        rows = fetch_all_rows(self.service, rid)
        self.assertEqual([it["values"] for it in rows],
                         [[7], [-3], [-4], [0], [7]])
        self.assertNotEqual(rows[0]["row_ref"], rows[4]["row_ref"])
        # 各自可展开到不同的源行
        for i, expect_pk in ((0, [1]), (4, [5])):
            ex = walk_to(self.service, rows[i]["row_ref"], "source")
            self.assertEqual(ex["derivation"]["source_row"]["pk"], expect_pk)
        rid = self.run_id("L6b_")
        rows = fetch_all_rows(self.service, rid)
        self.assertEqual([it["values"] for it in rows], [[7], [-3], [-4], [0]])
        ex = self.service.expand(rows[0]["row_ref"], 0, 10)
        self.assertEqual(ex["derivation"]["alternatives"]["total"], 2)

    def test_self_join_positions(self):
        rid = self.run_id("L7_")
        rows = fetch_all_rows(self.service, rid)
        self.assertEqual(len(rows), 7)
        got = fetch_all_pages(
            lambda p: self.service.lookup(rid, "A", "flows", [1],
                                          page=p, page_size=2))
        via = sorted((it["seq"], it["via"]["path"]) for it in got)
        self.assertEqual(via, [(0, "/i/l/i"), (0, "/i/r/i"),
                               (1, "/i/l/i"), (5, "/i/r/i")])
        left_only = self.service.lookup(rid, "A", "flows", [1], path="/i/l/i")
        self.assertEqual({it["seq"] for it in left_only["items"]}, {0, 1})
        # 源行身份五元组 + 输入位置都在命中身份里
        ids = left_only["matched_identities"]
        self.assertEqual(ids[0]["instance_id"], "inst-A-lin1")
        self.assertEqual(ids[0]["at"], 0)
        self.assertEqual(ids[0]["path"], "/i/l/i")


class TestSharing(LineageBase):
    def test_shared_subderivations_no_combinations(self):
        rid = self.run_id("L8_")
        import sqlite3
        ev = sqlite3.connect(os.path.join(self.service.dir, rid, "evidence.db"))
        n_occ = ev.execute("SELECT COUNT(*) FROM occs").fetchone()[0]
        n_edge = ev.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        n_usage = ev.execute("SELECT COUNT(*) FROM usage").fetchone()[0]
        ev.close()
        # 手写清点：28 出现、25 边、6 反查对；若枚举备选组合将远超此数
        self.assertEqual((n_occ, n_edge, n_usage), (28, 25, 6))
        rows = fetch_all_rows(self.service, rid)
        ex = self.service.expand(rows[0]["row_ref"], 0, 10)
        mem = ex["derivation"]["members"]["items"][0]
        jx = walk_to(self.service, mem["row_ref"], "join")
        self.assertEqual(len(jx["derivation"]["edges"]), 2)


class TestPagination(LineageBase):
    def test_big_group_pages(self):
        rid = self.run_id("L9_")
        rows = fetch_all_rows(self.service, rid)
        expected = sum((i % 7) - 3 for i in range(1, SCALE_N + 1))
        self.assertEqual([it["values"] for it in rows],
                         [[1, SCALE_N, expected, SCALE_N]])
        members = fetch_all_pages(
            lambda p: self.service.expand(rows[0]["row_ref"], p, 100)
            ["derivation"]["members"])
        self.assertEqual(len(members), SCALE_N)
        self.assertEqual(len({m["row_ref"] for m in members}), SCALE_N)
        total = sum(int(m["contribs"]["s"]["value"]) for m in members)
        self.assertEqual(total, expected)
        # 越界页为空
        ex = self.service.expand(rows[0]["row_ref"], 25, 100)
        self.assertEqual(ex["derivation"]["members"]["items"], [])

    def test_lookup_pages(self):
        rid = self.run_id("L10_")
        got = fetch_all_pages(
            lambda p: self.service.lookup(rid, "A", "anchor", [1],
                                          page=p, page_size=100))
        self.assertEqual(len(got), SCALE_N)
        self.assertEqual({it["seq"] for it in got}, set(range(SCALE_N)))
        one = self.service.lookup(rid, "B", "scale", [7])
        self.assertEqual(one["total"], 1)
        self.assertEqual(one["items"][0]["values"], [1, 7, -3])

    def test_rows_pages_stable_refs(self):
        rid = self.run_id("L10_")
        a = self.service.rows(rid, 0, 100)["items"]
        b = self.service.rows(rid, 0, 100)["items"]
        self.assertEqual([x["row_ref"] for x in a], [x["row_ref"] for x in b])
        p0 = self.service.rows(rid, 0, 50)["items"]
        p1 = self.service.rows(rid, 1, 50)["items"]
        self.assertEqual(a[:100], p0 + p1)      # 小页拼接 == 大页

    def test_page_params_validated(self):
        rid = self.run_id("L1_")
        with self.assertRaises(LineageError):
            self.service.rows(rid, -1, 10)
        with self.assertRaises(LineageError):
            self.service.rows(rid, 0, 0)
        with self.assertRaises(LineageError):
            self.service.expand(f"{rid}:1", 0, 100000)


class TestRetention(LineageBase):
    def test_filter_eliminated_not_in_lookup(self):
        rid = self.run_id("L5_")
        lk = self.service.lookup(rid, "A", "flows", [5])
        self.assertEqual(lk["total"], 0)

    def test_limit_eliminated_not_in_lookup(self):
        rid = self.run_id("L10b_")
        for pk, expect in ((1, 1), (2, 1), (3, 1), (4, 0), (100, 0)):
            lk = self.service.lookup(rid, "B", "scale", [pk])
            self.assertEqual(lk["total"], expect, f"pk={pk}")


class TestFailure(LineageBase):
    def test_failed_run_marked_and_refused(self):
        def hook(stage, count):
            if stage == "edge" and count >= 1:
                raise OSError("模拟证据写失败")
        with self.assertRaises(LineageError) as cm:
            self.coord.lineage_run(self.view_id, self.queries[
                "L1_主例_直接连接汇总"], name="fail", fail_hook=hook)
        bad = cm.exception.run_id
        cat = {r["run_id"]: r for r in self.service.list_runs()}
        self.assertEqual(cat[bad]["state"], "error")
        self.assertTrue(os.path.exists(
            os.path.join(self.service.dir, bad, "INCOMPLETE.txt")))
        with open(os.path.join(self.service.dir, bad, "run.json"),
                  encoding="utf-8") as f:
            self.assertEqual(json.load(f)["state"], "error")
        for fn in (lambda: self.service.rows(bad),
                   lambda: self.service.expand(bad + ":1"),
                   lambda: self.service.lookup(bad, "A", "accounts", [1])):
            with self.assertRaises(LineageStateError):
                fn()
        # 正常 run 不受影响
        self.assertEqual(cat[self.run_id("L1_")]["state"], "ok")
        self.assertTrue(self.service.show(self.run_id("L1_"))["verified"])

    def test_unknown_run_and_ref(self):
        with self.assertRaises(LineageNotFound):
            self.service.rows("lr-nonexistent")
        with self.assertRaises(LineageNotFound):
            self.service.expand(self.run_id("L1_") + ":999999")
        with self.assertRaises(LineageError):
            self.service.expand("bad-ref")


class TestClosedView(LineageBase):
    def test_navigate_after_close_and_source_stop(self):
        # 关掉本类的视图（在最后；其他测试不依赖视图再开）
        rid_l1 = self.run_id("L1_")
        rid_l2 = self.run_id("L2_")
        rid_l9 = self.run_id("L9_")
        self.coord.close(self.view_id)
        self.fleet.stop()
        svc2 = LineageService(self.coord.workdir)   # 新对象=新进程角色
        rows = fetch_all_rows(svc2, rid_l1)
        self.assertEqual([it["values"] for it in rows], [[20, 2]])
        ex = svc2.expand(rows[0]["row_ref"], 0, 10)
        self.assertEqual(ex["derivation"]["members"]["total"], 2)
        rows2 = fetch_all_rows(svc2, rid_l2)
        mem = svc2.expand(rows2[0]["row_ref"], 0, 10)["derivation"]["members"]
        dx = svc2.expand(mem["items"][0]["row_ref"], 0, 10)
        self.assertEqual(dx["derivation"]["alternatives"]["total"], 2)
        lk = svc2.lookup(rid_l9, "B", "scale", [1234])
        self.assertEqual(lk["total"], 1)
        self.assertTrue(svc2.show(rid_l1)["verified"])


class TestReferenceCrossCheck(LineageBase):
    def test_all_queries_vs_reference(self):
        for name, qjson, _ in lineage_queries():
            with self.subTest(name=name):
                big = name.startswith(("L9", "L10_"))
                check_run_vs_reference(
                    self.service, self.runs[name], qjson, self.data, self.meta,
                    expand_sample=6 if big else None,
                    lookup_sample=150 if big else None)


class TestLineageNotBusinessColumn(LineageBase):
    def test_row_ref_not_a_column(self):
        rid = self.run_id("L1_")
        self.assertEqual(self.service.rows(rid)["columns"], ["s", "c"])
        from fedq import expr as E, query as Q
        from fedq.scenarios import eq, filt, fld, lit_int, src
        bad = filt(eq(fld("row_ref"), lit_int(1)), src("A", "accounts"))
        with self.assertRaises((Q.QueryError, E.ExprError)):
            self.coord.lineage_run(self.view_id, bad, name="badcol")

    def test_normal_entry_still_works(self):
        # 普通执行入口（view-run 路径）不受血缘功能影响
        from fedq.scenarios import filt, fld, lit_int, lt, proj, sort, src
        q = sort([("id", True, False)],
                 filt(lt(lit_int(0), fld("amount")),
                      proj(["id", "acct", "amount"], src("A", "accounts"))))
        out = os.path.join(self.workdir, "normal.jsonl")
        _, result, _ = self.coord.run(self.view_id, q, out, name="normal")
        self.assertTrue(result["complete"])
        self.assertEqual(result["rows"],
                         [[1, "a1", 10], [2, "a2", 20], [3, "a3", 30]])


class TestBigIntSum(unittest.TestCase):
    """任意精度 sum 经血缘执行仍精确；sum 列上的排序按数值序（NUMTEXT）。"""

    def test_big_sum_and_sort_on_sum(self):
        import sqlite3
        workdir = tempfile.mkdtemp(prefix="fedq-lineage-big-")
        self.addCleanup(shutil.rmtree, workdir, True)
        frozen = os.path.join(workdir, "frozen.db")
        con = sqlite3.connect(frozen)
        con.execute("CREATE TABLE frozen_rows(tbl TEXT, page INTEGER,"
                    " rowidx INTEGER, jrow TEXT)")
        big = 2 ** 62
        for i, v in enumerate([big, big, -7]):
            con.execute("INSERT INTO frozen_rows VALUES(?,?,?,?)",
                        ("t", 0, i, json.dumps([i + 1, v])))
        con.commit()
        con.close()
        view_sources = [{
            "source": "A", "instance_id": "inst-big", "at": 0, "dialect": "A",
            "schema_json": json.dumps(
                {"t": {"columns": {"id": "int", "v": "int"}, "key": ["id"]}})}]
        from fedq.scenarios import agg, proj, sort, src
        svc = LineageService(workdir)
        # 单组：sum = 2^63 - 7（超 int64）
        q0 = agg([], [{"f": "sum", "field": "v", "out": "s"}],
                 proj(["id", "v"], src("A", "t")))
        info = svc.start_run("v-big", q0, view_sources, frozen, name="big")
        rows = fetch_all_rows(svc, info["run_id"])
        self.assertEqual([it["values"] for it in rows], [[2 ** 63 - 7]])
        ex = svc.expand(rows[0]["row_ref"], 0, 10)
        vals = [m["contribs"]["s"]["value"]
                for m in ex["derivation"]["members"]["items"]]
        self.assertEqual(vals, [str(big), str(big), "-7"])
        # 分组 + 按 sum 降序：验证 NUMTEXT 数值序（文本序会排错大数）
        q2 = sort([("s", False, False), ("id", True, False)],
                  agg(["id"], [{"f": "sum", "field": "v", "out": "s"}],
                      proj(["id", "v"], src("A", "t"))))
        info2 = svc.start_run("v-big", q2, view_sources, frozen, name="big2")
        rows2 = fetch_all_rows(svc, info2["run_id"])
        self.assertEqual([it["values"] for it in rows2],
                         [[1, big], [2, big], [3, -7]])


# 让 TestClosedView 最后执行（它会关闭共享视图）
def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for cls in (TestMainExample, TestSmallCases, TestSharing, TestPagination,
                TestRetention, TestFailure, TestReferenceCrossCheck,
                TestLineageNotBusinessColumn, TestBigIntSum, TestClosedView):
        suite.addTests(loader.loadTestsFromTestCase(cls))
    return suite


if __name__ == "__main__":
    unittest.main()
