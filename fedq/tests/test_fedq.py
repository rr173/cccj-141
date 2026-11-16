"""端到端与单元测试。运行：python -m unittest discover -s tests -v"""

import copy
import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fedq import datasets, semantics as S, expr as E, query as Q, reference
from fedq.client import Client, TokenError
from fedq.demo import demo_queries, start_servers
from fedq.executor import Executor, QueryFailed, read_result
from fedq.planner import Planner, analyze_pred, render_plan

BIG = 2 ** 62
BIG_SUM = 2 ** 63   # 超出 int64 的精确 sum


class TestSemantics(unittest.TestCase):
    def test_tvl(self):
        self.assertIs(S.tv_and(True, S.UNKNOWN), S.UNKNOWN)
        self.assertIs(S.tv_and(False, S.UNKNOWN), False)
        self.assertIs(S.tv_or(False, S.UNKNOWN), S.UNKNOWN)
        self.assertIs(S.tv_or(True, S.UNKNOWN), True)
        self.assertIs(S.tv_not(S.UNKNOWN), S.UNKNOWN)
        self.assertFalse(S.passes(S.UNKNOWN))
        self.assertTrue(S.passes(True))

    def test_text_order_codepoint(self):
        # 码点序、区分大小写、不去尾空格
        self.assertTrue(S.u_lt("X", "x"))       # 88 < 120
        self.assertTrue(S.u_lt("X", "X "))      # 前缀更短
        self.assertFalse(S.u_eq("X", "x"))
        self.assertFalse(S.u_eq("X", "X "))
        self.assertIs(S.u_eq("X", None), S.UNKNOWN)
        self.assertIs(S.u_lt(None, 1), S.UNKNOWN)

    def test_bool_not_int(self):
        with self.assertRaises(S.SemanticsError):
            S.u_eq(True, 1)
        with self.assertRaises(S.SemanticsError):
            S.u_lt(True, False)
        self.assertEqual(S.type_of(True), "bool")
        self.assertEqual(S.type_of(1), "int")


class TestValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.caps = {"A": datasets.source_a()[0], "B": datasets.source_b()[0]}

    def test_bool_int_mixed_rejected(self):
        q = Q.from_json({"op": "filter",
                         "pred": {"k": "eq", "a": {"k": "field", "name": "flag"},
                                  "b": {"k": "lit", "t": "int", "v": "1"}},
                         "input": {"op": "source", "source": "A", "table": "items"}})
        with self.assertRaises(S.SemanticsError):
            q.validate(self.caps)

    def test_limit_requires_sort(self):
        q = Q.from_json({"op": "limit", "n": 1,
                         "input": {"op": "source", "source": "A", "table": "items"}})
        with self.assertRaises(Q.QueryError):
            q.validate(self.caps)

    def test_sort_needs_unique_tiebreaker(self):
        q = Q.from_json({"op": "sort",
                         "keys": [{"field": "amount", "asc": True,
                                   "nulls_first": False}],
                         "input": {"op": "source", "source": "A", "table": "items"}})
        with self.assertRaises(Q.QueryError):
            q.validate(self.caps)

    def test_unknown_op_rejected(self):
        with self.assertRaises(Q.QueryError):
            Q.from_json({"op": "select_star_evil", "sql": "drop table"})


class TestPushdownAnalysis(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.caps = {"A": datasets.source_a()[0], "B": datasets.source_b()[0]}
        schema_a = {"name": "text", "amount": "int", "flag": "bool"}
        for c in cls.caps.values():
            c._schema = schema_a

    def _pa(self, pred, src="A"):
        pred.typecheck(self.caps[src]._schema)
        return analyze_pred(pred, self.caps[src])

    def test_a_text_eq_is_superset(self):
        pa = self._pa(E.Eq(E.Field("name"), E.Lit("text", "X")), "A")
        self.assertEqual(pa.mode, "superset")
        self.assertIsNotNone(pa.residual)

    def test_b_text_eq_exact_but_not_collapses(self):
        pa = self._pa(E.Eq(E.Field("name"), E.Lit("text", "X")), "B")
        self.assertEqual(pa.mode, "exact")
        self.assertTrue(pa.collapses_null)
        pa_not = self._pa(E.Not(E.Eq(E.Field("name"), E.Lit("text", "X"))), "B")
        self.assertEqual(pa_not.mode, "superset")  # B 的 NOT 只能候选下推
        self.assertIsNotNone(pa_not.residual)

    def test_a_not_over_superset_forbidden(self):
        pa = self._pa(E.Not(E.Eq(E.Field("name"), E.Lit("text", "X"))), "A")
        self.assertEqual(pa.mode, "no")  # 对超集取反是子集，禁止

    def test_int_lt_exact_both(self):
        for src in ("A", "B"):
            pa = self._pa(E.Lt(E.Lit("int", 5), E.Field("amount")), src)
            self.assertEqual(pa.mode, "exact")

    def test_a_text_lt_not_pushable(self):
        pa = self._pa(E.Lt(E.Field("name"), E.Lit("text", "b")), "A")
        self.assertEqual(pa.mode, "no")

    def test_b_text_lt_exact(self):
        pa = self._pa(E.Lt(E.Field("name"), E.Lit("text", "b")), "B")
        self.assertEqual(pa.mode, "exact")

    def test_or_partial_not_pushable(self):
        # OR(文本lt@A不可下推, 整数lt可下推) → 整体不可下推
        pa = self._pa(E.Or([E.Lt(E.Field("name"), E.Lit("text", "b")),
                            E.Lt(E.Lit("int", 5), E.Field("amount"))]), "A")
        self.assertEqual(pa.mode, "no")

    def test_and_partial_pushable(self):
        pa = self._pa(E.And([E.Lt(E.Field("name"), E.Lit("text", "b")),
                             E.Lt(E.Lit("int", 5), E.Field("amount"))]), "A")
        self.assertEqual(pa.mode, "superset")  # 整数部分下推，文本部分残余
        self.assertIsNotNone(pa.residual)


class EndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workdir = tempfile.mkdtemp(prefix="fedq-test-")
        cls.servers, (cls.url_a, cls.url_b, cls.url_b_tiny) = \
            start_servers(cls.workdir)
        cls.caps = {}
        for name, url in (("A", cls.url_a), ("B", cls.url_b)):
            cls.caps[name] = Client(name, url).capabilities()
        cls.data = {
            "A": {"items": {"columns": datasets.source_a()[1]["items"]["columns"],
                            "rows": datasets.A_ITEMS},
                  "badtypes": datasets.source_a()[1]["badtypes"]},
            "B": {"entries": {"columns": datasets.source_b()[1]["entries"]["columns"],
                              "rows": datasets.B_ENTRIES},
                  "flaky": datasets.source_b()[1]["flaky"],
                  "dupids": datasets.source_b()[1]["dupids"]},
        }

    @classmethod
    def tearDownClass(cls):
        for s in cls.servers:
            s.shutdown()
        shutil.rmtree(cls.workdir, ignore_errors=True)

    # ---- 辅助 ----
    def run_query(self, qjson, caps=None, url_b=None, tag="q"):
        clients = {"A": Client("A", self.url_a),
                   "B": Client("B", url_b or self.url_b)}
        planner = Planner(caps or self.caps)
        plan = planner.plan(Q.from_json(qjson))
        out_path = os.path.join(self.workdir, f"out_{tag}.jsonl")
        exe = Executor(clients, os.path.join(self.workdir, "work"))
        stats, _ = exe.execute(plan, out_path)
        result = read_result(out_path)
        return plan, result, clients

    def check_vs_reference(self, qjson, result):
        ref_cols, ref_rows = reference.run(qjson, self.data)
        self.assertEqual(ref_cols, result["columns"])
        self.assertEqual(ref_rows, result["rows"])

    def http_logs(self, source):
        path = os.path.join(self.workdir, f"{source}.http.jsonl")
        with open(path, encoding="utf-8") as f:
            return [json.loads(x) for x in f]

    # ---- 演示查询 ----
    def test_all_demos_vs_reference(self):
        for name, qjson, _ in demo_queries():
            with self.subTest(name=name):
                _, result, _ = self.run_query(qjson, tag=name)
                self.assertTrue(result["complete"])
                self.check_vs_reference(qjson, result)

    def test_q1_a_candidate_superset(self):
        _, qjson, _ = demo_queries()[0]
        _, result, clients = self.run_query(qjson, tag="q1")
        self.assertEqual(result["rows"], [[1, "X"]])
        # A 实际先回三个候选行（X、x、'X '），本地精筛为一行
        self.assertEqual(clients["A"].stats.rows, 3)
        self.assertEqual(len(result["rows"]), 1)

    def test_q2_b_not_null_excluded(self):
        _, qjson, _ = demo_queries()[1]
        _, result, clients = self.run_query(qjson, tag="q2")
        ids = [r[0] for r in result["rows"]]
        self.assertEqual(ids, [3, 4, 5, 6, 7, 8, 9, 10])
        self.assertNotIn(2, ids)                    # name=null 不得被 NOT 选中
        self.assertEqual(clients["B"].stats.rows, 9)  # B 远端把 null 行也回了（超集）

    def test_q3_limit_takes_correct_successor(self):
        _, qjson, _ = demo_queries()[2]
        plan, result, clients = self.run_query(qjson, tag="q3")
        # 远端排序首行 id=2 在本地被残余过滤；limit 2 必须取后继 id=5、id=3
        self.assertEqual([r[0] for r in result["rows"]], [5, 3])
        # limit 不得下推：远端回了全部候选（9 行）
        self.assertEqual(clients["B"].stats.rows, 9)
        text = render_plan(plan)
        self.assertIn("limit 留在本地", text)

    def test_q4_left_join_on_vs_where(self):
        _, q4a, _ = demo_queries()[3]
        _, q4b, _ = demo_queries()[4]
        _, ra, _ = self.run_query(q4a, tag="q4a")
        _, rb, _ = self.run_query(q4b, tag="q4b")
        self.assertEqual(ra["rows"], [
            ["Q", 1, 0, None],
            ["X", 2, 2, BIG_SUM],
            ["X ", 1, 0, None],
            ["Y", 1, 1, 4],
            ["Z", 1, 1, 4],
            ["x", 1, 1, 4],
            [None, 1, 0, None],
        ])
        self.assertEqual(rb["rows"], [
            ["X", 2, 2, BIG_SUM],
            ["Y", 1, 1, 4],
            ["Z", 1, 1, 4],
            ["x", 1, 1, 4],
        ])
        self.assertNotEqual(ra["rows"], rb["rows"])

    def test_q4_bigint_exact_and_allnull_sum(self):
        _, q4a, _ = demo_queries()[3]
        _, ra, _ = self.run_query(q4a, tag="q4big")
        x_row = [r for r in ra["rows"] if r[0] == "X"][0]
        self.assertEqual(x_row[3], BIG_SUM)       # 超过 int64 仍精确
        null_row = [r for r in ra["rows"] if r[0] == "X "][0]
        self.assertIsNone(null_row[3])            # 全 null 的 sum 为 null
        self.assertEqual(null_row[2], 0)          # count(字段) 不计 null

    def test_q5_distinct(self):
        _, qjson, _ = demo_queries()[5]
        _, result, _ = self.run_query(qjson, tag="q5")
        self.assertEqual(result["rows"],
                         [["Q"], ["X"], ["X "], ["Y"], ["Z"], ["x"], [None]])

    def test_q6_selective_filter_reduces_transfer(self):
        _, qjson, _ = demo_queries()[6]
        _, result, clients = self.run_query(qjson, tag="q6a")
        self.assertEqual(result["rows"], [[3, 30]])
        self.assertEqual(clients["A"].stats.rows, 1)           # 少传行
        self.assertEqual(set(clients["A"].stats.fields),
                         {"id", "amount"})                     # 少传字段

    def test_q6_aggregate_pushdown_returns_only_agg_rows(self):
        _, qjson, _ = demo_queries()[7]
        _, result, clients = self.run_query(qjson, tag="q6b")
        self.assertEqual(result["rows"], [[2, 80]])
        self.assertEqual(clients["A"].stats.rows, 1)           # 只回聚合行
        self.assertEqual(set(clients["A"].stats.fields), {"c", "s"})

    def test_q6_empty_set_aggregate(self):
        _, qjson, _ = demo_queries()[8]
        _, result, _ = self.run_query(qjson, tag="q6c")
        self.assertEqual(result["rows"], [[0, None]])

    def test_http_log_records_plan_rows_fields(self):
        _, qjson, _ = demo_queries()[6]
        self.run_query(qjson, tag="q6log")
        logs = [x for x in self.http_logs("A") if x.get("plan")]
        last = logs[-1]
        self.assertEqual(last["plan"]["op"], "project")
        self.assertEqual(last["rows_total"], 1)
        self.assertEqual(sorted(last["columns"]), ["amount", "id"])

    # ---- 失败边界 ----
    def test_bad_token_fails(self):
        c = Client("B", self.url_b)
        with self.assertRaises(TokenError):
            c._request("POST", "/query", {"token": "bogus", "page": 0})

    def _expect_stream_failure(self, table, url, source="B"):
        clients = {"A": Client("A", self.url_a), "B": Client("B", url)}
        plan = Planner(self.caps).plan(Q.from_json(
            {"op": "source", "source": source, "table": table}))
        out_path = os.path.join(self.workdir, f"fail_{table}.jsonl")
        exe = Executor(clients, os.path.join(self.workdir, "work"))
        with self.assertRaises(QueryFailed):
            exe.execute(plan, out_path)
        r = read_result(out_path)
        self.assertFalse(r["complete"])      # 没有成功结束标记
        self.assertEqual(r["status"], "error")
        return r

    def test_midway_http_error_fails(self):
        r = self._expect_stream_failure("flaky", self.url_b_tiny)
        # 失败前已从源读到若干行（部分流），但结果不得标为完整
        self.assertGreater(r["stats"]["sources"]["B"]["rows"], 0)

    def test_bad_field_type_fails(self):
        self._expect_stream_failure("badtypes", self.url_a, source="A")

    def test_duplicate_page_ids_fail(self):
        self._expect_stream_failure("dupids", self.url_b_tiny)

    # ---- 能力退化 ----
    def test_degraded_caps_still_correct(self):
        weak = dict(self.caps)
        weak_a = copy.deepcopy(self.caps["A"])
        weak_a.aggregates = frozenset()
        weak["A"] = weak_a
        _, qjson, _ = demo_queries()[7]
        _, result, _ = self.run_query(qjson, caps=weak, tag="degraded")
        self.assertEqual(result["rows"], [[2, 80]])


if __name__ == "__main__":
    unittest.main()
