"""跨源读取视图的单元与端到端测试。

运行：python -m unittest tests.test_views -v
覆盖：历史/元数据校验（含 bool 不冒充整数）、最大闭合向量、环不拒绝、
独立穷举一致性、视图冻结/推进/重启/淘汰/失败清理/实例替换/关闭时序/共享版本。
"""

import json
import os
import shutil
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fedq import scenarios as SC
from fedq.coordinator import (Coordinator, CoordinatorError, FrozenClient,
                              frozen_caps)
from fedq.demo_views import Fleet
from fedq.enumcheck import enumerate_greatest
from fedq.version_server import make_version_server
from fedq.versions import (Change, Commit, HistoryError, NoClosedVector,
                           SourceHistory, history_from_config, select_vector,
                           validate_metadata)
from fedq.viewstore import ViewClosed


def _servers(workdir, histories, page_bytes=200):
    servers, urls = {}, {}
    for n, h in histories.items():
        httpd = make_version_server(h, os.path.join(workdir, f"{n}.jsonl"),
                                    max_page_bytes=page_bytes)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        servers[n] = httpd
        urls[n] = f"http://127.0.0.1:{httpd.server_address[1]}"
    return servers, urls


class TestHistoryValidation(unittest.TestCase):
    def _schema(self):
        return {"t": {"columns": {"id": "int", "name": "text", "ok": "bool"},
                      "key": ["id"]}}

    def test_bool_not_position_int(self):
        # requires 的位置用 True 冒充 1 必须被拒（构造后篡改再显式校验）
        h = SourceHistory("X", "i", "A", self._schema(), {"t": []},
                          [Commit(1, {"t": Change("t", upserts=[[1, "a", True]])},
                                   {})])
        h.commits[0].requires = {"Y": True}
        with self.assertRaises(HistoryError):
            h.validate()

    def test_bool_not_int_value(self):
        h = SourceHistory("X", "i", "A", self._schema(), {"t": []},
                          [Commit(1, {"t": Change("t", upserts=[[1, "a", True]])},
                                   {})])
        # ok 列声明 bool，篡改成 int 值
        h.commits[0].changes["t"].upserts = [[1, "a", 1]]
        with self.assertRaises(HistoryError):
            h.validate()

    def test_negative_and_noncontinuous_rejected(self):
        base = SourceHistory("X", "i", "A", self._schema(), {"t": []},
                             [Commit(1, {"t": Change("t", upserts=[[1, "a", True]])},
                                      {})])
        bad1 = SourceHistory("X", "i", "A", self._schema(), {"t": []},
                             [Commit(1, {"t": Change("t", upserts=[[1, "a", True]])},
                                      {})])
        bad1.commits[0].requires = {"Y": -1}
        with self.assertRaises(HistoryError):
            bad1.validate()
        bad2 = SourceHistory("X", "i", "A", self._schema(), {"t": []},
                             [Commit(1, {"t": Change("t", upserts=[[1, "a", True]])},
                                      {})])
        bad2.commits[0].seq = 2
        with self.assertRaises(HistoryError):
            bad2.validate()
        self.assertTrue(base.validate() is None)

    def test_earliest_bounds(self):
        with self.assertRaises(HistoryError):
            SourceHistory("X", "i", "A", self._schema(), {"t": []},
                          [Commit(1, {"t": Change("t", upserts=[[1, "a", True]])},
                                   {})], head=1, earliest=2).validate()

    def test_metadata_unknown_source_rejected(self):
        h = SC.ring_histories()["A"]
        meta = h.metadata()
        with self.assertRaises(HistoryError):
            validate_metadata(meta, {"B", "C"})     # A 不在已知源中


class TestSelection(unittest.TestCase):
    def test_ring_handcompute(self):
        hists = SC.ring_histories()
        H = {n: h.head for n, h in hists.items()}
        L = {n: 0 for n in hists}
        F, trace = select_vector(hists, H, L)
        self.assertEqual(F, {"A": 1, "B": 1, "C": 1})
        g, feasible, total = enumerate_greatest(hists, sorted(hists), H, L)
        self.assertEqual(g, F)
        # 退回解释覆盖三个源
        retreat_sources = {t["source"] for t in trace}
        self.assertEqual(retreat_sources, {"A", "B", "C"})

    def test_earliest_makes_infeasible(self):
        hists = SC.ring_histories(earliests={"A": 2, "B": 0, "C": 0})
        H = {n: h.head for n, h in hists.items()}
        L = {n: h.earliest for n, h in hists.items()}
        with self.assertRaises(NoClosedVector):
            select_vector(hists, H, L)

    def test_independent_keeps_all_heads(self):
        hists = SC.independent_histories()
        H = {n: h.head for n, h in hists.items()}
        L = {n: 0 for n, hists_ in [(n, hists) for n in hists]}
        g, _, _ = enumerate_greatest(hists, sorted(hists), H, L)
        self.assertEqual(g, {"A": 3, "B": 2, "C": 1})

    def test_cycle_included_together(self):
        hists = SC.cycle_histories()
        H = {n: h.head for n, h in hists.items()}
        L = {n: 0 for n in hists}
        F, _ = select_vector(hists, H, L)
        g, _, _ = enumerate_greatest(hists, sorted(hists), H, L)
        self.assertEqual(F, g)
        self.assertEqual(F, {"A": 2, "B": 2, "C": 1})

    def test_state_replay_upsert_delete_order(self):
        h = SC.ring_histories()["A"]
        s1 = h.state_at(1)["items"]
        self.assertIn((3,), s1)                       # 第1笔插入 id=3
        s2 = h.state_at(2)["items"]
        self.assertNotIn((3,), s2)                    # 第2笔删除 id=3
        self.assertEqual(s2[(2,)][1], "b2")           # 同 id 更新按提交顺序生效
        self.assertEqual(s2[(1,)][1], "a1")           # 未涉及的行保持上一版


class TestViewLifecycle(unittest.TestCase):
    def setUp(self):
        self.workdir = tempfile.mkdtemp(prefix="fedq-views-test-")
        self.fleet = Fleet(self.workdir)
        self.fleet.start(SC.ring_histories())
        self.coord = Coordinator(os.path.join(self.workdir, "coord"))
        self.cfg = SC.coordinator_config(self.fleet.urls)
        self.qs = {n: q for n, q, _ in SC.view_queries()}

    def tearDown(self):
        self.fleet.stop()
        shutil.rmtree(self.workdir, ignore_errors=True)

    def _open(self):
        return self.coord.open(self.cfg, note=self._testMethodName)

    def _run(self, vid, qname):
        out = os.path.join(self.workdir, f"{self._testMethodName}.{qname}.jsonl")
        _, result, _ = self.coord.run(vid, self.qs[qname], out, name=qname)
        self.assertTrue(result["complete"])
        return result["rows"]

    def test_open_freeze_and_query_binding(self):
        r = self._open()
        self.assertEqual(r["F"], {"A": 1, "B": 1, "C": 1})
        rows = self._run(r["view_id"], "q1_A_items过滤排序")
        self.assertEqual(rows, [[1, "a1", 11, True], [2, "b0", 20, False],
                                [3, "c1", 30, True]])
        desc = self.coord.describe(r["view_id"])
        self.assertTrue(all(rd["at"] == 1 for rd in desc["http_reads"]))

    def test_advance_does_not_change_old_view(self):
        old = self._open()["view_id"]
        before = self._run(old, "q1_A_items过滤排序")
        self.fleet.admin("A", "/admin/publish", {"head": 4})
        self.fleet.admin("B", "/admin/publish", {"head": 4})
        self.fleet.admin("C", "/admin/publish", {"head": 3})
        self.assertEqual(self._run(old, "q1_A_items过滤排序"), before)
        fresh = self._open()
        self.assertEqual(fresh["F"], {"A": 4, "B": 4, "C": 3})

    def test_restart_uses_persisted_store(self):
        vid = self._open()["view_id"]
        before = self._run(vid, "q3_C_parts扫描")
        coord2 = Coordinator(self.coord.workdir)   # 新对象=新协调进程
        out = os.path.join(self.workdir, "restart.jsonl")
        _, result, _ = coord2.run(vid, self.qs["q3_C_parts扫描"], out,
                                  name="restart")
        self.assertEqual(result["rows"], before)

    def test_eviction_old_state_still_readable_locally(self):
        vid = self._open()["view_id"]
        # 先推进 head 再把 earliest 推到 head，使 F 所在的旧版本被淘汰
        self.fleet.admin("A", "/admin/publish", {"head": 4})
        self.fleet.admin("B", "/admin/publish", {"head": 4})
        self.fleet.admin("C", "/admin/publish", {"head": 3})
        for n, e in (("A", 4), ("B", 4), ("C", 3)):
            self.fleet.admin(n, "/admin/earliest", {"earliest": e})
        # 源端拒绝已淘汰版本的新快照
        with self.assertRaises(CoordinatorError):
            self.fleet.client("A").open_snapshot(1, "probe")
        rows = self._run(vid, "q1_A_items过滤排序")
        self.assertTrue(rows)
        for n in self.fleet.urls:
            self.fleet.admin(n, "/admin/earliest", {"earliest": 0})

    def test_third_source_failure_and_cleanup(self):
        self.fleet.admin("C", "/admin/fail", {"route": "/snapshot/page"})
        try:
            self.coord.open(self.cfg, note="fail")
            raise AssertionError("必须失败")
        except CoordinatorError:
            pass
        failed = [v for v in self.coord.list_views() if v["state"] == "failed"]
        self.assertEqual(len(failed), 1)
        held = self.fleet.admin("A", "/admin/state")["protected"]
        self.assertTrue(held)
        info = self.coord.cleanup_failed(failed[0]["view_id"])
        self.assertTrue(info["released_snapshots"])
        self.assertFalse(self.fleet.admin("A", "/admin/state")["protected"])

    def test_instance_replace_rejected_when_pinned(self):
        before = self._open()
        old = {s["source"]: s["instance_id"]
               for s in self.coord.describe(before["view_id"])["sources"]}
        pinned = {"sources": {n: {"url": u, "expect_instance": old[n]}
                              for n, u in self.fleet.urls.items()}}
        new_c = SC.ring_histories()["C"]
        new_c.instance_id = "inst-C-X"
        self.fleet.replace_at_same_address("C", new_c)
        try:
            self.coord.open(pinned, note="replace")
            raise AssertionError("实例替换必须拒绝")
        except CoordinatorError:
            pass
        self.fleet.replace_at_same_address("C", SC.ring_histories()["C"])

    def test_close_rejects_new_query_but_running_finishes(self):
        vid = self._open()["view_id"]
        fired = {"n": 0}

        def close_now(source, table, n):
            fired["n"] += 1
            if fired["n"] == 1:
                self.coord.close(vid)

        rows = self._run_with_cb(vid, "q3_C_parts扫描", close_now)
        self.assertEqual(rows, [[1, "p1", 101], [2, "p2", 200]])
        with self.assertRaises(ViewClosed):
            self.coord.run(vid, self.qs["q1_A_items过滤排序"],
                           os.path.join(self.workdir, "x.jsonl"), name="x")

    def _run_with_cb(self, vid, qname, cb):
        out = os.path.join(self.workdir, f"cb.{qname}.jsonl")
        _, result, _ = self.coord.run(vid, self.qs[qname], out, name=qname,
                                      on_batch=cb)
        return result["rows"]

    def test_two_views_share_version_close_one(self):
        v1 = self._open()["view_id"]
        v2 = self._open()["view_id"]
        r1 = self._run(v1, "q1_A_items过滤排序")
        self.coord.close(v1)
        r2 = self._run(v2, "q1_A_items过滤排序")
        self.assertEqual(r1, r2)
        with self.assertRaises(ViewClosed):
            self.coord.run(v1, self.qs["q1_A_items过滤排序"],
                           os.path.join(self.workdir, "y.jsonl"), name="y")

    def test_frozen_caps_only_project(self):
        caps = frozen_caps("A", "A", SC.SCHEMAS["A"])
        self.assertTrue(caps.project)
        self.assertEqual(caps.filter_ops, frozenset())
        self.assertFalse(caps.sort)
        self.assertEqual(caps.aggregates, frozenset())


if __name__ == "__main__":
    unittest.main()
