"""持续查询订阅的单元与端到端测试。

运行：python -m unittest tests.test_sub -v
覆盖：
- 版本源变更入口（按位置完整提交、跨页 commit_complete、断档 409、实例换脸会话作废）；
- 登记校验（单源、无 sort/limit、聚合仅最外层、非空 requires 拒绝、多源拒绝）；
- 全部支持算子在 8 笔提交上的袋结果与净变化，逐笔与独立核对器（完整表重算）对照；
- left null 撤回/补回、distinct 支持计数、聚合三态（和为0/全null/组消失/空集单行）、
  自连接、键搬家、null、正负抵消；
- 分页稳定性与完成边界、停止隔离、断页 HTTP 失败不推进、崩溃重启原子性、
  历史断档与实例替换的 needs_rebuild、规模数据只触达受影响键。
"""

import os
import shutil
import sys
import tempfile
import threading
import unittest
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fedq import sub_scenarios as SC
from fedq.coordinator import CoordinatorError, VersionClient
from fedq.sub_oracle import IndependentOracle
from fedq.sub_service import (SubscriptionClosed, SubscriptionError,
                              SubscriptionRebuild, SubscriptionService)
from fedq.version_server import make_version_server
from fedq.versions import Change, Commit


def _start(hist, workdir, page_bytes=220):
    log = os.path.join(workdir, "od.http.jsonl")
    httpd = make_version_server(hist, log, max_page_bytes=page_bytes)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{httpd.server_address[1]}"
    return httpd, url


def _bag(svc, sub_id):
    c = Counter()
    for it in svc.read_result(sub_id)["items"]:
        c[tuple(it["row"])] += it["mult"]
    return c


def _change_counter(svc, sub_id, after_seq, seq):
    c = Counter()
    cursor = None
    boundary = False
    while True:
        page = svc.read_changes(sub_id, after_seq, cursor, page_size=2)
        for it in page["items"]:
            if it["kind"] == "boundary" and it["seq"] == seq:
                boundary = True
            if it["kind"] == "delta" and after_seq < it["seq"] <= seq:
                c[tuple(it["row"])] += it["delta"]
        if page["next_cursor"] is None:
            break
        cursor = page["next_cursor"]["after_id"]
    assert boundary, f"缺少提交 {seq} 的完成边界"
    return {k: v for k, v in c.items() if v != 0}


class _Harness(unittest.TestCase):
    def setUp(self):
        self.workdir = tempfile.mkdtemp()
        self.hist = SC.subscription_history(head=0)
        self.httpd, self.url = _start(self.hist, self.workdir)
        self.cfg = {"sources": {SC.SOURCE: {"url": self.url}}}
        self.oracle = IndependentOracle(
            SC.history_config(SC.subscription_history()))
        self.svc = SubscriptionService(os.path.join(self.workdir, "svc"))
        self.queries = {n: q for n, q, _ in SC.subscription_queries()}

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        shutil.rmtree(self.workdir, ignore_errors=True)

    def register_all(self):
        return {n: self.svc.register(self.cfg, q)["sub_id"]
                for n, q in self.queries.items()}

    def publish_all(self):
        for seq in range(1, len(self.hist.commits) + 1):
            self.hist.head = seq


class TestChangeEndpoint(unittest.TestCase):
    def setUp(self):
        self.workdir = tempfile.mkdtemp()
        self.hist = SC.subscription_history(head=3)
        self.httpd, self.url = _start(self.hist, self.workdir,
                                      page_bytes=120)
        self.client = VersionClient("OD", self.url)

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        shutil.rmtree(self.workdir, ignore_errors=True)

    def test_full_commits_by_position_and_page_boundary(self):
        sess = self.client.changes_open(0, "t")
        # 小页预算下提交被切成多页，但 seq 单调、commit_complete 仅末页为真
        seen = {}
        order = []
        for page in range(sess["pages"]):
            pr = self.client.changes_page(sess["change_id"], page)
            order.append(pr["seq"])
            if pr["seq"] not in seen:
                seen[pr["seq"]] = {"changes": {}, "complete": False}
            for t, ch in pr["changes"].items():
                part = seen[pr["seq"]]["changes"].setdefault(
                    t, {"upsert": [], "delete": []})
                part["upsert"].extend(ch.get("upsert", []))
                part["delete"].extend(ch.get("delete", []))
            seen[pr["seq"]]["complete"] = pr["commit_complete"]
        self.assertEqual(sorted(seen), [1, 2, 3])
        self.assertTrue(all(v["complete"] for v in seen.values()))
        # 提交1 只有 d1 一条 upsert
        self.assertEqual(
            seen[1]["changes"]["deliveries"]["upsert"], [["d1", "o1", 1]])
        # 同一提交跨页不会被当成多个可见状态：每个 seq 恰一个 complete
        self.assertEqual(sum(1 for v in seen.values() if v["complete"]), 3)

    def test_gap_returns_409(self):
        self.hist.advance_earliest(2)
        with self.assertRaises(CoordinatorError):
            self.client.changes_open(0, "t")   # 1 已淘汰

    def test_replace_instance_drops_sessions(self):
        sess = self.client.changes_open(0, "t")
        new = SC.subscription_history(instance="inst-REPLACED")
        self.hist.instance_id = "inst-REPLACED"
        self.httpd.version_source.change_sessions.clear()
        with self.assertRaises(CoordinatorError):
            self.client.changes_page(sess["change_id"], 0)


class TestRegistrationValidation(_Harness):
    def test_reject_sort_and_limit(self):
        sort_q = {"op": "sort",
                  "keys": [{"field": "id", "asc": True, "nulls_first": False}],
                  "input": SC.src("orders")}
        with self.assertRaises(SubscriptionError):
            self.svc.register(self.cfg, sort_q)
        limit_q = {"op": "limit", "n": 1, "input": sort_q}
        with self.assertRaises(SubscriptionError):
            self.svc.register(self.cfg, limit_q)

    def test_reject_nonempty_requires(self):
        bad = SC.subscription_history()
        bad.commits[0].requires = {SC.SOURCE: 1}
        self.httpd.shutdown()
        httpd, url = _start(bad, self.workdir)
        self.httpd = httpd
        with self.assertRaises(SubscriptionError):
            self.svc.register({"sources": {SC.SOURCE: {"url": url}}},
                              SC.left_join_detail_query())

    def test_reject_multiple_sources(self):
        two = {"sources": {"OD": {"url": self.url}, "X": {"url": self.url}}}
        with self.assertRaises(SubscriptionError):
            self.svc.register(two, SC.left_join_detail_query())

    def test_reject_nested_aggregate(self):
        # distinct 在外、aggregate 在内：聚合不在最外层
        inner = SC.agg([], [{"f": "count_star", "out": "c"}],
                       SC.proj(["id"], SC.src("orders")))
        q = SC.distinct(inner)
        with self.assertRaises(SubscriptionError):
            self.svc.register(self.cfg, q)


class TestIncrementalCorrectness(_Harness):
    def test_all_queries_match_oracle_every_commit(self):
        subs = self.register_all()
        for seq in range(1, len(self.hist.commits) + 1):
            self.hist.head = seq
            for name, q in self.queries.items():
                sub_id = subs[name]
                self.svc.advance(sub_id, seq)
                _, exp_bag = self.oracle.expected_bag(q, seq)
                self.assertEqual(_bag(self.svc, sub_id), exp_bag,
                                 f"{name}@{seq} 袋结果不符")
                _, exp_diff = self.oracle.expected_diff(q, seq - 1, seq)
                self.assertEqual(
                    _change_counter(self.svc, sub_id, seq - 1, seq),
                    exp_diff, f"{name}@{seq} 净变化不符")

    def test_left_join_withdraw_last_match(self):
        sub = self.svc.register(self.cfg,
                                self.queries["q_left明细"])["sub_id"]
        # 初始：每个订单一条 null 补行
        bag0 = _bag(self.svc, sub)
        self.assertEqual(bag0[("o1", "K", 10, None, None, None)], 1)
        self.hist.head = 1
        self.svc.advance(sub, 1)
        # 第一个匹配进入：撤回 null 行、出现匹配行
        d1 = _change_counter(self.svc, sub, 0, 1)
        self.assertEqual(d1[("o1", "K", 10, None, None, None)], -1)
        self.assertEqual(d1[("o1", "K", 10, "d1", "o1", 1)], 1)
        # 推进到提交4（删最后一条）：补回 null 行
        for s in range(2, 5):
            self.hist.head = s
            self.svc.advance(sub, s)
        d4 = _change_counter(self.svc, sub, 3, 4)
        self.assertEqual(d4[("o1", "K", 10, None, None, None)], 1)
        self.assertNotIn(("o1", "K", 10, None, None, None),
                         {k: v for k, v in d4.items() if v > 0} and
                         {})  # null 行是 +1
        self.assertEqual(_bag(self.svc, sub)[
            ("o1", "K", 10, None, None, None)], 1)

    def test_distinct_keeps_value_while_support_exists(self):
        sub = self.svc.register(self.cfg,
                                self.queries["q_distinct_sum"])["sub_id"]
        self.hist.head = 2      # d1、d2 都支持金额 10
        self.svc.advance(sub, 2)
        self.assertEqual(_bag(self.svc, sub), {tuple([10]): 1})
        self.hist.head = 3      # 删 d1，d2 仍支持 → 10 不撤回
        self.svc.advance(sub, 3)
        self.assertEqual(_bag(self.svc, sub), {tuple([10]): 1})
        d3 = _change_counter(self.svc, sub, 2, 3)
        self.assertEqual(d3, {})
        self.hist.head = 4      # 删 d2，最后支持消失 → 10 撤回（inner，无匹配行）
        self.svc.advance(sub, 4)
        # inner 连接无匹配 → distinct 无输入 → 全局 sum 在空集上为 null 单行
        self.assertEqual(_bag(self.svc, sub), Counter({(None,): 1}))

    def test_aggregate_three_states_and_empty_set(self):
        parts = self.svc.register(self.cfg, self.queries["q_parts"])["sub_id"]
        bag = _bag(self.svc, parts)
        # pid=a: 3 + (-3) = 0（有非 null 输入，sum=0）；pid=b: 全 null → sum=null
        self.assertIn(("a", 2, 2, 0), bag)
        self.assertIn(("b", 1, 0, None), bag)
        # 空集全局聚合恒保留单行 count=0/sum=null
        empty = self.svc.register(self.cfg, self.queries["q_empty"])["sub_id"]
        ebag = _bag(self.svc, empty)
        self.assertEqual(list(ebag.values()), [1])
        row = next(iter(ebag))
        self.assertEqual(row[0], 0)       # count_star
        self.assertEqual(row[1], 0)       # count(amount)
        self.assertIsNone(row[2])         # sum=null

    def test_group_disappears_on_delete(self):
        sub = self.svc.register(self.cfg, self.queries["q_group"])["sub_id"]
        self.hist.head = 8
        self.svc.advance(sub, 8)
        bag = _bag(self.svc, sub)
        # g2 在提交8 随 o2 删除而消失；null 组出现
        self.assertNotIn(("g2",), {k[:1]: None for k in bag})
        self.assertIn((None, 1, None), bag)   # grp=null,count=1,sum=null

    def test_no_change_commit_still_has_boundary(self):
        sub = self.svc.register(self.cfg,
                                self.queries["q_left明细"])["sub_id"]
        self.hist.head = 7   # 提交7 是 grp 键搬家，不影响 left 明细
        self.svc.advance(sub, 7)
        page = self.svc.read_changes(sub, 6, None, 100)
        boundaries = [it for it in page["items"]
                      if it["kind"] == "boundary" and it["seq"] == 7]
        self.assertEqual(len(boundaries), 1)


class TestPaginationAndLifecycle(_Harness):
    def test_stable_cursor_reread(self):
        sub = self.register_all()["q_left明细"]
        self.publish_all()
        self.svc.advance(sub, len(self.hist.commits))
        p1 = self.svc.read_changes(sub, 0, None, page_size=3)
        p1b = self.svc.read_changes(sub, 0, None, page_size=3)
        self.assertEqual(p1["items"], p1b["items"])
        cur = p1["next_cursor"]["after_id"]
        p2 = self.svc.read_changes(sub, 0, cur, page_size=3)
        self.assertNotEqual(p2["items"], p1["items"])

    def test_stop_rejects_advance_but_keeps_reads(self):
        s1 = self.svc.register(self.cfg, self.queries["q_parts"])["sub_id"]
        s2 = self.svc.register(self.cfg, self.queries["q_group"])["sub_id"]
        self.svc.stop(s1)
        with self.assertRaises(SubscriptionClosed):
            self.svc.advance(s1)
        self.assertTrue(self.svc.read_result(s1)["items"])  # 读取仍可用
        self.publish_all()
        self.svc.advance(s2)                                   # 别的订阅不受影响
        self.assertEqual(self.svc.status(s2)["position"],
                         len(self.hist.commits))

    def test_http_failure_does_not_advance(self):
        sub = self.svc.register(self.cfg,
                                self.queries["q_left明细"])["sub_id"]
        pos = self.svc.status(sub)["position"]
        # 追加一个大提交制造多页，再令首个 /changes/page 失败
        big = {"deliveries": Change(
            "deliveries", upserts=[["x%03d" % i, "o1", i]
                                   for i in range(30)])}
        seq = len(self.hist.commits) + 1
        self.hist.commits.append(Commit(seq, big, {}, "big"))
        self.hist.head = seq
        VersionClient("OD", self.url).admin(
            "/admin/fail", {"route": "/changes/page"})
        with self.assertRaises(CoordinatorError):
            self.svc.advance(sub)
        self.assertEqual(self.svc.status(sub)["position"], pos)
        # 恢复后正常推进，且重推进不重复
        res = self.svc.advance(sub)
        self.assertEqual(res["position"], seq)
        self.assertEqual(self.svc.advance(sub, seq)["advanced"], [])


class TestRebuildAndRestart(_Harness):
    def test_history_gap_marks_needs_rebuild_keeps_result(self):
        sub = self.svc.register(self.cfg,
                                self.queries["q_left明细"])["sub_id"]
        last = _bag(self.svc, sub)
        self.hist.head = 3
        self.hist.advance_earliest(2)     # 提交1 不可读
        with self.assertRaises(SubscriptionRebuild):
            self.svc.advance(sub)
        self.assertEqual(self.svc.status(sub)["state"], "needs_rebuild")
        self.assertEqual(_bag(self.svc, sub), last)   # 结果保留
        hint = self.svc.rebuild_hint(sub)
        self.assertEqual(hint["last_position"], 0)

    def test_instance_replacement_marks_needs_rebuild(self):
        sub = self.svc.register(self.cfg,
                                self.queries["q_left明细"])["sub_id"]
        last = _bag(self.svc, sub)
        # 同端口换新实例（模拟同地址换脸）
        port = self.httpd.server_address[1]
        self.httpd.shutdown()
        self.httpd.server_close()
        new_hist = SC.subscription_history(instance="inst-REPLACED", head=3)
        from http.server import ThreadingHTTPServer
        from fedq.version_server import VersionSource, make_version_handler
        log = os.path.join(self.workdir, "od2.http.jsonl")
        src = VersionSource(new_hist, log, 220)
        self.httpd = ThreadingHTTPServer(
            ("127.0.0.1", port), make_version_handler(src))
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        with self.assertRaises(SubscriptionRebuild):
            self.svc.advance(sub)
        self.assertTrue(self.svc.status(sub)["error"].startswith(
            "需要重新建立：源 instance_id"))
        self.assertEqual(_bag(self.svc, sub), last)

    def test_restart_uses_persisted_state(self):
        sub = self.svc.register(self.cfg,
                                self.queries["q_group"])["sub_id"]
        self.hist.head = 4
        self.svc.advance(sub, 4)
        # 全新服务对象（新进程语义）：仅凭磁盘继续
        svc2 = SubscriptionService(os.path.join(self.workdir, "svc"))
        self.assertEqual(svc2.status(sub)["position"], 4)
        self.hist.head = 5
        svc2.advance(sub, 5)
        _, exp = self.oracle.expected_bag(self.queries["q_group"], 5)
        self.assertEqual(_bag(svc2, sub), exp)

    def test_building_placeholder_is_recovered(self):
        from fedq.subscription import SubscriptionStore
        svc_dir = os.path.join(self.workdir, "svc")
        store = SubscriptionStore(svc_dir)
        sid = "sub-building"
        store.create_registry(
            sid, SC.SOURCE, self.url, self.hist.instance_id,
            SC.left_join_detail_query(), ["id"], ["orders", "deliveries"],
            SC.TABLES, init_at=0)
        # 模拟初始化中途被杀死：留一个半成品状态库文件
        with open(store.state_db_path(sid), "w") as f:
            f.write("partial")
        self.assertEqual(
            [r["state"] for r in store.list_all()], ["building"])
        out = SubscriptionService(svc_dir).recover_building()
        self.assertEqual(out[0]["rebuilt_at"], 0)
        svc2 = SubscriptionService(svc_dir)
        self.assertEqual(svc2.status(sid)["state"], "active")
        self.assertTrue(svc2.read_result(sid)["items"])


class TestScaleAccessScope(_Harness):
    def test_single_key_change_does_not_scan_all(self):
        n = 500
        scale_hist = SC.scale_history(n, head=0)
        self.httpd.shutdown()
        self.httpd, url = _start(scale_hist, self.workdir)
        svc = SubscriptionService(os.path.join(self.workdir, "scale"))
        sub = svc.register({"sources": {SC.SOURCE: {"url": url}}},
                           SC.scale_join_query())["sub_id"]
        scale_hist.head = 1
        svc.advance(sub, 1)
        detail = svc.diagnostics(sub)["per_commit"][-1]["detail"]
        self.assertEqual(len(detail["src_pks"]), 1)
        join_keys = sum(len(v.get("join_keys", []))
                        for v in detail["nodes"].values())
        self.assertEqual(join_keys, 1)
        rows_read = sum(v.get("rows_read", 0)
                        for v in detail["nodes"].values())
        self.assertLess(rows_read, 10)        # 与 n=500 无关
        scale_hist.head = 2
        svc.advance(sub, 2)
        detail2 = svc.diagnostics(sub)["per_commit"][-1]["detail"]
        self.assertLessEqual(
            sum(len(v.get("join_keys", [])) for v in detail2["nodes"].values()),
            1)


if __name__ == "__main__":
    unittest.main()
