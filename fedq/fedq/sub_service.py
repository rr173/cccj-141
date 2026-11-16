"""持续查询订阅服务：登记 / 推进 / 读取 / 变更分页 / 状态 / 停止。

生命周期状态（subscriptions.state）：
- building       登记中：已写目录占位但初始化未共同落定；进程死后重启会重做初始化，
                  不会留下半成品结果。
- active         初始化已在明确位置 p 落定，可推进、读取。
- needs_rebuild  历史断档（缺必要变更）或源 instance_id 变更：保留最后完整结果供核对，
                  拒绝推进，必须由用户显式重新登记（sub-rebuild 仅给出重建指引）。
- stopped        用户显式停止：拒绝推进，不影响别的订阅；读取/变更/状态仍可用。

失败语义：
- HTTP 失败可重试；一笔提交没取全（跨页也一样）绝不推进位置。
- 每笔提交的传播、结果袋、变更台账、位置在一个 SQLite 事务里共同落定，
  进程被真正终止后只有“旧位置+旧结果”或“新位置+完整新变更”两种状态。
- 初始化后不再请求完整源表，也不重新下载全部历史；之后只走 /changes 变更入口。
"""

from __future__ import annotations

import os
import uuid

from . import query as Q
from .coordinator import VersionClient
from .subscription import (SubscriptionClosed, SubscriptionEngine,
                           SubscriptionError, SubscriptionRebuild,
                           SubscriptionStore, analyze)
from .versions import validate_metadata


class SubscriptionService:
    def __init__(self, workdir):
        self.workdir = workdir
        os.makedirs(workdir, exist_ok=True)
        self.store = SubscriptionStore(workdir)

    # ------------------------------------------------------------ 登记
    def register(self, config, query_json, note=""):
        sources = config.get("sources", {})
        if len(sources) != 1:
            raise SubscriptionError(
                f"持续查询订阅只接一个版本化源，配置含 {len(sources)} 个源："
                f"{sorted(sources)}，登记拒绝")
        source_name = next(iter(sources))
        url = sources[source_name]["url"]
        client = VersionClient(source_name, url)

        meta = client.metadata()
        validate_metadata(meta, {source_name})
        # 1) 不协调跨源依赖：任何已开放提交带非空 requires 即明确拒绝
        bad = [c["seq"] for c in meta.get("commits", []) if c.get("requires")]
        if bad:
            raise SubscriptionError(
                f"源 {source_name} 存在带非空 requires 的提交 {bad}；"
                "持续查询订阅不协调跨源依赖，登记拒绝")
        # 2) 查询树静态校验 + 订阅专属检查（单源、无 sort/limit、聚合仅最外层）
        caps = _meta_caps(source_name, meta)
        qnode = Q.from_json(query_json)
        qnode.validate({source_name: caps})
        infos, root, source_infos, q_source, tables = analyze(qnode)
        if q_source != source_name:
            raise SubscriptionError(
                f"查询引用源 {q_source!r} 与配置源 {source_name!r} 不一致，登记拒绝")
        for t in tables:
            if t not in meta["tables"]:
                raise SubscriptionError(f"查询引用未知表 {t}，登记拒绝")

        sub_id = "sub-" + uuid.uuid4().hex[:16]
        position = meta["head"]               # 明确位置 p：登记时的开放前缀
        self.store.create_registry(
            sub_id, source_name, url, meta["instance_id"], query_json,
            root.cols, tables, meta["tables"], init_at=position)
        try:
            self._initialize(sub_id, client, meta, position)
        except SubscriptionRebuild:
            raise
        except Exception as e:
            # 初始化失败：building 占位保留（无完整结果），可在新进程重做或停止
            self.store.update(sub_id, error=f"初始化失败: {e}")
            raise
        self.store.update(
            sub_id, state="active", position=position,
            head=meta["head"], error=None)
        rec = self.store.get(sub_id)
        return {"sub_id": sub_id, "state": rec["state"], "source": source_name,
                "instance_id": meta["instance_id"], "position": position,
                "init_at": position, "head": meta["head"],
                "columns": root.cols, "tables": tables, "note": note}

    def _engine(self, rec):
        return SubscriptionEngine(
            self.store, rec, VersionClient(rec["source"], rec["url"]))

    def _initialize(self, sub_id, client, meta, position):
        rec = self.store.get(sub_id)
        engine = self._engine(rec)
        diag = {"http_requests": 0, "http_rows": 0}
        try:
            # 明确位置 p 的真实表状态：一次快照、按页读取；之后只走变更入口
            resp = client.open_snapshot(position, sub_id)
            diag["http_requests"] += 1
            if resp["instance_id"] != rec["instance_id"]:
                raise SubscriptionRebuild(
                    f"源实例身份在初始化期间变化：{rec['instance_id']} → "
                    f"{resp['instance_id']}，订阅需要重新建立")
            snap_id = resp["snapshot_id"]

            def pages(table, page):
                pr = client.page(snap_id, table, page)
                diag["http_requests"] += 1
                if pr["instance_id"] != rec["instance_id"]:
                    raise SubscriptionRebuild(
                        "源实例身份在初始化翻页期间变化，订阅需要重新建立")
                return pr

            try:
                engine.build_initial(position, pages, diag)
            finally:
                try:
                    client.release(snap_id)
                except Exception:
                    pass
        finally:
            engine.close()

    # ------------------------------------------------------------ 推进
    def advance(self, sub_id, target_seq=None):
        rec = self.store.get(sub_id)
        # stopped 直接拒绝；needs_rebuild 仍走一次身份/断档探测，以便同地址换实例
        # 后把原因刷新为 instance_id 变化（结果始终保留，绝不自动恢复推进）。
        if rec["state"] == "stopped":
            self._require_active(rec)
        client = VersionClient(rec["source"], rec["url"])
        meta = self._check_identity(rec, client)
        if rec["state"] == "needs_rebuild":
            # 身份与历史都正常但订阅仍处于重建态：维持拒绝，不自动跳新位置
            self._mark_rebuild(rec, rec["error"].replace("需要重新建立：", "")
                               or "订阅需要重新建立")
        current = rec["position"]
        target = meta["head"] if target_seq is None else target_seq
        if not isinstance(target, int) or isinstance(target, bool):
            raise SubscriptionError("目标提交号必须是整数")
        if target < current:
            raise SubscriptionError(
                f"目标 {target} 早于已处理位置 {current}，订阅只能向前推进")
        if target > meta["head"]:
            raise SubscriptionError(
                f"目标 {target} 尚未开放（head={meta['head']}）")
        if target == current:
            return {"sub_id": sub_id, "advanced": [], "position": current}

        # 断档与实例身份检查在 _check_identity 内完成（命中即 needs_rebuild）
        sess = client.changes_open(current, sub_id)
        if sess["instance_id"] != rec["instance_id"]:
            self._mark_rebuild(
                rec, f"变更会话实例身份不符：{sess['instance_id']}")
        try:
            commits, http_requests = self._collect_commits(
                client, sess, current, target, rec)
        finally:
            try:
                client.changes_release(sess["change_id"])
            except Exception:
                pass

        engine = self._engine(rec)
        advanced = []
        try:
            for commit in commits:
                if commit["seq"] > target:
                    break
                info = engine.apply_commit(commit, http_requests.get(
                    commit["seq"], 0))
                self.store.update(sub_id, position=commit["seq"],
                                  head=meta["head"])
                advanced.append(info)
        finally:
            engine.close()
        if not advanced:
            return {"sub_id": sub_id, "advanced": [], "position": current}
        return {"sub_id": sub_id, "position": advanced[-1]["seq"],
                "head": meta["head"], "advanced": advanced}

    def _collect_commits(self, client, sess, current, target, rec):
        """按稳定分页取齐 current+1..target 的完整提交；跨页提交必须收齐。

        HTTP 失败由客户端重试；任一页始终取不回则抛错，位置不动（尚未取全不推进）。
        返回 (commits, http_requests_per_commit)。
        """
        pages = sess["pages"]
        by_seq = {}
        order = []
        requests = {}
        for page in range(pages):
            pr = client.changes_page(sess["change_id"], page)
            requests[pr["seq"]] = requests.get(pr["seq"], 0) + 1
            if pr["instance_id"] != rec["instance_id"]:
                self._mark_rebuild(rec, "翻页期间源 instance_id 变化")
            if pr["seq"] not in by_seq:
                by_seq[pr["seq"]] = {"seq": pr["seq"],
                                     "requires": pr["requires"],
                                     "changes": {}}
                order.append(pr["seq"])
            merged = by_seq[pr["seq"]]["changes"]
            for t, ch in pr["changes"].items():
                part = merged.setdefault(t, {"upsert": [], "delete": []})
                part["upsert"].extend(ch.get("upsert", []))
                part["delete"].extend(ch.get("delete", []))
            if not pr["commit_complete"]:
                continue
            if pr["seq"] == target:
                break
        commits = [by_seq[s] for s in order if current < s <= target]
        seqs = [c["seq"] for c in commits]
        expected = list(range(current + 1, target + 1))
        if seqs != expected:
            raise SubscriptionError(
                f"变更不完整：期望 {expected}，实际 {seqs}，本次不推进")
        return commits, requests

    def _check_identity(self, rec, client):
        """实例身份与历史断档检查；命中则置 needs_rebuild 并抛错，不改动位置。

        先查实例身份：换了实例后即便新实例历史连续，也必须按身份变化拦截，
        不能让断档判断掩盖“同地址换源”。
        """
        meta = client.metadata()
        if meta["instance_id"] != rec["instance_id"]:
            self._mark_rebuild(
                rec, f"源 instance_id 已变更：{rec['instance_id']} → "
                     f"{meta['instance_id']}（同地址换实例），拒绝混接新源")
        if meta["earliest"] > rec["position"] + 1:
            self._mark_rebuild(
                rec, f"历史断档：下一笔 {rec['position'] + 1} 早于 earliest="
                     f"{meta['earliest']}，必要变更缺失")
        return meta

    def _mark_rebuild(self, rec, reason):
        self.store.update(rec["sub_id"], state="needs_rebuild",
                          error=f"需要重新建立：{reason}")
        raise SubscriptionRebuild(
            f"订阅 {rec['sub_id']} 进入 needs_rebuild：{reason}。"
            "最后完整结果保留在本地，可读取核对；请重新登记订阅，不会跳到最新或混接新源")

    @staticmethod
    def _require_active(rec):
        if rec["state"] == "stopped":
            raise SubscriptionClosed(
                f"订阅 {rec['sub_id']} 已停止，拒绝推进（读取仍可用）")
        if rec["state"] == "needs_rebuild":
            raise SubscriptionRebuild(
                f"订阅 {rec['sub_id']} 处于 needs_rebuild：{rec['error']}")
        if rec["state"] != "active":
            raise SubscriptionError(
                f"订阅状态为 {rec['state']}，无法推进")

    # ------------------------------------------------------------ 读取
    def read_result(self, sub_id):
        rec = self.store.get(sub_id)
        engine = self._engine(rec)
        try:
            out = engine.read_result()
        finally:
            engine.close()
        out["sub_id"] = sub_id
        out["state"] = rec["state"]
        return out

    def read_changes(self, sub_id, after_seq, after_id=None, page_size=100):
        rec = self.store.get(sub_id)
        engine = self._engine(rec)
        try:
            out = engine.read_changes(after_seq, after_id, page_size)
        finally:
            engine.close()
        out["sub_id"] = sub_id
        out["state"] = rec["state"]
        return out

    def diagnostics(self, sub_id):
        rec = self.store.get(sub_id)
        engine = self._engine(rec)
        try:
            out = engine.diagnostics()
        finally:
            engine.close()
        out["sub_id"] = sub_id
        out["state"] = rec["state"]
        out["error"] = rec["error"]
        return out

    # ------------------------------------------------------------ 状态 / 停止
    def status(self, sub_id):
        rec = self.store.get(sub_id)
        return {k: rec[k] for k in
                ("sub_id", "state", "source", "url", "instance_id", "position",
                 "init_at", "head", "created", "stopped", "error")}

    def list_subscriptions(self):
        return self.store.list_all()

    def stop(self, sub_id):
        rec = self.store.get(sub_id)
        if rec["state"] == "stopped":
            return {"sub_id": sub_id, "already": True}
        self.store.update(sub_id, state="stopped", stopped=_now_str())
        return {"sub_id": sub_id, "stopped": True,
                "position": rec["position"]}

    def rebuild_hint(self, sub_id):
        """needs_rebuild 的显式重建指引：保留结果摘要，不自动跳转或混接。"""
        rec = self.store.get(sub_id)
        engine = self._engine(rec)
        try:
            result = engine.read_result()
        finally:
            engine.close()
        return {"sub_id": sub_id, "state": rec["state"], "reason": rec["error"],
                "last_position": rec["position"],
                "last_result_columns": result["columns"],
                "last_result_items": len(result["items"]),
                "hint": "用同一查询与当前源地址重新 sub-register；本订阅的本地结果"
                        "与变更台账保留到显式清理，不会自动跳到最新或混接新源"}

    # ------------------------------------------------------------ 崩溃恢复
    def recover_building(self):
        """进程重启后回收 building 占位：初始化是事务性的，重做即可得到完整状态。"""
        recovered = []
        for rec in self.store.list_building():
            # 状态库若存在半成品文件直接删除，随后按登记时的明确位置重做初始化
            path = self.store.state_db_path(rec["sub_id"])
            if os.path.exists(path):
                os.remove(path)
            client = VersionClient(rec["source"], rec["url"])
            pos = rec["init_at"]
            try:
                meta = client.metadata()
                if meta["instance_id"] != rec["instance_id"]:
                    self._mark_rebuild(rec, "恢复期间发现源 instance_id 已变更")
                if pos is None:
                    pos = meta["head"]
                if meta["earliest"] > pos:
                    self._mark_rebuild(
                        rec, f"恢复时初始化位置 {pos} 已淘汰（earliest="
                             f"{meta['earliest']}），无法重建初始快照")
                self._initialize(rec["sub_id"], client, meta, pos)
                self.store.update(rec["sub_id"], state="active",
                                  position=pos, head=meta["head"], error=None)
                recovered.append({"sub_id": rec["sub_id"], "rebuilt_at": pos})
            except SubscriptionRebuild as e:
                recovered.append({"sub_id": rec["sub_id"],
                                  "needs_rebuild": str(e)})
        return recovered


def _now_str():
    import time
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _meta_caps(source_name, meta):
    """用元数据里的表声明构造本地能力：所有算子本地执行，方言不影响订阅答案。"""
    from .coordinator import frozen_caps
    return frozen_caps(meta.get("dialect", "B"), source_name, meta["tables"])
