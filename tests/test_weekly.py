"""质量周报订阅测试。

覆盖核心诉求：
- 口径一致：同一份报告所有段落来自同一时间点冻结的快照，生成后底层数据
  继续变化，报告内容不变；
- 多订阅、多接收人、不同频率；合并 / 分别两种投递模式；
- 先预览（不落库不留痕）再发送（落库 + 每接收人 notify_events 留痕）；
- 定时到点触发、同分钟去重、暂停后不触发、恢复后触发。
"""

from __future__ import annotations

import datetime
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import (CoverageAnalyzer, DefectManager, NotificationManager,
                    ReportGenerator, Scheduler, TestExecutor,
                    WeeklyReportManager)
from storage import BuildStoreRegistry, StoreRegistry


def _make_env(data_root):
    registry = StoreRegistry(os.path.join(data_root, "store"), shard_size=50)
    builds = BuildStoreRegistry(os.path.join(data_root, "builds"))
    coverage = CoverageAnalyzer(builds)
    report = ReportGenerator(builds)
    defects = DefectManager(registry)
    notify = NotificationManager(registry)
    weekly = WeeklyReportManager(registry, builds, report, coverage, notify)
    sched = Scheduler(registry, builds, TestExecutor(), None, report,
                      coverage, defects, notify, tick_seconds=60,
                      weekly_manager=weekly)
    return registry, builds, coverage, notify, weekly, sched


def _make_build(builds, pid, bid, created_at, *, total=10, passed=8,
                failed=2, status="failed", skipped=0):
    """直接写入一场构建的聚合数据（绕过执行器），created_at 可控。

    计数完全由 record_result 增量聚合产生（与真实执行路径一致），因此
    ``passed + failed + skipped = total``。
    """
    store = builds.for_project(pid)
    store.create(bid, suite_id="suite_x", env_id="env_x", name=bid)
    # 计数完全由 record_result 增量聚合，避免与预置计数重复
    store.set_total(bid, total)
    # 写失败用例结果，供失败 Top 聚合
    for i in range(failed):
        store.record_result(bid, {
            "case_id": f"case_fail_{i}", "case_name": f"失败用例{i}",
            "group": "api", "priority": "P1", "status": "failed",
            "duration": 0.5,
            "assertions": [{"ok": False, "message": f"断言失败 {i}"}],
            "steps": [], "logs": [],
        })
    for i in range(passed):
        store.record_result(bid, {
            "case_id": f"case_ok_{pid[-3:]}_{i}", "case_name": f"通过{i}",
            "group": "api", "priority": "P2", "status": "passed",
            "duration": 0.2, "assertions": [], "steps": [], "logs": [],
        })
    for i in range(skipped):
        store.record_result(bid, {
            "case_id": f"case_skip_{pid[-3:]}_{i}", "case_name": f"跳过{i}",
            "group": "api", "priority": "P3", "status": "skipped",
            "duration": 0.0, "assertions": [], "steps": [], "logs": [],
        })
    store.finish(bid, status)
    # finish 会重算 duration 与 finished_at；把时间戳固定到可控位置
    store.update(bid, {"started_at": created_at, "finished_at": created_at + 100,
                       "created_at": created_at})
    return store


class WeeklyReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        (self.registry, self.builds, self.coverage, self.notify,
         self.weekly, self.sched) = _make_env(self.tmp.name)

    def tearDown(self):
        self.sched.shutdown()
        self.tmp.cleanup()

    def _project(self, name="项目A"):
        return self.registry.store("projects").insert({"name": name})

    def _recipient(self, pid, itype="email", address="qa@example.com",
                   fail=False):
        return self.notify.create(pid, {
            "type": itype, "name": f"{itype}-{address}",
            "config": {"address": address, "url": address, "fail": fail},
            "events": [],
        })["id"]

    def _subscription(self, pid, rids, **kw):
        payload = {
            "name": "周报", "frequency": "weekly_mon", "cron": "0 9 * * 1",
            "project_ids": [pid], "recipient_ids": rids,
            "sections": ["pass_trend", "top_failures", "open_defects",
                         "coverage_delta"],
            "delivery": "merged",
        }
        payload.update(kw)
        return self.weekly.create_subscription(payload)

    # ------------------------------------------------------------ 快照一致性
    def test_snapshot_freezes_data_at_one_point(self):
        pid = self._project()
        now = time.time()
        # 窗口内一场构建：10 个用例 8 通过
        _make_build(self.builds, pid, "b_old", now - 3 * 86400,
                    total=10, passed=8, failed=2)
        rid = self._recipient(pid)
        sub = self._subscription(pid, [rid])

        preview = self.weekly.preview(sub["id"], at=now)
        pdata = preview["snapshot"]["projects"][0]
        self.assertEqual(pdata["total_cases"], 10)
        self.assertEqual(pdata["total_passed"], 8)
        self.assertEqual(pdata["pass_rate"], 80.0)
        self.assertEqual(len(pdata["top_failures"]), 2)

        # 预览不落库
        self.assertEqual(self.weekly.list_reports(), [])

        # 「发送之后」再来一场全新构建（窗口内），已生成报告内容不应改变
        sent = self.weekly.send(sub["id"], at=now)
        frozen = self.weekly.get_report(sent["id"])
        _make_build(self.builds, pid, "b_new", now - 1 * 86400,
                    total=10, passed=10, failed=0, status="passed")
        refetched = self.weekly.get_report(sent["id"])
        self.assertEqual(refetched["snapshot"], frozen["snapshot"])
        self.assertEqual(refetched["html"], frozen["html"])

        # 重新生成（新的截取点）才能看到变化
        again = self.weekly.preview(sub["id"], at=now + 10)
        self.assertEqual(again["snapshot"]["projects"][0]["total_cases"], 20)

    def test_snapshot_window_excludes_out_of_range_builds(self):
        pid = self._project()
        # 截取点为某周一 09:00，窗口 = 过去 7 天 [anchor-7d, anchor)
        anchor = datetime.datetime(2026, 10, 5, 9, 0).timestamp()  # 周一
        _make_build(self.builds, pid, "in1", anchor - 2 * 86400,
                    total=4, passed=4, failed=0, status="passed")
        _make_build(self.builds, pid, "in2", anchor - 6 * 86400,
                    total=4, passed=2, failed=2)
        _make_build(self.builds, pid, "too_old", anchor - 9 * 86400,
                    total=4, passed=0, failed=4)
        _make_build(self.builds, pid, "too_new", anchor + 3600,
                    total=4, passed=4, failed=0, status="passed")
        rid = self._recipient(pid)
        sub = self._subscription(pid, [rid])
        snap = self.weekly.preview(sub["id"], at=anchor)["snapshot"]
        ids = {b["build_id"] for b in snap["projects"][0]["builds"]}
        self.assertEqual(ids, {"in1", "in2"})
        self.assertEqual(snap["projects"][0]["total_cases"], 8)
        self.assertAlmostEqual(snap["window_end"], anchor)
        self.assertAlmostEqual(snap["window_start"], anchor - 7 * 86400,
                               delta=1.0)

    def test_sections_respected(self):
        pid = self._project()
        _make_build(self.builds, pid, "b1", time.time() - 86400,
                    total=6, passed=5, failed=1)
        self.registry.store("defects").insert({
            "id": "d1", "project_id": pid, "title": "未关缺陷",
            "severity": "critical", "status": "open",
        })
        rid = self._recipient(pid)
        sub = self._subscription(pid, [rid], sections=["pass_trend"])
        report = self.weekly.preview(sub["id"], at=time.time())
        pdata = report["snapshot"]["projects"][0]
        self.assertTrue(pdata["trend"])
        self.assertEqual(pdata["top_failures"], [])
        self.assertEqual(pdata["open_defects"], [])
        self.assertIsNone(pdata["coverage"])
        self.assertNotIn("未关缺陷", report["html"])

    # ------------------------------------------------------------ 投递模式
    def test_merged_one_copy_per_recipient(self):
        p1, p2 = self._project("项目1"), self._project("项目2")
        _make_build(self.builds, p1, "a1", time.time() - 86400,
                    total=5, passed=5, failed=0, status="passed")
        _make_build(self.builds, p2, "b1", time.time() - 86400,
                    total=5, passed=4, failed=1)
        r1 = self._recipient(p1, "email", "lead@example.com")
        r2 = self._recipient(p1, "webhook", "https://hooks/x")
        sub = self._subscription(p1, [r1, r2], project_ids=[p1, p2],
                                 delivery="merged")
        sent = self.weekly.send(sub["id"])
        self.assertEqual(sent["status"], "sent")
        # 2 个接收人各一份（scope=all），共 2 次投递
        self.assertEqual(len(sent["deliveries"]), 2)
        self.assertTrue(all(d["status"] == "delivered" for d in sent["deliveries"]))
        self.assertTrue(all(d["project_scope"] == "all" for d in sent["deliveries"]))
        # 通知事件表也应有 2 条 weekly.report 留痕
        events = self.registry.store("notify_events").query(
            where=[("event", "eq", "weekly.report")])
        self.assertEqual(len(events), 2)

    def test_separate_one_copy_per_project_per_recipient(self):
        p1, p2 = self._project("项目1"), self._project("项目2")
        for pid, bid in ((p1, "a1"), (p2, "b1")):
            _make_build(self.builds, pid, bid, time.time() - 86400,
                        total=4, passed=4, failed=0, status="passed")
        r1 = self._recipient(p1, "email", "a@example.com")
        sub = self._subscription(p1, [r1], project_ids=[p1, p2],
                                 delivery="separate")
        sent = self.weekly.send(sub["id"])
        scopes = sorted(d["project_scope"] for d in sent["deliveries"])
        self.assertEqual(scopes, sorted([p1, p2]))
        # 每份 markdown 只含自己的项目名（分别投递不串数据）
        by_scope = {d["project_scope"]: d for d in sent["deliveries"]}
        for scope_pid, other_pid in ((p1, p2), (p2, p1)):
            payload = self.registry.store("notify_events").get(
                by_scope[scope_pid]["event_id"])["payload"]
            self.assertIn(self.weekly._project_name(scope_pid), payload["text"])
            self.assertNotIn(self.weekly._project_name(other_pid), payload["text"])

    def test_failed_delivery_marks_partial_or_failed(self):
        p1, p2 = self._project("P1"), self._project("P2")
        good = self._recipient(p1, "email", "ok@example.com")
        bad = self._recipient(p1, "email", "bad@example.com", fail=True)
        # fail=True 的集成配置没有 url 之外… 这里通过 config.fail 强制失败
        sub_good = self._subscription(p1, [good])
        sub_bad = self._subscription(p1, [bad])
        self.assertEqual(self.weekly.send(sub_bad["id"])["status"], "failed")
        mixed = self._subscription(p1, [good, bad])
        self.assertEqual(self.weekly.send(mixed["id"])["status"], "partial")

    # ------------------------------------------------------------ 校验
    def test_validation_requires_projects_and_recipients(self):
        pid = self._project()
        with self.assertRaises(ValueError):
            self.weekly.create_subscription({"cron": "0 9 * * 1", "project_ids": [],
                                             "recipient_ids": ["x"]})
        with self.assertRaises(ValueError):
            self.weekly.create_subscription({"cron": "0 9 * * 1", "project_ids": [pid],
                                             "recipient_ids": []})
        with self.assertRaises(ValueError):
            self.weekly.create_subscription({"cron": "not a cron",
                                             "project_ids": [pid], "recipient_ids": ["x"]})

    def test_partial_update_preserves_fields(self):
        # 只改名称与 top_n 时，项目 / 接收人 / 模块 / cron 均应保留
        pid = self._project()
        rid = self._recipient(pid)
        sub = self._subscription(pid, [rid])
        updated = self.weekly.update_subscription(sub["id"], {"name": "新名字", "top_n": 3})
        self.assertEqual(updated["name"], "新名字")
        self.assertEqual(updated["top_n"], 3)
        self.assertEqual(updated["project_ids"], [pid])
        self.assertEqual(updated["recipient_ids"], [rid])
        self.assertEqual(updated["sections"], sub["sections"])
        self.assertEqual(updated["cron"], sub["cron"])

    def test_pause_and_resume(self):
        pid = self._project()
        rid = self._recipient(pid)
        sub = self._subscription(pid, [rid], cron="* * * * *")
        self.weekly.set_paused(sub["id"], True)
        self.assertFalse(self.weekly.get_subscription(sub["id"])["enabled"])
        now = datetime.datetime.now()
        self.assertEqual(self.weekly.due_subscriptions(now), [])
        self.weekly.set_paused(sub["id"], False)
        self.assertEqual(len(self.weekly.due_subscriptions(now)), 1)

    # ------------------------------------------------------------ 定时扫描
    def test_scan_fires_and_dedupes_per_minute(self):
        pid = self._project()
        _make_build(self.builds, pid, "b1", time.time() - 86400,
                    total=4, passed=4, failed=0, status="passed")
        rid = self._recipient(pid)
        sub = self._subscription(pid, [rid], cron="* * * * *")
        now = datetime.datetime.now()
        sent1 = self.weekly.scan_and_send(now)
        sent2 = self.weekly.scan_and_send(now)
        self.assertEqual(len(sent1), 1)
        self.assertEqual(len(sent2), 0)  # 同分钟不重复
        reports = self.weekly.list_reports()
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0]["status"], "sent")
        # 下一分钟会再次触发
        next_minute = now + datetime.timedelta(minutes=1)
        self.assertEqual(len(self.weekly.scan_and_send(next_minute)), 1)

    def test_scheduler_tick_invokes_weekly_scan(self):
        pid = self._project()
        _make_build(self.builds, pid, "b1", time.time() - 86400,
                    total=4, passed=4, failed=0, status="passed")
        rid = self._recipient(pid)
        self._subscription(pid, [rid], cron="* * * * *")
        self.sched._scan_schedules()  # 走调度器统一扫描入口
        reports = self.weekly.list_reports()
        self.assertEqual(len(reports), 1)

    # ------------------------------------------------------------ 覆盖率变化
    def test_coverage_delta_sections(self):
        pid = self._project()
        now = time.time()
        _make_build(self.builds, pid, "prev", now - 10 * 86400,
                    total=4, passed=4, failed=0, status="passed")
        _make_build(self.builds, pid, "curr", now - 1 * 86400,
                    total=4, passed=4, failed=0, status="passed")
        rid = self._recipient(pid)
        sub = self._subscription(pid, [rid], sections=["coverage_delta"])
        report = self.weekly.preview(sub["id"], at=now)
        pdata = report["snapshot"]["projects"][0]
        self.assertIsNotNone(pdata["coverage"])
        self.assertIsNotNone(pdata["coverage_prev"])
        self.assertIsNotNone(pdata["coverage_delta"])


if __name__ == "__main__":
    unittest.main()
