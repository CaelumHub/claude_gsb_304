"""质量周报引擎测试。

覆盖：
- 周期切分（最近一个已完整结束的窗口、窗口天数）
- 数据快照：同一项目同一周期共享、不可变（快照后数据变化不影响）
- 不同订阅 / 临时接收人共享同一快照 → 数字口径一致
- 预览草稿复用；发送留痕；同一周期调度不重复推送
- 先预览（manual）模式只生成草稿不投递；暂停订阅到点不触发
"""

from __future__ import annotations

import datetime
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine import (CoverageAnalyzer, DefectManager, EnvironmentManager,
                    NotificationManager, Scheduler, TestExecutor,
                    WeeklyReportManager, period_for, render_report_text)
from storage import BuildStoreRegistry, StoreRegistry


def _manager(data_root):
    registry = StoreRegistry(os.path.join(data_root, "store"), shard_size=50)
    builds = BuildStoreRegistry(os.path.join(data_root, "builds"))
    env_mgr = EnvironmentManager(registry, data_root)
    coverage = CoverageAnalyzer(builds)
    defects = DefectManager(registry)
    notify = NotificationManager(registry)
    weekly = WeeklyReportManager(registry, builds, coverage, defects)
    scheduler = Scheduler(registry, builds, TestExecutor(), env_mgr,
                          None, coverage, defects, notify, tick_seconds=0.2)
    scheduler.weekly = weekly
    return registry, builds, env_mgr, weekly, scheduler


class TestPeriod(unittest.TestCase):
    def test_uses_last_closed_window(self):
        # 2026-10-07（周三）：7 天窗口应取 2026-09-28 ~ 2026-10-04
        at = datetime.datetime(2026, 10, 7, 10, 0).timestamp()
        p = period_for(at, 7)
        self.assertEqual(p["start"], datetime.date(2026, 9, 28))
        self.assertEqual(p["end"], datetime.date(2026, 10, 5))
        self.assertEqual(p["key"], "2026-09-28_7d")
        # cutoff = 窗口结束日 00:00
        self.assertEqual(datetime.datetime.fromtimestamp(p["cutoff_at"]),
                         datetime.datetime(2026, 10, 5, 0, 0))

    def test_monday_morning_gets_previous_week(self):
        # 周一 09:00 拿到的是上周一~周日
        at = datetime.datetime(2026, 10, 5, 9, 0).timestamp()
        p = period_for(at, 7)
        self.assertEqual(p["start"], datetime.date(2026, 9, 28))
        self.assertEqual(p["end"], datetime.date(2026, 10, 5))

    def test_window_14_days(self):
        at = datetime.datetime(2026, 10, 7).timestamp()
        p = period_for(at, 14)
        self.assertEqual((p["end"] - p["start"]).days, 14)
        # 14 天窗口的 key 与 7 天不同（订阅频率/窗口不同也各自成组）
        self.assertNotEqual(p["key"], period_for(at, 7)["key"])

    def test_same_window_days_share_period_regardless_of_cron(self):
        at = datetime.datetime(2026, 10, 7, 9, 30).timestamp()
        # 无论订阅是周一 9 点还是周三 15 点，7 天窗口此刻都指向同一周期
        self.assertEqual(period_for(at, 7)["key"], period_for(at, 7)["key"])


class TestWeeklyReports(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        (self.registry, self.builds, self.env_mgr,
         self.weekly, self.sched) = _manager(self.tmp.name)
        self.pid = self.registry.store("projects").insert({"name": "项目甲"})
        env = self.env_mgr.create(
            self.pid, {"name": "dev", "config": {"latency_ms": 0, "fail_rate": 0.0}})
        ids = [self.registry.store("cases").insert({
            "id": f"case_{i}", "project_id": self.pid, "name": f"用例{i}",
            "priority": "P1" if i < 2 else "P2", "tags": ["g"], "timeout": 10,
            "steps": [
                {"action": "request", "method": "GET", "url": "/api/x"},
                {"action": "assert", "type": "status",
                 "actual": "${resp.status}", "expected": 200},
            ]}) for i in range(5)]
        self.suite_id = self.registry.store("suites").insert({
            "id": "suite_1", "project_id": self.pid, "name": "回归",
            "env_id": env["id"], "case_ids": ids})
        self.period = period_for()

    def tearDown(self):
        self.sched.shutdown()
        self.tmp.cleanup()

    def _build_in_window(self, days_before_cutoff: int) -> str:
        bid = self.sched.submit_build(self.pid, self.suite_id)["id"]
        deadline = time.time() + 20
        while time.time() < deadline:
            b = self.builds.for_project(self.pid).get(bid)
            if b["status"] in ("passed", "failed", "cancelled", "error"):
                break
            time.sleep(0.05)
        when = self.period["cutoff_at"] - 86400 * days_before_cutoff
        self.builds.for_project(self.pid).update(
            bid, {"finished_at": when, "started_at": when - 5, "created_at": when})
        return bid

    def _make_sub(self, modules=None, recipients=None, mode="auto",
                  cron="0 9 * * 1", window=7):
        return self.weekly.create_subscription({
            "name": "周报", "project_ids": [self.pid], "cron": cron,
            "window_days": window, "mode": mode,
            "modules": modules or ["pass_rate_trend", "top_failures",
                                   "open_defects", "coverage_change"],
            "recipients": recipients or [
                {"type": "email", "name": "QA", "target": "qa@example.com"}]})

    # -- 校验 -------------------------------------------------------------
    def test_validation(self):
        r = self.weekly.create_subscription({"project_ids": [], "cron": "bad",
                                             "modules": [], "recipients": []})
        self.assertIn("error", r)
        ok = self._make_sub()
        self.assertNotIn("error", ok)

    # -- 快照口径一致 -----------------------------------------------------
    def test_snapshot_shared_and_immutable(self):
        self._build_in_window(3)
        self._build_in_window(2)
        defects_before = self.weekly.defects.list(self.pid)
        self.weekly.defects.create(
            self.pid, {"title": "未关闭缺陷A", "status": "open"})

        snap1 = self.weekly.snapshot_for(self.pid, self.period)
        snap2 = self.weekly.snapshot_for(self.pid, self.period)
        self.assertEqual(snap1["id"], snap2["id"])
        self.assertEqual(snap1["data"]["build_count"], 2)
        self.assertEqual(snap1["data"]["open_defects"]["total"], 1)

        # 快照之后再关闭缺陷，快照内容不变（不可变）
        d = self.weekly.defects.list(self.pid)[0]
        self.weekly.defects.update(d["id"], {"status": "closed"})
        snap3 = self.weekly.snapshot_for(self.pid, self.period)
        self.assertEqual(snap3["data"]["open_defects"]["total"], 1)

        # 窗口外的构建不计入（cutoff 之后完成的构建不会"穿越"进周报）
        outside = self.sched.submit_build(self.pid, self.suite_id)["id"]
        deadline = time.time() + 20
        while time.time() < deadline:
            b = self.builds.for_project(self.pid).get(outside)
            if b["status"] in ("passed", "failed", "cancelled", "error"):
                break
            time.sleep(0.05)
        snap4 = self.weekly.snapshot_for(self.pid, self.period)
        self.assertEqual(snap4["data"]["build_count"], 2)

    def test_all_sections_use_same_build_set(self):
        # 通过率趋势、top 失败、覆盖率的构建数必须一致（同一份快照）
        self._build_in_window(2)
        snap = self.weekly.snapshot_for(self.pid, self.period)["data"]
        n = snap["build_count"]
        self.assertEqual(len(snap["pass_rate_trend"]["points"]), n)
        self.assertEqual(len(snap["coverage_change"]["points"]), n)

    # -- 草稿复用 / 模块裁剪 ----------------------------------------------
    def test_preview_reuses_draft_and_modules_filtered(self):
        self._build_in_window(1)
        sub = self._make_sub(modules=["pass_rate_trend"])
        r1 = self.weekly.preview_subscription(sub["id"])
        r2 = self.weekly.preview_subscription(sub["id"])
        self.assertEqual(r1["id"], r2["id"])  # 同周期复用草稿
        self.assertEqual(r1["status"], "draft")
        proj = r1["content"]["projects"][0]
        self.assertIn("pass_rate_trend", proj["modules"])
        self.assertNotIn("top_failures", proj["modules"])

        # 改了模块配置 -> 生成新草稿，避免预览到旧内容
        self.weekly.update_subscription(
            sub["id"], {"modules": ["pass_rate_trend", "open_defects"]})
        r3 = self.weekly.preview_subscription(sub["id"])
        self.assertIn("open_defects", r3["content"]["projects"][0]["modules"])

    # -- 发送 / 留痕 ------------------------------------------------------
    def test_send_records_deliveries_and_marks_sent(self):
        self._build_in_window(1)
        sub = self._make_sub(recipients=[
            {"type": "email", "name": "A", "target": "a@example.com"},
            {"type": "dingtalk", "name": "B", "target": "room-1"}])
        report = self.weekly.send_subscription(sub["id"])
        self.assertEqual(report["status"], "sent")
        self.assertEqual(report["sent_stats"]["recipient_count"], 2)
        self.assertEqual(report["sent_stats"]["delivered"], 2)
        dels = self.weekly.list_deliveries()
        self.assertEqual(len(dels), 2)
        self.assertTrue(all(d["status"] == "delivered" for d in dels))

    def test_missing_target_fails_but_others_deliver(self):
        self._build_in_window(1)
        sub = self._make_sub(recipients=[
            {"type": "email", "name": "A", "target": "a@example.com"},
            {"type": "email", "name": "空", "target": ""}])
        report = self.weekly.send_subscription(sub["id"])
        self.assertEqual(report["sent_stats"]["delivered"], 1)
        self.assertEqual(report["sent_stats"]["failed"], 1)

    # -- 多订阅 / 多接收人口径一致 ----------------------------------------
    def test_single_and_merged_share_consistent_numbers(self):
        self._build_in_window(2)
        pid2 = self.registry.store("projects").insert({"name": "项目乙"})
        # 订阅 1：只发项目甲给某人；订阅 2：甲乙合并发给另一个人
        s1 = self._make_sub(recipients=[{"type": "email", "name": "单人",
                                         "target": "one@example.com"}])
        s2 = self.weekly.create_subscription({
            "name": "合并", "project_ids": [self.pid, pid2], "cron": "0 9 * * 1",
            "window_days": 7, "modules": ["pass_rate_trend"],
            "recipients": [{"type": "email", "name": "负责人",
                            "target": "lead@example.com"}]})
        r1 = self.weekly.send_subscription(s1["id"])
        r2 = self.weekly.send_subscription(s2["id"])
        # 合并报告里项目甲的通过率必须和单独报告完全一致
        single = r1["content"]["projects"][0]
        merged_p1 = next(p for p in r2["content"]["projects"]
                         if p["project_id"] == self.pid)
        self.assertEqual(single["pass_rate"], merged_p1["pass_rate"])
        self.assertEqual(single["build_count"], merged_p1["build_count"])
        self.assertEqual(single["modules"]["pass_rate_trend"]["points"],
                         merged_p1["modules"]["pass_rate_trend"]["points"])
        # 同一份报告还可临时投递给额外的人（send_report 自定义 recipients）
        again = self.weekly.send_report(r1["id"], [
            {"type": "email", "name": "临时", "target": "tmp@example.com"}])
        self.assertEqual(again["status"], "sent")
        # s1 的 1 个接收人 + s2 的 1 个 + 临时 1 个 = 3 条投递留痕
        self.assertEqual(len(self.weekly.list_deliveries()), 3)

    # -- 定时扫描：去重 / 手动模式 / 暂停 ----------------------------------
    def test_scan_fires_once_per_period(self):
        self._build_in_window(1)
        sub = self._make_sub(cron="0 9 * * 1")
        monday = datetime.datetime.combine(
            self.period["end"], datetime.time(9, 0)).timestamp()
        due1 = self.weekly.scan_due(at=monday)
        self.assertEqual(len(due1), 1)
        self.assertEqual(due1[0]["status"], "sent")
        # 同周期再扫（重启 / 漏 tick 场景）不重复
        self.assertEqual(self.weekly.scan_due(at=monday), [])
        # 非命中时刻不触发
        tuesday = monday + 86400
        self.assertEqual(self.weekly.scan_due(at=tuesday), [])

    def test_manual_mode_creates_draft_only(self):
        self._build_in_window(1)
        sub = self._make_sub(mode="manual", cron="0 9 * * 1")
        monday = datetime.datetime.combine(
            self.period["end"], datetime.time(9, 0)).timestamp()
        due = self.weekly.scan_due(at=monday)
        self.assertEqual(len(due), 1)
        self.assertEqual(due[0]["status"], "draft")
        # 草稿不产生投递
        self.assertEqual(self.weekly.list_deliveries(), [])
        # 人工确认后才发送
        report = self.weekly.send_report(due[0]["id"])
        self.assertEqual(report["status"], "sent")

    def test_paused_subscription_skipped(self):
        self._build_in_window(1)
        sub = self._make_sub(cron="0 9 * * 1")
        self.weekly.set_enabled(sub["id"], False)
        monday = datetime.datetime.combine(
            self.period["end"], datetime.time(9, 0)).timestamp()
        self.assertEqual(self.weekly.scan_due(at=monday), [])
        self.assertEqual(self.weekly.list_deliveries(), [])
        # 恢复后本周期仍可处理（暂停期间未写过去重标记）
        self.weekly.set_enabled(sub["id"], True)
        due = self.weekly.scan_due(at=monday)
        self.assertEqual(len(due), 1)

    # -- 文本渲染 ---------------------------------------------------------
    def test_render_text_contains_all_sections(self):
        self._build_in_window(1)
        self.weekly.defects.create(
            self.pid, {"title": "缺陷X", "severity": "critical",
                       "status": "open"})
        sub = self._make_sub()
        report = self.weekly.preview_subscription(sub["id"])
        text = render_report_text(report["content"])
        for token in ("统计周期", "整体通过率", "通过率趋势", "失败 Top 用例",
                      "未关闭缺陷", "覆盖率", "项目甲", "缺陷X"):
            self.assertIn(token, text)


if __name__ == "__main__":
    unittest.main()
