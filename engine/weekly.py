"""质量周报：订阅、数据快照、报告生成、投递与发送留痕。

背景
----
测试负责人每周一手工把三四个项目的数据拼成一份周报，但各项目构建与缺陷
更新时间参差不齐，同一份周报里前后段落的数字常常对不上。本模块把这个动作
自动化，核心是解决两件事：

1. **口径一致（快照）**：报告不在渲染时临时去查实时数据，而是先在一个固定
   时间点（周期截止时刻 ``cutoff``）把每个项目在统计窗口内的构建、失败用例、
   未关闭缺陷、覆盖率**一次性截取**成不可变快照
   （``weekly_snapshots``）。同一份报告的所有段落都从同一份快照读取；
   同一周期的不同订阅（不管接收人、频率如何不同）也共享同一快照，因此
   「单独发给某人」与「合并发给另一个人」的两份周报数字必然一致。
   快照一旦生成不再变化，之后构建再跑完、缺陷再关闭，也不会让已发周报
   「穿越」。
2. **订阅与留痕**：订阅描述接收人 / cron 频率 / 内容模块 / 统计窗口 /
   自动发送还是先预览；到点由调度器扫描生成报告；报告支持 draft（预览）
   → sent（发送）两个阶段，每次投递（模拟）都写入 ``weekly_deliveries``，
   订阅可随时暂停（``enabled=False``），恢复后本周期已处理过不会补发。

周期划分
--------
以固定锚点日（2026-01-05，周一）起，按订阅的 ``window_days``（默认 7 天）
把时间轴切成等长窗口。报告在某时刻生成时，取「**最近一个已经完整结束的
窗口**」作为统计周期（周一早上拿到的是上周一~周日的数据），其结束时刻
就是统一的截取时间点 ``cutoff``。``window_days`` 相同的所有订阅在同一时刻
得到相同的 period_key / cutoff，从而共享快照。

实体
----
- ``weekly_subscriptions`` 订阅（多项目 / 多接收人 / 模块 / cron / 模式 / 暂停）
- ``weekly_snapshots``     数据快照（period_key + project 唯一，不可变）
- ``weekly_reports``       报告（一次订阅一次周期一份；draft → sent）
- ``weekly_deliveries``    投递记录（一份报告对多个接收人逐条留痕）
"""

from __future__ import annotations

import datetime
import threading
from typing import Optional

from .cron import cron_matches, parse_cron
from .models import INTEGRATION_TYPES, new_id

# 周报可选内容模块
WEEKLY_MODULES = [
    "pass_rate_trend",   # 通过率趋势
    "top_failures",      # 失败 top 用例
    "open_defects",      # 未关闭缺陷
    "coverage_change",   # 覆盖率变化
]

MODULE_LABELS = {
    "pass_rate_trend": "通过率趋势",
    "top_failures": "失败 Top 用例",
    "open_defects": "未关闭缺陷",
    "coverage_change": "覆盖率变化",
}

FAILED_STATUSES = ("failed", "error", "timeout")
OPEN_DEFECT_STATUSES = ("open", "in_progress", "fixed", "verified", "reopened")
_SEVERITY_ORDER = {s: i for i, s in
                   enumerate(("blocker", "critical", "major", "minor", "trivial"))}

# 周期切分锚点：2026-01-05 周一（本地时间）
_ANCHOR_DATE = datetime.date(2026, 1, 5)

DEFAULT_WINDOW_DAYS = 7
TOP_FAILURES_LIMIT = 10
OPEN_DEFECTS_LIMIT = 20


# ---------------------------------------------------------------------------
# 周期计算
# ---------------------------------------------------------------------------

def period_for(at: Optional[float] = None,
               window_days: int = DEFAULT_WINDOW_DAYS) -> dict:
    """计算某时刻对应的「最近一个已完整结束」的统计窗口。

    返回 ``{key, index, start(date), end(date), cutoff_at, window_days}``。
    窗口为左闭右开 ``[start, end)``，``cutoff_at`` 即窗口结束时刻（=截取点）。
    """
    window_days = max(1, int(window_days))
    dt = datetime.datetime.fromtimestamp(at) if at else datetime.datetime.now()
    today = dt.date()
    elapsed_days = (today - _ANCHOR_DATE).days
    index = elapsed_days // window_days - 1  # 最近一个已关闭的窗口
    start = _ANCHOR_DATE + datetime.timedelta(days=index * window_days)
    end = start + datetime.timedelta(days=window_days)
    cutoff = datetime.datetime.combine(end, datetime.time.min)
    return {
        "key": f"{start.isoformat()}_{window_days}d",
        "index": index,
        "start": start,
        "end": end,
        "cutoff_at": cutoff.timestamp(),
        "window_days": window_days,
    }


def period_label(period: dict) -> str:
    return f"{period['start'].isoformat()} ~ {(period['end'] - datetime.timedelta(days=1)).isoformat()}"


# ---------------------------------------------------------------------------
# 周报管理器
# ---------------------------------------------------------------------------

class WeeklyReportManager:
    """质量周报订阅与报告管理。"""

    def __init__(self, registry, build_registry, coverage_analyzer, defect_manager):
        self.registry = registry
        self.builds = build_registry
        self.coverage = coverage_analyzer
        self.defects = defect_manager
        # 同一订阅同一周期的生成/投递串行化，防止调度线程与手动发送并发重复。
        # 用可重入锁：generate_report 持锁后会再进入 snapshot_for 取快照。
        self._gen_lock = threading.RLock()
        self._scan_lock = threading.Lock()

    # -- 存储句柄 ---------------------------------------------------------
    @property
    def _subs(self):
        return self.registry.store("weekly_subscriptions")

    @property
    def _snaps(self):
        return self.registry.store("weekly_snapshots")

    @property
    def _reports(self):
        return self.registry.store("weekly_reports")

    @property
    def _deliveries(self):
        return self.registry.store("weekly_deliveries")

    # ======================================================================
    # 订阅 CRUD
    # ======================================================================
    def list_subscriptions(self) -> list[dict]:
        subs = self._subs.all()
        for s in subs:
            s["period"] = period_label(period_for(window_days=s.get("window_days", 7)))
        return sorted(subs, key=lambda s: s.get("created_at", 0), reverse=True)

    def get_subscription(self, sub_id: str) -> Optional[dict]:
        return self._subs.get(sub_id)

    def create_subscription(self, payload: dict) -> dict:
        sub = self._normalize(payload, existing=None)
        if "error" in sub:
            return sub
        sub.update({
            "id": new_id("wsub"),
            "enabled": bool(payload.get("enabled", True)),
            "last_period_key": None,
            "created_at": __import__("time").time(),
        })
        self._subs.insert(sub)
        return sub

    def update_subscription(self, sub_id: str, patch: dict) -> Optional[dict]:
        existing = self._subs.get(sub_id)
        if existing is None:
            return None
        merged = dict(existing)
        merged.update({k: v for k, v in patch.items()
                       if k in ("name", "project_ids", "recipients", "cron",
                                "modules", "window_days", "mode", "enabled")})
        normalized = self._normalize(merged, existing=existing, partial=True)
        if "error" in normalized:
            return normalized
        # 改频率/窗口后重置已处理标记，保证新周期能触发
        if "cron" in patch or "window_days" in patch:
            normalized["last_period_key"] = None
        return self._subs.update(sub_id, normalized)

    def delete_subscription(self, sub_id: str) -> bool:
        return self._subs.delete(sub_id)

    def set_enabled(self, sub_id: str, enabled: bool) -> Optional[dict]:
        """暂停 / 恢复订阅。"""
        if self._subs.get(sub_id) is None:
            return None
        return self._subs.update(sub_id, {"enabled": bool(enabled)})

    def _normalize(self, data: dict, existing: Optional[dict],
                   partial: bool = False) -> dict:
        """校验并规范化订阅字段。失败返回 ``{"error": ...}``。"""
        def pick(key, default):
            if key in data and data[key] is not None:
                return data[key]
            return existing.get(key, default) if existing else default

        name = (pick("name", "质量周报") or "质量周报").strip() or "质量周报"
        project_ids = pick("project_ids", []) or []
        if not isinstance(project_ids, list) or not project_ids:
            return {"error": "至少选择一个项目"}
        projects_store = self.registry.store("projects")
        valid_pids = []
        for pid in project_ids:
            if projects_store.get(pid) and pid not in valid_pids:
                valid_pids.append(pid)
        if not valid_pids:
            return {"error": "所选项目不存在"}

        cron = (pick("cron", "0 9 * * 1") or "").strip()
        try:
            parse_cron(cron)
        except ValueError as exc:
            return {"error": f"cron 表达式无效：{exc}"}

        window_days = pick("window_days", DEFAULT_WINDOW_DAYS)
        try:
            window_days = int(window_days)
        except (TypeError, ValueError):
            return {"error": "统计窗口天数必须是整数"}
        if not 1 <= window_days <= 90:
            return {"error": "统计窗口天数需在 1~90 之间"}

        modules = pick("modules", list(WEEKLY_MODULES)) or []
        modules = [m for m in modules if m in WEEKLY_MODULES]
        if not modules:
            return {"error": "至少选择一个内容模块"}

        mode = pick("mode", "auto")
        if mode not in ("auto", "manual"):
            mode = "auto"

        raw_recipients = pick("recipients", []) or []
        recipients = self._normalize_recipients(raw_recipients)
        if not recipients:
            return {"error": "至少配置一个接收人"}

        return {
            "name": name,
            "project_ids": valid_pids,
            "recipients": recipients,
            "cron": cron,
            "window_days": window_days,
            "modules": modules,
            "mode": mode,
        }

    def _normalize_recipients(self, raw: list) -> list[dict]:
        out = []
        for r in raw:
            if not isinstance(r, dict):
                continue
            rtype = r.get("type", "email")
            if rtype not in INTEGRATION_TYPES:
                rtype = "email"
            target = (r.get("target") or "").strip()
            # 允许引用项目下已有的通知集成，复用其目标地址
            integration_id = r.get("integration_id")
            if not target and integration_id:
                integration = self.registry.store("integrations").get(integration_id)
                if integration:
                    cfg = integration.get("config") or {}
                    target = cfg.get("address") or cfg.get("url") or cfg.get("channel") or ""
            name = (r.get("name") or "").strip() or target or "接收人"
            if not target and not name:
                continue
            out.append({"type": rtype, "name": name, "target": target,
                        "integration_id": integration_id or None})
        return out

    # ======================================================================
    # 数据快照（口径一致的核心）
    # ======================================================================
    def snapshot_for(self, project_id: str, period: dict) -> dict:
        """获取（必要时生成）某项目某周期的不可变快照。

        快照 id 由 ``period_key + project_id`` 确定：同周期同项目永远只有
        一份快照；生成后内容不再随实时数据变化。
        """
        snap_id = f"wsnap_{period['key']}_{project_id}"
        existing = self._snaps.get(snap_id)
        if existing is not None:
            return existing
        with self._gen_lock:
            existing = self._snaps.get(snap_id)
            if existing is not None:
                return existing
            snap = self._build_snapshot(snap_id, project_id, period)
            # insert 会补 created_at；快照自身保留 generated_at 表示截取时刻
            self._snaps.insert(snap)
            return snap

    def _build_snapshot(self, snap_id: str, project_id: str, period: dict) -> dict:
        """在 cutoff 这一个时间点把该项目窗口内数据全部截出。"""
        start_ts = datetime.datetime.combine(period["start"],
                                             datetime.time.min).timestamp()
        end_ts = period["cutoff_at"]
        store = self.builds.for_project(project_id)
        project = self.registry.store("projects").get(project_id) or {}

        window_builds = []
        for b in store.list_builds():
            finished = b.get("finished_at")
            if finished is None:
                continue
            if start_ts <= finished < end_ts:
                window_builds.append(b)
        window_builds.sort(key=lambda b: b.get("finished_at", 0))  # 时间正序

        # -- 构建与通过率（同一批 builds，所有数字同源） -------------------
        total_runs = passed_runs = skipped_runs = 0
        durations = []
        trend = []
        for b in window_builds:
            total = b.get("total", 0)
            passed = b.get("passed", 0)
            skipped = b.get("skipped", 0)
            failed = (b.get("failed", 0) + b.get("error", 0)
                      + b.get("timeout", 0))
            finished_exec = total - skipped
            rate = round(passed / finished_exec * 100, 1) if finished_exec else 0.0
            total_runs += total
            passed_runs += passed
            skipped_runs += skipped
            if b.get("duration"):
                durations.append(b["duration"])
            trend.append({
                "build_id": b["id"],
                "name": b.get("name") or b["id"],
                "status": b.get("status"),
                "total": total,
                "passed": passed,
                "failed": failed,
                "pass_rate": rate,
                "duration": b.get("duration", 0.0),
                "finished_at": b.get("finished_at"),
            })
        finished_runs = total_runs - skipped_runs
        pass_rate = round(passed_runs / finished_runs * 100, 1) if finished_runs else 0.0
        first_rate = trend[0]["pass_rate"] if trend else 0.0
        last_rate = trend[-1]["pass_rate"] if trend else 0.0

        # -- 失败 top 用例（跨窗口内全部构建聚合） -------------------------
        failure_map: dict[str, dict] = {}
        for b in window_builds:
            for r in store.results(
                    b["id"],
                    where=[("status", "in", list(FAILED_STATUSES))]):
                key = r.get("case_id") or r.get("case_name") or "unknown"
                entry = failure_map.get(key)
                reason = self._failure_reason(r)
                if entry is None:
                    entry = {
                        "case_id": r.get("case_id"),
                        "case_name": r.get("case_name", key),
                        "group": r.get("group") or "默认",
                        "priority": r.get("priority") or "P3",
                        "fail_count": 0,
                        "last_status": r.get("status"),
                        "last_reason": reason,
                        "last_finished_at": b.get("finished_at", 0),
                    }
                    failure_map[key] = entry
                entry["fail_count"] += 1
                if b.get("finished_at", 0) >= entry["last_finished_at"]:
                    entry["last_status"] = r.get("status")
                    entry["last_reason"] = reason
                    entry["last_finished_at"] = b.get("finished_at", 0)
        top_failures = sorted(
            failure_map.values(),
            key=lambda e: (e["fail_count"], e["last_finished_at"]),
            reverse=True)[:TOP_FAILURES_LIMIT]

        # -- 未关闭缺陷（截取当前未关闭集合） ------------------------------
        all_defects = self.defects.list(project_id)
        open_defects = [d for d in all_defects
                        if d.get("status") in OPEN_DEFECT_STATUSES]
        by_status: dict[str, int] = {}
        by_severity: dict[str, int] = {}
        for d in open_defects:
            by_status[d.get("status", "open")] = by_status.get(d.get("status", "open"), 0) + 1
            by_severity[d.get("severity", "major")] = by_severity.get(d.get("severity", "major"), 0) + 1
        defect_items = sorted(
            open_defects,
            key=lambda d: (_SEVERITY_ORDER.get(d.get("severity", "major"), 9),
                           -(d.get("created_at") or 0)))[:OPEN_DEFECTS_LIMIT]
        open_defect_section = {
            "total": len(open_defects),
            "by_status": by_status,
            "by_severity": by_severity,
            "items": [{
                "id": d.get("id"),
                "title": d.get("title"),
                "severity": d.get("severity"),
                "status": d.get("status"),
                "assignee": d.get("assignee", ""),
                "created_at": d.get("created_at"),
            } for d in defect_items],
        }

        # -- 覆盖率变化（窗口内首末构建对比） ------------------------------
        cov_points = []
        for b in window_builds:
            cov = self.coverage.get(project_id, b["id"])
            cov_points.append({"build_id": b["id"],
                               "name": b.get("name") or b["id"],
                               "percent": cov.get("percent", 0.0),
                               "finished_at": b.get("finished_at")})
        first_cov = cov_points[0]["percent"] if cov_points else None
        last_cov = cov_points[-1]["percent"] if cov_points else None
        coverage_section = {
            "first_percent": first_cov,
            "last_percent": last_cov,
            "delta": round(last_cov - first_cov, 1)
            if first_cov is not None and last_cov is not None else 0.0,
            "points": cov_points,
        }

        avg_duration = round(sum(durations) / len(durations), 3) if durations else 0.0
        return {
            "id": snap_id,
            "project_id": project_id,
            "project_name": project.get("name", project_id),
            "period_key": period["key"],
            "window_start": start_ts,
            "window_end": end_ts,
            "cutoff_at": end_ts,
            "generated_at": __import__("time").time(),
            "data": {
                "build_count": len(window_builds),
                "total_runs": total_runs,
                "passed_runs": passed_runs,
                "failed_runs": total_runs - passed_runs - skipped_runs,
                "skipped_runs": skipped_runs,
                "pass_rate": pass_rate,
                "avg_duration": avg_duration,
                "pass_rate_trend": {
                    "points": trend,
                    "first_rate": first_rate,
                    "last_rate": last_rate,
                    "delta": round(last_rate - first_rate, 1),
                },
                "top_failures": top_failures,
                "open_defects": open_defect_section,
                "coverage_change": coverage_section,
            },
        }

    @staticmethod
    def _failure_reason(result: dict) -> str:
        failing_assert = next((a for a in result.get("assertions", [])
                               if not a.get("ok")), None)
        if failing_assert:
            return failing_assert.get("message", "")
        failing_step = next((s for s in result.get("steps", [])
                             if s.get("status") in ("failed", "error")), None)
        if failing_step:
            return failing_step.get("message", "")
        return result.get("message") or ""

    # ======================================================================
    # 报告生成（快照 → 按订阅模块裁剪 → 渲染）
    # ======================================================================
    def generate_report(self, *, project_ids: list[str], modules: list[str],
                        window_days: int = DEFAULT_WINDOW_DAYS,
                        at: Optional[float] = None,
                        subscription_id: Optional[str] = None,
                        title: str = "质量周报") -> dict:
        """基于共享快照生成一份周报（draft）。

        同周期同参数重复调用会复用已存在的草稿，保证预览与最终发送是同一份
        内容；快照不可变，内容天然一致。
        """
        period = period_for(at, window_days)
        with self._gen_lock:
            existing = None
            if subscription_id is not None:
                candidates = self._reports.query(where=[
                    ("period_key", "eq", period["key"]),
                    ("subscription_id", "eq", subscription_id),
                    ("status", "eq", "draft"),
                ], limit=1)
                # 仅当项目集与模块都没变时才复用草稿，避免改了配置看到旧内容
                if candidates and \
                        set(candidates[0].get("project_ids", [])) == set(project_ids) and \
                        set(candidates[0].get("modules", [])) == set(modules):
                    existing = candidates[0]
            if existing is not None:
                return existing

            projects_section = []
            for pid in project_ids:
                snap = self.snapshot_for(pid, period)
                projects_section.append(self._project_section(snap, modules))

            content = self._compose(title, period, modules,
                                    project_ids, projects_section)
            import time
            report = {
                "id": new_id("wrpt"),
                "subscription_id": subscription_id,
                "title": title,
                "project_ids": list(project_ids),
                "modules": modules,
                "period_key": period["key"],
                "period_label": period_label(period),
                "cutoff_at": period["cutoff_at"],
                "status": "draft",
                "content": content,
                "text": render_report_text(content),
                "created_at": time.time(),
                "updated_at": time.time(),
            }
            self._reports.insert(report)
            return report

    def _project_section(self, snap: dict, modules: list[str]) -> dict:
        data = snap["data"]
        section = {
            "project_id": snap["project_id"],
            "project_name": snap["project_name"],
            "build_count": data["build_count"],
            "total_runs": data["total_runs"],
            "passed_runs": data["passed_runs"],
            "failed_runs": data["failed_runs"],
            "skipped_runs": data["skipped_runs"],
            "pass_rate": data["pass_rate"],
            "avg_duration": data["avg_duration"],
            "modules": {},
        }
        for m in modules:
            section["modules"][m] = data[m]
        return section

    def _compose(self, title: str, period: dict, modules: list[str],
                 project_ids: list[str], projects_section: list[dict]) -> dict:
        build_count = sum(p["build_count"] for p in projects_section)
        total_runs = sum(p["total_runs"] for p in projects_section)
        passed_runs = sum(p["passed_runs"] for p in projects_section)
        skipped_runs = sum(p["skipped_runs"] for p in projects_section)
        finished_runs = total_runs - skipped_runs
        open_defect_count = sum(
            p["modules"]["open_defects"]["total"]
            for p in projects_section if "open_defects" in p["modules"])
        cov_deltas = [p["modules"]["coverage_change"]["delta"]
                      for p in projects_section
                      if "coverage_change" in p["modules"]
                      and p["modules"]["coverage_change"]["first_percent"] is not None]
        import time
        return {
            "title": title,
            "period_label": period_label(period),
            "period_key": period["key"],
            "window_days": period["window_days"],
            "cutoff_at": period["cutoff_at"],
            "generated_at": time.time(),
            "modules": modules,
            "module_labels": {m: MODULE_LABELS[m] for m in modules},
            "overview": {
                "project_count": len(project_ids),
                "build_count": build_count,
                "total_runs": total_runs,
                "passed_runs": passed_runs,
                "failed_runs": total_runs - passed_runs - skipped_runs,
                "pass_rate": round(passed_runs / finished_runs * 100, 1)
                if finished_runs else 0.0,
                "open_defect_count": open_defect_count,
                "avg_coverage_delta": round(sum(cov_deltas) / len(cov_deltas), 1)
                if cov_deltas else 0.0,
            },
            "projects": projects_section,
        }

    # ======================================================================
    # 预览 / 发送
    # ======================================================================
    def preview_subscription(self, sub_id: str,
                             at: Optional[float] = None) -> dict:
        """按订阅配置生成（或复用）预览草稿，不发送。"""
        sub = self._subs.get(sub_id)
        if sub is None:
            return {"error": "订阅不存在"}
        return self.generate_report(
            project_ids=sub["project_ids"], modules=sub["modules"],
            window_days=sub.get("window_days", 7), at=at,
            subscription_id=sub_id, title=sub["name"])

    def send_subscription(self, sub_id: str,
                          at: Optional[float] = None) -> dict:
        """按订阅配置生成报告并投递给全部接收人。"""
        sub = self._subs.get(sub_id)
        if sub is None:
            return {"error": "订阅不存在"}
        report = self.generate_report(
            project_ids=sub["project_ids"], modules=sub["modules"],
            window_days=sub.get("window_days", 7), at=at,
            subscription_id=sub_id, title=sub["name"])
        return self._send_report(report, sub["recipients"], sub)

    def send_report(self, report_id: str,
                    recipients: Optional[list[dict]] = None) -> dict:
        """发送一份已生成的报告（临时合并发送或从预览页确认发送）。"""
        report = self._reports.get(report_id)
        if report is None:
            return {"error": "报告不存在"}
        if recipients is None and report.get("subscription_id"):
            sub = self._subs.get(report["subscription_id"])
            recipients = sub["recipients"] if sub else []
        recipients = self._normalize_recipients(recipients or [])
        if not recipients:
            return {"error": "没有可投递的接收人"}
        return self._send_report(report, recipients, None)

    def _send_report(self, report: dict, recipients: list[dict],
                     sub: Optional[dict]) -> dict:
        import time
        deliveries = []
        for rcpt in recipients:
            outcome = self._deliver(rcpt, report)
            record = {
                "id": new_id("wdlv"),
                "report_id": report["id"],
                "subscription_id": report.get("subscription_id"),
                "recipient_type": rcpt["type"],
                "recipient_name": rcpt["name"],
                "recipient": rcpt["target"] or "未配置目标",
                "status": outcome["status"],
                "latency_ms": outcome["latency_ms"],
                "created_at": time.time(),
            }
            self._deliveries.insert(record)
            deliveries.append(record)

        delivered = sum(1 for d in deliveries if d["status"] == "delivered")
        import time as _t
        updated = self._reports.update(report["id"], {
            "status": "sent",
            "sent_at": _t.time(),
            "updated_at": _t.time(),
            "sent_stats": {
                "recipient_count": len(deliveries),
                "delivered": delivered,
                "failed": len(deliveries) - delivered,
            },
            "deliveries": deliveries,
        })
        # 记录本周期已发送，防止调度器同一周期重复自动推送
        if sub is not None:
            self._subs.update(sub["id"],
                              {"last_period_key": report["period_key"]})
        return updated

    def _deliver(self, recipient: dict, report: dict) -> dict:
        """模拟投递（平台离线运行，不发真实网络请求），结果确定性。"""
        import hashlib
        target = recipient.get("target") or ""
        failed = not target or target == "未配置目标"
        seed = int(hashlib.md5(
            "|".join((recipient.get("target") or "",
                      recipient.get("type", ""),
                      report["id"])).encode()).hexdigest()[:8], 16)
        return {
            "status": "failed" if failed else "delivered",
            "recipient": target or "未配置目标",
            "latency_ms": 10 + seed % 150,
        }

    # ======================================================================
    # 报告查询 / 留痕
    # ======================================================================
    def list_reports(self, subscription_id: Optional[str] = None,
                     limit: int = 50) -> list[dict]:
        if subscription_id:
            reports = self._reports.query(
                where=[("subscription_id", "eq", subscription_id)],
                order_by="created_at", order="desc", limit=limit)
        else:
            reports = self._reports.all()
            reports.sort(key=lambda r: r.get("created_at", 0), reverse=True)
            reports = reports[:limit]
        # 列表页只需要摘要，不带完整 content / text（明细走 get_report）
        return [{k: v for k, v in r.items()
                 if k not in ("content", "text", "deliveries")}
                for r in reports]

    def get_report(self, report_id: str) -> Optional[dict]:
        report = self._reports.get(report_id)
        if report is None:
            return None
        if "deliveries" not in report:
            report["deliveries"] = self._deliveries.query(
                where=[("report_id", "eq", report_id)],
                order_by="created_at", order="asc")
        return report

    def list_deliveries(self, limit: int = 100) -> list[dict]:
        return self._deliveries.query(order_by="created_at", order="desc",
                                      limit=limit)

    # ======================================================================
    # 定时扫描（由调度器 tick 调用）
    # ======================================================================
    def scan_due(self, at: Optional[float] = None) -> list[dict]:
        """扫描所有到点订阅，命中 cron 且本周期未处理则生成/发送报告。

        - 同一周期只处理一次（``last_period_key`` 去重，比按分钟去重更严格，
          即使调度器重启或漏 tick 也不会重复推送）；
        - 暂停的订阅直接跳过，且不动 last_period_key；
        - 自动模式到点即发；预览模式到点只生成草稿，等人确认后手动发送。
        """
        import datetime as _dt
        now_dt = _dt.datetime.fromtimestamp(at) if at else _dt.datetime.now()
        period = period_for(at or now_dt.timestamp())
        due_results = []
        with self._scan_lock:
            for sub in self._subs.all():
                if not sub.get("enabled", True):
                    continue
                if sub.get("last_period_key") == period["key"]:
                    continue
                try:
                    if not cron_matches(sub.get("cron", "0 9 * * 1"), now_dt):
                        continue
                except ValueError:
                    continue
                # 先占位，避免下一个 tick / 手动操作重复处理
                self._subs.update(sub["id"],
                                  {"last_period_key": period["key"]})
                try:
                    if sub.get("mode", "auto") == "manual":
                        report = self.preview_subscription(sub["id"], at=at)
                    else:
                        report = self.send_subscription(sub["id"], at=at)
                    due_results.append(report)
                except Exception as exc:  # noqa: BLE001
                    due_results.append({"error": str(exc),
                                        "subscription_id": sub["id"]})
        return due_results


# ---------------------------------------------------------------------------
# 文本渲染（邮件 / Webhook 正文，同时作为页面预览的纯文本版）
# ---------------------------------------------------------------------------

def _fmt_ts(ts: Optional[float]) -> str:
    if not ts:
        return "—"
    return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")


def render_report_text(content: dict) -> str:
    lines = [
        f"# {content['title']}",
        f"统计周期：{content['period_label']}（{content['window_days']} 天）",
        f"数据截止：{_fmt_ts(content['cutoff_at'])}",
        f"生成时间：{_fmt_ts(content['generated_at'])}",
        "",
        "## 总览",
        f"- 覆盖项目：{content['overview']['project_count']} 个",
        f"- 窗口内构建：{content['overview']['build_count']} 次",
        f"- 用例执行：{content['overview']['total_runs']} 次，"
        f"通过 {content['overview']['passed_runs']}，"
        f"失败 {content['overview']['failed_runs']}，"
        f"整体通过率 {content['overview']['pass_rate']}%",
    ]
    if "open_defects" in content["modules"]:
        lines.append(f"- 未关闭缺陷：{content['overview']['open_defect_count']} 个")
    if "coverage_change" in content["modules"]:
        delta = content["overview"]["avg_coverage_delta"]
        arrow = "上升" if delta > 0 else ("下降" if delta < 0 else "持平")
        lines.append(f"- 平均覆盖率较窗口初{arrow} {abs(delta)} 个百分点")

    for proj in content["projects"]:
        lines += ["", f"## 项目：{proj['project_name']}"]
        lines.append(
            f"- 构建 {proj['build_count']} 次 · 执行 {proj['total_runs']} · "
            f"通过率 {proj['pass_rate']}% · 平均耗时 {proj['avg_duration']}s")
        mods = proj["modules"]

        if "pass_rate_trend" in mods:
            sec = mods["pass_rate_trend"]
            arrow = "↑" if sec["delta"] > 0 else ("↓" if sec["delta"] < 0 else "→")
            lines.append(f"- 通过率趋势：{sec['first_rate']}% {arrow} "
                         f"{sec['last_rate']}%（{sec['delta']:+}）")
            for p in sec["points"]:
                lines.append(f"    · {p['name']}：{p['pass_rate']}%"
                             f"（{p['passed']}/{p['total']}）")

        if "top_failures" in mods:
            lines.append("- 失败 Top 用例：")
            items = mods["top_failures"]
            if not items:
                lines.append("    · 本周期无失败用例 🎉")
            for i, f in enumerate(items, 1):
                reason = f" — {f['last_reason']}" if f.get("last_reason") else ""
                lines.append(f"    {i}. [{f['priority']}] {f['case_name']} ×"
                             f"{f['fail_count']} 次{reason}")

        if "open_defects" in mods:
            sec = mods["open_defects"]
            dist = "，".join(f"{k}:{v}" for k, v in
                             sorted(sec["by_status"].items())) or "—"
            lines.append(f"- 未关闭缺陷：{sec['total']} 个（{dist}）")
            for d in sec["items"]:
                assignee = f" @{d['assignee']}" if d.get("assignee") else ""
                lines.append(f"    · [{d['severity']}/{d['status']}] "
                             f"{d['title']}{assignee}")

        if "coverage_change" in mods:
            sec = mods["coverage_change"]
            if sec["first_percent"] is None:
                lines.append("- 覆盖率变化：窗口内无构建数据")
            else:
                lines.append(f"- 覆盖率：{sec['first_percent']}% → "
                             f"{sec['last_percent']}%（{sec['delta']:+} 个百分点）")
    return "\n".join(lines)
