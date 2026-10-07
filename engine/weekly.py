"""质量周报（周期质量报告）订阅与生成。

背景
----
测试负责人每周一要手工把三四个项目的数据拼成一份质量周报，而各项目构建、
缺陷更新时间参差不齐，拼出来的周报前后段落数字常常对不上。本模块把这件事
自动化：

- 为项目配置**周报订阅**（接收人、频率、内容模块、合并/分别投递）；
- 到点由调度器扫描 cron，自动生成报告并推送；
- 支持先**预览**再**发送**，发送历史全程留痕；订阅可随时暂停。

口径一致（本模块最关键的设计）
------------------------------
同一份周报里「通过率趋势 / 失败 Top 用例 / 未关闭缺陷 / 覆盖率变化」必须
基于同一个时间点看到的数据，否则各段落之间会自相矛盾。做法是：

1. 生成报告时先确定一个**截取时间点** ``snapshot_at`` 与统计窗口
   ``[window_start, window_end)``；
2. :meth:`WeeklyReportManager._build_snapshot` 在一次调用里把每个项目的
   窗口内构建、用例结果、未关闭缺陷、覆盖率**一次性读取并冻结**进快照；
3. 之后所有内容模块（section）只从这个快照对象里取数，渲染（HTML /
   Markdown）与投递也只消费快照结果，绝不再回读底层数据。

底层数据在报告生成之后继续变化，也不会影响这份报告——预览、发送、留痕
看到的是同一份冻结快照。

接收人与投递模式
----------------
订阅通过一组通知集成 id（:class:`engine.notify.NotificationManager` 里的
webhook / slack / email / dingtalk）指定接收人：

- ``delivery = "merged"``：所有项目合并成**一份**报告，投递给每个接收人；
- ``delivery = "separate"``：同一批数据按项目拆成多份，每个项目单独投递给
  每个接收人。

两种模式共享同一份快照，只是渲染/投递时的分组方式不同。
"""

from __future__ import annotations

import datetime
import html
import time
from typing import Optional

from .cron import cron_matches
from .models import new_id

# 周报可包含的内容模块；"summary"（总览）恒定包含，其余可勾选
SECTIONS = ["pass_trend", "top_failures", "open_defects", "coverage_delta"]

SECTION_LABELS = {
    "summary": "质量总览",
    "pass_trend": "通过率趋势",
    "top_failures": "失败 Top 用例",
    "open_defects": "未关闭缺陷",
    "coverage_delta": "覆盖率变化",
}

# 频率预设：名称 -> (默认 cron, 人话语义)
FREQUENCY_PRESETS = {
    "daily": ("0 9 * * *", "每天 09:00"),
    "weekly_mon": ("0 9 * * 1", "每周一 09:00"),
    "biweekly_mon": ("0 9 * * 1,4", "每周一、周四 09:00"),
    "monthly": ("0 9 1 * *", "每月 1 日 09:00"),
}

WINDOW_DAYS = {
    "daily": 1,
    "weekly_mon": 7,
    "biweekly_mon": 3,
    "monthly": 30,
}

OPEN_DEFECT_STATUSES = ("open", "in_progress", "reopened")

DEFAULT_TOP_FAILURES = 10
DEFAULT_TREND_POINTS = 10


class WeeklyReportManager:
    """周报订阅管理 + 快照生成 + 渲染 + 发送留痕。"""

    def __init__(self, registry, build_registry, report_gen=None,
                 coverage_analyzer=None, notify_manager=None):
        self.registry = registry
        self.builds = build_registry
        self.report_gen = report_gen
        self.coverage = coverage_analyzer
        self.notify = notify_manager

    # ------------------------------------------------------------------ 订阅 CRUD
    def _subs(self):
        return self.registry.store("weekly_subscriptions")

    def create_subscription(self, payload: dict) -> dict:
        sub = self._validate(dict(payload), existing=None)
        sub.update({
            "id": new_id("wsub"),
            "enabled": bool(payload.get("enabled", True)),
            "last_fired_minute": None,
            "last_report_id": None,
            "created_at": time.time(),
        })
        self._subs().insert(sub)
        return self.get_subscription(sub["id"])

    def update_subscription(self, sub_id: str, patch: dict) -> Optional[dict]:
        existing = self._subs().get(sub_id)
        if existing is None:
            return None
        merged = self._validate({**existing, **patch}, existing=existing)
        # enabled / last_* 不由校验逻辑改写
        merged["enabled"] = bool(patch["enabled"]) if "enabled" in patch else existing.get("enabled", True)
        if "cron" in patch:
            merged["last_fired_minute"] = None  # 表达式变更后重置去重标记
        updated = self._subs().update(sub_id, merged)
        return updated

    def delete_subscription(self, sub_id: str) -> bool:
        return self._subs().delete(sub_id)

    def get_subscription(self, sub_id: str) -> Optional[dict]:
        sub = self._subs().get(sub_id)
        if sub:
            sub = dict(sub)
            sub["recipient_count"] = len(sub.get("recipient_ids") or [])
            sub["project_count"] = len(sub.get("project_ids") or [])
        return sub

    def list_subscriptions(self) -> list[dict]:
        out = []
        for sub in self._subs().all():
            out.append(self.get_subscription(sub["id"]))
        out.sort(key=lambda s: s.get("created_at", 0), reverse=True)
        return out

    def set_paused(self, sub_id: str, paused: bool) -> Optional[dict]:
        if self._subs().get(sub_id) is None:
            return None
        return self._subs().update(sub_id, {"enabled": not paused})

    def _validate(self, data: dict, existing: Optional[dict]) -> dict:
        name = (data.get("name") or "").strip() or "质量周报"

        # 频率：预设优先，也允许自定义 cron
        freq = data.get("frequency") or (existing or {}).get("frequency") or "weekly_mon"
        cron = (data.get("cron") or "").strip()
        if not cron:
            cron, _ = FREQUENCY_PRESETS.get(freq, FREQUENCY_PRESETS["weekly_mon"])
        from .cron import parse_cron
        parse_cron(cron)  # 校验失败会抛 ValueError，由路由层转成 400

        project_ids = data.get("project_ids")
        if project_ids is None:
            project_ids = (existing or {}).get("project_ids")
        project_ids = list(project_ids or [])
        projects_store = self.registry.store("projects")
        project_ids = [pid for pid in project_ids if projects_store.get(pid)]
        if not project_ids:
            raise ValueError("请至少选择一个项目")

        recipient_ids = data.get("recipient_ids")
        if recipient_ids is None:
            recipient_ids = (existing or {}).get("recipient_ids")
        recipient_ids = list(recipient_ids or [])
        # 过滤掉已删除的集成
        if self.notify is not None:
            recipient_ids = [rid for rid in recipient_ids if self.notify.get(rid)]
        if not recipient_ids:
            raise ValueError("请至少选择一个接收人（通知集成）")

        sections = data.get("sections")
        if sections is None:
            sections = (existing or {}).get("sections")
        sections = [s for s in (sections or SECTIONS) if s in SECTIONS]

        delivery = data.get("delivery") or (existing or {}).get("delivery") or "merged"
        if delivery not in ("merged", "separate"):
            delivery = "merged"

        return {
            "name": name,
            "frequency": freq,
            "cron": cron,
            "project_ids": project_ids,
            "recipient_ids": recipient_ids,
            "sections": sections,
            "delivery": delivery,
            "top_n": max(1, min(50, int(data.get("top_n") or DEFAULT_TOP_FAILURES))),
            "trend_points": max(2, min(50, int(data.get("trend_points") or DEFAULT_TREND_POINTS))),
            "window_days": max(1, int(data.get("window_days") or WINDOW_DAYS.get(freq, 7))),
        }

    # ------------------------------------------------------------------ 快照
    def _project_name(self, pid: str) -> str:
        p = self.registry.store("projects").get(pid)
        return p.get("name", pid) if p else pid

    def _window(self, window_days: int, at: Optional[float] = None):
        """根据截取时刻确定统计窗口 ``[start, end)``。

        结束点就是截取时刻 ``snapshot_at``，开始点往前推 ``window_days``
        天：窗口即「截取点之前的过去 N 天」。``snapshot_at`` 会冻结进报告，
        所以同一份报告无论预览多少次、分几个段落渲染，看到的都是同一个
        固定区间；重新生成（新的截取点）才会得到新窗口。
        """
        end = at if at is not None else time.time()
        start = end - window_days * 86400
        return start, end

    def _build_snapshot(self, project_ids: list[str], sections: list[str],
                        trend_points: int, at: Optional[float] = None,
                        window_days: int = 7) -> dict:
        """在一个固定时间点冻结所有项目的报告数据。

        这是「口径一致」的唯一入口：报告的每个模块都只能消费这里冻结下来
        的数据。``snapshot_at`` 是所有窗口过滤共用的时间基准。
        """
        snapshot_at = at if at is not None else time.time()
        window_start, window_end = self._window(window_days, snapshot_at)

        projects = []
        defects_store = self.registry.store("defects")
        for pid in project_ids:
            store = self.builds.for_project(pid)
            # 该项目当前全部构建（已按 created_at 倒序）
            all_builds = store.list_builds()

            # 窗口内构建：created_at 落在 [start, end)
            window_builds = [b for b in all_builds
                             if window_start <= b.get("created_at", 0) < window_end]
            window_builds_asc = list(reversed(window_builds))  # 时间正序

            entry = {
                "project_id": pid,
                "project_name": self._project_name(pid),
                "builds": [],               # 窗口内构建（时间正序）
                "total_cases": 0,
                "total_passed": 0,
                "pass_rate": 0.0,
                "trend": [],                # 最近 N 场构建通过率点
                "top_failures": [],         # 窗口内失败用例聚合
                "open_defects": [],         # 截至截取点未关闭缺陷
                "coverage": None,           # 窗口末覆盖率
                "coverage_prev": None,      # 窗口前最近一场覆盖率
                "coverage_delta": None,
            }

            # ---- 通过率总览 + 趋势 ----
            total_cases = total_passed = 0
            for b in window_builds_asc:
                finished = b.get("total", 0) - b.get("skipped", 0)
                rate = round(b.get("passed", 0) / finished * 100, 1) if finished else 0.0
                entry["builds"].append({
                    "build_id": b["id"],
                    "name": b.get("name") or b["id"],
                    "status": b.get("status"),
                    "total": b.get("total", 0),
                    "passed": b.get("passed", 0),
                    "failed": b.get("failed", 0) + b.get("error", 0) + b.get("timeout", 0),
                    "pass_rate": rate,
                    "duration": b.get("duration", 0.0),
                    "finished_at": b.get("finished_at"),
                })
                total_cases += finished
                total_passed += b.get("passed", 0)
            entry["total_cases"] = total_cases
            entry["total_passed"] = total_passed
            entry["pass_rate"] = round(total_passed / total_cases * 100, 1) if total_cases else 0.0

            # 趋势取「截至截取点最近 N 场」（含窗口外的最近构建也可作为参照）
            recent = []
            for b in all_builds[:trend_points]:
                if b.get("created_at", 0) <= snapshot_at:
                    finished = b.get("total", 0) - b.get("skipped", 0)
                    recent.append({
                        "build_id": b["id"],
                        "name": b.get("name") or b["id"],
                        "status": b.get("status"),
                        "pass_rate": round(b.get("passed", 0) / finished * 100, 1) if finished else 0.0,
                        "finished_at": b.get("finished_at"),
                    })
            entry["trend"] = list(reversed(recent))  # 时间正序画图

            # ---- 失败 Top 用例：扫窗口内构建的结果并按用例聚合 ----
            if "top_failures" in sections:
                entry["top_failures"] = self._aggregate_failures(store, window_builds_asc)

            # ---- 未关闭缺陷：只读一次，冻结列表 ----
            if "open_defects" in sections:
                defects = defects_store.query(where=[("project_id", "eq", pid)])
                open_defs = [d for d in defects
                             if d.get("status") in OPEN_DEFECT_STATUSES
                             and d.get("created_at", 0) <= snapshot_at]
                sev_rank = {s: i for i, s in
                            enumerate(("blocker", "critical", "major", "minor", "trivial"))}
                open_defs.sort(key=lambda d: (sev_rank.get(d.get("severity"), 9),
                                              d.get("created_at", 0)))
                entry["open_defects"] = [{
                    "defect_id": d["id"],
                    "title": d.get("title", "未命名缺陷"),
                    "severity": d.get("severity", "major"),
                    "status": d.get("status", "open"),
                    "assignee": d.get("assignee", ""),
                    "created_at": d.get("created_at"),
                } for d in open_defs]

            # ---- 覆盖率变化：窗口末最近一场 vs 窗口前最近一场 ----
            if "coverage_delta" in sections and self.coverage is not None:
                entry.update(self._coverage_change(pid, all_builds, window_start, snapshot_at))

            projects.append(entry)

        return {
            "snapshot_at": snapshot_at,
            "window_start": window_start,
            "window_end": window_end,
            "sections": sections,
            "projects": projects,
        }

    def _aggregate_failures(self, store, window_builds_asc: list[dict]) -> list[dict]:
        """聚合窗口内各场构建的失败用例，按用例合并、按失败次数排序。"""
        bucket: dict[str, dict] = {}
        for b in window_builds_asc:
            results = store.results(
                b["id"],
                where=[("status", "in", ["failed", "error", "timeout"])],
            )
            for r in results:
                key = r.get("case_id") or r.get("case_name") or "unknown"
                item = bucket.get(key)
                if item is None:
                    reason = ""
                    for a in r.get("assertions", []):
                        if not a.get("ok"):
                            reason = a.get("message", "")
                            break
                    if not reason:
                        for s in r.get("steps", []):
                            if s.get("status") in ("failed", "error"):
                                reason = s.get("message", "")
                                break
                    bucket[key] = {
                        "case_id": r.get("case_id"),
                        "case_name": r.get("case_name", key),
                        "group": r.get("group", "默认"),
                        "priority": r.get("priority", "P3"),
                        "fail_count": 1,
                        "builds": [b["id"]],
                        "last_status": r.get("status"),
                        "last_reason": reason or r.get("message", ""),
                    }
                else:
                    item["fail_count"] += 1
                    if b["id"] not in item["builds"]:
                        item["builds"].append(b["id"])
                    item["last_status"] = r.get("status", item["last_status"])
        out = list(bucket.values())
        out.sort(key=lambda x: (-x["fail_count"], x.get("priority", "P9")))
        return out

    def _coverage_change(self, project_id: str, all_builds_desc: list[dict],
                         window_start: float, snapshot_at: float) -> dict:
        """窗口末覆盖率（curr）对比窗口前最近一场（prev）。"""
        # all_builds_desc 已按 created_at 倒序
        curr_build = None
        prev_build = None
        for b in all_builds_desc:
            created = b.get("created_at", 0)
            if created > snapshot_at:
                continue
            if curr_build is None and created >= window_start:
                curr_build = b
            if created < window_start:
                prev_build = b
                break
        out = {"coverage": None, "coverage_prev": None, "coverage_delta": None}
        if curr_build is None:
            return out

        def _pct(build):
            if build is None:
                return None
            cov = self.coverage.get(project_id, build["id"])
            return {"build_id": build["id"], "name": build.get("name") or build["id"],
                    "percent": cov.get("percent", 0.0),
                    "covered_lines": cov.get("covered_lines", 0),
                    "total_lines": cov.get("total_lines", 0)}

        curr = _pct(curr_build)
        prev = _pct(prev_build)
        out["coverage"] = curr
        out["coverage_prev"] = prev
        if curr and prev:
            out["coverage_delta"] = round(curr["percent"] - prev["percent"], 1)
        return out

    # ------------------------------------------------------------------ 报告组装
    def generate_report(self, subscription: dict, *, persist: bool = False,
                        at: Optional[float] = None) -> dict:
        """按订阅生成一份报告。``persist=False`` 用于预览，不落库。"""
        sections = ["summary"] + [s for s in subscription.get("sections", []) if s in SECTIONS]
        snapshot = self._build_snapshot(
            subscription.get("project_ids") or [],
            sections,
            subscription.get("trend_points", DEFAULT_TREND_POINTS),
            at=at,
            window_days=subscription.get("window_days", 7),
        )

        # 合并视角的总体总览（同一份快照上汇总，绝不二读）
        agg = self._aggregate_snapshot(snapshot)

        report = {
            "id": new_id("wrpt"),
            "subscription_id": subscription["id"],
            "subscription_name": subscription.get("name"),
            "delivery": subscription.get("delivery", "merged"),
            "sections": sections,
            "snapshot": snapshot,
            "aggregate": agg,
            "top_n": subscription.get("top_n", DEFAULT_TOP_FAILURES),
            "html": self.render_html(subscription, snapshot, agg, scope="all"),
            "markdown": self.render_markdown(subscription, snapshot, agg, scope="all"),
            "status": "draft",
            "created_at": time.time(),
        }

        if persist:
            self.registry.store("weekly_reports").insert(report)
        return report

    @staticmethod
    def _aggregate_snapshot(snapshot: dict) -> dict:
        builds = cases = passed = 0
        open_defects = sum(len(p.get("open_defects") or []) for p in snapshot["projects"])
        rates = []
        for p in snapshot["projects"]:
            builds += len(p.get("builds") or [])
            cases += p.get("total_cases", 0)
            passed += p.get("total_passed", 0)
            if p.get("total_cases"):
                rates.append(p.get("pass_rate", 0.0))
        return {
            "project_count": len(snapshot["projects"]),
            "build_count": builds,
            "total_cases": cases,
            "total_passed": passed,
            "pass_rate": round(passed / cases * 100, 1) if cases else 0.0,
            "open_defects": open_defects,
        }

    # ------------------------------------------------------------------ 渲染
    def _fmt_ts(self, ts: Optional[float]) -> str:
        if not ts:
            return "—"
        return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")

    def _fmt_date(self, ts: float) -> str:
        return datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d")

    def render_markdown(self, subscription: dict, snapshot: dict, agg: dict,
                        scope: str = "all", project_id: Optional[str] = None) -> str:
        """渲染 Markdown 文本（邮件 / IM 投递用）。scope=all 为合并版。"""
        projects = snapshot["projects"]
        if scope == "project":
            projects = [p for p in projects if p["project_id"] == project_id]

        lines = [f"# {subscription.get('name', '质量周报')}",
                 "",
                 f"统计周期：{self._fmt_date(snapshot['window_start'])} ~ "
                 f"{self._fmt_date(snapshot['window_end'])}（数据截取于 "
                 f"{self._fmt_ts(snapshot['snapshot_at'])}）"]

        for p in projects:
            lines += ["", f"## {p['project_name']}", ""]
            lines.append(f"- 窗口内构建：{len(p.get('builds') or [])} 场")
            lines.append(f"- 用例通过率：{p.get('pass_rate', 0.0)}%"
                         f"（{p.get('total_passed', 0)}/{p.get('total_cases', 0)}）")
            lines.append(f"- 未关闭缺陷：{len(p.get('open_defects') or [])} 个")
            cov = p.get("coverage")
            if cov:
                delta = p.get("coverage_delta")
                delta_txt = f"（较上周 {('+' if (delta or 0) >= 0 else '')}{delta}%）" if delta is not None else ""
                lines.append(f"- 覆盖率：{cov['percent']}%{delta_txt}")

            if "pass_trend" in snapshot.get("sections", subscription.get("sections", [])) and p.get("trend"):
                lines += ["", "**通过率趋势**", ""]
                for t in p["trend"]:
                    lines.append(f"- {self._fmt_date(t.get('finished_at') or 0)} "
                                 f"{t['name']}: {t['pass_rate']}% ({t['status']})")

            if "top_failures" in snapshot.get("sections", subscription.get("sections", [])) and p.get("top_failures"):
                lines += ["", "**失败 Top 用例**", ""]
                for i, f in enumerate(p["top_failures"][:subscription.get("top_n", DEFAULT_TOP_FAILURES)], 1):
                    lines.append(f"{i}. [{f['priority']}] {f['case_name']} — 失败 {f['fail_count']} 次"
                                 f"{('；' + f['last_reason']) if f.get('last_reason') else ''}")

            if "open_defects" in snapshot.get("sections", subscription.get("sections", [])) and p.get("open_defects"):
                lines += ["", "**未关闭缺陷**", ""]
                for d in p["open_defects"]:
                    who = f" @{d['assignee']}" if d.get("assignee") else ""
                    lines.append(f"- [{d['severity']}/{d['status']}] {d['title']}{who}")

            if "coverage_delta" in snapshot.get("sections", subscription.get("sections", [])) and cov:
                prev = p.get("coverage_prev")
                if prev:
                    lines += ["", f"覆盖率：{prev['percent']}% → {cov['percent']}%"]
        return "\n".join(lines)

    def render_html(self, subscription: dict, snapshot: dict, agg: dict,
                    scope: str = "all", project_id: Optional[str] = None) -> str:
        """渲染自包含 HTML（预览页直接 iframe / innerHTML 展示）。"""
        projects = snapshot["projects"]
        if scope == "project":
            projects = [p for p in projects if p["project_id"] == project_id]
        sections = snapshot.get("sections", subscription.get("sections", []))
        e = html.escape
        top_n = subscription.get("top_n", DEFAULT_TOP_FAILURES)

        parts = [
            "<!DOCTYPE html><html lang='zh-CN'><head><meta charset='utf-8'>",
            "<style>body{font-family:-apple-system,'PingFang SC','Microsoft YaHei',sans-serif;"
            "color:#1f2430;margin:24px;line-height:1.6;font-size:14px}"
            "h1{font-size:22px;margin:0 0 4px}h2{font-size:17px;margin:24px 0 8px;"
            "padding-bottom:6px;border-bottom:2px solid #e5e7ee}"
            ".meta{color:#6b7280;font-size:13px;margin-bottom:8px}"
            ".cards{display:flex;gap:12px;flex-wrap:wrap;margin:12px 0}"
            ".card{background:#f8f9fc;border:1px solid #e5e7ee;border-radius:10px;padding:12px 18px;min-width:120px}"
            ".card .v{font-size:24px;font-weight:700}.card .k{color:#6b7280;font-size:12px}"
            "table{border-collapse:collapse;width:100%;margin:8px 0;font-size:13px}"
            "th,td{border:1px solid #e5e7ee;padding:6px 10px;text-align:left}"
            "th{background:#f8f9fc}.up{color:#16a34a}.down{color:#dc2626}"
            ".tag{display:inline-block;padding:1px 8px;border-radius:10px;background:#eef1ff;"
            "color:#3d5ce0;font-size:12px;margin-right:4px}"
            ".sev-blocker,.sev-critical{color:#dc2626;font-weight:700}"
            ".sev-major{color:#d97706}.bars{display:flex;align-items:flex-end;gap:6px;height:90px;margin:8px 0}"
            ".barwrap{flex:1;display:flex;flex-direction:column;justify-content:flex-end;text-align:center}"
            ".bar{background:#4f6ef7;border-radius:4px 4px 0 0;min-height:2px}"
            ".barlabel{font-size:11px;color:#6b7280;margin-top:4px;white-space:nowrap;"
            "overflow:hidden;text-overflow:ellipsis}</style></head><body>",
            f"<h1>{e(subscription.get('name', '质量周报'))}</h1>",
            f"<div class='meta'>统计周期 {self._fmt_date(snapshot['window_start'])} ~ "
            f"{self._fmt_date(snapshot['window_end'])} · 数据截取于 "
            f"{self._fmt_ts(snapshot['snapshot_at'])} · 投递模式："
            f"{'合并一份' if subscription.get('delivery') == 'merged' else '按项目分别'}</div>",
        ]

        for p in projects:
            parts.append(f"<h2>{e(p['project_name'])}</h2>")
            open_n = len(p.get("open_defects") or [])
            cov = p.get("coverage")
            cov_card = ""
            if cov:
                delta = p.get("coverage_delta")
                if delta is not None:
                    cls = "up" if delta >= 0 else "down"
                    arrow = "▲" if delta >= 0 else "▼"
                    cov_card = f"<div class='card'><div class='v'>{cov['percent']}%</div>" \
                               f"<div class='k'>覆盖率 <span class='{cls}'>{arrow} {abs(delta)}%</span></div></div>"
                else:
                    cov_card = f"<div class='card'><div class='v'>{cov['percent']}%</div><div class='k'>覆盖率</div></div>"
            parts.append(
                "<div class='cards'>"
                f"<div class='card'><div class='v'>{len(p.get('builds') or [])}</div><div class='k'>窗口内构建</div></div>"
                f"<div class='card'><div class='v'>{p.get('pass_rate', 0.0)}%</div>"
                f"<div class='k'>通过率 {p.get('total_passed', 0)}/{p.get('total_cases', 0)}</div></div>"
                f"<div class='card'><div class='v'>{open_n}</div><div class='k'>未关闭缺陷</div></div>"
                f"{cov_card}</div>"
            )

            if "pass_trend" in sections and p.get("trend"):
                parts.append("<div style='margin-top:10px'><strong>通过率趋势</strong></div><div class='bars'>")
                for t in p["trend"]:
                    h = max(2, int(t["pass_rate"] * 0.8))
                    color = "#16a34a" if t["status"] == "passed" else "#dc2626"
                    label = self._fmt_date(t.get("finished_at") or 0)[5:]
                    parts.append(
                        "<div class='barwrap' title='" + e(f"{t['name']} {t['pass_rate']}%") + "'>"
                        f"<div style='font-size:10px'>{t['pass_rate']}%</div>"
                        f"<div class='bar' style='height:{h}px;background:{color}'></div>"
                        f"<div class='barlabel'>{e(label)}</div></div>")
                parts.append("</div>")

            if "top_failures" in sections and p.get("top_failures"):
                parts.append("<strong>失败 Top 用例</strong><table><tr><th>#</th><th>用例</th>"
                             "<th>优先级</th><th>失败次数</th><th>最近原因</th></tr>")
                for i, f in enumerate(p["top_failures"][:top_n], 1):
                    parts.append(
                        "<tr>"
                        f"<td>{i}</td><td>{e(f['case_name'])}</td>"
                        f"<td><span class='tag'>{e(f['priority'])}</span>{e(f['group'])}</td>"
                        f"<td>{f['fail_count']}</td>"
                        f"<td>{e(f.get('last_reason', ''))}</td></tr>")
                parts.append("</table>")

            if "open_defects" in sections and p.get("open_defects"):
                parts.append("<strong>未关闭缺陷</strong><table><tr><th>严重级</th><th>状态</th>"
                             "<th>标题</th><th>负责人</th></tr>")
                for d in p["open_defects"]:
                    parts.append(
                        "<tr>"
                        f"<td class='sev-{e(d['severity'])}'>{e(d['severity'])}</td>"
                        f"<td>{e(d['status'])}</td><td>{e(d['title'])}</td>"
                        f"<td>{e(d.get('assignee', ''))}</td></tr>")
                parts.append("</table>")

            if "coverage_delta" in sections and cov and p.get("coverage_prev"):
                prev = p["coverage_prev"]
                delta = p.get("coverage_delta") or 0
                cls = "up" if delta >= 0 else "down"
                parts.append(
                    "<strong>覆盖率变化</strong><table><tr><th>窗口前</th><th>窗口末</th><th>变化</th></tr>"
                    f"<tr><td>{prev['percent']}%（{e(prev['name'])}）</td>"
                    f"<td>{cov['percent']}%（{e(cov['name'])}）</td>"
                    f"<td class='{cls}'>{'+' if delta >= 0 else ''}{delta}%</td></tr></table>")

        parts.append("</body></html>")
        return "".join(parts)

    # ------------------------------------------------------------------ 发送与留痕
    def preview(self, sub_id: str, at: Optional[float] = None) -> Optional[dict]:
        sub = self._subs().get(sub_id)
        if sub is None:
            return None
        return self.generate_report(sub, persist=False, at=at)

    def send(self, sub_id: str, *, trigger: str = "manual",
             at: Optional[float] = None) -> Optional[dict]:
        """生成并发送一份周报。

        - 先生成并持久化报告（快照冻结、状态 draft）；
        - 按订阅的投递模式对每个接收人投递（merged 一份 / separate 每项目一份）；
        - 每次投递写一条 ``notify_events``，并把投递结果回写到报告的
          ``deliveries``，状态置为 sent / partial / failed；
        - 订阅上记录 ``last_report_id``，便于列表页展示最近一次发送。
        """
        sub = self._subs().get(sub_id)
        if sub is None:
            return None
        report = self.generate_report(sub, persist=True, at=at)

        deliveries = self._deliver_report(sub, report, trigger=trigger)
        report["deliveries"] = deliveries
        statuses = {d["status"] for d in deliveries}
        if not deliveries:
            report["status"] = "failed"
        elif statuses == {"delivered"}:
            report["status"] = "sent"
        elif "delivered" in statuses:
            report["status"] = "partial"
        else:
            report["status"] = "failed"
        report["sent_at"] = time.time()
        self.registry.store("weekly_reports").update(report["id"], {
            "deliveries": deliveries, "status": report["status"],
            "sent_at": report["sent_at"],
            # 已发送的报告不再保留大体积 html 以外的内容也无妨；markdown 按
            # 投递拆分成多份时单独保存在 deliveries 里
        })
        self._subs().update(sub_id, {"last_report_id": report["id"]})
        report["subscription"] = self.get_subscription(sub_id)
        return report

    def _deliver_report(self, sub: dict, report: dict, trigger: str) -> list[dict]:
        """按投递模式把报告发给所有接收人，返回每次投递的留痕记录。"""
        snapshot = report["snapshot"]
        recipient_ids = sub.get("recipient_ids") or []
        jobs: list[tuple[str, str, str]] = []  # (project_id or "all", title, markdown)
        if sub.get("delivery", "merged") == "merged":
            jobs.append(("all", sub.get("name", "质量周报"), report.get("markdown", "")))
        else:
            for p in snapshot["projects"]:
                md = self.render_markdown(sub, snapshot, report["aggregate"],
                                          scope="project", project_id=p["project_id"])
                jobs.append((p["project_id"], f"{sub.get('name', '质量周报')} · {p['project_name']}", md))

        deliveries = []
        for project_scope, title, markdown in jobs:
            for rid in recipient_ids:
                record = self._deliver_one(
                    sub, rid, report, project_scope=project_scope,
                    title=title, markdown=markdown, trigger=trigger)
                deliveries.append(record)
        return deliveries

    def _deliver_one(self, sub: dict, integration_id: str, report: dict, *,
                     project_scope: str, title: str, markdown: str,
                     trigger: str) -> dict:
        """通过通知集成投递一份报告内容，并写一条 notify_events 留痕。"""
        payload = {
            "kind": "weekly_report",
            "report_id": report["id"],
            "subscription_id": sub["id"],
            "project_scope": project_scope,
            "title": title,
            "snapshot_at": report["snapshot"]["snapshot_at"],
            "window_start": report["snapshot"]["window_start"],
            "window_end": report["snapshot"]["window_end"],
            "text": markdown,
        }
        if self.notify is None:
            return {"integration_id": integration_id, "recipient": "未配置通知通道",
                    "status": "failed", "latency_ms": 0, "project_scope": project_scope}

        integration = self.notify.get(integration_id)
        if integration is None:
            return {"integration_id": integration_id, "recipient": "集成已删除",
                    "status": "failed", "latency_ms": 0, "project_scope": project_scope}

        # 周报是平台级汇总（可跨项目），事件挂在第一个项目下，保证通知历史页可见
        project_id = (sub.get("project_ids") or [None])[0]
        record = self.notify.deliver_report(integration, payload, project_id=project_id,
                                            trigger=trigger)
        return {
            "event_id": record.get("id"),
            "integration_id": integration_id,
            "integration_name": record.get("integration_name"),
            "type": record.get("type"),
            "recipient": record.get("recipient"),
            "status": record.get("status"),
            "latency_ms": record.get("latency_ms"),
            "project_scope": project_scope,
            "title": title,
            "created_at": record.get("created_at"),
        }

    def list_reports(self, sub_id: Optional[str] = None, limit: int = 50) -> list[dict]:
        where = [("subscription_id", "eq", sub_id)] if sub_id else None
        reports = self.registry.store("weekly_reports").query(
            where=where, order_by="created_at", order="desc", limit=limit)
        # 列表不回传大体积快照 / html，避免页面过重
        slim = []
        for r in reports:
            slim.append({
                "id": r["id"],
                "subscription_id": r.get("subscription_id"),
                "subscription_name": r.get("subscription_name"),
                "delivery": r.get("delivery"),
                "status": r.get("status"),
                "snapshot_at": (r.get("snapshot") or {}).get("snapshot_at"),
                "window_start": (r.get("snapshot") or {}).get("window_start"),
                "window_end": (r.get("snapshot") or {}).get("window_end"),
                "aggregate": r.get("aggregate"),
                "delivery_count": len(r.get("deliveries") or []),
                "deliveries": r.get("deliveries") or [],
                "created_at": r.get("created_at"),
                "sent_at": r.get("sent_at"),
            })
        return slim

    def get_report(self, report_id: str) -> Optional[dict]:
        return self.registry.store("weekly_reports").get(report_id)

    # ------------------------------------------------------------------ 定时扫描
    def due_subscriptions(self, now_dt: Optional[datetime.datetime] = None,
                          minute_key: Optional[str] = None) -> list[dict]:
        """返回当前时刻到点、启用且本分钟尚未发送的订阅。"""
        now_dt = now_dt or datetime.datetime.now()
        minute_key = minute_key or now_dt.strftime("%Y-%m-%d %H:%M")
        due = []
        for sub in self._subs().all():
            if not sub.get("enabled", True):
                continue
            if sub.get("last_fired_minute") == minute_key:
                continue
            try:
                if cron_matches(sub.get("cron", ""), now_dt):
                    due.append(sub)
            except ValueError:
                continue
        return due

    def scan_and_send(self, now_dt: Optional[datetime.datetime] = None) -> list[dict]:
        """扫描到点订阅并发送。串行执行，配合调度器的分钟去重。

        返回本次实际发送的报告列表。先统一标记本分钟已触发，再逐个生成
        发送——生成耗时不应跨过分钟边界后被重复触发。
        """
        now_dt = now_dt or datetime.datetime.now()
        minute_key = now_dt.strftime("%Y-%m-%d %H:%M")
        due = self.due_subscriptions(now_dt, minute_key)
        sent = []
        for sub in due:
            # 先占位去重标记，避免发送过程中下一次扫描重复发
            self._subs().update(sub["id"], {"last_fired_minute": minute_key})
            try:
                report = self.send(sub["id"], trigger="schedule")
                if report:
                    sent.append(report)
            except Exception:  # noqa: BLE001
                # 单个订阅失败不影响其它订阅；标记保留以免本分钟反复重试
                continue
        return sent
