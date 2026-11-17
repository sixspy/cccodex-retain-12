"""影子试算（隔离方式：影子只读）。

影子试算只读正式分区做快照，扫描→裁决→执行全链只写 shadow_* 影子轨表；
正式轨表由连接级守卫拦截，串轨写即拒；与正式轨写路径共用 STORAGE_WRITE_LOCK，
提交在锁内，两侧争用串行。
"""

import hashlib
import sqlite3
from datetime import date, datetime

from app.services.locks import STORAGE_WRITE_LOCK
from app.services.policies import get_active_policy

# 正式轨表：影子试算对它们只读，绝不写。
PROD_TABLES = frozenset(
    {
        "partitions",
        "judgments",
        "execute_runs",
        "scan_jobs",
        "reconcile_reports",
        "retain_policies",
    }
)

_WRITE_ACTIONS = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}


def guard_shadow_track(conn: sqlite3.Connection) -> None:
    """给连接挂上影子轨守卫：任何对正式轨表的写操作一律拒绝（串轨即拦）。"""

    def _authorizer(action, arg1, _arg2, _db, _trigger):
        if action in _WRITE_ACTIONS and arg1 in PROD_TABLES:
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    conn.set_authorizer(_authorizer)


def _parse_day(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _prod_checksum(conn: sqlite3.Connection) -> str:
    """正式分区队列指纹：试算前后各算一次，比对即知正式轨有没有被碰。"""
    rows = conn.execute(
        "SELECT id, storage_state, row_count, IFNULL(archive_uri, '-') "
        "FROM partitions ORDER BY id"
    ).fetchall()
    h = hashlib.sha256()
    for r in rows:
        h.update(f"{r[0]}|{r[1]}|{r[2]}|{r[3]}\n".encode("utf-8"))
    return h.hexdigest()


def run_shadow_trial(conn: sqlite3.Connection, as_of_date: str) -> dict:
    """影子试算：快照正式分区 → 影子扫描裁决 → 影子执行 → 影子报告落库。"""
    as_of = _parse_day(as_of_date)
    with STORAGE_WRITE_LOCK:
        before = _prod_checksum(conn)
        guard_shadow_track(conn)
        try:
            cur = conn.execute(
                "INSERT INTO shadow_scan_jobs (as_of_date, status, message) VALUES (?,?,?)",
                (as_of_date, "进行中", ""),
            )
            job_id = int(cur.lastrowid)

            # 1) 快照正式分区队列到影子轨（只读正式轨）
            parts = conn.execute(
                "SELECT id, domain_code, biz_date, row_count, storage_state, archive_uri "
                "FROM partitions ORDER BY id"
            ).fetchall()
            for p in parts:
                conn.execute(
                    "INSERT INTO shadow_partitions "
                    "(shadow_job_id, partition_id, domain_code, biz_date, row_count, "
                    "storage_state, archive_uri) VALUES (?,?,?,?,?,?,?)",
                    (
                        job_id,
                        p["id"],
                        p["domain_code"],
                        p["biz_date"],
                        p["row_count"],
                        p["storage_state"],
                        p["archive_uri"],
                    ),
                )

            # 2) 影子扫描 + 3) 影子执行：只改影子投影与影子裁决
            expired = 0
            for p in parts:
                if p["storage_state"] != "在线":
                    continue
                policy = get_active_policy(conn, p["domain_code"])
                if not policy:
                    continue
                age = (as_of - _parse_day(p["biz_date"])).days
                if age < int(policy["keep_days"]):
                    continue
                jcur = conn.execute(
                    "INSERT INTO shadow_judgments "
                    "(shadow_job_id, partition_id, domain_code, biz_date, policy_version, "
                    "keep_days, decided_action, status) VALUES (?,?,?,?,?,?,?, '待执行')",
                    (
                        job_id,
                        p["id"],
                        p["domain_code"],
                        p["biz_date"],
                        policy["version"],
                        policy["keep_days"],
                        policy["action_on_expire"],
                    ),
                )
                jid = int(jcur.lastrowid)
                action = policy["action_on_expire"]
                if action == "归档":
                    uri = f"archive://{p['domain_code']}/{p['biz_date']}"
                    conn.execute(
                        "UPDATE shadow_partitions SET storage_state = '已归档', "
                        "archive_uri = ? WHERE shadow_job_id = ? AND partition_id = ?",
                        (uri, job_id, p["id"]),
                    )
                    result_state, archive_uri = "已归档", uri
                    detail = "影子试算：分区将归档并保留可查归档地址（正式轨未动）"
                else:
                    conn.execute(
                        "UPDATE shadow_partitions SET storage_state = '已销毁', "
                        "archive_uri = NULL, row_count = 0 "
                        "WHERE shadow_job_id = ? AND partition_id = ?",
                        (job_id, p["id"]),
                    )
                    result_state, archive_uri = "已销毁", None
                    detail = "影子试算：分区将销毁、在线行数清零（正式轨未动）"
                conn.execute(
                    "UPDATE shadow_judgments SET status = '已完成' WHERE id = ?", (jid,)
                )
                conn.execute(
                    "INSERT INTO shadow_execute_runs "
                    "(shadow_judgment_id, action, result_state, archive_uri, ok, detail) "
                    "VALUES (?,?,?,?,1,?)",
                    (jid, action, result_state, archive_uri, detail),
                )
                expired += 1

            conn.execute(
                "UPDATE shadow_scan_jobs SET status = ?, expired_count = ?, message = ? "
                "WHERE id = ?",
                ("已完成", expired, f"影子试算生成裁决 {expired} 条，正式轨未动", job_id),
            )

            # 4) 影子报告：投影统计 + 正式轨指纹核验，落本机库可复核
            counts = {
                r["storage_state"]: r["c"]
                for r in conn.execute(
                    "SELECT storage_state, COUNT(*) AS c FROM shadow_partitions "
                    "WHERE shadow_job_id = ? GROUP BY storage_state",
                    (job_id,),
                ).fetchall()
            }
            online = int(counts.get("在线", 0))
            archived = int(counts.get("已归档", 0))
            destroyed = int(counts.get("已销毁", 0))
            prod_unchanged = 1 if _prod_checksum(conn) == before else 0
            summary = (
                f"影子投影：在线{online} / 已归档{archived} / 已销毁{destroyed} / "
                f"裁决{expired} / 正式轨{'未动' if prod_unchanged else '遭串轨改动'}"
            )
            conn.execute(
                "INSERT INTO shadow_reports "
                "(shadow_job_id, as_of_date, online_count, archived_count, destroyed_count, "
                "judged_count, prod_unchanged, summary) VALUES (?,?,?,?,?,?,?,?)",
                (
                    job_id,
                    as_of_date,
                    online,
                    archived,
                    destroyed,
                    expired,
                    prod_unchanged,
                    summary,
                ),
            )
            conn.commit()  # 提交在锁内：锁释放前结果已落库，另一侧才能进入
        finally:
            conn.set_authorizer(None)

    return get_shadow_run(conn, job_id)


def list_shadow_runs(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT j.id, j.as_of_date, j.status, j.expired_count, j.message, j.created_at, "
        "r.online_count, r.archived_count, r.destroyed_count, r.judged_count, "
        "r.prod_unchanged, r.summary "
        "FROM shadow_scan_jobs j "
        "LEFT JOIN shadow_reports r ON r.shadow_job_id = j.id "
        "ORDER BY j.id DESC"
    ).fetchall()
    return [{"track": "影子", **dict(r)} for r in rows]


def get_shadow_run(conn: sqlite3.Connection, shadow_job_id: int) -> dict | None:
    job_row = conn.execute(
        "SELECT id, as_of_date, status, expired_count, message, created_at "
        "FROM shadow_scan_jobs WHERE id = ?",
        (shadow_job_id,),
    ).fetchone()
    if not job_row:
        return None
    report_row = conn.execute(
        "SELECT id, shadow_job_id, as_of_date, online_count, archived_count, "
        "destroyed_count, judged_count, prod_unchanged, summary, created_at "
        "FROM shadow_reports WHERE shadow_job_id = ?",
        (shadow_job_id,),
    ).fetchone()
    judgments = conn.execute(
        "SELECT id, shadow_job_id, partition_id, domain_code, biz_date, policy_version, "
        "keep_days, decided_action, status, fail_reason, created_at "
        "FROM shadow_judgments WHERE shadow_job_id = ? ORDER BY id",
        (shadow_job_id,),
    ).fetchall()
    runs = conn.execute(
        "SELECT r.id, r.shadow_judgment_id, r.action, r.result_state, r.archive_uri, "
        "r.ok, r.detail, r.created_at "
        "FROM shadow_execute_runs r "
        "JOIN shadow_judgments j ON j.id = r.shadow_judgment_id "
        "WHERE j.shadow_job_id = ? ORDER BY r.id",
        (shadow_job_id,),
    ).fetchall()
    partitions = conn.execute(
        "SELECT partition_id, domain_code, biz_date, row_count, storage_state, archive_uri "
        "FROM shadow_partitions WHERE shadow_job_id = ? ORDER BY domain_code, biz_date",
        (shadow_job_id,),
    ).fetchall()

    # 落库可核：按影子轨持久化数据重算，对照试算时落库的报告
    report = dict(report_row) if report_row else None
    recount = {
        r["storage_state"]: r["c"]
        for r in conn.execute(
            "SELECT storage_state, COUNT(*) AS c FROM shadow_partitions "
            "WHERE shadow_job_id = ? GROUP BY storage_state",
            (shadow_job_id,),
        ).fetchall()
    }
    judged = conn.execute(
        "SELECT COUNT(*) AS c FROM shadow_judgments WHERE shadow_job_id = ?",
        (shadow_job_id,),
    ).fetchone()["c"]
    recount_ok = bool(report) and (
        int(recount.get("在线", 0)) == report["online_count"]
        and int(recount.get("已归档", 0)) == report["archived_count"]
        and int(recount.get("已销毁", 0)) == report["destroyed_count"]
        and int(judged) == report["judged_count"]
    )
    verify = {
        "recount_ok": recount_ok,
        "prod_unchanged": bool(report) and bool(report["prod_unchanged"]),
    }
    return {
        "track": "影子",
        "job": dict(job_row),
        "report": report,
        "judgments": [dict(j) for j in judgments],
        "runs": [dict(r) for r in runs],
        "partitions": [dict(p) for p in partitions],
        "verify": verify,
    }
