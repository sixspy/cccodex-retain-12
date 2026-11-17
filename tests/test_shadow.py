import sqlite3
import threading

import pytest

from app.db import get_db, init_db
from app.services.execute import execute_judgment
from app.services.scan import run_expire_scan
from app.services.shadow import guard_shadow_track


def test_shadow_trial_leaves_production_untouched(authed):
    before = authed.get("/api/partitions").json()["items"]
    r = authed.post("/api/shadow-runs", json={"as_of_date": "2026-11-16"})
    assert r.status_code == 200
    run = r.json()["run"]
    assert run["track"] == "影子"
    assert run["job"]["status"] == "已完成"
    assert run["job"]["expired_count"] >= 1
    assert run["report"]["prod_unchanged"] == 1
    assert run["verify"]["recount_ok"] is True
    assert run["verify"]["prod_unchanged"] is True

    # 正式分区队列一行未动，正式裁决与正式扫描台账均无串轨记录
    assert authed.get("/api/partitions").json()["items"] == before
    assert authed.get("/api/judgments").json()["items"] == []
    assert authed.get("/api/scan-jobs").json()["items"] == []

    # 影子投影里能看到归档与销毁的试算结果
    states = {p["storage_state"] for p in run["partitions"]}
    assert "已归档" in states
    assert "已销毁" in states
    assert run["runs"], "影子执行结果应落库"


def test_shadow_runs_listed_on_shadow_track_only(authed):
    authed.post("/api/shadow-runs", json={"as_of_date": "2026-11-16"})
    items = authed.get("/api/shadow-runs").json()["items"]
    assert len(items) == 1
    assert items[0]["track"] == "影子"
    detail = authed.get(f"/api/shadow-runs/{items[0]['id']}").json()
    assert detail["job"]["id"] == items[0]["id"]
    assert detail["report"]["judged_count"] >= 1
    missing = authed.get("/api/shadow-runs/99999")
    assert missing.status_code == 404


def test_shadow_and_production_tracks_are_independent(authed):
    authed.post("/api/shadow-runs", json={"as_of_date": "2026-11-16"})
    shadow_before = authed.get("/api/shadow-runs").json()["items"][0]

    # 正式轨照常扫描 + 执行一条
    authed.post("/api/scan-jobs", json={"as_of_date": "2026-11-16"})
    pending = authed.get("/api/judgments", params={"status": "待执行"}).json()["items"]
    assert pending
    exe = authed.post(f"/api/judgments/{pending[0]['id']}/execute")
    assert exe.status_code == 200

    # 影子轨记录不受正式轨影响；正式台账只有正式扫描那一条
    assert authed.get("/api/shadow-runs").json()["items"][0] == shadow_before
    jobs = authed.get("/api/scan-jobs").json()["items"]
    assert len(jobs) == 1


def test_cross_track_write_blocked(tmp_path):
    db = str(tmp_path / "t.db")
    init_db(db)
    with get_db(db) as conn:
        guard_shadow_track(conn)
        # 串轨即拦：影子守卫下写正式轨各表一律被拒
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("UPDATE partitions SET storage_state = '已销毁' WHERE id = 1")
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute(
                "INSERT INTO scan_jobs (as_of_date, status) VALUES ('2026-11-16','进行中')"
            )
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("DELETE FROM judgments")
        # 影子轨表与正式轨读不受影响
        conn.execute(
            "INSERT INTO shadow_scan_jobs (as_of_date, status) VALUES ('2026-11-16','进行中')"
        )
        assert conn.execute("SELECT COUNT(*) AS c FROM partitions").fetchone()["c"] >= 10


def test_execute_judgment_serialized_by_lock(tmp_path):
    db = str(tmp_path / "t.db")
    init_db(db)
    with get_db(db) as conn:
        run_expire_scan(conn, "2026-11-16")
        jid = conn.execute("SELECT id FROM judgments ORDER BY id LIMIT 1").fetchone()["id"]

    outcomes = []

    def worker():
        try:
            with get_db(db) as conn:
                run = execute_judgment(conn, jid)
            outcomes.append("ok" if run["ok"] == 1 else "fail")
        except ValueError:
            outcomes.append("conflict")

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 互斥锁写：同一裁决并发执行只成功一次，不会双成功盖轨
    assert outcomes.count("ok") == 1
    assert outcomes.count("conflict") == 7
    with get_db(db) as conn:
        row = conn.execute(
            "SELECT p.storage_state FROM partitions p "
            "JOIN judgments j ON j.partition_id = p.id WHERE j.id = ?",
            (jid,),
        ).fetchone()
    assert row["storage_state"] in ("已归档", "已销毁")
