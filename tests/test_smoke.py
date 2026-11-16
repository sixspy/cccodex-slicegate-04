import sqlite3
import threading
import time

import pytest
from fastapi.testclient import TestClient

import app as appmod
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    appmod.SESSIONS.clear()
    init_db()
    yield


def _login(c, username="admin", password="123456"):
    r = c.post("/api/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _register(c, h, no, biz, part, rows, expected=1000):
    r = c.post("/api/slices", headers=h, json={
        "slice_no": no, "biz_date": biz, "partition_key": part,
        "row_count": rows, "expected_count": expected})
    assert r.status_code == 200, r.text
    return r.json()["id"]


def _gate(c, h, biz_date):
    return c.get("/api/dependencies", headers=h, params={"biz_date": biz_date}).json()["items"][0]


def _watermark_rows(c, h):
    return c.get("/api/watermarks", headers=h).json()["items"]


def _group_watermark_rows(c, h):
    """闸内推进的组水位；闸前历史水位 legacy=True，不算。"""
    return [w for w in _watermark_rows(c, h) if not w["legacy"]]


def test_pipeline_plain_partition_unchanged():
    c = TestClient(app)
    assert c.get("/api/health").json()["product"] == "日切放行"
    h = _login(c)
    assert len(c.get("/api/slices", headers=h).json()["items"]) >= 1
    sid = _register(c, h, "SL-T-1", "2026-11-04", "mid", 10, 10)
    rel = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_x"})
    assert rel.status_code == 200, rel.text
    assert any(w["source_slice"] == "SL-T-1" for w in _watermark_rows(c, h))


def test_rubric_is_pinned_to_one_value():
    c = TestClient(app)
    h = _login(c)
    reg = c.get("/api/dependencies/register", headers=h, params={"biz_date": "2026-11-01"}).json()
    assert reg["canonical_rubric"] == "乙"
    assert [p["rubric"] for p in reg["policies"]] == ["乙"]
    # 当天不允许换口径
    r = c.post("/api/dependencies/policy", headers=h, json={"biz_date": "2026-11-01", "rubric": "甲"})
    assert r.status_code == 409
    assert "依赖口径偏了" in r.json()["detail"]
    events = c.get("/api/dependencies/events", headers=h, params={"biz_date": "2026-11-01"}).json()["items"]
    assert any(e["kind"] == "依赖口径偏了" for e in events)


def test_seed_shows_watermark_conflict_across_all_views():
    """东区已放行、西区未完整却推了水位：四处画面必须同源给出越界结论。"""
    c = TestClient(app)
    h = _login(c)
    gate = _gate(c, h, "2026-11-01")
    assert gate["rubric"] == "乙"
    assert gate["complete_all"] is False
    assert gate["status"] == "组未齐"

    # 列表旁注
    east = next(s for s in c.get("/api/slices", headers=h).json()["items"]
                if s["slice_no"] == "SL-2026-11-01-east")
    assert east["dependency"]["status"] == "组未齐"
    assert "水位不推进" in east["dependency"]["hint"]

    # 分区详情
    detail = c.get("/api/partitions/2026-11-01/east", headers=h).json()
    assert detail["group"]["status"] == gate["status"]
    assert detail["annotation"]["status"] == gate["status"]
    assert detail["watermarks"][0]["watermark_ok"] is False
    assert "依赖口径偏了" in detail["watermarks"][0]["watermark_note"]

    # 水位页
    wm = _watermark_rows(c, h)[0]
    assert wm["gate_ok"] is False
    assert wm["group_key"] == gate["group_key"]
    assert wm["group_status"] == gate["status"]


def test_incomplete_partition_release_does_not_move_watermark():
    c = TestClient(app)
    h = _login(c)
    west = next(s for s in c.get("/api/slices", headers=h).json()["items"]
                if s["slice_no"] == "SL-2026-11-01-west")
    r = c.post(f"/api/slices/{west['id']}/release", headers=h, json={"consumer": "ads_order"})
    assert r.status_code == 409
    assert not _group_watermark_rows(c, h)


def test_single_member_release_does_not_push_group_watermark():
    """乙口径核心：半员放行也禁止推水位。"""
    c = TestClient(app)
    h = _login(c)
    # 补齐西区，使其完整；东区本已完整，但东区已单独放行过（闸前）
    west_id = _register(c, h, "SL-2026-11-01-west-fix", "2026-11-01", "west", 1000, 1000)
    rel = c.post(f"/api/slices/{west_id}/release", headers=h, json={"consumer": "ads_order"})
    assert rel.status_code == 200, rel.text
    assert rel.json()["watermark_pushed"] is False
    # 全员已完整，但东区在闸内没有成功放行记录（闸前的单独放行不算）→ 不得推组水位
    gate = _gate(c, h, "2026-11-01")
    assert gate["complete_all"] is True
    assert gate["status"] == "待全组放行"
    assert not _group_watermark_rows(c, h)


def test_group_watermark_pushed_once_after_all_members_released():
    c = TestClient(app)
    h = _login(c)
    west_id = _register(c, h, "SL-2026-11-01-west-fix", "2026-11-01", "west", 1000, 1000)
    east_id = next(s["id"] for s in c.get("/api/slices", headers=h).json()["items"]
                   if s["slice_no"] == "SL-2026-11-01-east")
    r1 = c.post(f"/api/slices/{west_id}/release", headers=h, json={"consumer": "ads_order"})
    assert r1.status_code == 200 and r1.json()["watermark_pushed"] is False
    r2 = c.post(f"/api/slices/{east_id}/release", headers=h, json={"consumer": "ads_order"})
    assert r2.status_code == 200, r2.text
    assert r2.json()["watermark_pushed"] is True

    group_wms = _group_watermark_rows(c, h)
    assert len(group_wms) == 1
    # 闸前旧水位保留审计但标记为已被纠正，不再与现行水位打架
    legacy = [w for w in _watermark_rows(c, h) if w["legacy"]]
    assert legacy and all(w["superseded"] == 1 and w["gate_ok"] for w in legacy)
    assert all("已由依赖闸" in w["gate_note"] for w in legacy)
    assert group_wms[0]["gate_ok"] is True
    assert set(group_wms[0]["source_slice"].split("+")) == {
        "SL-2026-11-01-east", "SL-2026-11-01-west-fix"}

    # 重复放行不得再推水位
    r3 = c.post(f"/api/slices/{east_id}/release", headers=h, json={"consumer": "ads_order"})
    assert r3.status_code == 409
    assert len(_group_watermark_rows(c, h)) == 1


def test_bypass_claiming_no_dependency_is_blocked():
    """不得绕过依赖声明单分区偷放行：组成员切片即使请求上不报依赖也走闸。"""
    c = TestClient(app)
    h = _login(c)
    east_id = next(s["id"] for s in c.get("/api/slices", headers=h).json()["items"]
                   if s["slice_no"] == "SL-2026-11-01-east")
    # 东区完整，但组未齐 → 乙口径下单分区可登记，放行本身允许但不推水位
    r = c.post(f"/api/slices/{east_id}/release", headers=h, json={"consumer": "ads_bypass"})
    assert r.status_code == 200
    assert r.json()["watermark_pushed"] is False
    assert r.json()["group"] == _gate(c, h, "2026-11-01")["group_key"]
    # 组水位不得出现
    assert not _group_watermark_rows(c, h)


def test_declaring_partition_twice_is_deviation():
    c = TestClient(app)
    h = _login(c)
    r = c.post("/api/dependencies/groups", headers=h, json={
        "biz_date": "2026-11-01", "group_key": "偷放组", "partitions": ["east"]})
    assert r.status_code == 409
    assert "依赖口径偏了" in r.json()["detail"]
    events = c.get("/api/dependencies/events", headers=h,
                   params={"biz_date": "2026-11-01"}).json()["items"]
    assert any("east" in e["detail"] for e in events)


def test_new_biz_date_declaration_pins_rubric():
    c = TestClient(app)
    h = _login(c)
    r = c.post("/api/dependencies/groups", headers=h, json={
        "biz_date": "2026-11-05", "group_key": "结账组", "partitions": ["north", "south"]})
    assert r.status_code == 200, r.text
    assert r.json()["group"]["rubric"] == "乙"
    reg = c.get("/api/dependencies/register", headers=h, params={"biz_date": "2026-11-05"}).json()
    assert [p["rubric"] for p in reg["policies"]] == ["乙"]
    # 换口径被拒
    r2 = c.post("/api/dependencies/policy", headers=h, json={"biz_date": "2026-11-05", "rubric": "甲"})
    assert r2.status_code == 409


def test_concurrent_release_same_group_records_contention():
    """同一依赖组两名值班同时放行：后到记依赖争用，禁止默成。"""
    c = TestClient(app)
    h = _login(c)
    west_id = _register(c, h, "SL-2026-11-01-west-fix", "2026-11-01", "west", 1000, 1000)
    east_id = next(s["id"] for s in c.get("/api/slices", headers=h).json()["items"]
                   if s["slice_no"] == "SL-2026-11-01-east")

    results: list[int] = []
    barrier = threading.Barrier(2)

    def call(sid, consumer):
        client = TestClient(app)
        barrier.wait()
        r = client.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": consumer})
        results.append(r.status_code)

    threads = [
        threading.Thread(target=call, args=(west_id, "ads_a")),
        threading.Thread(target=call, args=(east_id, "ads_b")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert sorted(results) == [200, 409], results
    events = c.get("/api/dependencies/events", headers=h,
                   params={"biz_date": "2026-11-01"}).json()["items"]
    assert any(e["kind"] == "依赖争用" for e in events)
