import hashlib
import secrets
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

PRODUCT = "日切放行"
SKELETON = "业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账"

# ---------------------------------------------------------------------------
# 策略互斥框：口径只钉死一种，全库只实现这一种，绝不两边混写。
# 乙写法：各分区可独立登记完整度、可独立放行；消费水位只在组内全员完整且
# 全部成功放行后一次推进。任何“半员放行就推水位”“绕过依赖声明单分区偷放行”
# 都判为依赖口径偏了。
# ---------------------------------------------------------------------------
RUBRIC = "乙"
RUBRIC_SPEC = ("各分区独立登记完整度、独立放行；消费水位只在组内全员完整且全部成功放行后一次推进")
DEV_PREFIX = "依赖口径偏了"

# 依赖组放行互斥：group_id -> Lock。数据库 dependency_gate_locks 负责跨连接
# 判定，进程内锁把同进程并发请求也串成一条队。
_GROUP_LOCKS: dict[int, threading.Lock] = {}
_GROUP_LOCKS_GUARD = threading.Lock()

# 临界区保留时间，让并发请求真正落进同一窗口，从而可复现“后到记依赖争用”。
_GATE_HOLD_SECONDS = 0.12

# 依赖组水位来源切片的连接符：一条水位对应全组成员，可回查。
_SOURCE_JOIN = "+"


class _Conn(sqlite3.Connection):
    """with 退出时显式关闭，避免长跑进程里连接越堆越多。"""

    def __exit__(self, exc_type, exc, tb):
        try:
            return super().__exit__(exc_type, exc, tb)
        finally:
            self.close()


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10, factory=_Conn)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


@contextmanager
def tx(conn):
    """显式事务：依赖闸裁决与水位推进必须同事务，禁止半截提交。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


class LoginIn(BaseModel):
    username: str
    password: str


def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


app = FastAPI(title=PRODUCT)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# 表结构
# ---------------------------------------------------------------------------

def _columns(conn, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}


def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS slices(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_no TEXT UNIQUE NOT NULL,
          biz_date TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          expected_count INTEGER NOT NULL,
          completeness TEXT NOT NULL,
          release_status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS releases(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_id INTEGER NOT NULL,
          consumer TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS watermarks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          consumer TEXT NOT NULL,
          biz_date TEXT NOT NULL,
          watermark TEXT NOT NULL,
          source_slice TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dependency_policies(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          biz_date TEXT UNIQUE NOT NULL,
          rubric TEXT NOT NULL,
          rubric_spec TEXT NOT NULL,
          note TEXT NOT NULL DEFAULT '',
          pinned_by TEXT NOT NULL,
          pinned_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dependency_groups(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          biz_date TEXT NOT NULL,
          group_key TEXT NOT NULL,
          created_by TEXT NOT NULL,
          created_at TEXT NOT NULL,
          UNIQUE(biz_date, group_key)
        );
        CREATE TABLE IF NOT EXISTS dependency_members(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          group_id INTEGER NOT NULL,
          biz_date TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          UNIQUE(biz_date, partition_key)
        );
        CREATE TABLE IF NOT EXISTS group_releases(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          group_id INTEGER NOT NULL,
          biz_date TEXT NOT NULL,
          consumer TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          slice_id INTEGER NOT NULL,
          status TEXT NOT NULL,
          note TEXT NOT NULL,
          actor TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dependency_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          biz_date TEXT NOT NULL,
          group_id INTEGER,
          kind TEXT NOT NULL,
          actor TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dependency_gate_locks(
          group_id INTEGER PRIMARY KEY,
          token TEXT NOT NULL,
          holder TEXT NOT NULL,
          started_at TEXT NOT NULL,
          expires_at TEXT NOT NULL
        );
        """)
        # 旧库水位补 group_id：有它才能把水位与依赖组、来源切片绑在一起核。
        if "group_id" not in _columns(conn, "watermarks"):
            conn.execute("ALTER TABLE watermarks ADD COLUMN group_id INTEGER")
        # 闸前水位被依赖闸合规水位纠正后置 1，保留审计但不与现行水位打架。
        if "superseded" not in _columns(conn, "watermarks"):
            conn.execute("ALTER TABLE watermarks ADD COLUMN superseded INTEGER NOT NULL DEFAULT 0")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_watermark_group_consumer "
                     "ON watermarks(group_id, consumer, biz_date) WHERE group_id IS NOT NULL")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_group_releases_group "
                     "ON group_releases(group_id, consumer)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_events_group ON dependency_events(group_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS ix_events_date ON dependency_events(biz_date)")
        conn.execute("DELETE FROM dependency_gate_locks WHERE expires_at < ?", (now_iso(),))

        if conn.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"] == 0:
            # 全新库的演示现场：东区已完整放行、西区未完整，水位却已被推进。
            rows = [
                ("SL-2026-11-01-east", "2026-11-01", "east", 1000, 1000, "完整", "已放行"),
                ("SL-2026-11-01-west", "2026-11-01", "west", 960, 1000, "部分完整", "未放行"),
                ("SL-2026-11-02-east", "2026-11-02", "east", 0, 1000, "待检", "未放行"),
            ]
            for r in rows:
                conn.execute(
                    "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) "
                    "VALUES (?,?,?,?,?,?,?)", r)
            conn.execute(
                "INSERT INTO releases(slice_id,consumer,status,note,created_at) "
                "VALUES (1,'ads_order','成功','完整切片放行','2026-11-01T18:00:00')")
            conn.execute(
                "INSERT INTO watermarks(consumer,biz_date,watermark,source_slice,group_id) "
                "VALUES ('ads_order','2026-11-01','2026-11-01T18:00:00','SL-2026-11-01-east',NULL)")

        _seed_dependency_gate(conn)
        conn.commit()
    with _GROUP_LOCKS_GUARD:
        _GROUP_LOCKS.clear()


def _pin_policy_row(conn, biz_date: str, actor: str, note: str) -> sqlite3.Row:
    """把当天口径写进依赖闸口册；已有则原样返回，当天一律不许换口径。"""
    row = conn.execute("SELECT * FROM dependency_policies WHERE biz_date=?", (biz_date,)).fetchone()
    if row:
        return row
    conn.execute(
        "INSERT INTO dependency_policies(biz_date,rubric,rubric_spec,note,pinned_by,pinned_at) "
        "VALUES (?,?,?,?,?,?)",
        (biz_date, RUBRIC, RUBRIC_SPEC, note, actor, now_iso()))
    _record_event(conn, biz_date, None, "口径钉死", actor,
                  f"{biz_date} 钉死依赖闸{RUBRIC}口径：{RUBRIC_SPEC}；当天不得换口径")
    return conn.execute("SELECT * FROM dependency_policies WHERE biz_date=?", (biz_date,)).fetchone()


def _seed_dependency_gate(conn):
    """把既有现场纳入依赖闸，并写入当天口径；不改动既有切片/水位数据。

    只对“水位已推进、但同业务日分区并未全部完整”的业务日建组——这正是
    东/西区互相打架的现场，必须由依赖闸接管裁决。其余业务日不替值班声明依赖。
    """
    dates = [r["biz_date"] for r in conn.execute(
        "SELECT DISTINCT biz_date FROM slices ORDER BY biz_date").fetchall()]
    for biz_date in dates:
        _pin_policy_row(conn, biz_date, "system", "依赖闸上线：乙口径（分区独立登记/放行，水位组齐后一次推进）")
        has_group = conn.execute(
            "SELECT COUNT(*) c FROM dependency_groups WHERE biz_date=?", (biz_date,)).fetchone()["c"]
        if has_group:
            continue
        parts = [r["partition_key"] for r in conn.execute(
            "SELECT DISTINCT partition_key FROM slices WHERE biz_date=? ORDER BY partition_key",
            (biz_date,)).fetchall()]
        if len(parts) < 2:
            continue
        # 是否已存在该日水位，且并非全部分区完整？只有这种打架现场才自动纳闸。
        pushed = conn.execute(
            "SELECT 1 FROM watermarks WHERE biz_date=? LIMIT 1", (biz_date,)).fetchone() is not None
        all_complete = conn.execute(
            "SELECT 1 FROM slices WHERE biz_date=? AND completeness!='完整' LIMIT 1", (biz_date,)).fetchone() is None
        if not pushed or all_complete:
            continue
        cur = conn.execute(
            "INSERT INTO dependency_groups(biz_date,group_key,created_by,created_at) VALUES (?,?,?,?)",
            (biz_date, f"{biz_date} 日切依赖组", "system", now_iso()))
        gid = cur.lastrowid
        for p in parts:
            conn.execute(
                "INSERT OR IGNORE INTO dependency_members(group_id,biz_date,partition_key) VALUES (?,?,?)",
                (gid, biz_date, p))
        released = [r["slice_no"] for r in conn.execute(
            "SELECT slice_no FROM slices WHERE biz_date=? AND release_status='已放行'", (biz_date,)).fetchall()]
        not_ready = [r["slice_no"] for r in conn.execute(
            "SELECT slice_no FROM slices WHERE biz_date=? AND completeness!='完整'", (biz_date,)).fetchall()]
        _record_event(conn, biz_date, gid, "依赖组声明", "system",
                      f"{biz_date} 依赖组声明 {'+'.join(parts)} 互为依赖；"
                      f"历史已放行 {'/'.join(released) or '无'}，未完整 {'/'.join(not_ready) or '无'}，"
                      "闸前单分区放行与水位推进按依赖闸判为越界，等待整组补齐后按口径重放")


# ---------------------------------------------------------------------------
# 依赖闸同源计算：依赖目录 / 分区详情 / 水位页 / 列表旁注全部由此取数
# ---------------------------------------------------------------------------

def _active_slice(conn, biz_date: str, partition_key: str):
    """同业务日同分区多张切片时，以最新登记的一张作为当前生效切片。"""
    return conn.execute(
        "SELECT * FROM slices WHERE biz_date=? AND partition_key=? ORDER BY id DESC LIMIT 1",
        (biz_date, partition_key)).fetchone()


def _policy(conn, biz_date: str):
    return conn.execute("SELECT * FROM dependency_policies WHERE biz_date=?", (biz_date,)).fetchone()


def _group_for_partition(conn, biz_date: str, partition_key: str):
    return conn.execute(
        "SELECT g.* FROM dependency_groups g JOIN dependency_members m ON m.group_id=g.id "
        "WHERE g.biz_date=? AND m.partition_key=?", (biz_date, partition_key)).fetchone()


def _members(conn, group_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT partition_key, biz_date FROM dependency_members WHERE group_id=? ORDER BY partition_key",
        (group_id,)).fetchall()
    out = []
    for m in rows:
        s = _active_slice(conn, m["biz_date"], m["partition_key"])
        out.append({
            "partition_key": m["partition_key"],
            "biz_date": m["biz_date"],
            "registered": s is not None,
            "slice_id": s["id"] if s else None,
            "slice_no": s["slice_no"] if s else None,
            "row_count": s["row_count"] if s else None,
            "expected_count": s["expected_count"] if s else None,
            "completeness": s["completeness"] if s else "未登记",
            "release_status": s["release_status"] if s else "未登记",
            "complete": bool(s and s["completeness"] == "完整"),
        })
    return out


def _released_partitions(conn, group_id: int, consumer: str, members: list[dict]) -> list[str]:
    """组内已成功放行的分区；切片被重登记后旧放行不再算数。"""
    active = {m["partition_key"]: m["slice_id"] for m in members if m["slice_id"] is not None}
    rows = conn.execute(
        "SELECT DISTINCT partition_key, slice_id FROM group_releases "
        "WHERE group_id=? AND consumer=? AND status='成功'", (group_id, consumer)).fetchall()
    return sorted({r["partition_key"] for r in rows
                   if active.get(r["partition_key"]) == r["slice_id"]})


def _watermark_for(conn, biz_date: str, group_id: int, consumer: str):
    return conn.execute(
        "SELECT * FROM watermarks WHERE biz_date=? AND consumer=? AND group_id=? ORDER BY id DESC LIMIT 1",
        (biz_date, consumer, group_id)).fetchone()


def _legacy_watermarks_for(conn, biz_date: str, group_id: int) -> list:
    """闸前水位没有 group_id，按来源切片反查其所属依赖组。"""
    rows = conn.execute(
        "SELECT * FROM watermarks WHERE biz_date=? AND group_id IS NULL AND superseded=0 "
        "ORDER BY id DESC", (biz_date,)).fetchall()
    out = []
    for w in rows:
        s = conn.execute("SELECT * FROM slices WHERE slice_no=?", (w["source_slice"],)).fetchone()
        if not s:
            continue
        g = _group_for_partition(conn, s["biz_date"], s["partition_key"])
        if g and g["id"] == group_id:
            out.append(w)
    return out


def _consumers_for_group(conn, group) -> list[str]:
    names: set[str] = set()
    for r in conn.execute("SELECT DISTINCT consumer FROM group_releases WHERE group_id=?", (group["id"],)):
        names.add(r["consumer"])
    for r in conn.execute("SELECT DISTINCT consumer FROM watermarks WHERE group_id=?", (group["id"],)):
        names.add(r["consumer"])
    for w in _legacy_watermarks_for(conn, group["biz_date"], group["id"]):
        names.add(w["consumer"])
    for m in _members(conn, group["id"]):
        if m["slice_id"] is None:
            continue
        for r in conn.execute(
                "SELECT DISTINCT consumer FROM releases WHERE slice_id=? AND status='成功'", (m["slice_id"],)):
            names.add(r["consumer"])
    return sorted(names)


def _watermark_gate_ok(wm, group_id: int, complete_all: bool, all_released: bool) -> tuple[bool, str]:
    if wm["group_id"] == group_id:
        if complete_all and all_released:
            return True, "组内全员完整且全部成功放行，水位由依赖闸一次推进"
        return False, f"{DEV_PREFIX}：依赖组未齐却存在组水位，越界"
    if complete_all and all_released:
        return True, "闸前水位，当前组状态已补齐"
    return False, f"{DEV_PREFIX}：闸前单分区放行就推了水位，组内未全员完整/放行"


def _consumer_state(conn, group, consumer: str, members: list[dict]) -> dict:
    complete_all = all(m["complete"] for m in members)
    released = _released_partitions(conn, group["id"], consumer, members)
    all_released = {m["partition_key"] for m in members} <= set(released)
    wm = _watermark_for(conn, group["biz_date"], group["id"], consumer)
    legacy_wms = _legacy_watermarks_for(conn, group["biz_date"], group["id"])
    legacy_wm = next((w for w in legacy_wms if w["consumer"] == consumer), None)
    effective_wm = wm or legacy_wm
    if effective_wm is not None:
        wm_ok, wm_note = _watermark_gate_ok(effective_wm, group["id"], complete_all, all_released)
    else:
        wm_ok, wm_note = (False, "尚未推进水位")

    if not complete_all:
        status = "组未齐"
        hint = "乙口径：各分区可独立登记完整度；组内未全员完整，禁止推水位"
    elif not all_released:
        status = "待全组放行"
        hint = "乙口径：分区可独立放行；全员成功放行前禁止推水位"
    elif effective_wm is None:
        status = "待推水位"
        hint = "乙口径：全员已完整且全部成功放行，等待一次推水位"
    else:
        status = "水位已推"
        hint = "乙口径：全员完整且全部成功放行，水位已一次推进"

    return {
        "consumer": consumer,
        "released_partitions": released,
        "complete_all": complete_all,
        "all_released": all_released,
        "watermark_pushed": effective_wm is not None,
        "watermark_ok": wm_ok if effective_wm is not None else False,
        "watermark_note": wm_note,
        "watermark": dict(effective_wm) if effective_wm is not None else None,
        "legacy_watermark": legacy_wm is not None,
        "status": status,
        "hint": hint,
    }


def _group_view(conn, group) -> dict:
    members = _members(conn, group["id"])
    policy = _policy(conn, group["biz_date"])
    consumers = _consumers_for_group(conn, group)
    states = [_consumer_state(conn, group, c, members) for c in consumers]
    complete_all = all(m["complete"] for m in members)
    released_all_consumers = bool(states) and all(s["all_released"] for s in states)
    wm_ok_any = any(s["watermark_ok"] for s in states)

    if not complete_all:
        missing = [m["partition_key"] for m in members if not m["complete"]]
        status = "组未齐"
        hint = f"依赖组未齐（{'/'.join(missing)}），禁止推水位"
    elif not released_all_consumers:
        status = "待全组放行"
        hint = "依赖组已全员完整，等待各分区全部成功放行"
    elif wm_ok_any:
        status = "已放行"
        hint = "依赖组全员完整、全部成功放行，水位已按口径一次推进"
    elif any(s["watermark_pushed"] for s in states):
        status = "水位越界"
        hint = f"{DEV_PREFIX}：存在未按组齐推进的水位，需按口径纠正"
    else:
        status = "待推水位"
        hint = "全员完整且全部成功放行，等待一次推水位"

    return {
        "id": group["id"],
        "biz_date": group["biz_date"],
        "group_key": group["group_key"],
        "created_by": group["created_by"],
        "created_at": group["created_at"],
        "rubric": policy["rubric"] if policy else RUBRIC,
        "rubric_spec": policy["rubric_spec"] if policy else RUBRIC_SPEC,
        "policy_note": policy["note"] if policy else "",
        "members": members,
        "complete_all": complete_all,
        "status": status,
        "hint": hint,
        "consumers": states,
    }


def _group_annotation(conn, slice_row) -> dict | None:
    group = _group_for_partition(conn, slice_row["biz_date"], slice_row["partition_key"])
    if not group:
        return None
    view = _group_view(conn, group)
    complete = slice_row["completeness"] == "完整"
    if not complete:
        hint = "乙口径：本分区未完整，禁止放行；组水位不推进"
    elif not view["complete_all"]:
        hint = "乙口径：本分区可独立放行，但组未齐，水位不推进"
    else:
        hint = "乙口径：可独立放行；全员成功放行后水位一次推进"
    return {
        "group_id": group["id"],
        "group_key": group["group_key"],
        "rubric": view["rubric"],
        "status": view["status"],
        "complete_all": view["complete_all"],
        "hint": hint,
    }


def _record_event(conn, biz_date: str, group_id, kind: str, actor: str, detail: str):
    conn.execute(
        "INSERT INTO dependency_events(biz_date,group_id,kind,actor,detail,created_at) VALUES (?,?,?,?,?,?)",
        (biz_date, group_id, kind, actor, detail, now_iso()))


def _deviation(conn, biz_date: str, group_id, actor: str, detail: str):
    _record_event(conn, biz_date, group_id, DEV_PREFIX, actor, detail)
    conn.commit()
    raise HTTPException(409, f"{DEV_PREFIX}：{detail}")


# ---------------------------------------------------------------------------
# 依赖组放行互斥
# ---------------------------------------------------------------------------

def _group_lock(group_id: int) -> threading.Lock:
    with _GROUP_LOCKS_GUARD:
        return _GROUP_LOCKS.setdefault(group_id, threading.Lock())


def _claim_group(conn, group_id: int, token: str, actor: str, biz_date: str) -> bool:
    """抢占依赖组放行权；后到者必须显式记“依赖争用”，禁止默成。"""
    lock = _group_lock(group_id)
    if not lock.acquire(blocking=False):
        _record_event(conn, biz_date, group_id, "依赖争用", actor,
                      "同一依赖组已有值班在放行，后到请求排队失败，记依赖争用，拒绝默成")
        conn.commit()
        return False
    expires_iso = datetime.fromtimestamp(datetime.now().timestamp() + 15).isoformat(timespec="seconds")
    try:
        with tx(conn):
            row = conn.execute("SELECT * FROM dependency_gate_locks WHERE group_id=?", (group_id,)).fetchone()
            if row and row["expires_at"] > now_iso() and row["token"] != token:
                _record_event(conn, biz_date, group_id, "依赖争用", actor,
                              f"依赖组放行权由 {row['holder']} 持有，后到请求记依赖争用，拒绝默成")
                lock.release()
                return False
            conn.execute(
                "INSERT OR REPLACE INTO dependency_gate_locks(group_id,token,holder,started_at,expires_at) "
                "VALUES (?,?,?,?,?)", (group_id, token, actor, now_iso(), expires_iso))
    except Exception:
        if lock.locked():
            lock.release()
        raise
    return True


def _unclaim_group(conn, group_id: int, token: str):
    try:
        conn.execute("DELETE FROM dependency_gate_locks WHERE group_id=? AND token=?", (group_id, token))
        conn.commit()
    finally:
        lock = _group_lock(group_id)
        if lock.locked():
            try:
                lock.release()
            except RuntimeError:
                pass


# ---------------------------------------------------------------------------
# 启动与登录
# ---------------------------------------------------------------------------

@app.on_event("startup")
def _start():
    init_db()


@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "slices": n,
            "db": str(DB_PATH), "rubric": RUBRIC}


@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username, role FROM users WHERE username=? AND password_sha=?",
                        (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16)
    SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


# ---------------------------------------------------------------------------
# 切片与完整度
# ---------------------------------------------------------------------------

class SliceIn(BaseModel):
    slice_no: str
    biz_date: str
    partition_key: str
    row_count: int
    expected_count: int


def _completeness(row_count: int, expected_count: int) -> str:
    if expected_count > 0 and row_count >= expected_count:
        return "完整"
    return "部分完整" if row_count > 0 else "待检"


@app.get("/api/slices")
def list_slices(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM slices ORDER BY id").fetchall()
        items = []
        for r in rows:
            d = dict(r)
            d["dependency"] = _group_annotation(c, r)
            items.append(d)
    return {"items": items}


@app.post("/api/slices")
def add_slice(body: SliceIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    comp = _completeness(body.row_count, body.expected_count)
    with connect() as c:
        try:
            cur = c.execute(
                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) "
                "VALUES (?,?,?,?,?,?,?)",
                (body.slice_no, body.biz_date, body.partition_key, body.row_count,
                 body.expected_count, comp, "未放行"))
            c.commit()
            return {"id": cur.lastrowid, "completeness": comp}
        except sqlite3.IntegrityError:
            raise HTTPException(409, "切片号已存在")


@app.post("/api/slices/{sid}/evaluate")
def evaluate(sid: int, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        comp = _completeness(row["row_count"], row["expected_count"])
        c.execute("UPDATE slices SET completeness=? WHERE id=?", (comp, sid))
        c.commit()
    return {"id": sid, "completeness": comp}


# ---------------------------------------------------------------------------
# 依赖闸口册：口径、依赖组、审计事件
# ---------------------------------------------------------------------------

class PolicyIn(BaseModel):
    biz_date: str
    rubric: str
    note: str = ""


@app.post("/api/dependencies/policy")
def pin_policy(body: PolicyIn, authorization: str | None = Header(default=None)):
    actor = require_user(authorization)
    rubric = body.rubric.strip()
    with connect() as c:
        existing = _policy(c, body.biz_date)
        if existing:
            if rubric != existing["rubric"]:
                _deviation(c, body.biz_date, None, actor,
                           f"当天已钉死{existing['rubric']}口径，不得换成{rubric}")
            return {"ok": True, "biz_date": body.biz_date, "rubric": existing["rubric"],
                    "rubric_spec": existing["rubric_spec"], "already": True,
                    "pinned_at": existing["pinned_at"]}
        if rubric != RUBRIC:
            _deviation(c, body.biz_date, None, actor,
                       f"请求口径{rubric}与闸口册唯一口径{RUBRIC}不符，两边不得混写")
        note = body.note.strip() or "依赖闸乙口径上线"
        row = _pin_policy_row(c, body.biz_date, actor, note)
        c.commit()
    return {"ok": True, "biz_date": body.biz_date, "rubric": row["rubric"],
            "rubric_spec": row["rubric_spec"], "already": False}


class GroupIn(BaseModel):
    biz_date: str
    group_key: str
    partitions: list[str]
    rubric: str | None = None


@app.post("/api/dependencies/groups")
def declare_group(body: GroupIn, authorization: str | None = Header(default=None)):
    actor = require_user(authorization)
    partitions: list[str] = []
    for p in body.partitions:
        p = p.strip()
        if p and p not in partitions:
            partitions.append(p)
    if not body.group_key.strip() or not partitions:
        raise HTTPException(400, "依赖组须有组名与至少一个分区")
    with connect() as c:
        policy = _policy(c, body.biz_date)
        if not policy:
            with tx(c):
                policy = _pin_policy_row(c, body.biz_date, actor, "声明依赖组时钉死乙口径")
        if body.rubric and body.rubric.strip() != policy["rubric"]:
            _deviation(c, body.biz_date, None, actor,
                       f"声明携带{body.rubric}口径，与闸口册{policy['rubric']}口径不符")
        for p in partitions:
            exists = c.execute(
                "SELECT 1 FROM dependency_members WHERE biz_date=? AND partition_key=?",
                (body.biz_date, p)).fetchone()
            if exists:
                _deviation(c, body.biz_date, None, actor,
                           f"分区 {p} 已在当天其他依赖组中，绕过原依赖声明重复登记")
        with tx(c):
            try:
                cur = c.execute(
                    "INSERT INTO dependency_groups(biz_date,group_key,created_by,created_at) VALUES (?,?,?,?)",
                    (body.biz_date, body.group_key.strip(), actor, now_iso()))
            except sqlite3.IntegrityError:
                raise HTTPException(409, "同业务日依赖组名已存在")
            gid = cur.lastrowid
            for p in partitions:
                c.execute("INSERT INTO dependency_members(group_id,biz_date,partition_key) VALUES (?,?,?)",
                          (gid, body.biz_date, p))
            _record_event(c, body.biz_date, gid, "依赖组声明", actor,
                          f"声明 {body.group_key.strip()} 依赖组：{'/'.join(partitions)}；"
                          f"按{RUBRIC}口径裁决")
        group = c.execute("SELECT * FROM dependency_groups WHERE id=?", (gid,)).fetchone()
        view = _group_view(c, group)
    return {"ok": True, "group": view}


@app.get("/api/dependencies/policies")
def list_policies(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM dependency_policies ORDER BY biz_date DESC").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/dependencies/events")
def list_events(biz_date: str | None = None, group_id: int | None = None,
                authorization: str | None = Header(default=None)):
    require_user(authorization)
    sql = "SELECT * FROM dependency_events"
    args: list = []
    conds = []
    if biz_date:
        conds.append("biz_date=?")
        args.append(biz_date)
    if group_id is not None:
        conds.append("group_id=?")
        args.append(group_id)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY id DESC"
    with connect() as c:
        rows = c.execute(sql, args).fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/dependencies/register")
def gate_register(biz_date: str | None = None, authorization: str | None = Header(default=None)):
    """依赖闸口册：当天钉死口径 + 事件流水，供页面同源核验。"""
    user = require_user(authorization)
    with connect() as c:
        if biz_date:
            policies = c.execute("SELECT * FROM dependency_policies WHERE biz_date=?", (biz_date,)).fetchall()
            rows = c.execute("SELECT * FROM dependency_events WHERE biz_date=? ORDER BY id DESC",
                             (biz_date,)).fetchall()
        else:
            policies = c.execute("SELECT * FROM dependency_policies ORDER BY biz_date DESC").fetchall()
            rows = c.execute("SELECT * FROM dependency_events ORDER BY id DESC").fetchall()
    return {"viewer": user, "canonical_rubric": RUBRIC,
            "policies": [dict(p) for p in policies],
            "events": [dict(e) for e in rows]}


@app.get("/api/dependencies")
def list_dependencies(biz_date: str | None = None, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        if biz_date:
            groups = c.execute("SELECT * FROM dependency_groups WHERE biz_date=? ORDER BY id",
                               (biz_date,)).fetchall()
        else:
            groups = c.execute("SELECT * FROM dependency_groups ORDER BY biz_date, id").fetchall()
        items = [_group_view(c, g) for g in groups]
    return {"items": items, "canonical_rubric": RUBRIC, "rubric_spec": RUBRIC_SPEC}


@app.get("/api/partitions/{biz_date}/{partition_key}")
def partition_detail(biz_date: str, partition_key: str,
                     authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        slices = c.execute(
            "SELECT * FROM slices WHERE biz_date=? AND partition_key=? ORDER BY id DESC",
            (biz_date, partition_key)).fetchall()
        active = slices[0] if slices else None
        group = _group_for_partition(c, biz_date, partition_key)
        group_view = _group_view(c, group) if group else None
        policy = _policy(c, biz_date)
        releases = c.execute(
            "SELECT r.*, s.slice_no FROM releases r JOIN slices s ON s.id=r.slice_id "
            "WHERE s.biz_date=? AND s.partition_key=? ORDER BY r.id DESC",
            (biz_date, partition_key)).fetchall()
        group_releases = c.execute(
            "SELECT * FROM group_releases WHERE biz_date=? AND partition_key=? ORDER BY id DESC",
            (biz_date, partition_key)).fetchall()
        watermarks = []
        if group:
            for consumer in _consumers_for_group(c, group):
                state = _consumer_state(c, group, consumer, _members(c, group["id"]))
                watermarks.append({
                    "consumer": consumer,
                    "watermark": state["watermark"],
                    "watermark_ok": state["watermark_ok"],
                    "watermark_note": state["watermark_note"],
                    "legacy_watermark": state["legacy_watermark"],
                    "group_status": state["status"],
                })
        else:
            watermarks = [dict(w) for w in c.execute(
                "SELECT * FROM watermarks WHERE biz_date=? AND source_slice IN "
                "(SELECT slice_no FROM slices WHERE biz_date=? AND partition_key=?)",
                (biz_date, biz_date, partition_key)).fetchall()]
        events = []
        if group:
            events = [dict(e) for e in c.execute(
                "SELECT * FROM dependency_events WHERE group_id=? ORDER BY id DESC",
                (group["id"],)).fetchall()]
        annotation = _group_annotation(c, active) if active else None
    return {
        "biz_date": biz_date,
        "partition_key": partition_key,
        "rubric": policy["rubric"] if policy else RUBRIC,
        "rubric_spec": policy["rubric_spec"] if policy else RUBRIC_SPEC,
        "active_slice": dict(active) if active else None,
        "slices": [dict(s) for s in slices],
        "group": group_view,
        "annotation": annotation,
        "releases": [dict(r) for r in releases],
        "group_releases": [dict(r) for r in group_releases],
        "watermarks": watermarks,
        "events": events,
        "gate_hint": (annotation["hint"] if annotation
                      else "未声明跨分区依赖，按单分区规则放行"),
    }


# ---------------------------------------------------------------------------
# 放行：单分区路径 + 依赖闸路径
# ---------------------------------------------------------------------------

class ReleaseIn(BaseModel):
    consumer: str
    rubric: str | None = None


@app.post("/api/slices/{sid}/release")
def release(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    actor = require_user(authorization)
    consumer = body.consumer.strip()
    if not consumer:
        raise HTTPException(400, "下游消费方不能为空")
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        group = _group_for_partition(c, row["biz_date"], row["partition_key"])
        if group is None:
            return _plain_release(c, row, consumer, actor)

        policy = _policy(c, row["biz_date"])
        if not policy:
            _deviation(c, row["biz_date"], group["id"], actor,
                       f"切片 {row['slice_no']} 属于依赖组 {group['group_key']}，但当天口径未写入闸口册")
        if body.rubric and body.rubric.strip() != policy["rubric"]:
            _deviation(c, row["biz_date"], group["id"], actor,
                       f"放行请求携带{body.rubric}口径，与闸口册钉死的{policy['rubric']}口径不符")

        token = uuid.uuid4().hex
        if not _claim_group(c, group["id"], token, actor, row["biz_date"]):
            raise HTTPException(409, "依赖争用：同一依赖组已有值班在放行，后到请求拒绝默成")
        try:
            time.sleep(_GATE_HOLD_SECONDS)  # 让真实并发落进同一临界区
            return _release_group(c, group, row, consumer, actor)
        finally:
            _unclaim_group(c, group["id"], token)


def _plain_release(c, row, consumer: str, actor: str) -> dict:
    """未声明跨分区依赖的单分区：完整即放行并推水位。"""
    if row["completeness"] != "完整":
        with tx(c):
            c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                      (row["id"], consumer, "拒绝", "完整度未达标", now_iso()))
        raise HTTPException(409, "未完整禁止放行")
    with tx(c):
        dup = c.execute(
            "SELECT 1 FROM releases WHERE slice_id=? AND consumer=? AND status='成功' LIMIT 1",
            (row["id"], consumer)).fetchone()
        if dup:
            raise HTTPException(409, "该切片已放行")
        c.execute("UPDATE slices SET release_status='已放行' WHERE id=?", (row["id"],))
        c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                  (row["id"], consumer, "成功", "无依赖声明，完整切片放行", now_iso()))
        c.execute("INSERT INTO watermarks(consumer,biz_date,watermark,source_slice,group_id) "
                  "VALUES (?,?,?,?,NULL)",
                  (consumer, row["biz_date"], now_iso(), row["slice_no"]))
    return {"ok": True, "group": None, "released": [row["partition_key"]],
            "watermark_pushed": True, "rubric": None,
            "hint": "未声明跨分区依赖，按单分区规则放行并推水位"}


def _release_group(c, group, row, consumer: str, actor: str) -> dict:
    """乙口径：分区可独立放行；水位只在全员完整且全部成功放行后一次推进。"""
    members = _members(c, group["id"])
    active = next((m for m in members if m["partition_key"] == row["partition_key"]), None)
    if active is None or active["slice_id"] != row["id"]:
        with tx(c):
            c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                      (row["id"], consumer, "拒绝", "非依赖组当前生效切片，禁止放行", now_iso()))
            _record_event(c, group["biz_date"], group["id"], "依赖闸拒绝", actor,
                          f"切片 {row['slice_no']} 不是 {row['partition_key']} 的当前生效切片，"
                          "禁止绕过依赖组按旧切片放行")
        raise HTTPException(409, "该切片不是当前生效切片，禁止放行；请对当前生效切片操作")
    if not active["complete"]:
        with tx(c):
            c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                      (row["id"], consumer, "拒绝", "乙口径：本分区未完整，禁止放行", now_iso()))
            _record_event(c, group["biz_date"], group["id"], "依赖闸拒绝", actor,
                          f"乙口径分区放行被拒：{row['partition_key']} 未完整，水位不推进")
        raise HTTPException(409, "本分区未完整，禁止放行；依赖组水位不推进")

    if row["partition_key"] in _released_partitions(c, group["id"], consumer, members):
        raise HTTPException(409, "该分区已成功放行，禁止重复放行推水位")

    with tx(c):
        c.execute(
            "INSERT INTO group_releases(group_id,biz_date,consumer,partition_key,slice_id,status,note,actor,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (group["id"], group["biz_date"], consumer, active["partition_key"], active["slice_id"],
             "成功", f"乙口径：{group['group_key']} 分区 {active['partition_key']} 独立放行",
             actor, now_iso()))
        c.execute("UPDATE slices SET release_status='已放行' WHERE id=?", (active["slice_id"],))
        c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                  (active["slice_id"], consumer, "成功",
                   f"乙口径：依赖组 {group['group_key']} 分区独立放行", now_iso()))

        # 以事务内最新状态裁决是否允许一次推水位，杜绝“半员放行就推水位”。
        fresh = _members(c, group["id"])
        complete_all = all(m["complete"] for m in fresh)
        released = set(_released_partitions(c, group["id"], consumer, fresh))
        all_released = {m["partition_key"] for m in fresh} <= released
        pushed = False
        if complete_all and all_released:
            # 先把闸前不合规水位标记为已被纠正，避免同屏两条水位打架。
            superseded = _legacy_watermarks_for(c, group["biz_date"], group["id"])
            exists = c.execute(
                "SELECT 1 FROM watermarks WHERE group_id=? AND consumer=? AND biz_date=?",
                (group["id"], consumer, group["biz_date"])).fetchone()
            source = _SOURCE_JOIN.join(m["slice_no"] for m in fresh if m["slice_no"])
            if not exists:
                c.execute("INSERT INTO watermarks(consumer,biz_date,watermark,source_slice,group_id) "
                          "VALUES (?,?,?,?,?)",
                          (consumer, group["biz_date"], now_iso(), source, group["id"]))
                pushed = True
                for w in superseded:
                    if w["consumer"] == consumer:
                        c.execute("UPDATE watermarks SET superseded=1 WHERE id=?", (w["id"],))
            _record_event(c, group["biz_date"], group["id"], "水位推进", actor,
                          f"乙口径：{group['group_key']} 全员完整且全部成功放行，水位一次推进"
                          f"（来源 {source}）")
        else:
            missing = [m["partition_key"] for m in fresh if not m["complete"]]
            pending = sorted({m["partition_key"] for m in fresh} - released)
            detail = []
            if missing:
                detail.append(f"未完整:{'/'.join(missing)}")
            if pending:
                detail.append(f"未放行:{'/'.join(pending)}")
            _record_event(c, group["biz_date"], group["id"], "分区放行", actor,
                          f"乙口径：{active['partition_key']} 独立放行；组未齐（{'；'.join(detail)}），水位不推进")
    return {"ok": True, "group": group["group_key"], "rubric": RUBRIC,
            "released": [active["partition_key"]],
            "watermark_pushed": pushed,
            "hint": ("乙口径：全员完整且全部成功放行，水位已一次推进" if pushed
                     else "乙口径：分区已放行，但组未齐，水位不推进")}


# ---------------------------------------------------------------------------
# 放行流水与水位（同源核验）
# ---------------------------------------------------------------------------

@app.get("/api/releases")
def list_releases(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute(
            "SELECT r.*, s.slice_no, s.biz_date, s.partition_key FROM releases r "
            "JOIN slices s ON s.id=r.slice_id ORDER BY r.id DESC").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/watermarks")
def list_wm(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM watermarks ORDER BY id DESC").fetchall()
        items = []
        for w in rows:
            d = dict(w)
            group = None
            if w["group_id"] is not None:
                group = c.execute("SELECT * FROM dependency_groups WHERE id=?", (w["group_id"],)).fetchone()
            if group is None:
                s = c.execute("SELECT * FROM slices WHERE slice_no=?", (w["source_slice"],)).fetchone()
                if s:
                    group = _group_for_partition(c, s["biz_date"], s["partition_key"])
            if group is not None:
                status = _group_view(c, group)["status"]
                complete_all = all(m["complete"] for m in _members(c, group["id"]))
                released = set(_released_partitions(c, group["id"], w["consumer"], _members(c, group["id"])))
                all_released = {m["partition_key"] for m in _members(c, group["id"])} <= released
                ok, note = _watermark_gate_ok(w, group["id"], complete_all, all_released)
                if w["superseded"]:
                    ok, note = True, "闸前越界水位，已由依赖闸按口径重新推进，保留作审计"
                d.update({
                    "resolved_group_id": group["id"],
                    "group_key": group["group_key"],
                    "rubric": RUBRIC,
                    "gate_ok": ok,
                    "gate_note": note,
                    "group_status": status,
                    "legacy": w["group_id"] is None,
                })
            else:
                d.update({"resolved_group_id": None, "group_key": None, "rubric": None,
                          "gate_ok": True, "gate_note": "未声明跨分区依赖，单分区水位",
                          "group_status": "无依赖", "legacy": False})
            items.append(d)
    return {"items": items, "rubric": RUBRIC}


# ---------------------------------------------------------------------------
# 前端
# ---------------------------------------------------------------------------

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
