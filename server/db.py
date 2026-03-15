"""DuckDB storage layer for Daily Stack."""

import calendar
import os
from datetime import date, datetime, timedelta, timezone

import duckdb

DB_DIR = os.path.expanduser("~/data/daily-stack")
DB_PATH = os.path.join(DB_DIR, "daily-stack.duckdb")

_conn = None


def get_conn() -> duckdb.DuckDBPyConnection:
    """Return a module-level DuckDB connection, creating DB dir + schema if needed."""
    global _conn
    if _conn is not None:
        return _conn
    os.makedirs(DB_DIR, exist_ok=True)
    _conn = duckdb.connect(DB_PATH)
    _init_schema(_conn)
    return _conn


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS habits (
            date      VARCHAR NOT NULL,
            habit_id  VARCHAR NOT NULL,
            done      BOOLEAN NOT NULL DEFAULT true,
            toggled_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (date, habit_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS focus (
            date       VARCHAR PRIMARY KEY,
            text       VARCHAR NOT NULL DEFAULT '',
            updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reflections (
            date       VARCHAR PRIMARY KEY,
            text       VARCHAR NOT NULL DEFAULT '',
            updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS journal (
            date       VARCHAR NOT NULL,
            period     VARCHAR NOT NULL,
            text       VARCHAR NOT NULL DEFAULT '',
            updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (date, period)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS habit_definitions (
            id         VARCHAR PRIMARY KEY,
            name       VARCHAR NOT NULL,
            type       VARCHAR NOT NULL DEFAULT 'daily',
            goal       INTEGER NOT NULL DEFAULT 1,
            position   INTEGER NOT NULL DEFAULT 0,
            active     BOOLEAN NOT NULL DEFAULT true,
            created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- Habits ---


def upsert_habit(date: str, habit_id: str, done: bool) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO habits (date, habit_id, done, toggled_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (date, habit_id)
        DO UPDATE SET done = excluded.done, toggled_at = excluded.toggled_at
        """,
        [date, habit_id, done, _now()],
    )


def get_habits(date: str) -> dict[str, bool]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT habit_id, done FROM habits WHERE date = ?", [date]
    ).fetchall()
    return {row[0]: row[1] for row in rows if row[1]}


# --- Habit Definitions ---


def get_habit_definitions() -> list[dict]:
    """Return all active habit definitions ordered by position."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, name, type, goal, position FROM habit_definitions "
        "WHERE active = true ORDER BY position"
    ).fetchall()
    return [
        {"id": r[0], "name": r[1], "type": r[2], "goal": r[3], "position": r[4]}
        for r in rows
    ]


def upsert_habit_definition(
    id: str, name: str, type: str, goal: int, position: int
) -> None:
    """Insert or update a habit definition."""
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO habit_definitions (id, name, type, goal, position, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (id)
        DO UPDATE SET name = excluded.name, type = excluded.type,
                      goal = excluded.goal, position = excluded.position
        """,
        [id, name, type, goal, position, _now()],
    )


def delete_habit_definition(id: str) -> None:
    """Soft-delete a habit definition by setting active=false."""
    conn = get_conn()
    conn.execute("UPDATE habit_definitions SET active = false WHERE id = ?", [id])


def seed_default_habits(habits: list[dict]) -> None:
    """Seed default habits if the table is empty."""
    conn = get_conn()
    count = conn.execute("SELECT COUNT(*) FROM habit_definitions").fetchone()[0]
    if count > 0:
        return
    for i, h in enumerate(habits):
        conn.execute(
            """
            INSERT INTO habit_definitions (id, name, type, goal, position, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [h["id"], h["name"], h["type"], h["goal"], i, _now()],
        )


# --- Focus ---


def upsert_focus(date: str, text: str) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO focus (date, text, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT (date)
        DO UPDATE SET text = excluded.text, updated_at = excluded.updated_at
        """,
        [date, text, _now()],
    )


def get_focus(date: str) -> dict:
    conn = get_conn()
    row = conn.execute(
        "SELECT text, updated_at FROM focus WHERE date = ?", [date]
    ).fetchone()
    if not row:
        return {"text": "", "updated_at": None}
    return {"text": row[0], "updated_at": str(row[1])}


# --- Reflections ---


def upsert_reflection(date: str, text: str) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO reflections (date, text, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT (date)
        DO UPDATE SET text = excluded.text, updated_at = excluded.updated_at
        """,
        [date, text, _now()],
    )


def get_reflection(date: str) -> dict:
    conn = get_conn()
    row = conn.execute(
        "SELECT text, updated_at FROM reflections WHERE date = ?", [date]
    ).fetchone()
    if not row:
        return {"text": "", "updated_at": None}
    return {"text": row[0], "updated_at": str(row[1])}


# --- Journal ---


def upsert_journal(date: str, period: str, text: str) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO journal (date, period, text, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (date, period)
        DO UPDATE SET text = excluded.text, updated_at = excluded.updated_at
        """,
        [date, period, text, _now()],
    )


def get_journal(date: str) -> dict:
    conn = get_conn()
    rows = conn.execute(
        "SELECT period, text, updated_at FROM journal WHERE date = ?", [date]
    ).fetchall()
    result = {
        "morning": {"text": "", "updated_at": None},
        "evening": {"text": "", "updated_at": None},
    }
    for period, text, updated_at in rows:
        result[period] = {"text": text, "updated_at": str(updated_at)}
    return result


# --- Review / Summaries ---


def get_weekly_summary(week_start: str) -> dict:
    """Return habit completion counts per day for the week, plus journal/focus/reflection."""
    conn = get_conn()
    start = date.fromisoformat(week_start)
    dates = [(start + timedelta(days=i)).isoformat() for i in range(7)]

    # Habit completions per day
    rows = conn.execute(
        "SELECT date, habit_id, done FROM habits "
        "WHERE date >= ? AND date <= ? AND done = true",
        [dates[0], dates[-1]],
    ).fetchall()
    habits_by_day: dict[str, list[str]] = {d: [] for d in dates}
    for d, habit_id, _ in rows:
        if d in habits_by_day:
            habits_by_day[d].append(habit_id)

    # Journal entries
    journal_rows = conn.execute(
        "SELECT date, period, text FROM journal "
        "WHERE date >= ? AND date <= ? AND text != ''",
        [dates[0], dates[-1]],
    ).fetchall()
    journal_by_day: dict[str, dict[str, str]] = {}
    for d, period, text in journal_rows:
        journal_by_day.setdefault(d, {})[period] = text

    # Focus entries
    focus_rows = conn.execute(
        "SELECT date, text FROM focus WHERE date >= ? AND date <= ? AND text != ''",
        [dates[0], dates[-1]],
    ).fetchall()
    focus_by_day = {r[0]: r[1] for r in focus_rows}

    # Reflection entries
    reflection_rows = conn.execute(
        "SELECT date, text FROM reflections "
        "WHERE date >= ? AND date <= ? AND text != ''",
        [dates[0], dates[-1]],
    ).fetchall()
    reflections_by_day = {r[0]: r[1] for r in reflection_rows}

    return {
        "week_start": week_start,
        "dates": dates,
        "habits": habits_by_day,
        "journal": journal_by_day,
        "focus": focus_by_day,
        "reflections": reflections_by_day,
    }


def get_monthly_summary(year: int, month: int) -> dict:
    """Return habit completion rates per habit for the month."""
    conn = get_conn()
    days_in_month = calendar.monthrange(year, month)[1]
    first_day = date(year, month, 1).isoformat()
    last_day = date(year, month, days_in_month).isoformat()

    # All habit completions for the month
    rows = conn.execute(
        "SELECT date, habit_id FROM habits "
        "WHERE date >= ? AND date <= ? AND done = true",
        [first_day, last_day],
    ).fetchall()

    # Completions per habit
    completions_per_habit: dict[str, int] = {}
    active_dates: set[str] = set()
    daily_counts: dict[str, int] = {}
    for d, habit_id in rows:
        completions_per_habit[habit_id] = completions_per_habit.get(habit_id, 0) + 1
        active_dates.add(d)
        daily_counts[d] = daily_counts.get(d, 0) + 1

    # Completion rates (completions / days_in_month)
    completion_rates = {
        habit_id: round(count / days_in_month, 2)
        for habit_id, count in completions_per_habit.items()
    }

    return {
        "year": year,
        "month": month,
        "days_in_month": days_in_month,
        "total_active_days": len(active_dates),
        "completion_rates": completion_rates,
        "daily_counts": daily_counts,
    }


# --- Export ---


def export_all() -> dict:
    conn = get_conn()

    habits_rows = conn.execute(
        "SELECT date, habit_id, done, toggled_at FROM habits ORDER BY date, habit_id"
    ).fetchall()
    habits = {}
    for date, habit_id, done, toggled_at in habits_rows:
        habits.setdefault(date, {})[habit_id] = {
            "done": done,
            "toggled_at": str(toggled_at),
        }

    focus_rows = conn.execute(
        "SELECT date, text, updated_at FROM focus ORDER BY date"
    ).fetchall()
    focus = {row[0]: {"text": row[1], "updated_at": str(row[2])} for row in focus_rows}

    reflection_rows = conn.execute(
        "SELECT date, text, updated_at FROM reflections ORDER BY date"
    ).fetchall()
    reflections = {
        row[0]: {"text": row[1], "updated_at": str(row[2])} for row in reflection_rows
    }

    journal_rows = conn.execute(
        "SELECT date, period, text, updated_at FROM journal ORDER BY date, period"
    ).fetchall()
    journal = {}
    for date, period, text, updated_at in journal_rows:
        journal.setdefault(date, {})[period] = {
            "text": text,
            "updated_at": str(updated_at),
        }

    return {
        "habits": habits,
        "focus": focus,
        "reflections": reflections,
        "journal": journal,
    }
