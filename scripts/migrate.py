"""One-time migration: localStorage JSON export → DuckDB.

Usage:
    1. In browser console, run:
       JSON.stringify(Object.fromEntries(
         Object.entries(localStorage).filter(([k]) => k.startsWith("ds_"))
       ))
    2. Save output to a file, e.g. ~/data/daily-stack/localstorage-export.json
    3. Run: python scripts/migrate.py ~/data/daily-stack/localstorage-export.json
"""

import json
import sys
from pathlib import Path

# Add project root to path so we can import server.db
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from server import db


def migrate(export_path: str) -> None:
    data = json.loads(Path(export_path).read_text())

    habits_count = 0
    focus_count = 0
    reflection_count = 0

    for key, value in data.items():
        if not key.startswith("ds_"):
            continue

        # Parse key format: ds_{date}_{type}
        parts = key.split("_", 2)
        if len(parts) < 3:
            continue

        date_str = parts[1]
        data_type = parts[2]

        if data_type == "habits":
            habits = json.loads(value)
            for habit_id, done in habits.items():
                if done:
                    db.upsert_habit(date_str, habit_id, True)
                    habits_count += 1

        elif data_type == "focus":
            if value:
                db.upsert_focus(date_str, value)
                focus_count += 1

        elif data_type == "reflection":
            if value:
                db.upsert_reflection(date_str, value)
                reflection_count += 1

    print(
        f"Migrated: {habits_count} habits, {focus_count} focus entries, {reflection_count} reflections"
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <localstorage-export.json>")
        sys.exit(1)
    migrate(sys.argv[1])
