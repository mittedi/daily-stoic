"""FastAPI application for Daily Stack V4."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from server import db

DEFAULT_HABITS = [
    {"id": "walk", "name": "Gå dagligt", "type": "daily", "goal": 1},
    {"id": "run", "name": "Løb", "type": "weekly", "goal": 3},
    {"id": "strength", "name": "Styrketræning", "type": "weekly", "goal": 3},
    {"id": "eggs", "name": "2 æg", "type": "daily", "goal": 1},
    {"id": "sauerkraut", "name": "Surkål", "type": "daily", "goal": 1},
    {"id": "no_sugar", "name": "Ingen sukker", "type": "daily", "goal": 1},
    {"id": "meditation", "name": "Meditation 15 min", "type": "daily", "goal": 1},
    {"id": "bed_early", "name": "I seng 20:00–20:30", "type": "daily", "goal": 1},
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.seed_default_habits(DEFAULT_HABITS)
    yield


app = FastAPI(title="Daily Stack", version="5.0.0", lifespan=lifespan)

STATIC_DIR = Path(__file__).resolve().parent.parent


# --- Request models ---


class HabitRequest(BaseModel):
    date: str
    habit_id: str
    done: bool


class TextRequest(BaseModel):
    date: str
    text: str


class JournalRequest(BaseModel):
    date: str
    period: Literal["morning", "evening"]
    text: str


class HabitDefinitionRequest(BaseModel):
    id: str
    name: str
    type: Literal["daily", "weekly"] = "daily"
    goal: int = 1
    position: int = 0


# --- API endpoints ---


@app.post("/api/habits")
def post_habit(req: HabitRequest):
    db.upsert_habit(req.date, req.habit_id, req.done)
    return {"ok": True}


@app.get("/api/habits/{date}")
def get_habits(date: str):
    return db.get_habits(date)


@app.post("/api/focus")
def post_focus(req: TextRequest):
    db.upsert_focus(req.date, req.text)
    return {"ok": True}


@app.get("/api/focus/{date}")
def get_focus(date: str):
    return db.get_focus(date)


@app.post("/api/reflection")
def post_reflection(req: TextRequest):
    db.upsert_reflection(req.date, req.text)
    return {"ok": True}


@app.get("/api/reflection/{date}")
def get_reflection(date: str):
    return db.get_reflection(date)


@app.post("/api/journal")
def post_journal(req: JournalRequest):
    db.upsert_journal(req.date, req.period, req.text)
    return {"ok": True}


@app.get("/api/journal/{date}")
def get_journal(date: str):
    return db.get_journal(date)


@app.get("/api/export")
def export_all():
    return db.export_all()


# --- Habit definitions ---


@app.get("/api/habits-config")
def get_habits_config():
    return db.get_habit_definitions()


@app.post("/api/habits-config")
def post_habits_config(req: HabitDefinitionRequest):
    db.upsert_habit_definition(req.id, req.name, req.type, req.goal, req.position)
    return {"ok": True}


@app.delete("/api/habits-config/{habit_id}")
def delete_habits_config(habit_id: str):
    db.delete_habit_definition(habit_id)
    return {"ok": True}


# --- Review / Summaries ---


@app.get("/api/review/weekly/{week_start}")
def get_weekly_review(week_start: str):
    return db.get_weekly_summary(week_start)


@app.get("/api/review/monthly/{year}/{month}")
def get_monthly_review(year: int, month: int):
    return db.get_monthly_summary(year, month)


# --- Static file serving ---


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


# Mount static files for everything else (icons, manifest, sw.js, etc.)
app.mount("/", StaticFiles(directory=str(STATIC_DIR)), name="static")
