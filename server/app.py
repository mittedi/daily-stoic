"""FastAPI application for Daily Stack V3."""

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from server import db

app = FastAPI(title="Daily Stack", version="3.0.0")

STATIC_DIR = Path(__file__).resolve().parent.parent


# --- Request models ---


class HabitRequest(BaseModel):
    date: str
    habit_id: str
    done: bool


class TextRequest(BaseModel):
    date: str
    text: str


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
    result = db.get_focus(date)
    if result is None:
        return {"text": "", "updated_at": None}
    return result


@app.post("/api/reflection")
def post_reflection(req: TextRequest):
    db.upsert_reflection(req.date, req.text)
    return {"ok": True}


@app.get("/api/reflection/{date}")
def get_reflection(date: str):
    result = db.get_reflection(date)
    if result is None:
        return {"text": "", "updated_at": None}
    return result


@app.get("/api/export")
def export_all():
    return db.export_all()


# --- Static file serving ---


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


# Mount static files for everything else (icons, manifest, sw.js, etc.)
app.mount("/", StaticFiles(directory=str(STATIC_DIR)), name="static")
