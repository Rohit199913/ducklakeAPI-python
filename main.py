import os
import secrets
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Depends
from pydantic import BaseModel
from database import get_conn, init_db

API_KEY = os.getenv("API_KEY", "your-secret-api-key")  # Byt ut mot en säker nyckel


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="DuckLake Weather API", lifespan=lifespan)


def verify_key(x_api_key: str = Header(...)):
    if not secrets.compare_digest(x_api_key.encode(), API_KEY.encode()):
        raise HTTPException(status_code=401, detail="Ogiltig API-nyckel")


# ── MODELS ────────────────────────────────────────────────────────────────────

class NyVader(BaseModel):
    datum: str
    stad: str
    temperatur: float


# ── VÄDER ────────────────────────────────────────────────────────────────────

@app.get("/api/vader")
def get_vader():
    with get_conn() as con:
        rows = con.execute("SELECT datum, stad, temperatur FROM lake.vader ORDER BY datum").fetchall()
    return [{"datum": str(r[0]), "stad": r[1], "temperatur": r[2]} for r in rows]


@app.post("/api/vader", status_code=201, dependencies=[Depends(verify_key)])
def ny_vader(v: NyVader):
    with get_conn() as con:
        con.execute("INSERT INTO lake.vader VALUES (?, ?, ?)", [v.datum, v.stad, v.temperatur])
    return {"datum": v.datum, "stad": v.stad, "temperatur": v.temperatur}


@app.delete("/api/vader/{datum}", dependencies=[Depends(verify_key)])
def radera_vader(datum: str):
    with get_conn() as con:
        con.execute("DELETE FROM lake.vader WHERE datum = ?", [datum])
    return {"deleted": datum}


# ── HEALTH ────────────────────────────────────────────────────────────────────

@app.get("/healthz")
def health():
    return {"status": "ok"}
