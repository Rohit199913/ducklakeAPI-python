from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from database import get_conn, init_db

app = FastAPI()

# Initiera databasen vid start
@app.on_event("startup")
def startup_event():
    init_db()

# API-nyckel för skydd
API_KEY = "your-secret-api-key"  # Byt ut mot en säker nyckel eller från env
api_key_header = APIKeyHeader(name="X-API-Key")

def verify_key(api_key: str = Depends(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return api_key

class NyVader(BaseModel):
    datum: str
    stad: str
    temperatur: float

@app.get("/api/vader")
def get_vader():
    con = get_conn()
    rows = con.execute("SELECT datum, stad, temperatur FROM lake.vader ORDER BY datum").fetchall()
    con.close()
    return [{"datum": str(r[0]), "stad": r[1], "temperatur": r[2]} for r in rows]

@app.post("/api/vader", status_code=201, dependencies=[Depends(verify_key)])
def ny_vader(v: NyVader):
    con = get_conn()
    con.execute("INSERT INTO lake.vader VALUES (?, ?, ?)", [v.datum, v.stad, v.temperatur])
    con.close()
    return {"datum": v.datum, "stad": v.stad, "temperatur": v.temperatur}

@app.delete("/api/vader/{datum}", dependencies=[Depends(verify_key)])
def radera_vader(datum: str):
    con = get_conn()
    con.execute("DELETE FROM lake.vader WHERE datum = ?", [datum])
    con.close()
    return {"deleted": datum}
