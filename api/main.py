import os
import secrets
import tempfile
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Depends, UploadFile, File, Form
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from database import get_conn, init_db

# Hämtar din hemliga API-nyckel från KTH Cloud-inställningarna
API_KEY = os.getenv("API_KEY", "change-me")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Körs när appen startar: skapar tabeller och lägger in start-data"""
    init_db()
    with get_conn() as con:
        # Kolla om tabellen kunder är tom – om ja, lägg in 'seed-data'
        if con.execute("SELECT COUNT(*) FROM lake.kunder").fetchone()[0] == 0:
            con.executemany("INSERT INTO lake.kunder VALUES (?, ?, ?, ?)", [
                (1, "Anna Svensson",   "anna@example.com",  "070-1234567"),
                (2, "Erik Johansson",  "erik@example.com",  "073-9876543"),
                (3, "Maria Lindqvist", "maria@example.com", "076-5551234"),
            ])
            # Lägger även in produkter och ordrar för att appen ska fungera direkt
            con.executemany("INSERT INTO lake.produkter VALUES (?, ?, ?, ?)", [
                (1, "Laptop",      9999.0, 15),
                (2, "Hörlurar",     799.0, 50),
                (3, "Tangentbord", 1299.0, 30),
            ])
            con.executemany("INSERT INTO lake.ordrar (id, kund_id, produkt_id, antal) VALUES (?, ?, ?, ?)", [
                (1, 1, 1, 1), (2, 1, 2, 2), (3, 2, 3, 1),
            ])
    yield

app = FastAPI(title="DuckLake Cloud API", lifespan=lifespan)

def verify_key(x_api_key: str = Header(...)):
    """Säkerhetskontroll: Jämför nyckeln i anropet med din miljövariabel"""
    if not secrets.compare_digest(x_api_key.encode(), API_KEY.encode()):
        raise HTTPException(status_code=401, detail="Ogiltig API-nyckel")

# ── MODELLER ──────────────────────────────────────────────────────────────────

class NyKund(BaseModel):
    namn: str
    email: str
    telefon: Optional[str] = None

class NyProdukt(BaseModel):
    namn: str
    pris: float
    lagersaldo: Optional[int] = 0

class NyOrder(BaseModel):
    kund_id: int
    produkt_id: int
    antal: int

# ── KUNDER ────────────────────────────────────────────────────────────────────

@app.get("/api/kunder")
def get_kunder():
    with get_conn() as con:
        rows = con.execute("SELECT id, namn, email, telefon FROM lake.kunder ORDER BY id").fetchall()
    return [{"id": r[0], "namn": r[1], "email": r[2], "telefon": r[3]} for r in rows]

@app.post("/api/kunder", status_code=201, dependencies=[Depends(verify_key)])
def ny_kund(kund: NyKund):
    with get_conn() as con:
        nid = con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM lake.kunder").fetchone()[0]
        con.execute("INSERT INTO lake.kunder VALUES (?, ?, ?, ?)", [nid, kund.namn, kund.email, kund.telefon])
    return {"id": nid, "namn": kund.namn, "email": kund.email}

@app.delete("/api/kunder/{kund_id}", dependencies=[Depends(verify_key)])
def radera_kund(kund_id: int):
    with get_conn() as con:
        con.execute("DELETE FROM lake.kunder WHERE id = ?", [kund_id])
    return {"deleted": kund_id}

# ── PRODUKTER (Dessa saknades i din förra version!) ──────────────────────────

@app.get("/api/produkter")
def get_produkter():
    """Hämtar alla produkter"""
    with get_conn() as con:
        rows = con.execute("SELECT id, namn, pris, lagersaldo FROM lake.produkter ORDER BY id").fetchall()
    return [{"id": r[0], "namn": r[1], "pris": r[2], "lagersaldo": r[3]} for r in rows]

@app.post("/api/produkter", status_code=201, dependencies=[Depends(verify_key)])
def ny_produkt(produkt: NyProdukt):
    """Skapar en ny produkt"""
    with get_conn() as con:
        nid = con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM lake.produkter").fetchone()[0]
        con.execute("INSERT INTO lake.produkter VALUES (?, ?, ?, ?)", [nid, produkt.namn, produkt.pris, produkt.lagersaldo])
    return {"id": nid, "namn": produkt.namn, "pris": produkt.pris}

@app.delete("/api/produkter/{produkt_id}", dependencies=[Depends(verify_key)])
def radera_produkt(produkt_id: int):
    """Tar bort en produkt"""
    with get_conn() as con:
        con.execute("DELETE FROM lake.produkter WHERE id = ?", [produkt_id])
    return {"deleted": produkt_id}

# ── ORDRAR (Dessa saknades också!) ──────────────────────────────────────────

@app.get("/api/ordrar")
def get_ordrar():
    """Hämtar alla ordrar med kund- och produktnamn"""
    with get_conn() as con:
        rows = con.execute("""
            SELECT o.id, k.namn, p.namn, o.antal, o.skapad
            FROM lake.ordrar o
            JOIN lake.kunder k    ON k.id = o.kund_id
            JOIN lake.produkter p ON p.id = o.produkt_id
            ORDER BY o.id
        """).fetchall()
    return [{"id": r[0], "kund": r[1], "produkt": r[2], "antal": r[3], "skapad": str(r[4])} for r in rows]

@app.post("/api/ordrar", status_code=201, dependencies=[Depends(verify_key)])
def ny_order(order: NyOrder):
    """Skapar en ny order"""
    with get_conn() as con:
        nid = con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM lake.ordrar").fetchone()[0]
        con.execute("INSERT INTO lake.ordrar (id, kund_id, produkt_id, antal) VALUES (?, ?, ?, ?)",
                    [nid, order.kund_id, order.produkt_id, order.antal])
    return {"id": nid, "kund_id": order.kund_id, "produkt_id": order.produkt_id, "antal": order.antal}

# ── DATASETS & UPLOAD ─────────────────────────────────────────────────────────

@app.get("/api/datasets")
def lista_datasets():
    with get_conn() as con:
        tabeller = con.execute("SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake'").fetchall()
    return [{"namn": r[0]} for r in tabeller]

@app.post("/api/datasets/upload", status_code=201, dependencies=[Depends(verify_key)])
async def ladda_upp(fil: UploadFile = File(...), tabellnamn: str = Form(...)):
    if not tabellnamn.isidentifier():
        raise HTTPException(status_code=400, detail="Ogiltigt tabellnamn")
    suffix = ".csv" if fil.filename.endswith(".csv") else ".parquet"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(await fil.read())
        tmp_path = tmp.name
    try:
        with get_conn() as con:
            if suffix == ".csv":
                con.execute(f"CREATE TABLE lake.{tabellnamn} AS SELECT * FROM read_csv_auto(?)", [tmp_path])
            else:
                con.execute(f"CREATE TABLE lake.{tabellnamn} AS SELECT * FROM read_parquet(?)", [tmp_path])
    finally:
        os.unlink(tmp_path)
    return {"namn": tabellnamn, "status": "skapad i Data Lake"}

# ── HEALTH ────────────────────────────────────────────────────────────────────

@app.get("/healthz")
def health():
    return {"status": "ok"}