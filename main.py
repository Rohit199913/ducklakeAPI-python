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
