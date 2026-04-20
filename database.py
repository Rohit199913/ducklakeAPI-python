import duckdb
import os

POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "ducklake")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "duck")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
S3_KEY_ID   = os.getenv("S3_KEY_ID",   "minioadmin")
S3_SECRET   = os.getenv("S3_SECRET",   "minioadmin")
S3_BUCKET   = os.getenv("S3_BUCKET",   "ducklake")
S3_REGION   = os.getenv("S3_REGION",   "local")

def _ensure_bucket():
    if not S3_ENDPOINT:
        return
    from minio import Minio
    client = Minio(S3_ENDPOINT, access_key=S3_KEY_ID, secret_key=S3_SECRET, secure=False)
    if not client.bucket_exists(S3_BUCKET):
        client.make_bucket(S3_BUCKET)


def get_conn():
    _ensure_bucket()

    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake")
    con.execute("INSTALL postgres; LOAD postgres")

    # PORT hårdkodas till 5432 — undviker Kubernetes POSTGRES_PORT-konflikt
    con.execute(f"""
        CREATE OR REPLACE SECRET (
            TYPE postgres,
            HOST '{POSTGRES_HOST}',
            PORT 5432,
            DATABASE '{POSTGRES_DB}',
            USER '{POSTGRES_USER}',
            PASSWORD '{POSTGRES_PASSWORD}'
        )
    """)

    if S3_ENDPOINT:
        con.execute("INSTALL httpfs; LOAD httpfs")
        con.execute(f"""
            CREATE OR REPLACE SECRET (
                TYPE s3,
                KEY_ID '{S3_KEY_ID}',
                SECRET '{S3_SECRET}',
                REGION '{S3_REGION}',
                ENDPOINT '{S3_ENDPOINT}',
                URL_STYLE 'path',
                USE_SSL false
            )
        """)
        data_path = f"s3://{S3_BUCKET}/"
    else:
        data_path = os.getenv("DATA_PATH", "./data/lake/")
        os.makedirs(data_path, exist_ok=True)

    # Använd bara dbname i ATTACH — SECRET hanterar autentiseringen
    con.execute(f"""
        ATTACH 'ducklake:postgres:dbname={POSTGRES_DB}'
        AS lake (DATA_PATH '{data_path}')
    """)
    return con

def init_db():
    with get_conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS lake.vader (
                datum DATE,
                stad VARCHAR,
                temperatur DOUBLE
            )
        """)
        if con.execute("SELECT COUNT(*) FROM lake.vader").fetchone()[0] == 0:
            con.executemany("INSERT INTO lake.vader VALUES (?, ?, ?)", [
                ("2024-01-01", "Stockholm", -2.0),
                ("2024-07-01", "Göteborg", 22.5),
            ])
