import duckdb
import os

# Miljövariabler för PostgreSQL (Katalogen)
POSTGRES_HOST     = os.getenv("POSTGRES_HOST",     "localhost")
POSTGRES_DB       = os.getenv("POSTGRES_DB",       "ducklake")
POSTGRES_USER     = os.getenv("POSTGRES_USER",     "duck")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# Miljövariabler för MinIO (Lagringen)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
S3_KEY_ID   = os.getenv("S3_KEY_ID",   "minioadmin")
S3_SECRET   = os.getenv("S3_SECRET",   "minioadmin")
S3_BUCKET   = os.getenv("S3_BUCKET",   "ducklake")
S3_REGION   = os.getenv("S3_REGION",   "local")

def _ensure_bucket():
    """Ser till att mappen (bucket) finns i MinIO innan vi startar."""
    if not S3_ENDPOINT:
        return
    from minio import Minio
    # Vi använder secure=False eftersom vi kör internt i KTH Cloud utan HTTPS
    client = Minio(S3_ENDPOINT, access_key=S3_KEY_ID, secret_key=S3_SECRET, secure=False)
    if not client.bucket_exists(S3_BUCKET):
        client.make_bucket(S3_BUCKET)

def get_conn() -> duckdb.DuckDBPyConnection:
    """Skapar en anslutning till DuckLake genom att koppla ihop Postgres och MinIO."""
    _ensure_bucket()

    con = duckdb.connect()
    # Installerar och laddar nödvändiga tillägg
    con.execute("INSTALL ducklake; LOAD ducklake")
    con.execute("INSTALL postgres;  LOAD postgres")

    # VIKTIGT: Vi hårdkodar PORT 5432 här för att undvika att Kubernetes 
    # skriver över våra inställningar med felaktig data
    con.execute(f"""
        CREATE OR REPLACE SECRET (
            TYPE     postgres,
            HOST     '{POSTGRES_HOST}',
            PORT     5432,
            DATABASE '{POSTGRES_DB}',
            USER     '{POSTGRES_USER}',
            PASSWORD '{POSTGRES_PASSWORD}'
        )
    """)

    if S3_ENDPOINT:
        # Om vi är i molnet: Använd S3-lagring
        con.execute("INSTALL httpfs; LOAD httpfs")
        con.execute(f"""
            CREATE OR REPLACE SECRET (
                TYPE      s3,
                KEY_ID    '{S3_KEY_ID}',
                SECRET    '{S3_SECRET}',
                REGION    '{S3_REGION}',
                ENDPOINT  '{S3_ENDPOINT}',
                URL_STYLE 'path',
                USE_SSL   false
            )
        """)
        data_path = f"s3://{S3_BUCKET}/"
    else:
        # Om vi kör lokalt: Spara filer i en lokal mapp
        data_path = os.getenv("DATA_PATH", "./data/lake/")
        os.makedirs(data_path, exist_ok=True)

    # Kopplar ihop allt under namnet 'lake'
    con.execute(f"""
        ATTACH 'ducklake:postgres:dbname={POSTGRES_DB}'
        AS lake (DATA_PATH '{data_path}')
    """)

    return con

def init_db():
    """Skapar de grundläggande tabellerna i din Data Lake om de inte redan finns."""
    with get_conn() as con:
        # Vi använder prefixet 'lake.' för att DuckDB ska veta att dessa ska 
        # sparas permanent i molnet
        con.execute("""
            CREATE TABLE IF NOT EXISTS lake.kunder (
                id INTEGER, namn VARCHAR NOT NULL,
                email VARCHAR NOT NULL, telefon VARCHAR
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS lake.produkter (
                id INTEGER, namn VARCHAR NOT NULL,
                pris DOUBLE NOT NULL, lagersaldo INTEGER DEFAULT 0
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS lake.ordrar (
                id INTEGER, kund_id INTEGER, produkt_id INTEGER,
                antal INTEGER NOT NULL, skapad TIMESTAMP DEFAULT current_timestamp
            )
        """)