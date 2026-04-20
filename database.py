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

def get_conn():
    con = duckdb.connect()
    con.execute("LOAD ducklake")
    con.execute("LOAD postgres")

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
        con.execute("LOAD httpfs")
        con.execute(f"""
            CREATE OR REPLACE SECRET (
                TYPE s3,
                KEY_ID '{S3_KEY_ID}',
                SECRET '{S3_SECRET}',
                ENDPOINT '{S3_ENDPOINT}',
                URL_STYLE 'path',
                USE_SSL false
            )
        """)
        data_path = f"s3://{S3_BUCKET}/"
    else:
        data_path = "./data/lake/"

    # Använd bara dbname i ATTACH — SECRET hanterar autentiseringen
    con.execute(f"""
        ATTACH 'ducklake:postgres:dbname={POSTGRES_DB}'
        AS lake (DATA_PATH '{data_path}')
    """)
    return con