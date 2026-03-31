import os
import re
import io
import csv
import requests
import msal
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv("D:/MTW/.env.2g")

# =========================
# ENV - Graph (App-only)
# =========================
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

USER_UPN = os.getenv("USER_UPN")  # contoh: adminpbimatawari@pbimatawari.onmicrosoft.com
FOLDER_PATH = os.getenv("ONEDRIVE_FOLDER_PATH", "KPI")  # My files > KPI

# =========================
# ENV - PostgreSQL
# =========================
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")
PG_TABLE = os.getenv("PG_TABLE", "2G_Daily")

# =========================
# File filter
# - terima 2G_daily_*.csv dan 2G_daily *.csv
# =========================
PATTERN = re.compile(r"^2G_daily[\s_].*\.csv$", re.IGNORECASE)


# -------------------------
# Utils
# -------------------------
def require_env(*keys):
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        raise ValueError(f"ENV belum lengkap, missing: {missing}")


def pg_engine():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(url, pool_pre_ping=True)


def get_token() -> str:
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise RuntimeError(f"Gagal ambil token: {result}")
    return result["access_token"]


def graph_get(url: str, token: str) -> dict:
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    if r.status_code >= 400:
        raise RuntimeError(f"Graph error {r.status_code}: {r.text}")
    return r.json()


def list_all_items_in_folder(token: str):
    """
    List semua item di folder OneDrive root:/KPI:/children (handle pagination).
    """
    url = f"https://graph.microsoft.com/v1.0/users/{USER_UPN}/drive/root:/{FOLDER_PATH}:/children"
    items = []
    while url:
        data = graph_get(url, token)
        items.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    return items


def normalize_colname(c: str) -> str:
    c = (c or "").strip().lower()
    c = re.sub(r"\s+", "_", c)
    c = re.sub(r"[^a-z0-9_]", "_", c)
    c = re.sub(r"_+", "_", c).strip("_")
    return c if c else "col"


def clean_null_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ubah semua variasi null menjadi None:
    - pandas NaN
    - string: 'nan', 'NaN', 'NULL', 'null', 'none', '' (empty), whitespace-only
    Hasil: None -> saat COPY CSV jadi field kosong -> PostgreSQL NULL.
    """
    df = df.where(pd.notna(df), None)

    for col in df.columns:
        s = df[col].astype(object)

        # whitespace-only -> None
        s = s.apply(lambda x: None if isinstance(x, str) and x.strip() == "" else x)

        # null-like strings (case-insensitive)
        s = s.apply(
            lambda x: None
            if isinstance(x, str) and x.strip().lower() in {"nan", "null", "none"}
            else x
        )

        df[col] = s

    return df


def ensure_meta_table(conn):
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.ingest_log_files (
            id bigserial PRIMARY KEY,
            table_name text NOT NULL,
            source_file text NOT NULL,
            ingested_at timestamptz NOT NULL DEFAULT now(),
            row_count bigint,
            UNIQUE(table_name, source_file)
        );
    """))


def ensure_target_table(conn, columns):
    """
    Buat table target jika belum ada.
    Semua kolom TEXT dulu (staging-friendly).
    """
    cols_sql = ",\n".join([f'"{c}" text' for c in columns])
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {PG_SCHEMA}."{PG_TABLE}" (
            {cols_sql},
            source_file text,
            ingested_at timestamptz DEFAULT now()
        );
    """))


def get_existing_columns(conn):
    """
    Ambil daftar kolom yang sudah ada di table target.
    """
    q = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
    """)
    rows = conn.execute(q, {"schema": PG_SCHEMA, "table": PG_TABLE}).fetchall()
    return {r[0] for r in rows}


def add_missing_columns(conn, df_columns):
    """
    Jika ada kolom baru di file, tambahkan ke table (TEXT).
    """
    existing = get_existing_columns(conn)
    missing = [c for c in df_columns if c not in existing]

    for c in missing:
        conn.execute(text(f'ALTER TABLE {PG_SCHEMA}."{PG_TABLE}" ADD COLUMN IF NOT EXISTS "{c}" text;'))

    # pastikan kolom metadata ada
    conn.execute(text(f'ALTER TABLE {PG_SCHEMA}."{PG_TABLE}" ADD COLUMN IF NOT EXISTS "source_file" text;'))
    conn.execute(text(f'ALTER TABLE {PG_SCHEMA}."{PG_TABLE}" ADD COLUMN IF NOT EXISTS "ingested_at" timestamptz;'))


def already_ingested_log(conn, filename: str) -> bool:
    q = text(f"""
        SELECT 1
        FROM {PG_SCHEMA}.ingest_log_files
        WHERE table_name = :t AND source_file = :f
        LIMIT 1
    """)
    return conn.execute(q, {"t": PG_TABLE, "f": filename}).first() is not None


def already_ingested_data(conn, filename: str) -> bool:
    """
    Cek berdasarkan data table (safety).
    """
    # kalau table belum ada, berarti belum ingest
    q_exists = text("""
        SELECT to_regclass(:tbl) IS NOT NULL
    """)
    tbl_name = f'{PG_SCHEMA}."{PG_TABLE}"'
    exists = conn.execute(q_exists, {"tbl": tbl_name}).scalar()
    if not exists:
        return False

    q = text(f"""
        SELECT 1
        FROM {PG_SCHEMA}."{PG_TABLE}"
        WHERE source_file = :f
        LIMIT 1
    """)
    return conn.execute(q, {"f": filename}).first() is not None


def mark_ingested(conn, filename: str, row_count: int):
    q = text(f"""
        INSERT INTO {PG_SCHEMA}.ingest_log_files(table_name, source_file, row_count)
        VALUES(:t, :f, :rc)
        ON CONFLICT (table_name, source_file) DO NOTHING
    """)
    conn.execute(q, {"t": PG_TABLE, "f": filename, "rc": row_count})


def read_csv_from_download_url(download_url: str) -> pd.DataFrame:
    """
    Download CSV dari downloadUrl -> pandas.
    keep_default_na=False: biar string "nan" tidak otomatis jadi NaN semua.
    """
    r = requests.get(download_url, stream=True)
    r.raise_for_status()

    content = r.content
    df = pd.read_csv(
        io.BytesIO(content),
        dtype=str,
        low_memory=False,
        keep_default_na=False,
        na_values=["NaN", "nan", "NULL", "null", "None", "none"]
    )
    return df


def copy_df_to_pg(engine, df: pd.DataFrame, source_file: str) -> int:
    """
    Fast load via COPY (CSV).
    None akan ditulis jadi field kosong -> PostgreSQL NULL.
    Table harus sudah punya semua kolom df + source_file.
    """
    df2 = df.copy()
    df2["source_file"] = source_file

    cols = list(df2.columns)

    buf = io.StringIO()
    df2.to_csv(
        buf,
        index=False,
        header=False,
        sep=",",
        quoting=csv.QUOTE_MINIMAL,
        na_rep=""  # None -> kosong
    )
    buf.seek(0)

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            col_list = ", ".join([f'"{c}"' for c in cols])
            sql = f'COPY {PG_SCHEMA}."{PG_TABLE}" ({col_list}) FROM STDIN WITH (FORMAT CSV)'
            cur.copy_expert(sql, buf)
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()

    return len(df2)


# -------------------------
# MAIN
# -------------------------
def main():
    require_env(
        "TENANT_ID", "CLIENT_ID", "CLIENT_SECRET",
        "USER_UPN", "ONEDRIVE_FOLDER_PATH",
        "PG_DB", "PG_USER", "PG_PASSWORD"
    )

    print("== Start ingest 2G_daily* (Graph) -> PostgreSQL ==")

    token = get_token()
    print("✅ Token OK")

    engine = pg_engine()

    # ensure meta log table
    with engine.begin() as conn:
        ensure_meta_table(conn)

    # list items from folder
    items = list_all_items_in_folder(token)
    print(f"Total item di folder '{FOLDER_PATH}': {len(items)}")

    # filter LTE files
    files = []
    ignored = 0
    for it in items:
        name = it.get("name", "")
        if not PATTERN.match(name):
            ignored += 1
            continue
        if "file" not in it:
            continue
        files.append(it)

    files = sorted(files, key=lambda x: x.get("name", ""))
    print(f"✅ Ketemu {len(files)} file 2G_daily*.csv (ignored non-match: {ignored})")

    if not files:
        print("Tidak ada file yang match pattern. Selesai.")
        return

    processed = 0
    skipped = 0

    for f in files:
        filename = f["name"]

        # skip jika sudah ada di log atau data
        with engine.begin() as conn:
            if already_ingested_log(conn, filename) or already_ingested_data(conn, filename):
                print(f"[SKIP] sudah ada di DB: {filename}")
                skipped += 1
                continue

        download_url = f.get("@microsoft.graph.downloadUrl")
        if not download_url:
            print(f"[SKIP] tidak ada downloadUrl: {filename}")
            skipped += 1
            continue

        print(f"[DOWNLOAD+READ] {filename}")
        df = read_csv_from_download_url(download_url)

        # normalize columns
        orig_cols = list(df.columns)
        norm_cols = []
        seen = set()
        for c in orig_cols:
            nc = normalize_colname(c)
            base = nc
            i = 2
            while nc in seen:
                nc = f"{base}_{i}"
                i += 1
            seen.add(nc)
            norm_cols.append(nc)
        df.columns = norm_cols

        # clean null-like values
        df = clean_null_values(df)

        # ensure table exists + add missing columns (based on this file)
        with engine.begin() as conn:
            ensure_target_table(conn, df.columns)
            add_missing_columns(conn, df.columns)

        # COPY to postgres
        print(f"[COPY] rows={len(df)} -> {PG_SCHEMA}.{PG_TABLE}")
        inserted = copy_df_to_pg(engine, df, filename)

        # mark ingest AFTER success
        with engine.begin() as conn:
            mark_ingested(conn, filename, inserted)

        print(f"[DONE] {filename} inserted={inserted}")
        processed += 1

    print("== Summary ==")
    print(f"Processed: {processed}")
    print(f"Skipped:   {skipped}")
    print("== All done ==")


if __name__ == "__main__":
    main()
