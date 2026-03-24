"""
ETL Pipeline v2 — SF Housing Intelligence
------------------------------------------
Pulls real SF Open Data, runs clustering, computes risk scores,
writes processed results to:
  - RDS Postgres (queryable tables)
  - S3 (raw snapshot archive)

Run locally for v1. Promote to Lambda later if it stays lean.

Usage:
    pip install pandas scikit-learn psycopg2-binary boto3 requests python-dotenv
    python etl.py

Env vars (put in .env):
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    S3_BUCKET, AWS_REGION
"""

import os
import json
import datetime
import argparse
import requests
import boto3
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# SF Open Data endpoints — verified dataset IDs
# ---------------------------------------------------------------------------

# DBI Complaints (All Divisions) — gm2e-bten
# Housing, Building, Electrical, Plumbing, Code Enforcement complaints.
# Actively updated. Columns confirmed from DataSF schema.
VIOLATIONS_URL = (
    "https://data.sfgov.org/resource/gm2e-bten.json"
    "?$limit=50000"
    "&$select=complaint_number,block,lot,parcel_number,street_number,street_name,"
    "street_suffix,zip_code,complaint_description,status,date_filed,date_abated,"
    "analysis_neighborhood,point"
    "&$order=date_filed DESC"
)

# Assessor Historical Secured Property Tax Rolls — wv5m-vpq2
# Filter to most recent closed roll year (2024 = FY July 2023–June 2024).
# Columns confirmed from raw CSV export:
#   parcel_number, block, lot, property_location, year_property_built,
#   number_of_units, assessed_land_value, assessed_improvement_value,
#   assessed_fixtures_value, property_class_code_definition,
#   analysis_neighborhood, the_geom (GeoJSON point — lat/lng included!)
ASSESSMENTS_URL = (
    "https://data.sfgov.org/resource/wv5m-vpq2.json"
    "?$limit=50000"
    "&$where=closed_roll_year='2024'"
    "&$select=parcel_number,block,lot,property_location,year_property_built,"
    "number_of_units,assessed_land_value,assessed_improvement_value,"
    "assessed_fixtures_value,property_class_code_definition,"
    "analysis_neighborhood,the_geom"
)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db_conn():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", 5432),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        sslmode="require",  # RDS requires SSL
    )


def run_migrations(conn):
    """Create tables if they don't exist."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE EXTENSION IF NOT EXISTS postgis;

            -- Buildings / properties
            CREATE TABLE IF NOT EXISTS buildings (
                blklot          TEXT PRIMARY KEY,
                address         TEXT,
                neighborhood    TEXT,
                units           INTEGER,
                year_built      INTEGER,
                assessed_value  INTEGER,
                property_type   TEXT,
                risk_score      NUMERIC(4,2),
                cluster         INTEGER,
                lat             NUMERIC(9,6),
                lng             NUMERIC(9,6),
                -- PostGIS geometry for future spatial queries
                geom            GEOMETRY(Point, 4326),
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_buildings_neighborhood
                ON buildings(neighborhood);
            CREATE INDEX IF NOT EXISTS idx_buildings_risk
                ON buildings(risk_score DESC);
            CREATE INDEX IF NOT EXISTS idx_buildings_geom
                ON buildings USING GIST(geom);

            -- Violations
            DROP TABLE IF EXISTS violations;
            CREATE TABLE IF NOT EXISTS violations (
                id              SERIAL PRIMARY KEY,
                blklot          TEXT,
                address         TEXT,
                neighborhood    TEXT,
                complaint_type  TEXT,
                status          TEXT,
                date_filed      DATE,
                date_closed     DATE,
                disposition     TEXT,
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_violations_blklot
                ON violations(blklot);
            CREATE INDEX IF NOT EXISTS idx_violations_neighborhood
                ON violations(neighborhood);
            CREATE INDEX IF NOT EXISTS idx_violations_status
                ON violations(status);

            -- Neighborhood summaries (pre-aggregated for fast dashboard loads)
            DROP TABLE IF EXISTS neighborhood_stats;
            CREATE TABLE IF NOT EXISTS neighborhood_stats (
                neighborhood        TEXT PRIMARY KEY,
                total_buildings     INTEGER,
                total_violations    INTEGER,
                open_violations     INTEGER,
                violation_rate      NUMERIC(8,2),
                median_risk_score   NUMERIC(4,2),
                median_assessed     INTEGER,
                cluster             INTEGER,
                top_violation_type  TEXT,
                updated_at          TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        conn.commit()
    print("Migrations complete.")


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_json(url, label):
    print(f"Fetching {label}...")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = pd.DataFrame(r.json())
    print(f"  → {len(df)} rows")
    return df


# ---------------------------------------------------------------------------
# Clean & transform
# ---------------------------------------------------------------------------

def clean_violations(df):
    df.columns = [c.lower() for c in df.columns]
    df = df.rename(columns={
        "analysis_neighborhood": "neighborhood",
        "date_abated": "date_closed",
        "complaint_description": "complaint_type",
        "point": "the_geom",
    })

    df["neighborhood"] = df["neighborhood"].str.strip().str.title().fillna("Unknown")
    df["date_filed"] = pd.to_datetime(df.get("date_filed"), errors="coerce").dt.date
    df["date_closed"] = pd.to_datetime(df.get("date_closed"), errors="coerce").dt.date
    df["status"] = df["status"].str.strip().str.lower().fillna("unknown")
    # Normalize to open/closed
    df["status"] = df["status"].map({
        "active": "open",
        "not active": "closed",
    }).fillna(df["status"])
    df["complaint_type"] = df["complaint_type"].str.strip().str.title().fillna("Other")

    # Build address from components
    df["address"] = (
        df["street_number"].fillna("") + " " +
        df["street_name"].fillna("") + " " +
        df["street_suffix"].fillna("")
    ).str.strip()

    # blklot from parcel_number (already in BBBBLL format)
    df["blklot"] = df["parcel_number"].astype(str).str.strip().str.zfill(7)

    return df[["blklot", "address", "neighborhood", "complaint_type", "status",
               "date_filed", "date_closed"]].copy()


def clean_assessments(df):
    df.columns = [c.lower() for c in df.columns]

    # wv5m-vpq2 column mapping (confirmed from CSV export):
    #   parcel_number           → blklot (format: BBBBLLLL, 7 chars, no separator)
    #   property_location       → address
    #   analysis_neighborhood   → neighborhood (cleaner than assessor_neighborhood_district)
    #   number_of_units         → units
    #   year_property_built     → year_built
    #   property_class_code_definition → property_type
    #   the_geom                → GeoJSON Point with coordinates [lng, lat]
    df = df.rename(columns={
        "parcel_number":                 "blklot",
        "property_location":             "address",
        "analysis_neighborhood":         "neighborhood",
        "number_of_units":               "units",
        "year_property_built":           "year_built",
        "property_class_code_definition":"property_type",
    })

    df["neighborhood"] = df["neighborhood"].str.strip().str.title().fillna("Unknown")
    df["units"] = pd.to_numeric(df.get("units"), errors="coerce").fillna(0).astype(int)
    df["year_built"] = pd.to_numeric(df.get("year_built"), errors="coerce")

    # Assessed value = land + improvement + fixtures
    for col in ["assessed_land_value", "assessed_improvement_value", "assessed_fixtures_value"]:
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)
    df["assessed_value"] = (
        df["assessed_land_value"] + df["assessed_improvement_value"] + df["assessed_fixtures_value"]
    ).astype(int)

    # Extract lat/lng from the_geom GeoJSON column.
    # the_geom arrives as a dict: {"type": "Point", "coordinates": [lng, lat]}
    def parse_geom(g):
        try:
            if isinstance(g, dict) and g.get("type") == "Point":
                coords = g["coordinates"]
                return float(coords[1]), float(coords[0])  # lat, lng
            if isinstance(g, str):
                import json as _json
                obj = _json.loads(g)
                coords = obj["coordinates"]
                return float(coords[1]), float(coords[0])
        except Exception:
            pass
        return None, None

    if "the_geom" in df.columns:
        df[["lat", "lng"]] = pd.DataFrame(
            df["the_geom"].apply(parse_geom).tolist(), index=df.index
        )
    else:
        df["lat"] = None
        df["lng"] = None

    # blklot format from parcel_number: already "BBBBLLL" — ensure 7 chars, no separator
    df["blklot"] = df["blklot"].astype(str).str.strip().str.replace("-", "")

    return df[["blklot", "address", "neighborhood", "units", "year_built",
               "assessed_value", "property_type", "lat", "lng"]].dropna(subset=["blklot"])


# ---------------------------------------------------------------------------
# Risk scoring & clustering
# ---------------------------------------------------------------------------

def compute_risk_scores(buildings: pd.DataFrame, violations: pd.DataFrame) -> pd.DataFrame:
    """
    Risk score (0–10) — v4 fix.

    Bug in original: dividing all violation signals by units collapsed variance.
    A 200-unit building with 100 violations scored the same as a house with 1.
    Since most SF buildings are multi-unit, all scores clustered near 0.

    Fix: log-scale absolute counts (dampens outliers without destroying signal),
    then normalize using 1st–99th percentile so the full 0–10 range is used.
    """
    SEVERITY = {
        "Habitability": 3.0, "Fire Safety": 3.0, "Lead/Asbestos": 3.0,
        "Structural": 2.5, "Electrical": 2.5, "Plumbing/Water": 2.0,
        "Mold/Moisture": 2.0, "Pest Infestation": 1.5, "Garbage/Sanitation": 1.0,
    }

    # Absolute violation counts per building (no per-unit division)
    open_viol = (
        violations[violations["status"] == "open"]
        .groupby("blklot")
        .size()
        .rename("open_count")
    )
    severity_score = (
        violations.assign(sev=violations["complaint_type"].map(SEVERITY).fillna(1.0))
        .groupby("blklot")["sev"]
        .sum()
        .rename("severity_sum")
    )
    recent_count = (
        violations[violations["date_filed"] >= datetime.date.today() - datetime.timedelta(days=730)]
        .groupby("blklot")
        .size()
        .rename("recent_count")
    )

    df = buildings.join(open_viol, on="blklot").join(severity_score, on="blklot").join(recent_count, on="blklot")
    df[["open_count", "severity_sum", "recent_count"]] = df[["open_count", "severity_sum", "recent_count"]].fillna(0)

    age = (datetime.date.today().year - df["year_built"].fillna(1960)).clip(lower=0)
    age_score = (age / 150).clip(upper=1)

    # Log-scale counts to dampen extreme outliers
    # log1p(0)=0, log1p(1)≈0.69, log1p(10)≈2.4, log1p(100)≈4.6
    log_open     = np.log1p(df["open_count"])
    log_severity = np.log1p(df["severity_sum"])
    log_recent   = np.log1p(df["recent_count"])

    raw = (
        log_open     * 3.5 +
        log_severity * 2.5 +
        log_recent   * 2.0 +
        age_score    * 2.0
    )

    # Normalize using 1st–99th percentile so full 0–10 range is used
    p01 = raw.quantile(0.01)
    p99 = raw.quantile(0.99)
    df["risk_score"] = ((raw.clip(p01, p99) - p01) / (p99 - p01 + 1e-9) * 10).round(2)

    print(f"  Risk scores: min={df['risk_score'].min():.2f}, "
          f"mean={df['risk_score'].mean():.2f}, "
          f"max={df['risk_score'].max():.2f}")
    print(f"  Score > 5: {(df['risk_score'] > 5).sum()} buildings")
    print(f"  Score > 7: {(df['risk_score'] > 7).sum()} buildings")

    return df


def run_clustering(df: pd.DataFrame, k: int = 3) -> pd.DataFrame:
    features = df[["risk_score", "units", "assessed_value"]].fillna(0)
    X = StandardScaler().fit_transform(features)
    df["cluster"] = KMeans(n_clusters=k, random_state=42, n_init=10).fit_predict(X)
    # Sort clusters by mean risk: 0 = lowest, 2 = highest
    cluster_risk = df.groupby("cluster")["risk_score"].mean().sort_values()
    remap = {old: new for new, old in enumerate(cluster_risk.index)}
    df["cluster"] = df["cluster"].map(remap)
    return df


# ---------------------------------------------------------------------------
# Aggregate neighborhood stats
# ---------------------------------------------------------------------------

def aggregate_neighborhoods(buildings: pd.DataFrame, violations: pd.DataFrame) -> pd.DataFrame:
    nbhd_buildings = buildings.groupby("neighborhood").agg(
        total_buildings=("blklot", "count"),
        median_risk_score=("risk_score", "median"),
        median_assessed=("assessed_value", "median"),
        cluster=("cluster", lambda x: int(x.mode()[0]) if len(x) > 0 else 0),
    ).reset_index()

    nbhd_violations = violations.groupby("neighborhood").agg(
        total_violations=("id" if "id" in violations.columns else "blklot", "count"),
        open_violations=("status", lambda s: (s == "open").sum()),
        top_violation_type=("complaint_type", lambda s: s.value_counts().idxmax()),
    ).reset_index()

    # violation rate = open violations per building
    df = nbhd_buildings.merge(nbhd_violations, on="neighborhood", how="left").fillna(0)
    df["violation_rate"] = (df["open_violations"] / df["total_buildings"].clip(lower=1) * 100).round(2)
    return df


# ---------------------------------------------------------------------------
# Write to Postgres
# ---------------------------------------------------------------------------

def upsert_buildings(conn, df: pd.DataFrame):
    rows = []
    for _, row in df.iterrows():
        lat = float(row.lat) if pd.notna(row.get("lat")) else None
        lng = float(row.lng) if pd.notna(row.get("lng")) else None
        # Build PostGIS POINT only when we have both coords
        geom = f"SRID=4326;POINT({lng} {lat})" if lat and lng else None
        rows.append((
            row.blklot,
            row.get("address", "") or "",
            row.neighborhood,
            int(row.units),
            int(row.year_built) if pd.notna(row.year_built) else None,
            int(row.assessed_value),
            row.get("property_type", "") or "",
            float(row.risk_score),
            int(row.cluster),
            lat,
            lng,
            geom,
        ))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO buildings (blklot, address, neighborhood, units, year_built,
                assessed_value, property_type, risk_score, cluster, lat, lng, geom)
            VALUES %s
            ON CONFLICT (blklot) DO UPDATE SET
                risk_score     = EXCLUDED.risk_score,
                cluster        = EXCLUDED.cluster,
                assessed_value = EXCLUDED.assessed_value,
                lat            = EXCLUDED.lat,
                lng            = EXCLUDED.lng,
                geom           = EXCLUDED.geom,
                updated_at     = NOW()
        """, rows, template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_GeomFromEWKT(%s))")
    conn.commit()
    print(f"Upserted {len(rows)} buildings ({sum(1 for r in rows if r[10]) } with coordinates).")


def upsert_violations(conn, df: pd.DataFrame):
    with conn.cursor() as cur:
        cur.execute("SELECT blklot FROM buildings")
        valid_blklots = {row[0] for row in cur.fetchall()}
    rows = []
    for _, row in df.iterrows():
        rows.append((
            row.blklot,
            row.address,
            row.neighborhood,
            row.complaint_type,
            row.status,
            row.date_filed if pd.notna(row.date_filed) else None,
            row.date_closed if pd.notna(row.date_closed) else None,
        ))
    with conn.cursor() as cur:
        cur.execute("TRUNCATE violations RESTART IDENTITY CASCADE")
        execute_values(cur, """
            INSERT INTO violations (blklot, address, neighborhood, complaint_type,
                status, date_filed, date_closed)
            VALUES %s
        """, rows)
    conn.commit()
    print(f"Inserted {len(rows)} violations.")


def upsert_neighborhood_stats(conn, df: pd.DataFrame):
    rows = [
        (
            row.neighborhood, int(row.total_buildings), int(row.total_violations),
            int(row.open_violations), float(row.violation_rate),
            float(row.median_risk_score), int(row.median_assessed),
            int(row.cluster), str(row.top_violation_type),
        )
        for _, row in df.iterrows()
    ]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO neighborhood_stats (neighborhood, total_buildings, total_violations,
                open_violations, violation_rate, median_risk_score, median_assessed,
                cluster, top_violation_type)
            VALUES %s
            ON CONFLICT (neighborhood) DO UPDATE SET
                total_buildings   = EXCLUDED.total_buildings,
                total_violations  = EXCLUDED.total_violations,
                open_violations   = EXCLUDED.open_violations,
                violation_rate    = EXCLUDED.violation_rate,
                median_risk_score = EXCLUDED.median_risk_score,
                cluster           = EXCLUDED.cluster,
                updated_at        = NOW()
        """, rows)
    conn.commit()
    print(f"Upserted {len(rows)} neighborhood stats.")


# ---------------------------------------------------------------------------
# Archive to S3
# ---------------------------------------------------------------------------

def archive_to_s3(buildings: pd.DataFrame, violations: pd.DataFrame):
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        print("S3_BUCKET not set — skipping archive.")
        return

    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-west-2"))
    ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    for label, df in [("buildings", buildings), ("violations", violations)]:
        key = f"raw/{label}_{ts}.json"
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=df.to_json(orient="records", date_format="iso"),
            ContentType="application/json",
        )
        print(f"Archived s3://{bucket}/{key}")


# ---------------------------------------------------------------------------
# Lambda handler wrapper (for future promotion)
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """AWS Lambda entry point. Same logic, different trigger."""
    main()
    return {"statusCode": 200, "body": "ETL complete"}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="SF Housing ETL")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch + process but skip all writes (DB + S3)")
    parser.add_argument("--sample", type=int, default=0,
                        help="Limit fetch to N rows per dataset (e.g. --sample 500 for local testing)")
    args = parser.parse_args()

    print(f"\n=== SF Housing ETL — {datetime.datetime.utcnow().isoformat()} ===")
    if args.dry_run:  print("DRY RUN — no writes")
    if args.sample:   print(f"SAMPLE MODE — {args.sample} rows per dataset")
    print()

    # Build URLs with optional row limit for sample mode
    violations_url = VIOLATIONS_URL.replace("$limit=50000", f"$limit={args.sample}") if args.sample else VIOLATIONS_URL
    assessments_url = ASSESSMENTS_URL.replace("$limit=50000", f"$limit={args.sample}") if args.sample else ASSESSMENTS_URL

    # 1. Fetch
    raw_violations = fetch_json(violations_url, "violations")
    raw_assessments = fetch_json(assessments_url, "assessments")

    # 2. Clean
    violations = clean_violations(raw_violations)
    buildings = clean_assessments(raw_assessments)

    print(f"\nCleaned: {len(buildings)} buildings, {len(violations)} violations")

    # 3. Score & cluster
    buildings = compute_risk_scores(buildings, violations)
    buildings = run_clustering(buildings)

    # 4. Neighborhood rollup
    nbhd_stats = aggregate_neighborhoods(buildings, violations)

    print(f"Neighborhoods: {len(nbhd_stats)}")
    print(f"Buildings with coordinates: {buildings['lat'].notna().sum()} / {len(buildings)}")
    print(f"Risk score range: {buildings['risk_score'].min():.2f} – {buildings['risk_score'].max():.2f}")

    if args.dry_run:
        print("\n[dry-run] Skipping DB + S3 writes.")
        print("\nSample buildings:")
        print(buildings[["blklot", "address", "neighborhood", "risk_score", "cluster", "lat", "lng"]].head(10).to_string())
        print("\nSample neighborhood stats:")
        print(nbhd_stats[["neighborhood", "total_buildings", "open_violations", "median_risk_score"]].head(10).to_string())
        return

    # 5. Write to Postgres
    conn = get_db_conn()
    run_migrations(conn)
    upsert_buildings(conn, buildings)
    upsert_violations(conn, violations)
    upsert_neighborhood_stats(conn, nbhd_stats)
    conn.close()

    # 6. Archive raw to S3
    archive_to_s3(buildings, violations)

    print("\n=== ETL complete ===")


if __name__ == "__main__":
    main()