"""Incremental Salesforce -> SQL Server pipeline for Opportunities.

Run it on a schedule. The first run loads every opportunity; every run after that pulls only the records Salesforce
changed since the last successful load, and MERGEs them into dbo.Opportunity (insert new IDs, update existing ones).

Authentication is OAuth 2.0 client credentials through a Salesforce External Client App that runs as a dedicated
integration user. No password or security token: Salesforce is retiring username/password (SOAP login) in Summer '27.
Credentials come from environment variables (or a local .env file that never goes in source control):
    SF_DOMAIN_URL      your My Domain, e.g. https://yourcompany.my.salesforce.com
    SF_CLIENT_ID       the External Client App's consumer key
    SF_CLIENT_SECRET   the External Client App's consumer secret
    SQL_CONN           optional ODBC connection string (default: local SQL Server, database SalesOpsDemo, Windows auth)
"""
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyodbc
import requests
from dotenv import load_dotenv
from simple_salesforce import Salesforce

load_dotenv()

SQL_CONN = os.environ.get("SQL_CONN", "Driver={ODBC Driver 17 for SQL Server};Server=localhost;"
                                      "Database=SalesOpsDemo;Trusted_Connection=yes;")
OBJECT = "Opportunity"
FIELDS = ["Id", "Name", "Amount", "StageName", "CloseDate", "IsDeleted", "SystemModstamp"]
LOOKBACK = timedelta(minutes=10)   # re-read a little overlap every run; the MERGE makes repeats harmless


# ---------------------------------------------------------------- 1. connect
def connect_salesforce():
    """Ask Salesforce for an access token with the app's client ID and secret (client credentials flow).
    The token runs as the integration user set on the app, so it sees exactly what that user is allowed to see."""
    resp = requests.post(f"{os.environ['SF_DOMAIN_URL']}/services/oauth2/token", data={
        "grant_type": "client_credentials",
        "client_id": os.environ["SF_CLIENT_ID"],
        "client_secret": os.environ["SF_CLIENT_SECRET"],
    }, timeout=30)
    resp.raise_for_status()
    token = resp.json()
    sf = Salesforce(instance_url=token["instance_url"], session_id=token["access_token"])
    sf.org_id = token["id"].split("/")[-2]   # token id is .../id/<orgId>/<userId>
    return sf


def connect_sql():
    return pyodbc.connect(SQL_CONN, autocommit=False)


# ---------------------------------------------------------------- 2. the watermark (control table)
def get_watermark(db):
    """Latest SystemModstamp we have successfully loaded, or None on the very first run."""
    row = db.execute("SELECT watermark FROM dbo.pipeline_watermark WHERE object_name = ?", OBJECT).fetchone()
    return row[0] if row else None


def set_watermark(db, watermark, rows):
    db.execute("""
        MERGE dbo.pipeline_watermark AS t
        USING (SELECT ? AS object_name, ? AS watermark, ? AS rows_loaded) AS s ON t.object_name = s.object_name
        WHEN MATCHED THEN UPDATE SET watermark = s.watermark, rows_loaded = s.rows_loaded, last_run = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (object_name, watermark, rows_loaded, last_run)
                              VALUES (s.object_name, s.watermark, s.rows_loaded, SYSUTCDATETIME());""",
               OBJECT, watermark, rows)


# ---------------------------------------------------------------- 3. extract (full first time, incremental after)
def extract(sf, watermark):
    soql = f"SELECT {', '.join(FIELDS)} FROM {OBJECT}"
    if watermark is not None:
        since = (watermark - LOOKBACK).strftime("%Y-%m-%dT%H:%M:%SZ")
        soql += f" WHERE SystemModstamp > {since}"
    soql += " ORDER BY SystemModstamp"
    # query_all follows Salesforce's 2,000-row pages; include_deleted also returns records in the Recycle Bin
    return sf.query_all(soql, include_deleted=True)["records"]


def to_frame(records):
    df = pd.DataFrame(records).drop(columns="attributes", errors="ignore")
    if df.empty:
        return pd.DataFrame(columns=FIELDS)
    df["CloseDate"] = pd.to_datetime(df["CloseDate"]).dt.date
    df["SystemModstamp"] = pd.to_datetime(df["SystemModstamp"], utc=True).dt.tz_localize(None)
    return df[FIELDS]


# ---------------------------------------------------------------- 4. load: stage, then MERGE (upsert) on the Salesforce Id
MERGE_SQL = """
MERGE dbo.Opportunity AS t
USING #stage AS s
   ON t.Id = s.Id
WHEN MATCHED AND s.SystemModstamp > t.SystemModstamp THEN   -- only if Salesforce has a newer version
    UPDATE SET Name = s.Name, Amount = s.Amount, StageName = s.StageName, CloseDate = s.CloseDate,
               IsDeleted = s.IsDeleted, SystemModstamp = s.SystemModstamp, LoadedAt = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (Id, Name, Amount, StageName, CloseDate, IsDeleted, SystemModstamp, LoadedAt)
    VALUES (s.Id, s.Name, s.Amount, s.StageName, s.CloseDate, s.IsDeleted, s.SystemModstamp, SYSUTCDATETIME())
OUTPUT $action;"""


def load(db, df):
    """Upsert the batch. Returns (inserted, updated). The caller commits."""
    cur = db.cursor()
    cur.execute("""CREATE TABLE #stage (Id CHAR(18) PRIMARY KEY, Name NVARCHAR(120), Amount DECIMAL(18,2),
                   StageName NVARCHAR(40), CloseDate DATE, IsDeleted BIT, SystemModstamp DATETIME2(0));""")
    cur.fast_executemany = True
    rows = [tuple(None if pd.isna(v) else v for v in r) for r in df.itertuples(index=False)]
    cur.executemany("INSERT INTO #stage VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    actions = [r[0] for r in cur.execute(MERGE_SQL).fetchall()]
    cur.execute("DROP TABLE #stage")
    return actions.count("INSERT"), actions.count("UPDATE")


# ---------------------------------------------------------------- 5. deletes
def reconcile_deletes(sf, db, days=14):
    """Catch HARD deletes. A deleted record sits in the Recycle Bin for 15 days (the extract sees it there and flags
    IsDeleted), but once it is purged no query can return it. Salesforce's getDeleted endpoint lists records deleted
    in the last 15 days (less if an admin empties the Recycle Bin early), so we flag those too on every run. We never physically delete rows: the flag keeps
    history, and the active view hides them from reports."""
    end = datetime.now(timezone.utc)
    first_load = db.execute("SELECT MIN(LoadedAt) FROM dbo.Opportunity").fetchone()[0]   # nothing to un-delete before this
    start = max(end - timedelta(days=days), first_load.replace(tzinfo=timezone.utc) - timedelta(hours=1))
    deleted = getattr(sf, OBJECT).deleted(start, end).get("deletedRecords", [])
    ids = [d["id"] for d in deleted]
    flagged = 0
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        flagged += db.execute(f"UPDATE dbo.Opportunity SET IsDeleted = 1, LoadedAt = SYSUTCDATETIME() "
                              f"WHERE IsDeleted = 0 AND Id IN ({', '.join('?' * len(chunk))})", *chunk).rowcount
    return flagged


def full_id_check(sf, db):
    """Weekly safety net: any active row whose Id no longer exists in Salesforce was deleted. Only Ids are pulled."""
    live = {r["Id"] for r in sf.query_all(f"SELECT Id FROM {OBJECT}")["records"]}
    rows = db.execute("SELECT Id FROM dbo.Opportunity WHERE IsDeleted = 0").fetchall()
    gone = [r[0] for r in rows if r[0] not in live]
    for i in gone:
        db.execute("UPDATE dbo.Opportunity SET IsDeleted = 1, LoadedAt = SYSUTCDATETIME() WHERE Id = ?", i)
    return len(gone)


# ---------------------------------------------------------------- 6. one run
def run():
    sf, db = connect_salesforce(), connect_sql()
    watermark = get_watermark(db)
    df = to_frame(extract(sf, watermark))
    if watermark is None:
        deleted = int(df["IsDeleted"].sum())
        print(f"First run: pulled all {len(df) - deleted} opportunities (+{deleted} already deleted, kept and flagged)")
    else:
        new = int((df["SystemModstamp"] > watermark).sum())
        print(f"Incremental run: {new} changed since {watermark:%Y-%m-%d %H:%M:%S} UTC"
              f" (+{len(df) - new} re-checked from the {LOOKBACK.seconds // 60}-minute overlap)")
    if df.empty:
        print("Nothing new. Watermark unchanged.")
    else:
        inserted, updated = load(db, df)
        # the new watermark comes from the DATA we loaded, not the clock, and only moves once the load commits
        new_watermark = max(df["SystemModstamp"].max().to_pydatetime(), watermark or datetime.min)
        set_watermark(db, new_watermark, len(df))
        db.commit()   # rows and watermark commit together: a failed run changes nothing and simply retries
        print(f"MERGE: {inserted} inserted, {updated} updated. Watermark -> {new_watermark:%Y-%m-%d %H:%M:%S} UTC")
    flagged = reconcile_deletes(sf, db)
    db.commit()
    print(f"Deletes: {flagged} newly flagged from Salesforce's deleted-records log")


def setup():
    """Create the destination and control tables (safe to run more than once)."""
    db = connect_sql()
    db.execute("""
        IF OBJECT_ID('dbo.Opportunity') IS NULL
        CREATE TABLE dbo.Opportunity (Id CHAR(18) PRIMARY KEY, Name NVARCHAR(120), Amount DECIMAL(18,2),
            StageName NVARCHAR(40), CloseDate DATE, IsDeleted BIT, SystemModstamp DATETIME2(0), LoadedAt DATETIME2(0));
        IF OBJECT_ID('dbo.pipeline_watermark') IS NULL
        CREATE TABLE dbo.pipeline_watermark (object_name SYSNAME PRIMARY KEY, watermark DATETIME2(0),
            rows_loaded INT, last_run DATETIME2(0));""")
    db.execute("""CREATE OR ALTER VIEW dbo.vw_Opportunity_Active AS
                  SELECT Id, Name, Amount, StageName, CloseDate, SystemModstamp FROM dbo.Opportunity WHERE IsDeleted = 0;""")
    db.commit()


if __name__ == "__main__":
    import sys
    setup()
    run()
    if "--full-check" in sys.argv:   # e.g. Sundays
        n = full_id_check(connect_salesforce(), (db := connect_sql()))
        db.commit()
        print(f"Full Id check: {n} more deletes found")
