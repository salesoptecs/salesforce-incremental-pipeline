# Salesforce Incremental Pipeline

A small, production-minded pipeline that copies Salesforce Opportunities into SQL Server and keeps them current.
Built for Sales Ops and RevOps teams who want their CRM data in a warehouse without buying a connector.

- **Incremental:** after the first full load, each run pulls only records changed since the last successful run (a SystemModstamp watermark).
- **Upsert:** one MERGE keyed on the Salesforce ID inserts new records and updates changed ones. Safe to rerun.
- **Doesn't lose changes:** the watermark comes from the loaded data, not the clock, with a 10-minute lookback, and it commits together with the rows.
- **Handles deletes:** Recycle Bin deletes are flagged from the extract, purged deletes from Salesforce's deleted-records log, and a weekly full ID check catches the rest. Rows are flagged, never removed, and `vw_Opportunity_Active` hides them from reports.
- **Optional near real time:** `cdc_listener.py` applies Change Data Capture events with the same MERGE.
- **OAuth, not passwords:** uses the client credentials flow with an External Client App and an integration user. Salesforce retires username, password and security token login for integrations in Summer '27.

Walkthrough video and the decision guide behind it: [salesoptecs.com](https://salesoptecs.com) ("Do You Actually Need a Real-Time Salesforce Mirror? CDC vs. API Watermarks vs. Managed Connectors").

## 1. Salesforce setup (your admin, once)

1. **API access:** Enterprise, Unlimited, Performance or Developer edition.
2. **Integration user:** user license *Salesforce Integration* (Enterprise, Unlimited and Performance orgs include five free), profile *Minimum Access - API Only Integrations*, and the *Salesforce API Integration* permission set license.
3. **Permission set:** Read on Opportunity **with View All**, plus read access to the fields you pull (for example Amount). Without View All, sharing rules hide records from the integration user and the pipeline silently misses them.
4. **External Client App:** enable OAuth with the `api` scope, turn on the **client credentials flow**, and set the run-as user to the integration user. Copy the consumer key and secret.
5. **Optional, for CDC:** Setup > Change Data Capture > select Opportunity. The default allowance is five selected objects; more objects or higher event volume needs the paid Change Data Capture add-on.

## 2. Run it

```bash
pip install -r requirements.txt
cp .env.example .env        # fill in SF_DOMAIN_URL, SF_CLIENT_ID, SF_CLIENT_SECRET (and SQL_CONN if not local)
python sf_pipeline.py              # schedule this, e.g. hourly
python sf_pipeline.py --full-check # weekly: full ID comparison to catch old deletes
python cdc_listener.py             # optional: apply change events within seconds
```

The default destination is a local SQL Server database named `SalesOpsDemo` with Windows authentication. Create it first
(`CREATE DATABASE SalesOpsDemo`) or point `SQL_CONN` at your own server. Tables and the view are created on first run.

If the bundled gRPC stubs don't match your installed protobuf version, regenerate them:
`python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. pubsub_api.proto`
(`pubsub_api.proto` is from [forcedotcom/pub-sub-api](https://github.com/forcedotcom/pub-sub-api), CC0.)

## Things to know

- **Formula fields:** a formula that reads another record or uses TODAY() can change without SystemModstamp changing, so incremental loads won't refresh it, and change events never include formulas. Load the base fields and compute downstream.
- **Compound fields:** select address parts (BillingStreet, BillingCity...) rather than BillingAddress; the Bulk API rejects compound fields.
- **getDeleted** covers only the last 15 days (less if the Recycle Bin is emptied early), so run the weekly `--full-check`.
- **CDC** events are retained for 72 hours, deliveries count against a daily allocation (shared with high-volume platform events, per subscriber), and gap events carry no field data. The listener therefore re-reads changed records instead of trusting the payload. Keep the scheduled run as the safety net.

MIT licensed.
