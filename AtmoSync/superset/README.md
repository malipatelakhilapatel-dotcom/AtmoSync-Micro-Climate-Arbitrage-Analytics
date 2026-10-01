# AtmoSync — Apache Superset & BI Layer

**Module Owner:** Member 3 — Superset & Documentation

This directory contains the dashboard design, chart queries, export JSON definitions, and an interactive local web preview of the **AtmoSync Micro-Climate Arbitrage Console**.

---

## Deliverables in this Directory

1. **`dashboard_design.md`**: Complete layout design specifying 7 KPI cards, 7 charts, and 6 dynamic filters.
2. **`charts.md`**: Production SQL queries for each chart referencing Snowflake `ANALYTICS` and `INTERMEDIATE` tables.
3. **`dashboard_export.json`**: Standard Apache Superset importable JSON bundle containing dataset definitions, native filters, slice configurations, and dashboard layouts.
4. **`web_preview/`**: Interactive, zero-dependency standalone dashboard preview (`index.html`, `styles.css`, `app.js`) that lets traders view the live map, time-series charts, and interactive Spoilage Arbitrage matrix directly in any browser.

---

## Connecting Apache Superset to Snowflake

### 1. Install Snowflake SQLAlchemy Driver
Ensure the Superset container or environment has the driver:
```bash
pip install snowflake-sqlalchemy
```

### 2. Add Snowflake Database in Superset UI
In Superset:
1. Navigate to **Data** → **Databases** → **+ Database**.
2. Select **Snowflake**.
3. Set the SQLAlchemy URI:
   ```text
   snowflake://<user>:<password>@<account>/ATMOSYNC_DB/ANALYTICS?warehouse=ATMOSYNC_WH&role=ATMOSYNC_ROLE
   ```
4. Click **Test Connection** and then **Connect**.

### 3. Import Dashboard Bundle
1. In the Superset top navigation, click **Dashboards**.
2. Click the three dots menu (**...**) in the top right and select **Import Dashboard**.
3. Select `superset/dashboard_export.json`.
4. Open the newly imported dashboard: **AtmoSync — Micro-Climate Arbitrage Analytics**.

---

## Instant Local Web Preview

To preview the exact BI console immediately without spinning up Superset:
1. Open `superset/web_preview/index.html` in any web browser.
2. Or serve it via Python:
   ```bash
   python -m http.server 3000 --directory superset/web_preview
   ```
3. Visit [http://localhost:3000](http://localhost:3000) to interact with the map, live charts, filters, and reroute simulator!