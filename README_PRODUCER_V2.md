# PRODUCER-V2.PY SETUP GUIDE

## Direct Eventhouse Ingestion for CTRM Live POC

---

## 📋 OVERVIEW

This guide will help you set up **producer-v2.py** which sends CTRM trade data directly to Microsoft Fabric Eventhouse, bypassing EventStream.

**Key Differences from producer-v1.py:**

- ❌ No Kafka/EventStream dependency
- ✅ Direct ingestion to Eventhouse using Azure Kusto Python SDK
- ✅ Same business logic (commodities, trades, PnL calculations)
- ✅ Lower latency option with streaming ingestion

---

## 🛠️ STEP 1: CREATE OR CONFIGURE EVENTHOUSE

### Option A: Use Existing Eventhouse with New Database

1. Navigate to your Fabric workspace
2. Open your existing Eventhouse
3. Click **"+ New Database"**
4. Name it: `CTRMDatabase_V2` or similar
5. Note the **Database ID** and **Eventhouse name**

### Option B: Create New Eventhouse

1. Go to your Fabric workspace
2. Click **"+ New"** → **"More options"**
3. Select **"Eventhouse"**
4. Name it: `CTRM_Eventhouse_V2`
5. Click **"Create"**
6. Create a new database inside: `CTRMDatabase`

---

## 🔑 STEP 2: GET EVENTHOUSE CONNECTION DETAILS

1. Open your Eventhouse in Fabric
2. Click **"Details"** or **"Settings"** (usually top-right)
3. Copy the following URIs:
   - **Query URI**: `https://your-eventhouse.z22.kusto.fabric.microsoft.com`
   - **Ingestion URI**: `https://ingest-your-eventhouse.z22.kusto.fabric.microsoft.com`

**Alternative Method:**

- Navigate to your KQL Database
- Click on **"Database details"**
- Copy both URIs from there

---

## 📊 STEP 3: CREATE TABLE AND MAPPING

1. Open your KQL Database in Fabric
2. Click **"Query"** or **"KQL Queryset"**
3. Copy and run the KQL commands from **`eventhouse_setup.kql`**
4. This will create:
   - ✅ CTRMTrades table with proper schema
   - ✅ JSON mapping for ingestion
   - ✅ Streaming ingestion policy (optional)
   - ✅ Ingestion batching policy (optional)

---

## 🔐 STEP 4: SET UP AUTHENTICATION

### Recommended: Interactive Login (for testing)

No additional setup needed - the script will prompt for login.

### Production: Service Principal (recommended)

1. Create an Azure AD App Registration
2. Add it as a user to your database:
   ```kql
   .add database CTRMDatabase users ('aadapp=<APP_ID>;<TENANT_ID>') 'CTRM Producer'
   ```
3. Update producer-v2.py authentication:
   ```python
   kcsb_ingest = KustoConnectionStringBuilder.with_aad_application_key_authentication(
       EVENTHOUSE_INGEST_URI,
       "<APP_ID>",
       "<APP_KEY>",
       "<TENANT_ID>"
   )
   ```

### Alternative: Managed Identity

If running in Azure (Function App, VM, etc.):

```python
kcsb_ingest = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(
    EVENTHOUSE_INGEST_URI
)
```

---

## 🐍 STEP 5: INSTALL PYTHON DEPENDENCIES

```bash
pip install -r requirements.txt
```

Or install individually:

```bash
pip install azure-kusto-data==4.3.1
pip install azure-kusto-ingest==4.3.1
pip install azure-identity==1.15.0
pip install python-dotenv==1.0.0
```

---

## ⚙️ STEP 6: CONFIGURE ENVIRONMENT VARIABLES

1. Edit `.env` and fill in your Eventhouse details:
   ```
   EVENTHOUSE_QUERY_URI=https://your-eventhouse.z22.kusto.fabric.microsoft.com
   EVENTHOUSE_INGEST_URI=https://ingest-your-eventhouse.z22.kusto.fabric.microsoft.com
   EVENTHOUSE_DATABASE=CTRMDatabase
   EVENTHOUSE_TABLE=CTRMTrades
   ```

---

## ▶️ STEP 7: RUN PRODUCER-V2.PY

```bash
python producer-v2.py
```

**Expected Output:**

```
============================================================
CTRM EVENTHOUSE PRODUCER - DIRECT INGESTION
============================================================
✓ Ingested: PriceTick - Oil - TradeID: 0
✓ Ingested: PriceTick - Gold - TradeID: 0
✓ Ingested: PriceTick - Wheat - TradeID: 0
📊 NEW TRADE: Long 543.21 Gold @ 102.34
✓ Ingested: TradeOpened - Gold - TradeID: 1
...
```

---

## 🔍 STEP 8: VERIFY DATA IN EVENTHOUSE

Run these KQL queries in your database:

```kql
// Check if data is arriving
CTRMTrades
| count

// View latest records
CTRMTrades
| top 10 by timestamp desc

// Breakdown by event type
CTRMTrades
| summarize Count=count() by EventType

// View open trades
CTRMTrades
| where Status == "Open"
| summarize by TradeID, commodity, Quantity, EntryPrice, UnrealizedPnL
```

---

## 📈 STEP 9: CONNECT POWER BI TO EVENTHOUSE

### Method 1: Power BI Desktop (Direct Query)

1. Open **Power BI Desktop**
2. Click **"Get Data"** → **"More..."**
3. Search for **"Kusto"** or **"Azure Data Explorer"**
4. Select **"Azure Data Explorer (Kusto)"**
5. Enter connection details:
   - **Cluster**: Your Query URI (e.g., `https://your-eventhouse.z22.kusto.fabric.microsoft.com`)
   - **Database**: `CTRMDatabase`
   - **Table**: `CTRMTrades`
6. Select **"DirectQuery"** mode (recommended for real-time)
7. Click **"OK"** and authenticate
8. Select the `CTRMTrades` table and click **"Load"**

### Method 2: Power BI Service (Fabric Integration)

1. Go to your **Fabric workspace**
2. Click **"+ New"** → **"Report"**
3. Click **"Pick a published semantic model"** or **"Build from scratch"**
4. Select **"KQL Database"** as data source
5. Navigate to your Eventhouse → Database → `CTRMTrades`
6. Click **"Connect"**
7. Build your visuals

---

## 📊 STEP 10: CREATE POWER BI DASHBOARD

### Recommended Visuals:

1. **Real-Time Price Chart**

   - Visual: Line chart
   - X-axis: `timestamp`
   - Y-axis: `CurrentPrice`
   - Legend: `commodity`
   - Filter: `EventType = "PriceTick"`

2. **Open Positions Table**

   - Visual: Table
   - Columns: `TradeID`, `commodity`, `PositionType`, `Quantity`, `EntryPrice`, `CurrentPrice`, `UnrealizedPnL`
   - Filter: `Status = "Open"`

3. **Realized PnL by Commodity**

   - Visual: Clustered bar chart
   - X-axis: `commodity`
   - Y-axis: `Sum of RealizedPnL`
   - Filter: `EventType = "TradeClosed"`

4. **Trade Volume Card**

   - Visual: Card
   - Field: `Count of TradeID (distinct)`
   - Filter: `EventType = "TradeOpened"`

5. **Current Unrealized PnL Gauge**
   - Visual: Gauge
   - Value: `Sum of UnrealizedPnL`
   - Filter: `Status = "Open"` (latest per TradeID)

---

## 🔧 TROUBLESHOOTING

### Issue: "Authentication failed"

**Solution:** Make sure your account has permissions:

```kql
.add database CTRMDatabase ingestors ('user=your-email@domain.com')
```

### Issue: "No data appearing"

**Solution:** Check ingestion status:

```kql
.show ingestion failures
| where Table == "CTRMTrades"
| where FailedOn > ago(1h)
```

### Issue: "High latency"

**Solution:** Enable streaming ingestion:

```kql
.alter table CTRMTrades policy streamingingestion enable
```

Then update producer-v2.py to use streaming:

```python
from azure.kusto.ingest import KustoStreamingIngestClient
ingest_client = KustoStreamingIngestClient(kcsb_ingest)
```

### Issue: "Power BI not refreshing"

**Solution:**

- Make sure you're using **DirectQuery** mode, not Import
- Set auto-refresh interval in Power BI Service (minimum 15 minutes for Pro, 1 minute for Premium)

---

## 📞 ADDITIONAL RESOURCES

- [Azure Data Explorer Python SDK Documentation](https://learn.microsoft.com/en-us/azure/data-explorer/python-ingest-data)
- [Fabric Eventhouse Documentation](https://learn.microsoft.com/en-us/fabric/real-time-analytics/eventhouse)
- [KQL Query Language Reference](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/)
- [Power BI Kusto Connector Guide](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/connect-kql-database-power-bi)

---
