---
date: "2026-04-04T00:00:00+02:00"
description: Serve pipeline results to Power BI, Excel, Grafana, or any OData-compatible BI tool via OData v4.
draft: false
title: Serve Data via OData
weight: 8
---
Add `@expose` to any model. Run `ondatrasql odata <port>`. BI tools connect directly.

## 1. Mark Models for Serving

Add `@expose` to any materialized SQL model:

```sql
-- models/mart/revenue.sql
-- @kind: table
-- @expose order_id

SELECT
    order_id,
    customer,
    amount,
    order_date
FROM staging.orders
```

The column after `@expose` is the OData primary key.

## 2. Run the Pipeline

```bash
ondatrasql run
```

## 3. Start the Server

```bash
ondatrasql odata 8090
```

```
OData server starting...
  Endpoint: http://127.0.0.1:8090/odata
  mart.revenue (4 columns)
```

## 4. Connect a Tool

### Power BI

1. **Get Data** → **OData Feed**
2. Enter: `http://localhost:8090/odata` (OData binds to 127.0.0.1 — use an SSH tunnel or reverse proxy for remote access)
3. Select tables → **Load**

### Excel

1. **Data** → **Get Data** → **From Other Sources** → **From OData Feed**
2. Enter: `http://your-server:8090/odata`
3. Select tables → **Load**

### Tableau

1. **Connect** → **OData**
2. Enter: `http://your-server:8090/odata`

### Grafana

1. Install the [Infinity datasource plugin](https://grafana.com/grafana/plugins/yesoreyeram-infinity-datasource/)
2. **Connections** → **Data Sources** → **Add data source** → **Infinity**
3. URL: `http://your-server:8090/odata/mart_revenue`
4. Set Rows/Root to `value`

### Spotfire

1. **Data** → **Add Data Connection** → **OData**
2. Enter: `http://your-server:8090/odata`

### Salesforce

1. **Setup** → **External Data Sources** → **New**
2. Type: **OData 4.0**
3. URL: `http://your-server:8090/odata`

### curl / Python

```bash
curl "http://localhost:8090/odata/mart_revenue?\$top=5" | jq .
```

```python
import pandas as pd, requests
df = pd.json_normalize(requests.get("http://localhost:8090/odata/mart_revenue").json()["value"])
```

For the full query reference, see [OData API Reference](/reference/data-access/odata-api/).
