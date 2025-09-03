# 🚀 Azure Data Engineering Portfolio Project

## 📂 Project Overview
This project demonstrates an **end-to-end Data Engineering pipeline on Azure**, following the **Medallion Architecture (Bronze → Silver → Gold)**.  
The pipeline ingests data from multiple sources, processes it using PySpark in Databricks, and stores the curated data in Azure Data Lake with Delta tables.  
Finally, the data is visualized in **Tableau Public**.

---

## 🏗️ Architecture
1. **Bronze Layer**  
   - Stores raw data (CSV, API JSON, Streaming JSON).  
   - Data remains unaltered for traceability.

2. **Silver Layer**  
   - Cleansed and transformed data.  
   - Standardized schemas, consistent datatypes, enriched fields.

3. **Gold Layer**  
   - Unified analytics-ready dataset.  
   - Aggregated metrics: revenue, profit, orders by category, sub-category, region, and time.

---

## ⚙️ Technologies Used
- **Azure Data Lake Gen2** (storage)  
- **Azure Databricks** (ETL with PySpark, Delta Lake)  
- **Delta Lake** (ACID-compliant storage format)  
- **Tableau Public** (visualization)  

---

## 📊 Gold Queries for Analytics
Example SQL queries run on the **Gold Layer**:

### 1. Daily Revenue Trend
```sql
SELECT 
    order_date,
    ROUND(SUM(daily_sales), 2) AS total_revenue,
    ROUND(SUM(daily_profit), 2) AS total_profit,
    SUM(orders_count) AS total_orders
FROM gold_sales_aggregates
WHERE order_date IS NOT NULL
GROUP BY order_date
ORDER BY order_date;
```

### 2. Top 10 Categories by Revenue
```sql
SELECT 
    category,
    ROUND(SUM(daily_sales), 2) AS total_revenue,
    ROUND(SUM(daily_profit), 2) AS total_profit
FROM gold_sales_aggregates
WHERE category IS NOT NULL
GROUP BY category
ORDER BY total_revenue DESC
LIMIT 10;
```

### 3. Sub-Category Performance
```sql
SELECT 
    category,
    subcategory,
    ROUND(SUM(daily_sales), 2) AS total_revenue,
    ROUND(SUM(daily_profit), 2) AS total_profit
FROM gold_sales_aggregates
WHERE subcategory IS NOT NULL
GROUP BY category, subcategory
ORDER BY category, total_revenue DESC;
```

### 4. Regional Sales Contribution
```sql
SELECT 
    region,
    ROUND(SUM(daily_sales), 2) AS total_revenue,
    ROUND(SUM(daily_profit), 2) AS total_profit,
    SUM(orders_count) AS total_orders
FROM gold_sales_aggregates
WHERE region IS NOT NULL
GROUP BY region
ORDER BY total_revenue DESC;
```

### 5. Month-over-Month Growth
```sql
SELECT 
    DATE_FORMAT(order_date, 'yyyy-MM') AS month,
    ROUND(SUM(daily_sales), 2) AS total_revenue,
    ROUND(SUM(daily_profit), 2) AS total_profit,
    SUM(orders_count) AS total_orders
FROM gold_sales_aggregates
WHERE order_date IS NOT NULL
GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
ORDER BY month;
```

---

## 📊 Interactive Tableau Dashboard

You can explore the interactive dashboard here:  
👉 [View on Tableau Public](https://public.tableau.com/views/SuperStore_17568570133710/Dashboard1)

![Tableau Dashboard Preview](https://public.tableau.com/static/images/Su/SuperStore_17568570133710/Dashboard1/1.png)

---

## 🚀 How to Reproduce
1. Clone this repository.  
2. Set up **Azure Data Lake Gen2** and upload sample data (CSV, JSON).  
3. Create a **Databricks cluster** and run the provided PySpark notebooks.  
4. Verify Bronze → Silver → Gold tables in Delta format.  
5. Export Gold data as CSV for Tableau Public.  
6. Connect Tableau to the exported datasets and publish dashboards.

---

## 📌 Key Learnings
- Implemented Medallion Architecture on Azure.  
- Built ETL pipelines with PySpark + Delta Lake.  
- Unified batch, API, and streaming data into Gold analytics.  
- Published interactive Tableau dashboards for insights.  
