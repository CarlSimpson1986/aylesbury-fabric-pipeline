# Aylesbury Analytics - Automated Financial Reporting Pipeline

## 🎯 Business Problem

**Client:** My Fit Pod Aylesbury - 24/7 automated fitness facility

**Challenges:**
- Monthly financial data exported as **26+ separate CSV files** (one per month)
- Manual Excel manipulation taking **2+ hours** to create client reports
- No historical trend analysis or automated insights
- Risk of human error in manual calculations
- Client expects professional monthly deliverables showing revenue trends, category splits, and top-performing products

**Business Impact:**
- Time wasted on repetitive manual work
- Inability to scale reporting as business grows
- No systematic tracking of historical performance
- Delayed insights preventing timely business decisions

---

## 💡 Solution Overview

Built an **end-to-end automated data pipeline** in Microsoft Fabric that:

✅ **Ingests** 26+ months of transaction data from CSV files  
✅ **Processes** data through Bronze → Silver → Gold medallion architecture  
✅ **Transforms** raw transactions into business-ready analytics  
✅ **Generates** automated reports with Power BI  
✅ **Scales** to handle future monthly data with zero manual intervention  

**Result:** 2+ hours of manual work → **One click** (run pipeline)

---

## 🏗️ Architecture

### Medallion Architecture (Bronze → Silver → Gold)

**Bronze Layer (Raw Data)**
- 26+ CSV files (Feb 2023 - Nov 2025)
- 4,100+ transactions total
- Schema: Date, Item, Quantity, Amount, Category, Customer

**Silver Layer (Cleaned Data)**
- Deduplicated transactions (4,132 unique records)
- Date parsing and feature engineering
- Data quality validation
- Added: Year, Month, Quarter, Month_Order fields

**Gold Layer (Analytics-Ready)**
- gold_monthly_summary (26+ months)
- gold_yearly_summary (3 years: 2023, 2024, 2025)
- gold_category_summary (3 categories by year)
- gold_quarterly_summary (9+ quarters)
- gold_item_performance (top revenue-generating products by year)

**Outputs**
- Power BI Interactive Dashboard (2 pages)

---

## 🔑 Key Technical Decisions

### 1. **Medallion Architecture (Bronze/Silver/Gold)**

**Problem:** Raw CSV data had duplicates, inconsistent formatting, and wasn't analysis-ready.

**Solution:** Implemented three-layer architecture:
- **Bronze:** Immutable raw data preservation (audit trail)
- **Silver:** Clean, validated, single source of truth
- **Gold:** Pre-aggregated for performance (sub-second query response)

**Why:** Separates concerns, enables reusability, production-ready pattern

---

### 2. **Incremental Loading with Metadata Tracking**

**Problem:** Re-processing all files every month wastes compute and time.

**Solution:** Built metadata tracker that logs processed files:

**metadata_file_tracker table:**
- file_name
- file_path  
- processed_date
- row_count

**Benefits:**
- Pipeline only processes NEW files (10 seconds vs 2 minutes)
- Idempotent (safe to re-run without duplicating data)
- Audit trail of all data loads

---

### 3. **Automated Deduplication at Silver Layer**

**Problem:** Source data contained 316 duplicate transactions (7% of dataset).

**Solution:** Applied dropDuplicates() on composite key:
- Date + Item + Amount + Customer + Quantity + Category

**Impact:**
- Prevented revenue double-counting
- Maintained data integrity for financial reporting
- 4,448 rows → 4,132 unique transactions

---

### 4. **Star Schema with Year Dimension**

**Problem:** Multiple Gold tables with no relationships = slicers don't filter across visuals.

**Solution:** Created star schema with gold_yearly_summary as hub:

**gold_yearly_summary** (Dimension - unique Years)
- Connected to gold_monthly_summary (One-to-Many)
- Connected to gold_quarterly_summary (One-to-Many)
- Connected to gold_category_summary (One-to-Many)
- Connected to gold_item_performance (One-to-Many)

**Benefits:**
- Single Year slicer filters ALL Power BI visuals
- Follows dimensional modeling best practices
- Avoids Many-to-Many complexity

---

### 5. **Data Quality Gate with Pipeline Failure**

**Problem:** Bad data could corrupt Gold layer and reports.

**Solution:** Implemented QA notebook with automated validation:
- Duplicate detection
- Null value checks
- Data type verification
- Business rule validation
- **Raises exception on critical failures** → stops pipeline

**Why:** Production pipelines need quality gates to prevent bad data propagation

---

## 📊 Business Value Delivered

### Time Savings
- **Before:** 2+ hours manual Excel work per month
- **After:** One click (5 minutes total)
- **Annual Savings:** ~24 hours = 3 working days

### Data Quality
- **Eliminated:** 316 duplicate transactions (7% of data)
- **Standardized:** Date formats, null handling, data types
- **Validated:** Automated QA checks catch issues before reporting

### Insights Enabled
- Month-over-month revenue trends across 26+ months
- Category performance (62% PAYG vs 38% Membership)
- Top revenue-generating products
- Customer engagement metrics
- Year-over-year growth tracking (2023 vs 2024 vs 2025)

### Scalability
- Current: 26 months, 4K+ transactions
- Future: Handles 10 years, 100K+ transactions with no code changes
- Incremental loading ensures consistent performance

---

## 🛠️ Technologies Used

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Infrastructure** | Microsoft Fabric | Cloud data platform |
| **Storage** | OneLake (Lakehouse) | Delta Lake tables |
| **Processing** | PySpark | Data transformations |
| **Orchestration** | Fabric Data Pipeline | Workflow automation |
| **Semantic Layer** | Power BI Semantic Model | Business logic & relationships |
| **Visualization** | Power BI Service | Interactive dashboards |
| **Libraries** | pandas, matplotlib | Data manipulation & visualization |

---

## 📁 Repository Structure
