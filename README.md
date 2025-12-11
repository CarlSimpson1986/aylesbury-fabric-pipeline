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
- Automated PDF Report (optional client deliverable)

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
| **Reporting** | Python (Semantic Link) | Automated PDF generation |
| **Libraries** | matplotlib, pandas, MLflow | Visualization & tracking |

---

## 📁 Repository Structure
```
aylesbury-fabric-pipeline/
├── README.md
├── notebooks/
│   ├── 01_Bronze_to_Silver_Incremental.py
│   ├── 02_Silver_to_Gold.py
│   ├── 03_Generate_PDF_Report.py
│   └── QA_Data_Quality_Checks.py
├── docs/
│   ├── power_bi_page1.png
│   ├── power_bi_page2.png
│   ├── pipeline_architecture.png
│   ├── semantic_model_relationships.png
│   └── lakehouse_structure.png
├── pipeline/
│   └── pipeline_definition.json
└── sample_data/
    └── sample_transactions.csv
```

---

## 📸 Screenshots

### Power BI Dashboard - Page 1: Monthly Overview
![Monthly Trends Dashboard](docs/power_bi_page1.png)

### Power BI Dashboard - Page 2: Product Analysis
![Product Analysis](docs/power_bi_page2.png)

### Data Pipeline Architecture
![Pipeline](docs/pipeline_architecture.png)

### Semantic Model - Star Schema
![Star Schema](docs/semantic_model_relationships.png)

### Lakehouse Structure - Bronze/Silver/Gold
![Lakehouse](docs/lakehouse_structure.png)

---

## 🚀 Pipeline Workflow

### Automated Execution Flow

**Step 1:** Upload new monthly CSV to Files/Bronze/

**Step 2:** Run ETL Pipeline (one click)
- Activity 1: Bronze to Silver (incremental load)
- Activity 2: QA Checks (validation gate)
- Activity 3: Silver to Gold (aggregations)
- Activity 4: Generate PDF (optional)

**Step 3:** Refresh Power BI semantic model

**Step 4:** Dashboard updates with new data

### Monthly Runtime
- Bronze to Silver: ~30 seconds (incremental)
- QA Checks: ~10 seconds
- Silver to Gold: ~30 seconds
- PDF Generation: ~30 seconds
- **Total: ~2 minutes**

---

## 📈 Results & Impact

### Quantifiable Outcomes
- **Processing time:** 2+ hours → 2 minutes (98.3% reduction)
- **Data quality:** 7% duplicate removal
- **Scalability:** 4K → 100K+ transactions supported
- **Reliability:** Automated QA catches issues before reporting
- **Maintainability:** Modular design, production-ready patterns

### Power BI Dashboard Features
- **Page 1:** Monthly revenue trends, KPI cards, category breakdown
- **Page 2:** Top products analysis, quarterly performance, customer metrics
- **Filters:** Year slicer enables year-over-year comparison
- **Real-time:** Updates automatically when semantic model refreshes

---

## 🎓 Key Learnings

### What Went Well
✅ Medallion architecture provided clear separation of concerns  
✅ Incremental loading drastically reduced processing time  
✅ Metadata tracking enabled idempotent pipeline runs  
✅ Star schema simplified semantic model complexity  
✅ QA gate prevented bad data from reaching production  

### Challenges Overcome

**Challenge 1: Power BI Sorting**
- **Problem:** Power BI web can't set "Sort by Column" like Desktop
- **Solution:** Engineered Month_Short format ("02/23") that sorts correctly alphabetically

**Challenge 2: Duplicate Transactions**
- **Problem:** 316 duplicate transactions corrupting revenue totals
- **Solution:** Implemented deduplication at Silver layer with composite key

**Challenge 3: Cross-Visual Filtering**
- **Problem:** Year slicer didn't filter all visuals
- **Solution:** Re-architected Gold tables to include Year dimension + star schema

**Challenge 4: Fabric Filesystem Access**
- **Problem:** Matplotlib couldn't write directly to Fabric filesystem
- **Solution:** Write to temp location, then copy with mssparkutils

---

## 🔮 Future Enhancements

### Technical Roadmap
- [ ] **ML Churn Prediction:** Identify at-risk customers before they cancel
- [ ] **Real-time Streaming:** Process transactions as they occur
- [ ] **Multi-location Support:** Expand to 5 gym locations
- [ ] **Predictive Forecasting:** Revenue and attendance predictions
- [ ] **Alert System:** Email notifications for anomalies

### Business Features
- [ ] Email automation (send PDF directly to client)
- [ ] Mobile-friendly dashboard
- [ ] Custom date range selection
- [ ] Benchmark against industry standards
- [ ] Customer lifetime value analysis

---

## 👨‍💻 About This Project

**Built by:** Carl Simpson  
**Purpose:** Client deliverable + portfolio demonstration  
**Duration:** 1 week end-to-end implementation  

**Skills Demonstrated:**
- Data Engineering (ETL, pipeline orchestration, incremental loading)
- Data Modeling (star schema, dimensional design)
- Python (PySpark, pandas, matplotlib)
- Business Intelligence (Power BI, DAX, semantic modeling)
- Problem-solving (overcame 5+ technical roadblocks)
- Production thinking (error handling, QA gates, metadata tracking)


## 📄 License

This project is for portfolio demonstration purposes. Sample data is anonymized and representative only.

---

**⭐ If this project demonstrates skills relevant to your organization, let's connect!**
