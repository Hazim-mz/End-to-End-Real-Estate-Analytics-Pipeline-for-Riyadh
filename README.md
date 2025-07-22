# 🏠 End-to-End Real Estate Analytics Pipeline for Riyadh

A modern, production-ready ETL and analytics pipeline for real estate data in Riyadh, Saudi Arabia. This project enables automated data collection, transformation, storage, and advanced analysis for property market insights.

---

## 🚀 Features
- **Automated ETL**: Scrapes, cleans, and loads real estate data from multiple sources (web + historical CSV) into PostgreSQL.
- **Airflow Orchestration**: Robust, scheduled workflows with monitoring and error handling.
- **Streaming Data**: Uses Airflow XCom for efficient, file-free data transfer between tasks.
- **Comprehensive Analysis**: Calculates rent/sale trends, ROI, price per sqm, correlation matrices, and more.
- **Power BI Ready**: Database schema and analysis tables designed for direct BI integration.
- **Dockerized**: One-command startup for Airflow, Postgres, and all dependencies.

---

## 📂 Project Structure
```
realestate_pipeline/
├── dags/                # Airflow DAGs (ETL orchestration)
├── scripts/             # Scraper, transformer, loader modules
├── config/              # Airflow & DB config
├── data/                # Raw and processed data
├── logs/                # Airflow logs
├── docker-compose.yml   # Multi-service orchestration
├── Dockerfile           # Custom Airflow image
├── requirements.txt     # Python dependencies
└── README.md            # Project documentation
```

---

## ⚡ Quickstart

### Prerequisites
- Docker & Docker Compose
- (Optional) Power BI Desktop for visualization

### 1. Clone the Repository
```bash
git clone https://github.com/Hazim-mz/End-to-End-Real-Estate-Analytics-Pipeline-for-Riyadh.git
cd End-to-End-Real-Estate-Analytics-Pipeline-for-Riyadh
```

### 2. Start the Pipeline
```bash
docker-compose up -d
```

### 3. Access Airflow & Database
- Airflow UI: [http://localhost:8080](http://localhost:8080) (user: airflow / pass: airflow)
- PgAdmin: [http://localhost:5050](http://localhost:5050)

### 4. Trigger the ETL Pipeline
- In Airflow UI, trigger the `realestate_etl_pipeline_v2` DAG (runs weekly on Saturdays by default).

---

## 🏗️ ETL Pipeline Overview
- **Scraper**: Collects property data from DealApp and historical CSVs.
- **Transformer**: Cleans, merges, and standardizes data (deduplication, type mapping, price per sqm, etc.).
- **Loader**: Loads data into PostgreSQL, performs upserts, and runs analysis queries.
- **Analysis**: Generates tables for rent/sale trends, ROI, price per sqm, correlation, and more.

---

## 📊 Key Analyses & Insights
- **Rent Increase Analysis**: Average rate increase from 2BR to 3BR apartments (rent & sale).
- **Sales Valuation**: Rental properties valued at 6% ROI, with price per sqm by type/district.
- **Correlation Matrix**: Price drivers (area, bedrooms, price per sqm, etc.) for rent and sale.
- **Neighborhood Comparison**: Price per sqm by district to reveal premium/bargain areas.
- **General Insights**: Price/area distributions, property type mix, and market trends.

---

## 📈 Visualize in Power BI
- Connect Power BI to your Postgres DB (localhost:5432, user: airflow, pass: airflow).
- Use analysis tables (e.g., `sales_valuation_analysis`, `rent_increase_analysis`, `correlation_analysis`) for dashboards.
- Recommended visuals: price per sqm by neighborhood, rent/sale trends, correlation heatmaps.

---

## 🛠️ Customization
- **Add new data sources**: Extend `scripts/scraper.py` and update the DAG.
- **Modify analysis**: Edit `scripts/loader.py` for new metrics or business logic.
- **Change schedule**: Update the `schedule` parameter in the DAG for different ETL frequency.

---

## 🐳 Docker & Environment
- All services (Airflow, Postgres, PgAdmin) run in containers.
- Environment variables can be set in `.env` for DB credentials and scraping configs.
- `.gitignore` excludes logs, venv, and sensitive files.

---

## 🤝 Contributing
1. Fork the repo
2. Create a feature branch
3. Make your changes
4. Add tests/docs
5. Submit a pull request

---

## 📄 License
MIT License

---

## 💡 Project Status
- Production-ready, modular, and scalable
- Designed for real estate analytics in Riyadh
- Easily extensible for new sources, analyses, or markets

---

**For questions or support, open an issue or contact the maintainer via GitHub.** 
