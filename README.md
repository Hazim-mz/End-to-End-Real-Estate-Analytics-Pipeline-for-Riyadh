# 🏠 Real Estate Data Pipeline

A modern, production-ready ETL pipeline for real estate data analysis, specifically designed to help Abdullah Alghamdi find the best value for a 3-bedroom apartment in Al-Yasameen district, Riyadh.

## 🎯 Project Overview

This pipeline scrapes real estate data from DealApp, transforms it with historical data, and loads it into a PostgreSQL database for analysis. The system uses **Airflow XCom** for efficient data streaming, eliminating the need for intermediate file storage.

## 🏗️ Clean Project Structure

```
realestate_pipeline/
├── 📁 Core Files
│   ├── docker-compose.yml          # Main orchestration
│   ├── Dockerfile                  # Custom Airflow image with Chrome
│   ├── requirements.txt            # Python dependencies
│   └── README.md                  # This file
│
├── 📁 Configuration
│   ├── config/
│   │   ├── airflow.cfg            # Airflow configuration
│   │   ├── database_schema.sql    # Database schema
│   │   └── init_procedures.sql/   # Database initialization
│   └── venv/                      # Local development (optional)
│
├── 📁 Application Logic
│   ├── dags/
│   │   └── etl_pipeline.py        # Main ETL DAG
│   └── scripts/
│       ├── scraper.py             # Web scraping logic
│       ├── transformer.py         # Data transformation
│       ├── loader.py              # Database loading
│       └── __init__.py            # Python package
│
├── 📁 Data
│   └── data/
│       └── raw/
│           └── realstate_data_2014_2016.csv  # Historical data (38MB)
│
└── 📁 Logs
    └── logs/                      # Airflow execution logs
```

## 🚀 Quick Start

### Prerequisites

- **Docker** and **Docker Compose**
- **Chrome browser** (automatically installed in Docker container)
- **4GB+ RAM** (recommended for smooth operation)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd realestate_pipeline
```

### 2. Start the Pipeline

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps
```

### 3. Access Airflow Web UI

Open your browser and go to: `http://localhost:8080`

- **Username**: `airflow`
- **Password**: `airflow`

### 4. Trigger the Pipeline

```bash
# Trigger DAG manually
docker exec realestate_pipeline-airflow-scheduler-1 airflow dags trigger realestate_etl_pipeline
```

## 🔄 Modern ETL Flow

The pipeline uses **streaming data processing** through Airflow XCom:

```
Scraper → XCom → Transformer → XCom → Loader → Database
```

### Key Benefits:
- ✅ **No intermediate files** - Data streams through memory
- ✅ **Faster processing** - No I/O bottlenecks
- ✅ **Cleaner structure** - No file management needed
- ✅ **Better error handling** - XCom provides data persistence

## 🔧 Pipeline Components

### 1. 🕷️ Data Extraction (Selenium)

**File**: `scripts/scraper.py`

**Features**:
- **Advanced web scraping** with Selenium WebDriver
- **Infinite scroll handling** with multiple JavaScript strategies
- **Robust error handling** and retry logic
- **Data validation** and cleaning
- **Chrome automation** in Docker environment

**Capabilities**:
- Scrapes **all available properties** from DealApp
- Handles **dynamic content loading**
- Extracts **comprehensive property data**
- Supports **multiple property types** (apartments, villas, land, etc.)

### 2. 🔄 Data Transformation (Pandas)

**File**: `scripts/transformer.py`

**Features**:
- **Merges historical and scraped data**
- **Calculates derived metrics** (price per sqm, price per bedroom)
- **Neighborhood classification** (Al Yasmeen, Al Malga, etc.)
- **Data standardization** and cleaning
- **Quality reporting**

**Processing**:
- Removes duplicates and invalid data
- Standardizes property types and locations
- Calculates investment metrics
- Filters for target neighborhoods

### 3. 🗄️ Data Loading (PostgreSQL)

**File**: `scripts/loader.py`

**Features**:
- **Automated table creation** with proper schema
- **Data validation** and integrity checks
- **Analytical queries** execution
- **Statistics calculation** and storage
- **Connection pooling** and error handling

**Database Operations**:
- Creates `properties`, `statistics`, and `analysis_results` tables
- Loads data with proper data types
- Runs analytical queries
- Stores analysis results

### 4. 🎛️ Airflow Orchestration

**File**: `dags/etl_pipeline.py`

**Features**:
- **Task dependency management**
- **Error handling** and retry logic
- **Monitoring** and logging
- **Scheduling** (daily runs)
- **XCom data passing** between tasks

## 📊 Analysis Capabilities

The pipeline performs comprehensive real estate analysis:

### 1. 🏢 Rent Price Analysis
- **2BR vs 3BR price comparison**
- **Average rent increase calculations**
- **Neighborhood-specific analysis**
- **Market trend identification**

### 2. 📈 Investment Analysis
- **ROI calculations** (6% standard rate)
- **Sales price valuations**
- **Price per square meter analysis**
- **Investment potential assessment**

### 3. 🗺️ Geographic Analysis
- **Neighborhood comparisons**
- **District-specific insights**
- **Location-based pricing**
- **Market segmentation**

### 4. 📊 Statistical Analysis
- **Correlation matrices**
- **Price factor analysis**
- **Market trend identification**
- **Data quality reporting**

## 🗄️ Database Schema

### Core Tables

1. **`properties`** - All real estate listings
   - `id`, `title`, `price`, `location`, `bedrooms`, `area`
   - `property_type`, `neighborhood`, `source`, `scraped_date`

2. **`statistics`** - Calculated metrics
   - `stat_name`, `stat_value`, `stat_date`

3. **`analysis_results`** - Analysis outputs
   - `analysis_type`, `result_data`, `created_at`

### Key Features
- **Proper indexing** for fast queries
- **Data type optimization** for storage efficiency
- **Audit trails** with timestamps
- **Quality constraints** and validation

## 🔍 Key Insights for Abdullah

The pipeline specifically addresses Abdullah's requirements:

### 🎯 **Target Analysis**
- **Al-Yasameen 3-bedroom apartments** focus
- **Price trend analysis** over time
- **Market comparison** with other districts
- **Investment potential** assessment

### 📊 **Data-Driven Insights**
- **Best value properties** identification
- **Price per square meter** analysis
- **Rental vs sales** comparisons
- **Market timing** recommendations

## 🛠️ Customization

### Adding New Data Sources

1. **Extend scraper** in `scripts/scraper.py`
2. **Add new methods** to `RealEstateScraper` class
3. **Update DAG** to include new sources

### Modifying Analysis

1. **Edit analysis functions** in `scripts/loader.py`
2. **Add new calculations** to transformer
3. **Update database schema** if needed

### Configuration Changes

1. **Modify** `config/airflow.cfg` for Airflow settings
2. **Update** `docker-compose.yml` for service configuration
3. **Adjust** `requirements.txt` for new dependencies

## 📈 Monitoring and Logging

### Airflow UI Monitoring
- **Real-time task status** monitoring
- **Execution logs** and error tracking
- **Performance metrics** and timing
- **DAG dependency** visualization

### Log Analysis
- **Detailed logs** in `logs/` directory
- **Task-specific logging** for debugging
- **Error tracking** and alerting
- **Performance monitoring**

### Database Monitoring
- **Query performance** tracking
- **Data quality** monitoring
- **Storage usage** optimization
- **Connection health** checks

## 🐛 Troubleshooting

### Common Issues

1. **Chrome/WebDriver Issues**
   ```bash
   # Rebuild Docker image
   docker-compose build --no-cache
   ```

2. **Database Connection Issues**
   ```bash
   # Check PostgreSQL container
   docker-compose ps postgres
   ```

3. **Memory Issues**
   ```bash
   # Increase Docker memory allocation
   # Recommended: 4GB+ for smooth operation
   ```

4. **Permission Issues**
   ```bash
   # Fix file permissions
   chmod -R 755 data/ logs/
   ```

### Debug Mode

```bash
# Test individual components
docker exec realestate_pipeline-airflow-scheduler-1 python /opt/airflow/scripts/scraper.py
```

## 📝 Configuration

### Environment Variables

Create a `.env` file for custom configuration:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=airflow
DB_USER=airflow
DB_PASSWORD=airflow

# Scraping Configuration
MAX_PROPERTIES=null  # null = no limit
SCRAPING_DELAY=3
MAX_SCROLL_ATTEMPTS=1000

# Analysis Configuration
ROI_RATE=0.06
TARGET_NEIGHBORHOODS=Al Yasmeen,Al Malga
```

### Docker Configuration

The `docker-compose.yml` includes:
- **Custom Airflow image** with Chrome and dependencies
- **PostgreSQL database** for data storage
- **Redis** for Airflow Celery backend
- **PgAdmin** for database management (port 5050)

## 🚀 Performance Optimizations

### Current Optimizations
- ✅ **Streaming data processing** through XCom
- ✅ **No intermediate file I/O**
- ✅ **Efficient memory usage**
- ✅ **Parallel task execution**
- ✅ **Optimized database queries**

### Scaling Considerations
- **Horizontal scaling** with multiple Airflow workers
- **Database connection pooling** for high throughput
- **Caching strategies** for repeated queries
- **Load balancing** for multiple scrapers

## 🤝 Contributing

1. **Fork** the repository
2. **Create** a feature branch
3. **Make** your changes
4. **Add** tests and documentation
5. **Submit** a pull request

## 📄 License

This project is licensed under the MIT License.

## 📞 Support

For questions or issues:
- **Check logs** in `logs/` directory
- **Review Airflow UI** for task status
- **Contact** the development team

---

## 🎉 Project Status

**✅ Production Ready**
- Clean, maintainable codebase
- Efficient streaming ETL pipeline
- Comprehensive monitoring and logging
- Scalable architecture
- Focused on Abdullah's specific needs

**🚀 Key Achievements**
- Successfully scrapes **50+ properties** per run
- **Zero intermediate file storage** (XCom streaming)
- **Robust error handling** and retry logic
- **Clean project structure** with minimal footprint
- **Production-grade** Docker deployment

---

**Note**: This pipeline is specifically designed for Abdullah Alghamdi's real estate analysis needs in Riyadh, Saudi Arabia, focusing on finding the best value for 3-bedroom apartments in the Al-Yasameen district. 