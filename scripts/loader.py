"""
Real Estate Data Loader
Loads transformed data into PostgreSQL database
"""

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import logging
import os
from datetime import datetime
import json
from pandas import Timestamp
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealEstateLoader:
    def __init__(self, db_config=None):
        self.db_config = db_config or {
            'host': 'postgres',  # Use Docker service name
            'port': 5432,
            'database': 'postgres',  # Use postgres database
            'user': 'airflow',
            'password': 'airflow'
        }
        self.engine = None
        self.connection = None
        # Set Saudi Arabia timezone (GMT+3)
        self.saudi_tz = pytz.timezone('Asia/Riyadh')
    
    def get_saudi_time(self):
        """Get current time in Saudi Arabia timezone (GMT+3)"""
        return datetime.now(self.saudi_tz).strftime('%Y-%m-%d %H:%M:%S')

    def connect_to_database(self):
        """Establish connection to PostgreSQL database"""
        try:
            # Create SQLAlchemy engine
            connection_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            self.engine = create_engine(connection_string)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Successfully connected to PostgreSQL database")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            return False
    
    def create_tables(self):
        """Create necessary tables for real estate data (DESTRUCTIVE: for development/reset only!)"""
        try:
            with self.engine.connect() as conn:
                
                # Create properties table with proper unique constraint and created_at
                properties_table = """
                CREATE TABLE IF NOT EXISTS properties (
                    id SERIAL PRIMARY KEY,
                    property_id VARCHAR(100) UNIQUE NOT NULL,
                    price DECIMAL(25,2),
                    bedrooms INTEGER,
                    area DECIMAL(15,2),
                    bathrooms INTEGER,
                    property_type VARCHAR(50),
                    property_type_standard VARCHAR(50),
                    price_per_sqm DECIMAL(25,2),
                    price_per_bedroom DECIMAL(25,2),
                    city VARCHAR(100),
                    neighborhood VARCHAR(100),
                    listing_type VARCHAR(20),
                    source VARCHAR(50),
                    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                # Create statistics table
                statistics_table = """
                CREATE TABLE IF NOT EXISTS statistics (
                    id SERIAL PRIMARY KEY,
                    stat_name VARCHAR(100),
                    stat_value VARCHAR(100),
                    stat_category VARCHAR(50),
                    stat_unit VARCHAR(20),
                    stat_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                # Create analysis tables
                rent_increase_table = """
                CREATE TABLE IF NOT EXISTS rent_increase_analysis (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(100),
                    neighborhood VARCHAR(100),
                    listing_type VARCHAR(20),
                    two_bedroom_avg_price DECIMAL(20,2),
                    three_bedroom_avg_price DECIMAL(20,2),
                    price_increase_rate DECIMAL(10,4),
                    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                correlation_table = """
                CREATE TABLE IF NOT EXISTS correlation_analysis (
                    id SERIAL PRIMARY KEY,
                    feature1 VARCHAR(50),
                    feature2 VARCHAR(50),
                    correlation_value DECIMAL(10,6),
                    context VARCHAR(50),
                    normalized_method VARCHAR(50),
                    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                sales_valuation_table = """
                CREATE TABLE IF NOT EXISTS sales_valuation_analysis (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(100),
                    neighborhood VARCHAR(100),
                    property_type VARCHAR(50),
                    avg_rent DECIMAL(20,2),
                    estimated_sales_price DECIMAL(20,2),
                    price_per_sqm_sales DECIMAL(20,2),
                    property_count INTEGER,
                    roi_rate DECIMAL(5,4),
                    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                avg_price_location_table = """
                CREATE TABLE IF NOT EXISTS avg_price_by_location (
                    id SERIAL PRIMARY KEY,
                    neighborhood VARCHAR(100),
                    property_type_standard VARCHAR(50),
                    property_count INTEGER,
                    avg_price DECIMAL(20,2),
                    min_price DECIMAL(20,2),
                    max_price DECIMAL(20,2),
                    avg_price_per_sqm DECIMAL(20,2),
                    avg_price_per_bedroom DECIMAL(20,2),
                    listing_type VARCHAR(20),
                    city VARCHAR(100),
                    standardized_price_score DECIMAL(10,4),
                    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                avg_price_bedroom_table = """
                CREATE TABLE IF NOT EXISTS avg_price_by_bedroom (
                    id SERIAL PRIMARY KEY,
                    bedrooms INTEGER,
                    property_type_standard VARCHAR(50),
                    neighborhood VARCHAR(100),
                    property_count INTEGER,
                    avg_price DECIMAL(20,2),
                    min_price DECIMAL(20,2),
                    max_price DECIMAL(20,2),
                    avg_price_per_sqm DECIMAL(20,2),
                    avg_price_per_bedroom DECIMAL(20,2),
                    avg_area DECIMAL(10,2),
                    listing_type VARCHAR(20),
                    city VARCHAR(100),
                    total_area_sum DECIMAL(20,2),
                    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                yasameen_3br_table = """
                CREATE TABLE IF NOT EXISTS yasameen_3br_report (
                    id SERIAL PRIMARY KEY,
                    property_id VARCHAR(100),
                    price DECIMAL(20,2),
                    neighborhood VARCHAR(100),
                    area DECIMAL(10,2),
                    bedrooms INTEGER,
                    property_type VARCHAR(50),
                    price_per_sqm DECIMAL(20,2),
                    price_per_bedroom DECIMAL(20,2),
                    analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                # Execute all table creation statements
                # conn.execute(text(drop_properties_table)) # Commented out as per edit hint
                conn.execute(text(properties_table))
                conn.execute(text(statistics_table))
                conn.execute(text(rent_increase_table))
                conn.execute(text(correlation_table))
                conn.execute(text(sales_valuation_table))
                conn.execute(text(avg_price_location_table))
                conn.execute(text(avg_price_bedroom_table))
                conn.execute(text(yasameen_3br_table))
                
                logger.info("Database tables created successfully")
                
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
    
    def create_tables_if_not_exist(self):
        """Create necessary tables only if they don't exist (preserves existing data)"""
        try:
            with self.engine.connect() as conn:
                # List of all required tables
                required_tables = [
                    'properties', 'statistics', 'rent_increase_analysis', 'correlation_analysis',
                    'sales_valuation_analysis', 'avg_price_by_location', 'avg_price_by_bedroom', 'yasameen_3br_report'
                ]
                # Query for all tables in public schema
                check_tables_query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public';
                """
                result = conn.execute(text(check_tables_query))
                existing_tables = {row[0] for row in result}
                missing_tables = [t for t in required_tables if t not in existing_tables]
                if missing_tables:
                    logger.info(f"Missing tables: {missing_tables}. Creating them now...")
                    self.create_tables()  # This will create all tables if any are missing
                else:
                    logger.info("All required tables already exist. Skipping creation.")
                logger.info(f"Tables in public schema: {sorted(existing_tables)}")
                return sorted(existing_tables)
        except Exception as e:
            logger.error(f"Error checking/creating tables: {e}")
            return False
    
    def load_properties_data(self, data_file):
        """Load properties data from CSV to PostgreSQL"""
        try:
            # Read the CSV file
            df = pd.read_csv(data_file)
            
            # Convert timestamp columns
            if 'processed_date' in df.columns:
                df['processed_date'] = pd.to_datetime(df['processed_date'])
            
            # Load data to database
            df.to_sql('properties', self.engine, if_exists='append', index=False)
            
            logger.info(f"Successfully loaded {len(df)} properties to database")
            return True
            
        except Exception as e:
            logger.error(f"Error loading properties data: {e}")
            return False
    
    def load_properties_dataframe(self, df):
        """Load properties data from DataFrame to PostgreSQL"""
        try:
            for col in ['title', 'scraped_date']:
                if col in df.columns:
                    df = df.drop(columns=[col])
            logger.info(f"Loading {len(df)} properties to database")
            logger.info(f"Property IDs in data: {df['property_id'].nunique()} unique IDs")
            existing_ids = self.get_existing_property_ids()
            new_ids = set(df['property_id'].dropna())
            duplicates = new_ids.intersection(existing_ids)
            if duplicates:
                logger.info(f"Found {len(duplicates)} existing property_ids that will be updated")
            else:
                logger.info("All property_ids are new - will be inserted")
            if 'processed_date' in df.columns:
                df['processed_date'] = pd.to_datetime(df['processed_date'], errors='coerce')
                df['processed_date'] = df['processed_date'].fillna(pd.Timestamp.now(self.saudi_tz))
            if 'created_at' not in df.columns:
                df['created_at'] = pd.Timestamp.now(self.saudi_tz)
            for _, row in df.iterrows():
                row_dict = {col: (None if pd.isna(row[col]) else row[col]) for col in df.columns}
                columns = ', '.join(df.columns)
                placeholders = ', '.join([f':{col}' for col in df.columns])
                update_set = ', '.join([f'{col} = EXCLUDED.{col}' for col in df.columns if col not in ['id', 'created_at', 'title', 'scraped_date']])
                upsert_query = f"""
                INSERT INTO properties ({columns})
                VALUES ({placeholders})
                ON CONFLICT (property_id) DO UPDATE SET
                    {update_set}
                """
                with self.engine.connect() as conn:
                    conn.execute(text(upsert_query), row_dict)
            logger.info(f"Successfully upserted {len(df)} properties to database from DataFrame")
            return True
        except Exception as e:
            logger.error(f"Error loading properties data from DataFrame: {e}")
            return False

    def load_properties_dataframe_batch(self, df):
        """Load properties data from DataFrame to PostgreSQL using batch UPSERT for better performance"""
        try:
            # Remove 'title' and 'scraped_date' columns if present
            for col in ['title', 'scraped_date']:
                if col in df.columns:
                    df = df.drop(columns=[col])
            logger.info(f"Loading {len(df)} properties to database using batch UPSERT")
            logger.info(f"Property IDs in data: {df['property_id'].nunique()} unique IDs")
            existing_ids = self.get_existing_property_ids()
            new_ids = set(df['property_id'].dropna())
            duplicates = new_ids.intersection(existing_ids)
            if duplicates:
                logger.info(f"Found {len(duplicates)} existing property_ids that will be updated")
                logger.info(f"Sample duplicate IDs: {list(duplicates)[:5]}")
            else:
                logger.info("All property_ids are new - will be inserted")
            if 'processed_date' in df.columns:
                df['processed_date'] = pd.to_datetime(df['processed_date'], errors='coerce')
                df['processed_date'] = df['processed_date'].fillna(pd.Timestamp.now(self.saudi_tz))
            if 'created_at' not in df.columns:
                df['created_at'] = pd.Timestamp.now(self.saudi_tz)
            records = []
            for _, row in df.iterrows():
                row_dict = {col: (None if pd.isna(row[col]) else row[col]) for col in df.columns}
                records.append(row_dict)
            columns = ', '.join(df.columns)
            placeholders = ', '.join([f':{col}' for col in df.columns])
            update_set = ', '.join([f'{col} = EXCLUDED.{col}' for col in df.columns if col not in ['id', 'created_at', 'title', 'scraped_date']])
            upsert_query = f"""
            INSERT INTO properties ({columns})
            VALUES ({placeholders})
            ON CONFLICT (property_id) DO UPDATE SET
                {update_set}
            """
            with self.engine.connect() as conn:
                logger.info(f"Executing batch UPSERT with {len(records)} records")
                logger.info(f"UPSERT Query: {upsert_query}")
                result = conn.execute(text(upsert_query), records)
                logger.info(f"Batch UPSERT completed successfully")
            logger.info(f"Successfully batch upserted {len(df)} properties to database from DataFrame")
            return True
        except Exception as e:
            logger.error(f"Error loading properties data from DataFrame: {e}")
            logger.error(f"Error details: {str(e)}")
            return False

    def get_existing_property_ids(self):
        """Get list of existing property_ids from database"""
        try:
            with self.engine.connect() as conn:
                query = "SELECT property_id FROM properties WHERE property_id IS NOT NULL"
                result = conn.execute(text(query))
                existing_ids = {row.property_id for row in result}
                logger.info(f"Found {len(existing_ids)} existing property_ids in database")
                return existing_ids
        except Exception as e:
            logger.error(f"Error getting existing property_ids: {e}")
            return set()

    def verify_unique_constraint(self):
        """Verify that the unique constraint on property_id exists and is working"""
        try:
            with self.engine.connect() as conn:
                # Check if unique constraint exists
                constraint_query = """
                SELECT constraint_name 
                FROM information_schema.table_constraints 
                WHERE table_name = 'properties' 
                AND constraint_type = 'UNIQUE'
                AND constraint_name LIKE '%property_id%'
                """
                result = conn.execute(text(constraint_query))
                constraints = [row.constraint_name for row in result]
                
                if constraints:
                    logger.info(f"Found unique constraint(s): {constraints}")
                else:
                    logger.warning("No unique constraint found on property_id!")
        except Exception as e:
            logger.error(f"Error verifying unique constraint: {e}")
    
    def load_statistics(self, stats_file):
        """Load statistics data from JSON to PostgreSQL"""
        try:
            with open(stats_file, 'r') as f:
                stats = json.load(f)
            
            # Convert to DataFrame
            stats_df = pd.DataFrame([
                {'stat_name': key, 'stat_value': value}
                for key, value in stats.items()
                if isinstance(value, (int, float))
            ])
            
            # Load to database
            stats_df.to_sql('statistics', self.engine, if_exists='append', index=False)
            
            logger.info(f"Successfully loaded {len(stats_df)} statistics to database")
            return True
            
        except Exception as e:
            logger.error(f"Error loading statistics: {e}")
            return False
    
    def load_analysis_results(self, analysis_type, results_data):
        """Load analysis results to database"""
        try:
            # Convert results to JSON
            results_json = json.dumps(results_data)
            # Insert into analysis_results table
            with self.engine.connect() as conn:
                insert_query = """
                INSERT INTO analysis_results (analysis_type, result_data)
                VALUES (:analysis_type, :result_data)
                """
                conn.execute(text(insert_query), {
                    'analysis_type': analysis_type,
                    'result_data': results_json
                })
            logger.info(f"Successfully loaded {analysis_type} analysis results")
            return True
        except Exception as e:
            logger.error(f"Error loading analysis results: {e}")
            return False
    
    def calculate_rent_increase_analysis(self):
        """Calculate rent price increase from 2BR to 3BR, grouped by listing_type (and families/singles if available)"""
        try:
            with self.engine.connect() as conn:
                # Group by listing_type (e.g., 'rent', 'sale')
                query = """
                SELECT 
                    listing_type,
                    bedrooms,
                    AVG(price) as avg_price,
                    COUNT(*) as property_count
                FROM properties 
                WHERE property_type_standard = 'apartment' 
                    AND bedrooms IN (2, 3)
                GROUP BY listing_type, bedrooms
                ORDER BY listing_type, bedrooms
                """
                result = conn.execute(text(query))
                data = result.fetchall()
                # Insert for each listing_type
                for listing_type in set(row.listing_type for row in data):
                    two_br = next((row for row in data if row.listing_type == listing_type and row.bedrooms == 2), None)
                    three_br = next((row for row in data if row.listing_type == listing_type and row.bedrooms == 3), None)
                    if two_br and three_br:
                        increase_rate = ((three_br.avg_price - two_br.avg_price) / two_br.avg_price) * 100
                        insert_query = """
                        INSERT INTO rent_increase_analysis 
                        (listing_type, two_bedroom_avg_price, three_bedroom_avg_price, price_increase_rate)
                        VALUES (:listing_type, :two_br_avg, :three_br_avg, :increase_rate)
                        """
                        conn.execute(text(insert_query), {
                            'listing_type': listing_type,
                            'two_br_avg': float(two_br.avg_price),
                            'three_br_avg': float(three_br.avg_price),
                            'increase_rate': float(increase_rate)
                        })
                        logger.info(f"Rent increase analysis for {listing_type}: {increase_rate:.2f}%")
        except Exception as e:
            logger.error(f"Error in rent increase analysis: {e}")
            return None

    def calculate_correlation_analysis(self):
        """Calculate correlation matrix for price affecting parameters, by listing_type, with normalization and context"""
        try:
            with self.engine.connect() as conn:
                for listing_type in ['rent', 'sale']:
                    query = f"""
                    SELECT price, area, bedrooms, price_per_sqm, price_per_bedroom
                    FROM properties 
                    WHERE price > 0 AND area > 0 AND bedrooms > 0 AND listing_type = '{listing_type}'
                    """
                    df = pd.read_sql(query, self.engine)
                    if df.empty:
                        continue
                    # Normalize (z-score)
                    df_norm = (df - df.mean()) / df.std()
                    correlation_matrix = df_norm.corr()
                    for feature1 in correlation_matrix.columns:
                        for feature2 in correlation_matrix.columns:
                            if feature1 != feature2:
                                corr_value = correlation_matrix.loc[feature1, feature2]
                                insert_query = """
                                INSERT INTO correlation_analysis 
                                (feature1, feature2, correlation_value, context, normalized_method)
                                VALUES (:feature1, :feature2, :corr_value, :context, :normalized_method)
                                """
                                conn.execute(text(insert_query), {
                                    'feature1': feature1,
                                    'feature2': feature2,
                                    'corr_value': float(corr_value),
                                    'context': listing_type,
                                    'normalized_method': 'z-score'
                                })
            logger.info("Correlation analysis completed for rent and sale")
        except Exception as e:
            logger.error(f"Error in correlation analysis: {e}")
            return None

    def calculate_sales_valuation(self):
        """Calculate sales price valuation based on 6% ROI, add listing_type, city, meter price distribution"""
        try:
            with self.engine.connect() as conn:
                query = """
                SELECT 
                    city,
                    neighborhood,
                    property_type_standard,
                    listing_type,
                    AVG(price) as avg_rent,
                    AVG(area) as avg_area,
                    COUNT(*) as property_count
                FROM properties 
                WHERE price > 0 AND area > 0 AND listing_type = 'rent'
                GROUP BY city, neighborhood, property_type_standard, listing_type
                """
                result = conn.execute(text(query))
                data = result.fetchall()
                for row in data:
                    annual_rent = float(row.avg_rent) * 12
                    estimated_sales_price = annual_rent / 0.06
                    price_per_sqm_sales = estimated_sales_price / float(row.avg_area)
                    insert_query = """
                    INSERT INTO sales_valuation_analysis 
                    (city, neighborhood, property_type, avg_rent, estimated_sales_price, 
                     price_per_sqm_sales, property_count, roi_rate, listing_type)
                    VALUES (:city, :neighborhood, :property_type, :avg_rent, :estimated_sales_price,
                            :price_per_sqm_sales, :property_count, :roi_rate, :listing_type)
                    """
                    conn.execute(text(insert_query), {
                        'city': row.city,
                        'neighborhood': row.neighborhood,
                        'property_type': row.property_type_standard,
                        'avg_rent': float(row.avg_rent),
                        'estimated_sales_price': float(estimated_sales_price),
                        'price_per_sqm_sales': float(price_per_sqm_sales),
                        'property_count': row.property_count,
                        'roi_rate': 0.06,
                        'listing_type': row.listing_type
                    })
            logger.info("Sales valuation analysis completed")
        except Exception as e:
            logger.error(f"Error in sales valuation analysis: {e}")
            return None

    def calculate_meter_price_distribution(self):
        """Estimate meter price distribution per property type and district"""
        try:
            with self.engine.connect() as conn:
                query = """
                SELECT 
                    city,
                    neighborhood,
                    property_type_standard,
                    listing_type,
                    AVG(price_per_sqm) as avg_price_per_sqm,
                    MIN(price_per_sqm) as min_price_per_sqm,
                    MAX(price_per_sqm) as max_price_per_sqm,
                    COUNT(*) as property_count
                FROM properties 
                WHERE price_per_sqm > 0
                GROUP BY city, neighborhood, property_type_standard, listing_type
                ORDER BY avg_price_per_sqm DESC
                """
                result = conn.execute(text(query))
                data = result.fetchall()
                # TODO: Store or print as needed
                logger.info("Meter price distribution calculated")
        except Exception as e:
            logger.error(f"Error in meter price distribution: {e}")
            return None

    def calculate_general_insights(self):
        """Generate general insights from the data (e.g., outliers, trends, data quality)"""
        try:
            with self.engine.connect() as conn:
                # Example: Count properties by type
                query = """
                SELECT property_type_standard, COUNT(*) as count
                FROM properties
                GROUP BY property_type_standard
                ORDER BY count DESC
                """
                result = conn.execute(text(query))
                data = result.fetchall()
                logger.info("General insights: property type counts:")
                for row in data:
                    logger.info(f"  {row.property_type_standard}: {row.count}")
                # Add more insights as needed
        except Exception as e:
            logger.error(f"Error in general insights: {e}")
            return None

    def populate_avg_price_tables(self):
        """Populate average price analysis tables"""
        try:
            with self.engine.connect() as conn:
                
                # 1. Average prices by location
                avg_price_location_query = """
                INSERT INTO avg_price_by_location 
                (neighborhood, property_type_standard, property_count, 
                 avg_price, min_price, max_price, avg_price_per_sqm, avg_price_per_bedroom)
                SELECT 
                    neighborhood,
                    property_type_standard,
                    COUNT(*) as property_count,
                    AVG(price) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    AVG(price_per_sqm) as avg_price_per_sqm,
                    AVG(price_per_bedroom) as avg_price_per_bedroom
                FROM properties 
                WHERE price > 0
                GROUP BY neighborhood, property_type_standard
                ORDER BY avg_price DESC
                """
                
                # 2. Average prices by bedroom count
                avg_price_bedroom_query = """
                INSERT INTO avg_price_by_bedroom 
                (bedrooms, property_type_standard, neighborhood, property_count,
                 avg_price, min_price, max_price, avg_price_per_sqm, avg_price_per_bedroom, avg_area)
                SELECT 
                    bedrooms,
                    property_type_standard,
                    neighborhood,
                    COUNT(*) as property_count,
                    AVG(price) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    AVG(price_per_sqm) as avg_price_per_sqm,
                    AVG(price_per_bedroom) as avg_price_per_bedroom,
                    AVG(area) as avg_area
                FROM properties 
                WHERE price > 0 AND bedrooms > 0
                GROUP BY bedrooms, property_type_standard, neighborhood
                ORDER BY bedrooms, avg_price DESC
                """
                
                # Clear existing data
                conn.execute(text("DELETE FROM avg_price_by_location"))
                conn.execute(text("DELETE FROM avg_price_by_bedroom"))
                
                # Insert new data
                conn.execute(text(avg_price_location_query))
                conn.execute(text(avg_price_bedroom_query))
                
                logger.info("Average price tables populated successfully")
                
        except Exception as e:
            logger.error(f"Error populating average price tables: {e}")
    
    def populate_yasameen_3br_report(self):
        """Populate Yasmeen 3BR report table"""
        try:
            with self.engine.connect() as conn:
                
                # Query for Yasmeen 3BR properties
                yasameen_query = """
                INSERT INTO yasameen_3br_report 
                (price, neighborhood, area, bedrooms, property_type, 
                 price_per_sqm, price_per_bedroom)
                SELECT 
                    price,
                    neighborhood,
                    area,
                    bedrooms,
                    property_type,
                    price_per_sqm,
                    price_per_bedroom
                FROM properties 
                WHERE neighborhood = 'Al Yasmeen' 
                    AND bedrooms = 3
                    AND price > 0
                ORDER BY price ASC
                LIMIT 20
                """
                
                # Clear existing data
                conn.execute(text("DELETE FROM yasameen_3br_report"))
                
                # Insert new data
                conn.execute(text(yasameen_query))
                
                logger.info("Yasameen 3BR report populated successfully")
                
        except Exception as e:
            logger.error(f"Error populating Yasmeen 3BR report: {e}")
    
    def create_analysis_tables(self):
        """Create analysis tables and views for Abdullah's reports"""
        try:
            with self.engine.connect() as conn:
                
                # Drop existing views if they exist
                try:
                    conn.execute(text("DROP VIEW IF EXISTS avg_price_by_location_view"))
                    conn.execute(text("DROP VIEW IF EXISTS avg_price_by_bedroom_view"))
                    conn.execute(text("DROP VIEW IF EXISTS rent_increase_trends_view"))
                    conn.execute(text("DROP VIEW IF EXISTS price_correlation_matrix_view"))
                    conn.execute(text("DROP VIEW IF EXISTS yasameen_3br_report_view"))
                except Exception as e:
                    logger.debug(f"Error dropping views (may not exist): {e}")
                
                # 1. Average prices by location
                avg_price_by_location_view = """
                CREATE OR REPLACE VIEW avg_price_by_location_view AS
                SELECT 
                    neighborhood,
                    property_type_standard,
                    COUNT(*) as property_count,
                    AVG(price) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    AVG(price_per_sqm) as avg_price_per_sqm,
                    AVG(price_per_bedroom) as avg_price_per_bedroom
                FROM properties 
                WHERE price > 0
                GROUP BY neighborhood, property_type_standard
                ORDER BY avg_price DESC
                """
                
                # 2. Average prices by bedroom count
                avg_price_by_bedroom_view = """
                CREATE OR REPLACE VIEW avg_price_by_bedroom_view AS
                SELECT 
                    bedrooms,
                    property_type_standard,
                    neighborhood,
                    COUNT(*) as property_count,
                    AVG(price) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    AVG(price_per_sqm) as avg_price_per_sqm,
                    AVG(price_per_bedroom) as avg_price_per_bedroom,
                    AVG(area) as avg_area
                FROM properties 
                WHERE price > 0 AND bedrooms > 0
                GROUP BY bedrooms, property_type_standard, neighborhood
                ORDER BY bedrooms, avg_price DESC
                """
                
                # 3. Rent increase trends (monthly/yearly)
                rent_increase_trends_view = """
                CREATE OR REPLACE VIEW rent_increase_trends_view AS
                SELECT 
                    DATE_TRUNC('month', processed_date) as month,
                    neighborhood,
                    property_type_standard,
                    bedrooms,
                    COUNT(*) as listings_count,
                    AVG(price) as avg_price,
                    LAG(AVG(price)) OVER (
                        PARTITION BY neighborhood, property_type_standard, bedrooms 
                        ORDER BY DATE_TRUNC('month', processed_date)
                    ) as prev_month_avg_price,
                    CASE 
                        WHEN LAG(AVG(price)) OVER (
                            PARTITION BY neighborhood, property_type_standard, bedrooms 
                            ORDER BY DATE_TRUNC('month', processed_date)
                        ) > 0 
                        THEN ((AVG(price) - LAG(AVG(price)) OVER (
                            PARTITION BY neighborhood, property_type_standard, bedrooms 
                            ORDER BY DATE_TRUNC('month', processed_date)
                        )) / LAG(AVG(price)) OVER (
                            PARTITION BY neighborhood, property_type_standard, bedrooms 
                            ORDER BY DATE_TRUNC('month', processed_date)
                        )) * 100
                        ELSE 0
                    END as price_change_percentage
                FROM properties 
                WHERE price > 0 AND processed_date IS NOT NULL
                GROUP BY DATE_TRUNC('month', processed_date), neighborhood, property_type_standard, bedrooms
                ORDER BY month DESC, neighborhood, property_type_standard, bedrooms
                """
                
                # 4. Price correlation matrix (flattened)
                price_correlation_matrix_view = """
                CREATE OR REPLACE VIEW price_correlation_matrix_view AS
                WITH correlations AS (
                    SELECT 
                        'price_vs_area' as correlation_pair,
                        CORR(price, area) as correlation_value
                    FROM properties 
                    WHERE price > 0 AND area > 0
                    
                    UNION ALL
                    
                    SELECT 
                        'price_vs_bedrooms' as correlation_pair,
                        CORR(price, bedrooms) as correlation_value
                    FROM properties 
                    WHERE price > 0 AND bedrooms > 0
                    
                    UNION ALL
                    
                    SELECT 
                        'price_vs_price_per_sqm' as correlation_pair,
                        CORR(price, price_per_sqm) as correlation_value
                    FROM properties 
                    WHERE price > 0 AND price_per_sqm > 0
                    
                    UNION ALL
                    
                    SELECT 
                        'price_vs_price_per_bedroom' as correlation_pair,
                        CORR(price, price_per_bedroom) as correlation_value
                    FROM properties 
                    WHERE price > 0 AND price_per_bedroom > 0
                    
                    UNION ALL
                    
                    SELECT 
                        'area_vs_bedrooms' as correlation_pair,
                        CORR(area, bedrooms) as correlation_value
                    FROM properties 
                    WHERE area > 0 AND bedrooms > 0
                )
                SELECT 
                    correlation_pair,
                    correlation_value,
                    CASE 
                        WHEN ABS(correlation_value) >= 0.7 THEN 'Strong'
                        WHEN ABS(correlation_value) >= 0.3 THEN 'Moderate'
                        ELSE 'Weak'
                    END as correlation_strength
                FROM correlations
                ORDER BY ABS(correlation_value) DESC
                """
                
                # 5. Abdullah's specific report for Al Yasameen 3BR apartments
                yasameen_3br_report_view = """
                CREATE OR REPLACE VIEW yasameen_3br_report_view AS
                SELECT 
                    price,
                    neighborhood,
                    area,
                    price_per_sqm,
                    price_per_bedroom,
                    property_type_standard,
                    processed_date,
                    source,
                    CASE 
                        WHEN price_per_sqm <= (SELECT AVG(price_per_sqm) FROM properties WHERE neighborhood LIKE '%Yasameen%' AND bedrooms = 3) THEN 'Below Average'
                        WHEN price_per_sqm <= (SELECT AVG(price_per_sqm) * 1.2 FROM properties WHERE neighborhood LIKE '%Yasameen%' AND bedrooms = 3) THEN 'Average'
                        ELSE 'Above Average'
                    END as price_category,
                    CASE 
                        WHEN price <= (SELECT AVG(price) FROM properties WHERE neighborhood LIKE '%Yasameen%' AND bedrooms = 3) THEN 'Good Value'
                        WHEN price <= (SELECT AVG(price) * 1.15 FROM properties WHERE neighborhood LIKE '%Yasameen%' AND bedrooms = 3) THEN 'Fair Value'
                        ELSE 'Premium'
                    END as value_assessment
                FROM properties 
                WHERE neighborhood LIKE '%Yasameen%' 
                    AND bedrooms = 3 
                    AND property_type_standard = 'apartment'
                    AND price > 0
                ORDER BY price_per_sqm ASC, price ASC
                """
                
                # Execute all view creations
                conn.execute(text(avg_price_by_location_view))
                conn.execute(text(avg_price_by_bedroom_view))
                conn.execute(text(rent_increase_trends_view))
                conn.execute(text(price_correlation_matrix_view))
                conn.execute(text(yasameen_3br_report_view))
                
                logger.info("Analysis tables and views created successfully")
                return True
                
        except Exception as e:
            logger.error(f"Error creating analysis tables: {e}")
            return False
    
    def generate_analysis_report(self):
        """Generate comprehensive analysis report for Abdullah"""
        try:
            report = {
                'generated_date': datetime.now().isoformat(),
                'summary': {},
                'recommendations': [],
                'market_insights': {}
            }
            
            # 1. Market Summary
            with self.engine.connect() as conn:
                # Total properties
                result = conn.execute(text("SELECT COUNT(*) as total FROM properties"))
                total_properties = result.fetchone().total
                
                # Yasameen 3BR apartments
                result = conn.execute(text("""
                    SELECT COUNT(*) as count, AVG(price) as avg_price, AVG(price_per_sqm) as avg_price_per_sqm
                    FROM properties 
                    WHERE neighborhood LIKE '%Yasameen%' AND bedrooms = 3 AND property_type_standard = 'apartment'
                """))
                yasameen_data = result.fetchone()
                
                # Price range analysis
                result = conn.execute(text("""
                    SELECT MIN(price) as min_price, MAX(price) as max_price, 
                           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price
                    FROM properties 
                    WHERE neighborhood LIKE '%Yasameen%' AND bedrooms = 3 AND property_type_standard = 'apartment'
                """))
                price_range = result.fetchone()
                
                report['summary'] = {
                    'total_properties': total_properties,
                    'yasameen_3br_count': yasameen_data.count if yasameen_data.count else 0,
                    'yasameen_3br_avg_price': float(yasameen_data.avg_price) if yasameen_data.avg_price else 0,
                    'yasameen_3br_avg_price_per_sqm': float(yasameen_data.avg_price_per_sqm) if yasameen_data.avg_price_per_sqm else 0,
                    'price_range': {
                        'min': float(price_range.min_price) if price_range.min_price else 0,
                        'max': float(price_range.max_price) if price_range.max_price else 0,
                        'median': float(price_range.median_price) if price_range.median_price else 0
                    }
                }
            
            # 2. Generate recommendations
            if yasameen_data.count and yasameen_data.avg_price:
                avg_price = float(yasameen_data.avg_price)
                recommendations = []
                
                # Find best value properties
                result = conn.execute(text("""
                    SELECT price, price_per_sqm, neighborhood
                    FROM yasameen_3br_report 
                    WHERE value_assessment = 'Good Value'
                    ORDER BY price_per_sqm ASC
                    LIMIT 5
                """))
                best_value_properties = result.fetchall()
                
                for prop in best_value_properties:
                    recommendations.append({
                        'type': 'Best Value Property',
                        'price': float(prop.price),
                        'price_per_sqm': float(prop.price_per_sqm),
                        'location': prop.neighborhood,
                        'reason': 'Below average price per square meter'
                    })
                
                # Market timing recommendations
                if avg_price > 0:
                    recommendations.append({
                        'type': 'Market Insight',
                        'insight': f'Average 3BR apartment in Al Yasameen costs {avg_price:,.0f} SAR',
                        'recommendation': 'Consider properties below this average for better value'
                    })
                
                report['recommendations'] = recommendations
            
            # 3. Market insights
            report['market_insights'] = {
                'analysis_views_created': [
                    'avg_price_by_location',
                    'avg_price_by_bedroom', 
                    'rent_increase_trends',
                    'price_correlation_matrix',
                    'yasameen_3br_report'
                ],
                'next_steps': [
                    'Query yasameen_3br_report for Abdullah\'s specific needs',
                    'Monitor rent_increase_trends for market timing',
                    'Use price_correlation_matrix for investment decisions',
                    'Compare with avg_price_by_location for neighborhood analysis'
                ]
            }
            
            # Save report to analysis_results table
            self.load_analysis_results('comprehensive_report', report)
            
            logger.info("Comprehensive analysis report generated successfully")
            return report
            
        except Exception as e:
            logger.error(f"Error generating analysis report: {e}")
            return None
    
    def run_complete_analysis(self):
        """Run all analysis components"""
        try:
            logger.info("Starting comprehensive analysis...")
            
            # Create analysis tables
            if not self.create_analysis_tables():
                logger.error("Failed to create analysis tables")
                return False
            
            # Run existing analyses (now using dedicated tables)
            self.calculate_rent_increase_analysis()
            self.calculate_correlation_analysis()
            self.calculate_sales_valuation()
            self.calculate_meter_price_distribution() # Added meter price distribution
            self.calculate_general_insights() # Added general insights
            
            # Populate average price tables
            self.populate_avg_price_tables()
            self.populate_yasameen_3br_report()
            
            # Generate comprehensive report
            report = self.generate_analysis_report()
            
            if report:
                logger.info("Complete analysis finished successfully")
                logger.info("Created dedicated analysis tables:")
                logger.info("- rent_increase_analysis")
                logger.info("- correlation_analysis") 
                logger.info("- sales_valuation_analysis")
                logger.info("- avg_price_by_location")
                logger.info("- avg_price_by_bedroom")
                logger.info("- yasameen_3br_report")
                logger.info(f"Created {len(report['recommendations'])} recommendations")
                return True
            else:
                logger.error("Failed to generate analysis report")
                return False
                
        except Exception as e:
            logger.error(f"Error in complete analysis: {e}")
            return False
    
    def close_connection(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")

def main():
    loader = RealEstateLoader()
    if loader.connect_to_database():
        loader.create_tables()
        loader.run_complete_analysis()
        loader.close_connection()

if __name__ == "__main__":
    main() 