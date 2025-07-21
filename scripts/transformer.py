"""
Real Estate Data Transformer
Cleans and transforms scraped real estate data
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os
import glob
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealEstateTransformer:
    def __init__(self):
        self.processed_data = None
        # Set Saudi Arabia timezone (GMT+3)
        self.saudi_tz = pytz.timezone('Asia/Riyadh')

    def get_saudi_time(self):
        """Get current time in Saudi Arabia timezone (GMT+3)"""
        return datetime.now(self.saudi_tz).strftime('%Y-%m-%d %H:%M:%S')

    def load_raw_data(self, data_dir="data/raw", extra_files=None):
        """Load only the historical file realstate_data_2014_2016.csv from data/raw/."""
        logger.info("Loading only the historical data file...")
        all_data = []
        import os
        historical_file = [os.path.join("data", "raw", "realstate_data_2014_2016.csv")]
        csv_files = historical_file
        for file in csv_files:
            try:
                df = pd.read_csv(file, encoding='utf-8')
                logger.info(f"Loaded {len(df)} records from {file} (utf-8)")
                df['source_file'] = os.path.basename(file)
                all_data.append(df)
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(file, encoding='cp1256')
                    logger.info(f"Loaded {len(df)} records from {file} (cp1256)")
                    df['source_file'] = os.path.basename(file)
                    all_data.append(df)
                except Exception as e2:
                    logger.error(f"Error loading {file} with cp1256: {e2}")
            except Exception as e:
                logger.error(f"Error loading {file}: {e}")
        if all_data:
            self.raw_data = pd.concat(all_data, ignore_index=True)
            logger.info(f"Total raw records loaded: {len(self.raw_data)}")
        else:
            logger.warning("No raw data files found")
            self.raw_data = pd.DataFrame()

    def classify_neighborhood(self, location_series):
        """Classify properties by neighborhood (supports both Arabic and English names)"""
        neighborhoods = []
        for location in location_series:
            location_lower = str(location).lower()
            if any(keyword in location_lower for keyword in ['الملقا', 'malga', 'al malga']):
                neighborhoods.append('Al Malga')
            elif any(keyword in location_lower for keyword in ['الياسمين', 'yasmeen', 'al yasmeen']):
                neighborhoods.append('Al Yasmeen')
            else:
                neighborhoods.append('Other')
        return neighborhoods

    def run_merge_and_store_warehouse(self):
        """Merge DealApp and historical data, filter for Al Yasmeen and Al Malga, check data quality, and store in warehouse CSV."""
        self.load_raw_data()
        df = self.raw_data.copy()
        
        # If no historical data is available, return empty DataFrame
        if df.empty:
            logger.warning("No historical data available, proceeding with scraped data only")
            return pd.DataFrame()
        
        # إذا لم يوجد عمود location، أنشئه من district_name_ar أو district_name_en
        if 'location' not in df.columns:
            if 'district_name_ar' in df.columns:
                df['location'] = df['district_name_ar']
            elif 'district_name_en' in df.columns:
                df['location'] = df['district_name_en']
            else:
                df['location'] = ''
        
        # Ensure location column is string type before using str accessor
        df['location'] = df['location'].astype(str)
        df['neighborhood'] = self.classify_neighborhood(df['location'])
        
        # فلترة الأحياء المطلوبة بأي تهجئة أو لغة
        # Ensure neighborhood column is string type before using str accessor
        df['neighborhood'] = df['neighborhood'].astype(str)
        df = df[df['neighborhood'].str.contains('yasmeen|الْيَاسْمِين|الياسمين|malga|الملقا', case=False, na=False)]
        
        quality_report = {
            'total_records': len(df),
            'missing_per_column': df.isnull().sum().to_dict(),
            'dtypes': {col: str(df[col].dtype) for col in df.columns},
            'sample': df.head(5).to_dict()
        }
        import json
        os.makedirs('data/warehouse', exist_ok=True)
        with open('data/warehouse/quality_report.json', 'w', encoding='utf-8') as f:
            json.dump(quality_report, f, ensure_ascii=False, indent=2)
        warehouse_path = os.path.join('data', 'warehouse', 'riyadh_yasmeen_malga.csv')
        # Map DataFrame columns to match the properties table schema
        column_mapping = {
            "Advertisement Number": "property_id",  # Use advertisement number as property_id
            "Price": "price",
            "Location": "location",
            "Number of Beadrooms": "bedrooms",
            "Area Dimension": "area",  # Correct column name from CSV
            "Property Type": "property_type",
            "Number of Bathrooms": "bathrooms",  # Map bathrooms column
            "Fore sale or Rent": "listing_type",  # Map sale/rent column
            # Add more mappings as needed
        }
        df = df.rename(columns=column_mapping)

        # Convert property_id to string to match database VARCHAR type
        if 'property_id' in df.columns:
            df['property_id'] = df['property_id'].astype(str)

        # Enrich DataFrame with required columns and defaults
        df['title'] = df.get('title', None)
        df['property_type_standard'] = df.get('property_type_standard', None)
        df['price_per_sqm'] = df.get('price_per_sqm', None)
        df['price_per_bedroom'] = df.get('price_per_bedroom', None)
        df['source'] = 'historical'
        df['scraped_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Set current time for historical data
        df['processed_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Standardize property_type for historical data
        type_map = {
            'شقة': 'apartment',
            'فيلا': 'villa',
            'دور': 'floor',
            'محل': 'shop',
            'عمارة': 'building',
            'أرض': 'land',
            'استراحة': 'farmhouse',
            'بيت': 'house',
            'مكتب تجاري': 'commercial_office',
            'مستودع': 'warehouse',
            'apartment': 'apartment',
            'villa': 'villa',
            'floor': 'floor',
            'shop': 'shop',
            'building': 'building',
            'land': 'land',
            'farmhouse': 'farmhouse',
            'house': 'house',
            'commercial_office': 'commercial_office',
            'warehouse': 'warehouse',
            # Add more mappings as needed
        }
        df['property_type_standard'] = df['property_type'].map(type_map).fillna(df['property_type'])
        
        # Convert area to numeric and calculate derived fields
        if 'area' in df.columns:
            # Convert area to numeric, handling any non-numeric values
            df['area'] = pd.to_numeric(df['area'], errors='coerce')
            # Convert area = 0 to None (NULL) to handle missing data
            df['area'] = df['area'].replace(0, None)
            # Cap area at 1,000,000 (reasonable max for Riyadh properties)
            df['area'] = df['area'].clip(upper=1000000)
        
        # Convert price to numeric
        if 'price' in df.columns:
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            # Cap price at 100,000,000 (reasonable max for Riyadh properties)
            df['price'] = df['price'].clip(upper=100000000)
        
        # Convert bedrooms to numeric
        if 'bedrooms' in df.columns:
            df['bedrooms'] = pd.to_numeric(df['bedrooms'], errors='coerce')
            # Convert bedrooms = 0 to None (NULL) to handle missing data
            df['bedrooms'] = df['bedrooms'].replace(0, None)
            # Cap bedrooms at 20 (reasonable max)
            df['bedrooms'] = df['bedrooms'].clip(upper=20)
        
        # Convert bathrooms to numeric
        if 'bathrooms' in df.columns:
            df['bathrooms'] = pd.to_numeric(df['bathrooms'], errors='coerce')
            # Convert bathrooms = 0 to None (NULL) to handle missing data
            df['bathrooms'] = df['bathrooms'].replace(0, None)
            # Cap bathrooms at 10 (reasonable max)
            df['bathrooms'] = df['bathrooms'].clip(upper=10)
        
        # Calculate price_per_sqm and price_per_bedroom
        df['price_per_sqm'] = df.apply(
            lambda row: row['price'] / row['area'] if pd.notnull(row.get('price')) and pd.notnull(row.get('area')) and row.get('area', 0) > 0 else None, 
            axis=1
        )
        df['price_per_bedroom'] = df.apply(
            lambda row: row['price'] / row['bedrooms'] if pd.notnull(row.get('price')) and pd.notnull(row.get('bedrooms')) and row.get('bedrooms', 0) > 0 else None, 
            axis=1
        )
        
        # Cap extreme values to prevent database overflow
        # Cap price_per_sqm at 100,000 (reasonable max for Riyadh real estate)
        df['price_per_sqm'] = df['price_per_sqm'].clip(upper=100000)
        
        # Cap price_per_bedroom at 1,000,000 (reasonable max for Riyadh real estate)
        df['price_per_bedroom'] = df['price_per_bedroom'].clip(upper=1000000)
        
        # Log statistics about the calculated values
        logger.info(f"Price per sqm - Min: {df['price_per_sqm'].min()}, Max: {df['price_per_sqm'].max()}, Mean: {df['price_per_sqm'].mean()}")
        logger.info(f"Price per bedroom - Min: {df['price_per_bedroom'].min()}, Max: {df['price_per_bedroom'].max()}, Mean: {df['price_per_bedroom'].mean()}")
        
        # Add missing columns for historical data
        df['city'] = 'Riyadh'  # All historical data is from Riyadh
        
        # Clean up listing_type values
        if 'listing_type' in df.columns:
            df['listing_type'] = df['listing_type'].map({
                'للبيع': 'sale',
                'للإيجار': 'rent',
                'for sale': 'sale',
                'for rent': 'rent'
            }).fillna('unknown')
        
        # Append city to location
        df['location'] = df['location'].astype(str) + ', Riyadh'
        
        # Set created_at for all historical records using Saudi Arabia time
        from datetime import datetime
        import pytz
        saudi_tz = pytz.timezone('Asia/Riyadh')
        df['created_at'] = datetime.now(saudi_tz).strftime('%Y-%m-%d %H:%M:%S')

        # Ensure all expected columns exist and are ordered
        expected_columns = [
            "property_id", "title", "price", "bedrooms", "area", "bathrooms", "property_type",
            "property_type_standard", "price_per_sqm", "price_per_bedroom",
            "city", "neighborhood", "listing_type", "source", "scraped_date", "processed_date", "created_at"
        ]
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
        df = df[expected_columns]
        df.to_csv(warehouse_path, index=False, encoding='utf-8-sig')
        logger.info(f"Warehouse data saved to {warehouse_path}")
        logger.info(f"Data quality report saved to data/warehouse/quality_report.json")
        return df  # Return the processed DataFrame

    def transform_scraped_data(self, df):
        """Transform a DataFrame from the scraper to match the properties table schema."""
        try:
            # Ensure property_id exists and is unique
            if 'property_id' not in df.columns:
                logger.warning("No property_id found in scraped data, generating unique IDs")
                df['property_id'] = [f"scraped_{i}_{datetime.now().strftime('%Y%m%d_%H%M%S')}" for i in range(len(df))]
            
            # Convert property_id to string
            df['property_id'] = df['property_id'].astype(str)
            
            # Drop rows with missing or empty property_id
            before_drop = len(df)
            df = df[df['property_id'].notnull() & (df['property_id'].str.strip() != '')]
            dropped = before_drop - len(df)
            if dropped > 0:
                logger.warning(f"Dropped {dropped} rows with missing or empty property_id")
            
            # Remove any rows with duplicate property_id
            initial_count = len(df)
            df = df.drop_duplicates(subset=['property_id'], keep='first')
            if len(df) < initial_count:
                logger.info(f"Removed {initial_count - len(df)} duplicate property_id records")
            
            # If 'location' is missing, try to create it from other columns
            if 'location' not in df.columns:
                if 'district_name_ar' in df.columns:
                    df['location'] = df['district_name_ar']
                elif 'district_name_en' in df.columns:
                    df['location'] = df['district_name_en']
                else:
                    df['location'] = ''
            
            # Ensure location column is string type before using str accessor
            df['location'] = df['location'].astype(str)
            df['neighborhood'] = self.classify_neighborhood(df['location'])
            
            # Filter for Al Yasmeen and Al Malga
            df['neighborhood'] = df['neighborhood'].astype(str)
            df = df[df['neighborhood'].str.contains('yasmeen|الْيَاسْمِين|الياسمين|malga|الملقا', case=False, na=False)]
            
            # Map DataFrame columns to match the properties table schema
            column_mapping = {
                "Advertisement Number": "id",
                "Price": "price",
                "Location": "location",
                "Number of Beadrooms": "bedrooms",
                "Area_Dimension": "area",
                "Property Type": "property_type",
                # Add more mappings as needed
            }
            df = df.rename(columns=column_mapping)
            
            # Enrich DataFrame with required columns and defaults
            if 'title' not in df.columns:
                df['title'] = None
            if 'scraped_date' not in df.columns:
                df['scraped_date'] = None
            
            # Standardize property_type
            type_map = {
                'شقة': 'apartment',
                'فيلا': 'villa',
                'دور': 'floor',
                # Add more mappings as needed
            }
            df['property_type_standard'] = df['property_type'].map(type_map).fillna(df['property_type'])
            
            # Convert and validate numeric fields
            # Convert area to numeric and handle 0 values
            if 'area' in df.columns:
                df['area'] = pd.to_numeric(df['area'], errors='coerce')
                # Convert area = 0 to None (NULL) to handle missing data
                df['area'] = df['area'].replace(0, None)
                # Cap area at 1,000,000 (reasonable max for Riyadh properties)
                df['area'] = df['area'].clip(upper=1000000)
            
            # Convert price to numeric
            if 'price' in df.columns:
                df['price'] = pd.to_numeric(df['price'], errors='coerce')
                # Cap price at 100,000,000 (reasonable max for Riyadh properties)
                df['price'] = df['price'].clip(upper=100000000)
            
            # Convert bedrooms to numeric
            if 'bedrooms' in df.columns:
                df['bedrooms'] = pd.to_numeric(df['bedrooms'], errors='coerce')
                # Convert bedrooms = 0 to None (NULL) to handle missing data
                df['bedrooms'] = df['bedrooms'].replace(0, None)
                # Cap bedrooms at 20 (reasonable max)
                df['bedrooms'] = df['bedrooms'].clip(upper=20)
            
            # Convert bathrooms to numeric
            if 'bathrooms' in df.columns:
                df['bathrooms'] = pd.to_numeric(df['bathrooms'], errors='coerce')
                # Convert bathrooms = 0 to None (NULL) to handle missing data
                df['bathrooms'] = df['bathrooms'].replace(0, None)
                # Cap bathrooms at 10 (reasonable max)
                df['bathrooms'] = df['bathrooms'].clip(upper=10)
            
            # Calculate price_per_sqm and price_per_bedroom
            df['price_per_sqm'] = df.apply(
                lambda row: row['price'] / row['area'] if pd.notnull(row.get('price')) and pd.notnull(row.get('area')) and row.get('area', 0) > 0 else None, 
                axis=1
            )
            df['price_per_bedroom'] = df.apply(
                lambda row: row['price'] / row['bedrooms'] if pd.notnull(row.get('price')) and pd.notnull(row.get('bedrooms')) and row.get('bedrooms', 0) > 0 else None, 
                axis=1
            )
            
            # Cap extreme values to prevent database overflow
            df['price_per_sqm'] = df['price_per_sqm'].clip(upper=100000)
            df['price_per_bedroom'] = df['price_per_bedroom'].clip(upper=1000000)
            
            df['source'] = 'scraped'
            df['scraped_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['processed_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Append city to location
            df['location'] = df['location'].astype(str) + ', Riyadh'
            
            # Ensure all expected columns exist and are ordered (without 'title' and 'scraped_date')
            expected_columns = [
                "property_id", "price", "bedrooms", "area", "bathrooms", "property_type",
                "property_type_standard", "price_per_sqm", "price_per_bedroom",
                "city", "neighborhood", "listing_type", "source", "processed_date"
            ]
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None
            
            # Ensure property_id is string type
            df['property_id'] = df['property_id'].astype(str)
            
            # Set city (default to Riyadh for now)
            df['city'] = 'Riyadh'
            
            # Set listing_type based on title
            def get_listing_type(title):
                if pd.isnull(title):
                    return None
                if 'للبيع' in str(title):
                    return 'sale'
                if 'للإيجار' in str(title):
                    return 'rent'
                return None
            df['listing_type'] = df['title'].apply(get_listing_type)
            
            # Remove the title and scraped_date columns if they exist
            for col in ['title', 'scraped_date']:
                if col in df.columns:
                    df = df.drop(columns=[col])
            
            # Ensure all expected columns exist and are ordered (without 'title' and 'scraped_date')
            expected_columns = [
                "property_id", "price", "bedrooms", "area", "bathrooms", "property_type",
                "property_type_standard", "price_per_sqm", "price_per_bedroom",
                "city", "neighborhood", "listing_type", "source", "processed_date"
            ]
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None
            df = df[expected_columns]
            return df
        except Exception as e:
            logger.error(f"Error transforming scraped data: {e}")
            return None

    def merge_historical_and_scraped(self, df_scraped):
        """Merge historical and scraped data, transform both, and return a single DataFrame."""
        # Historical
        df_hist = self.run_merge_and_store_warehouse()
        
        # Scraped (already transformed)
        df_scraped = self.transform_scraped_data(df_scraped)
        
        # If historical data is empty, return only scraped data
        if df_hist.empty:
            logger.info("No historical data available, returning scraped data only")
            return df_scraped
        
        # Set source for historical data
        df_hist['source'] = 'historical'
        
        # Concatenate
        df_merged = pd.concat([df_hist, df_scraped], ignore_index=True)
        logger.info(f"Merged {len(df_hist)} historical records with {len(df_scraped)} scraped records")
        # Ensure all expected columns exist and are ordered
        expected_columns = [
            "property_id", "price", "bedrooms", "area", "bathrooms", "property_type",
            "property_type_standard", "price_per_sqm", "price_per_bedroom",
            "city", "neighborhood", "listing_type", "source", "processed_date"
        ]
        for col in expected_columns:
            if col not in df_merged.columns:
                df_merged[col] = None
        df_merged['city'] = 'Riyadh'
        def get_listing_type(title):
            if pd.isnull(title):
                return None
            if 'للبيع' in title:
                return 'sale'
            if 'للإيجار' in title:
                return 'rent'
            return None
        df_merged['listing_type'] = df_merged['title'].apply(get_listing_type)
        df_merged = df_merged[expected_columns]
        return df_merged

if __name__ == "__main__":
    transformer = RealEstateTransformer()
    df = transformer.run_merge_and_store_warehouse()  # Now returns the DataFrame 