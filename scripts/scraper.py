"""
Real Estate Data Scraper
Scrapes property data from DealApp (https://dealapp.sa)
"""

import time
import json
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import logging
import os
import platform
import re
import pytz

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealEstateScraper:
    def __init__(self):
        self.driver = None
        # Set Saudi Arabia timezone (GMT+3)
        self.saudi_tz = pytz.timezone('Asia/Riyadh')
        self.setup_driver()
    
    def setup_driver(self):
        """Setup Chrome driver with appropriate options for Docker environment"""
        try:
            chrome_options = Options()
            # Docker-specific options
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--headless")  # Run in headless mode
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")  # Don't load images for faster scraping
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Set environment variables for webdriver-manager
            os.environ['WDM_LOG_LEVEL'] = '0'  # Reduce logging
            os.environ['WDM_LOCAL'] = '1'  # Use local cache
            
            # Try to install and use ChromeDriver
            try:
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                logger.info("Chrome driver initialized successfully")
            except Exception as chrome_error:
                logger.warning(f"ChromeDriver setup failed: {chrome_error}")
                # Fallback: try to use system Chrome if available
                try:
                    self.driver = webdriver.Chrome(options=chrome_options)
                    self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                    logger.info("Chrome driver initialized with system Chrome")
                except Exception as system_error:
                    logger.error(f"System Chrome also failed: {system_error}")
                    raise Exception("No Chrome installation found. Please install Chrome in the Docker container.")
            
        except Exception as e:
            logger.error(f"Error setting up Chrome driver: {e}")
            raise
    
    def scroll_to_load_all(self, max_scroll_attempts=1000, scroll_pause=3, max_properties=None):
        """Scroll until all properties are loaded or max_properties reached"""
        if not self.driver:
            logger.error("Driver is not initialized")
            return []
        
        logger.info(f"Starting scroll to load properties (max: {max_properties if max_properties else 'ALL'})...")
        
        # Wait for initial content to load
        time.sleep(5)
        
        # Initialize tracking variables
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        attempts = 0
        no_new_content_count = 0
        last_property_count = 0
        
        while attempts < max_scroll_attempts:
            # Check current property count
            property_cards = self.driver.find_elements(By.CSS_SELECTOR, "app-main-ad-card")
            current_count = len(property_cards)
            logger.info(f"Attempt {attempts + 1}: Found {current_count} properties")
            
            # Stop if we have enough properties
            if max_properties and current_count >= max_properties:
                logger.info(f"Reached target of {max_properties} properties, stopping scroll")
                break
            
            # Check if we're getting new properties
            if current_count == last_property_count:
                no_new_content_count += 1
                if no_new_content_count >= 3:
                    logger.info("No new properties found for 3 attempts, trying enhanced loading")
                    
                    # Enhanced loading approach
                    self._enhanced_loading_attempt()
                    
                    # Check again after enhanced loading
                    new_cards = self.driver.find_elements(By.CSS_SELECTOR, "app-main-ad-card")
                    if len(new_cards) == current_count:
                        logger.info("No new content after enhanced loading, stopping")
                        break
                    else:
                        no_new_content_count = 0  # Reset counter if we got new content
            else:
                no_new_content_count = 0  # Reset counter if we got new content
                last_property_count = current_count
            
            # Perform scroll with multiple techniques
            self._perform_scroll()
            time.sleep(scroll_pause)
            
            # Check if new content has loaded
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            
            # Check for infinite scroll completion
            infinite_scroll = self.driver.find_elements(By.CSS_SELECTOR, "ion-infinite-scroll")
            if infinite_scroll:
                state = self.driver.execute_script("return arguments[0].getAttribute('disabled')", infinite_scroll[0])
                if state == 'true':
                    logger.info("Infinite scroll completed or disabled")
                    break
            
            if new_height > last_height:
                last_height = new_height
                attempts = 0  # Reset attempts if new content is loaded
            else:
                attempts += 1
            
            # Add random delay to avoid rate limiting
            if attempts % 5 == 0:
                time.sleep(2)
        
        logger.info(f"Scrolling completed after {attempts + 1} attempts")
        property_cards = self.driver.find_elements(By.CSS_SELECTOR, "app-main-ad-card")
        return property_cards if not max_properties else property_cards[:max_properties]
    
    def _perform_scroll(self):
        """Perform comprehensive scroll with multiple techniques"""
        try:
            # Primary scroll technique
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            # Secondary scroll technique for better coverage
            self.driver.execute_script("window.scrollBy(0, 1000);")
            
            # Alternative scroll technique
            self.driver.execute_script("document.body.scrollTop = document.body.scrollHeight;")
            
            # Trigger infinite scroll events
            infinite_scrolls = self.driver.find_elements(By.CSS_SELECTOR, "ion-infinite-scroll")
            for scroll in infinite_scrolls:
                try:
                    self.driver.execute_script("arguments[0].dispatchEvent(new Event('ionInfinite'));", scroll)
                except Exception as e:
                    logger.debug(f"Error triggering infinite scroll: {e}")
                    
        except Exception as e:
            logger.error(f"Error during scroll: {e}")
    
    def _enhanced_loading_attempt(self):
        """Enhanced loading attempt when standard scrolling fails"""
        try:
            # Try different scroll heights
            scroll_scripts = [
                "window.scrollTo(0, document.body.scrollHeight);",
                "window.scrollTo(0, document.documentElement.scrollHeight);",
                "window.scrollBy(0, 2000);",
                "document.documentElement.scrollTop = document.documentElement.scrollHeight;"
            ]
            
            for script in scroll_scripts:
                try:
                    self.driver.execute_script(script)
                    time.sleep(1)
                    
                    # Trigger infinite scroll after each script
                    infinite_scrolls = self.driver.find_elements(By.CSS_SELECTOR, "ion-infinite-scroll")
                    for scroll in infinite_scrolls:
                        try:
                            self.driver.execute_script("arguments[0].dispatchEvent(new Event('ionInfinite'));", scroll)
                            time.sleep(1)
                        except Exception as e:
                            logger.debug(f"Error triggering infinite scroll in enhanced loading: {e}")
                            
                except Exception as e:
                    logger.debug(f"Error executing enhanced scroll script: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error during enhanced loading attempt: {e}")

    def apply_filters(self, city="الرياض", district: str | None = None):
        """Apply filters for city and district"""
        if not self.driver:
            logger.error("Driver is not initialized")
            return
        logger.info(f"Applying filters for city: {city}, district: {district}")
        try:
            # Wait for filter section to be visible
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "app-filter-ad-header"))
            )
            
            # Debug: Check what elements are available
            logger.info("Checking available filter elements...")
            filter_elements = self.driver.find_elements(By.CSS_SELECTOR, "*")
            logger.info(f"Found {len(filter_elements)} total elements on page")
            
            # Click city filter
            city_filter = self.driver.find_element(By.CSS_SELECTOR, "#requests-filter-city")
            if city_filter.text.strip() != city:
                city_filter.click()
                time.sleep(1)
                city_input = self.driver.find_element(By.CSS_SELECTOR, "ion-searchbar input")
                city_input.send_keys(city)
                city_input.send_keys(Keys.ENTER)
                time.sleep(2)
            # Apply district filter if available and not None
            if district:
                search_bar = self.driver.find_elements(By.CSS_SELECTOR, "ion-searchbar")
                if search_bar:
                    search_bar[0].find_element(By.CSS_SELECTOR, "input").send_keys(district)
                    search_bar[0].find_element(By.CSS_SELECTOR, "input").send_keys(Keys.ENTER)
                    time.sleep(2)
            logger.info("Filters applied successfully")
        except Exception as e:
            logger.error(f"Error applying filters: {e}")

    def scrape_dealapp(self, max_pages=20, district: str | None = None, max_properties=100):
        """
        Scrape real estate data from DealApp, focusing on Riyadh and optionally a specific district
        """
        if not self.driver:
            logger.error("Driver is not initialized")
            return []
        logger.info(f"Starting DealApp scraping with max_properties={'ALL' if max_properties is None else max_properties}...")
        properties = []
        seen_properties = set()
        
        # Try different URLs if the main one doesn't work
        urls_to_try = [
            "https://dealapp.sa/ar/عقارات/الرياض/جميع_العقارات/بيع_و_إيجار",
            "https://dealapp.sa/ar/عقارات/الرياض",
            "https://dealapp.sa/ar/عقارات",
            "https://dealapp.sa/ar/عقارات/الرياض/جميع_العقارات"
        ]
        
        for url_index, properties_url in enumerate(urls_to_try):
            try:
                logger.info(f"Trying URL {url_index + 1}/{len(urls_to_try)}: {properties_url}")
                self.driver.get(properties_url)
                
                # Wait for the listings page to load
                try:
                    WebDriverWait(self.driver, 30).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "app-ads-list"))
                    )
                    logger.info("Listings page loaded")
                except:
                    logger.warning("Could not find app-ads-list, trying alternative selectors")
                    # Try alternative selectors
                    alternative_selectors = ["ion-content", "main", ".content", "#content"]
                    page_loaded = False
                    for selector in alternative_selectors:
                        try:
                            WebDriverWait(self.driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                            )
                            logger.info(f"Page loaded with selector: {selector}")
                            page_loaded = True
                            break
                        except:
                            continue
                    
                    if not page_loaded:
                        logger.warning("Page did not load properly, trying next URL")
                        continue
                
                # Wait a bit more for dynamic content to load
                time.sleep(3)
                
                # Apply filters for Riyadh and optionally a district
                self.apply_filters(city="الرياض", district=district)
                
                # Wait for filters to be applied
                time.sleep(2)
                
                # Scroll to load all properties with improved parameters
                property_cards = self.scroll_to_load_all(max_scroll_attempts=1000, scroll_pause=3, max_properties=max_properties)
                logger.info(f"Found {len(property_cards)} property cards after scrolling")
                
                # If we found some properties, process them
                if property_cards:
                    # Process each property card
                    for i, card in enumerate(property_cards):
                        try:
                            if i % 50 == 0:  # Log progress every 50 cards
                                logger.info(f"Processing card {i+1}/{len(property_cards)}")
                            
                            property_data = self.extract_property_data_angular(card)
                            if property_data:
                                # Accept all properties if no district specified, or if district matches
                                if not district or district in property_data.get('location', ''):
                                    # Check if unique
                                    key = self.get_property_key(property_data)
                                    if key not in seen_properties:
                                        properties.append(property_data)
                                        seen_properties.add(key)
                                        if len(properties) % 100 == 0:  # Log every 100 properties added
                                            logger.info(f"Added {len(properties)} properties so far...")
                                    else:
                                        logger.debug(f"Skipped duplicate: {property_data.get('title', 'N/A')}")
                                else:
                                    logger.debug(f"Skipped (wrong district): {property_data.get('location', 'N/A')}")
                            else:
                                if i < 10:  # Only show details for first 10 cards
                                    logger.warning(f"Card {i+1}: No property data extracted - likely empty or invalid HTML")
                                else:
                                    logger.debug(f"Card {i+1}: No property data extracted")
                        except Exception as e:
                            logger.error(f"Error extracting property data from card {i+1}: {e}")
                            continue
                        
                        # Stop if we've reached max_properties
                        if max_properties and len(properties) >= max_properties:
                            logger.info(f"Reached max_properties limit of {max_properties}, stopping processing")
                            break
                    
                    # If we found properties, break out of URL loop
                    if properties:
                        logger.info(f"Successfully found properties with URL {url_index + 1}")
                        break
                    else:
                        logger.warning(f"No properties extracted from URL {url_index + 1}, trying next URL")
                else:
                    logger.warning(f"No property cards found with URL {url_index + 1}, trying next URL")
                    
            except Exception as e:
                logger.error(f"Error with URL {url_index + 1}: {e}")
                continue
        
        # If no properties found, log and return what we have
        if not properties:
            logger.warning("No properties found after trying all URLs")
        logger.info(f"Successfully scraped {len(properties)} unique properties from DealApp")
        return properties
    
    def is_unique_property(self, property_data, seen_properties):
        """Check if property is unique based on key attributes"""
        if not property_data:
            return False
        
        key = self.get_property_key(property_data)
        return key not in seen_properties
    
    def get_property_key(self, property_data):
        """Create a unique key for property deduplication"""
        title = property_data.get('title', '').strip()
        price = property_data.get('price', 0)
        location = property_data.get('location', '').strip()
        area = property_data.get('area', 0)
        bedrooms = property_data.get('bedrooms', 0)
        
        # Include area and bedrooms for better uniqueness
        return f"{title}_{price}_{location}_{area}_{bedrooms}"
    
    def extract_property_data_angular(self, card):
        """Extract property information from an Angular card element"""
        try:
            property_id = None  # Always define at the top!
            # Try both .text and innerHTML
            card_text = card.text.strip()
            card_html = card.get_attribute("innerHTML")

            if (not card_text or len(card_text) < 10) and (not card_html or len(card_html) < 10):
                logger.warning("Skipped card: too short or empty (both text and HTML)")
                return None
            
            # If .text is empty, try to extract fields from innerHTML using regex or BeautifulSoup
            if not card_text or len(card_text) < 10:
                from bs4 import BeautifulSoup
                import re
                soup = BeautifulSoup(card_html, "html.parser")
                # Try to extract title
                title_elem = soup.select_one(".ad-card-main-title")
                title = title_elem.get_text(strip=True) if title_elem else ""
                # Try to extract price
                price_elem = soup.select_one(".ad-card-main-price")
                price_text = price_elem.get_text(strip=True) if price_elem else ""
                price = float(''.join(filter(str.isdigit, price_text))) if price_text else 0
                # Try to extract location
                location_elem = soup.select_one(".ad-card-location-container")
                location = location_elem.get_text(strip=True) if location_elem else ""
                # Try to extract features
                bedrooms = area = bathrooms = 0
                for feat in soup.select(".ad-card-feature-text"):
                    text = feat.get_text(strip=True)
                    if "غرف" in text or "غرفة" in text or "غرفتين" in text:
                        bedrooms = self.extract_number(text)
                    elif "م²" in text or "²" in text:
                        area_text = text.replace("²", "").replace("م²", "").strip()
                        area = float(''.join(filter(str.isdigit, area_text))) if area_text else 0
                    elif "حمام" in text or "حمامين" in text:
                        bathrooms = self.extract_number(text)
                property_type = self.determine_property_type(title)
                logger.debug(f"[HTML] Extracted title: {title}, price: {price}, location: {location}, bedrooms: {bedrooms}, area: {area}, bathrooms: {bathrooms}, property_type: {property_type}")
                if not title and price == 0 and area == 0:
                    logger.warning("Skipped card: no meaningful data extracted from HTML")
                    return None
                # --- NEW: Extract property_id from the image URL ---
                property_id = None
                soup = BeautifulSoup(card_html, "html.parser")
                img_tag = soup.find('img', class_='ad-card-main-image')
                if img_tag and 'src' in img_tag.attrs:
                    match = re.search(r'/([a-f0-9\-]{36})\.webp', img_tag['src'])
                    if match:
                        property_id = match.group(1)
                return {
                    'property_id': property_id,
                    'title': title,
                    'price': price,
                    'location': location,
                    'bedrooms': bedrooms,
                    'area': area,
                    'bathrooms': bathrooms,
                    'property_type': property_type,
                    'source': 'dealapp'
                }
            # Extract title using the correct selector
            title = ""
            try:
                title_elem = card.find_element(By.CSS_SELECTOR, ".ad-card-main-title")
                title = title_elem.text.strip() if title_elem else ""
            except Exception as e:
                logger.debug(f"No title found: {e}")
                # Fallback: look for property type in card text
                lines = card_text.split('\n')
                for line in lines:
                    line = line.strip()
                    if line and any(keyword in line for keyword in ['فيلا', 'شقة', 'استوديو', 'دوبلكس', 'عمارة']):
                        title = line
                        break
            logger.debug(f"Extracted title: {title}")
            
            # Extract price using the correct selector
            price = 0
            try:
                price_elem = card.find_element(By.CSS_SELECTOR, ".ad-card-main-price")
                price_text = price_elem.text.strip() if price_elem else ""
                if price_text:
                    price = float(''.join(filter(str.isdigit, price_text)))
            except Exception as e:
                logger.debug(f"No price found: {e}")
                # Fallback: extract from card text
                import re
                price_match = re.search(r'(\d+(?:,\d+)*)', card_text)
                if price_match:
                    price = float(price_match.group(1).replace(',', ''))
            logger.debug(f"Extracted price: {price}")
            
            # Extract location using the correct selector
            location = ""
            try:
                location_elem = card.find_element(By.CSS_SELECTOR, ".ad-card-location-container")
                location = location_elem.text.strip() if location_elem else ""
            except Exception as e:
                logger.debug(f"No location found: {e}")
                # Fallback: extract from card text
                location_keywords = ['الرياض', 'الياسمين', 'الملقا', 'النسيم', 'الشفا', 'الدرعية', 'المهدية']
                for keyword in location_keywords:
                    if keyword in card_text:
                        location = keyword
                        break
            logger.debug(f"Extracted location: {location}")
            
            # Extract bedrooms and other features
            bedrooms = 0
            area = 0
            bathrooms = 0
            try:
                feature_elems = card.find_elements(By.CSS_SELECTOR, ".ad-card-feature-badge")
                for elem in feature_elems:
                    text = elem.text.strip()
                    if "غرف" in text or "غرفة" in text:
                        bedrooms = self.extract_number(text)
                    elif "م²" in text or "²" in text:
                        area_text = text.replace("²", "").replace("م²", "").strip()
                        area = float(''.join(filter(str.isdigit, area_text))) if area_text else 0
                    elif "حمام" in text or "حمامين" in text:
                        bathrooms = self.extract_number(text)
            except Exception as e:
                logger.debug(f"No features found: {e}")
                # Fallback: extract from card text
                import re
                bedroom_match = re.search(r'(\d+)\s*غرف', card_text)
                if bedroom_match:
                    bedrooms = int(bedroom_match.group(1))
                area_match = re.search(r'(\d+(?:\.\d+)?)\s*م²', card_text)
                if area_match:
                    area = float(area_match.group(1))
                bathroom_match = re.search(r'(\d+)\s*حمام', card_text)
                if bathroom_match:
                    bathrooms = int(bathroom_match.group(1))
            logger.debug(f"Extracted bedrooms: {bedrooms}, area: {area}, bathrooms: {bathrooms}")
            
            # Determine property type
            property_type = self.determine_property_type(title)
            logger.debug(f"Extracted property_type: {property_type}")
            
            # Only return if we have meaningful data
            if not title and price == 0 and area == 0:
                logger.warning("Skipped card: no meaningful data extracted")
                return None
            
            return {
                'property_id': property_id,
                'title': title,
                'price': price,
                'location': location,
                'bedrooms': bedrooms,
                'area': area,
                'bathrooms': bathrooms,
                'property_type': property_type,
                'source': 'dealapp'
            }
        except Exception as e:
            logger.error(f"Error extracting property data: {e}")
            return None
    
    def extract_number(self, text):
        """Extract number from text"""
        import re
        # Handle specific Arabic number words
        if 'غرفتين' in text or 'حمامين' in text:
            return 2
         
        # Use regex to extract digits
        match = re.search(r'(\d+)', text)
        return int(match.group(1)) if match else 0
    
    def determine_property_type(self, text):
        """Determine property type from text"""
        text = text.lower()
        type_mappings = {
            'فيلا': 'villa',
            'شقة': 'apartment',
            'استوديو': 'studio',
            'دوبلكس': 'duplex',
            'عمارة': 'building',
            'غرفة': 'room',
            'أرض': 'land',
            'محل': 'commercial',
            'مكتب': 'office',
            'دور': 'floor',
            'برج': 'tower',
            'استراحة': 'chalet',
            'مزرعة': 'farm'
        }
        for key, value in type_mappings.items():
            if key in text:
                return value
        return 'apartment'  # Default
    
    def save_to_csv(self, properties, filename):
        """Save scraped data to CSV"""
        df = pd.DataFrame(properties)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logger.info(f"Saved {len(properties)} properties to {filename}")
    
    def close(self):
        """Close the browser"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Browser closed")
            except Exception as e:
                logger.warning(f"Error closing browser: {e}")

def main():
    """Main scraping function"""
    scraper = RealEstateScraper()
    
    try:
        # Scrape DealApp for all Riyadh properties
        properties = scraper.scrape_dealapp(max_pages=20, district=None)
        
        # Save to raw data directory at project root
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Get absolute path to project root
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        raw_dir = os.path.join(project_root, "data", "raw")
        os.makedirs(raw_dir, exist_ok=True)
        filename = os.path.join(raw_dir, f"dealapp_properties_{timestamp}.csv")
        scraper.save_to_csv(properties, filename)
        
        logger.info(f"Scraping completed. Found {len(properties)} properties")
        
    except Exception as e:
        logger.error(f"Error in main scraping: {e}")
    
    finally:
        scraper.close()

if __name__ == "__main__":
    main() 