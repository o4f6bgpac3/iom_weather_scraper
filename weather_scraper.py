import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
import requests
import logging
import hashlib

# Set up file logging as a fallback
logging.basicConfig(filename='weather_scraper.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define custom adapters for datetime and date
def adapt_datetime(dt):
    return dt.isoformat()

def adapt_date(d):
    return d.isoformat()

def convert_datetime(s):
    return datetime.fromisoformat(s.decode())

def convert_date(s):
    return date.fromisoformat(s.decode())

# Register the adapters and converters
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_adapter(date, adapt_date)
sqlite3.register_converter("datetime", convert_datetime)
sqlite3.register_converter("date", convert_date)

class DatabaseLogger:
    def __init__(self, conn, cursor):
        self.conn = conn
        self.cursor = cursor
        self.html_content_id = None

    def set_html_content_id(self, html_content_id):
        self.html_content_id = html_content_id

    def log(self, level, message):
        try:
            self.cursor.execute('''
            INSERT INTO scraper_logs (timestamp, level, message, html_content_id)
            VALUES (?, ?, ?, ?)
            ''', (datetime.now(), level, message, self.html_content_id))
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Failed to log to database: {e}")
            logging.error(f"{level}: {message}")

def fetch_html_content(url, logger):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logger.log("ERROR", f"An error occurred while fetching the URL: {e}")
        return None

def calculate_date(issued_on, date_str, logger):
    try:
        if date_str == 'Today':
            return issued_on.date()
        elif date_str == 'Tomorrow':
            return issued_on.date() + timedelta(days=1)
        else:
            day_date = datetime.strptime(date_str, '%A, %d %B').replace(year=issued_on.year)
            if day_date.date() < issued_on.date():
                day_date = day_date.replace(year=issued_on.year + 1)
            return day_date.date()
    except ValueError as e:
        logger.log("ERROR", f"Error calculating date for '{date_str}': {e}")
        return None

def safe_find(element, selector, attribute=None, logger=None):
    try:
        found = element.select_one(selector)
        if found:
            return found.get(attribute) if attribute else found.text.strip()
    except AttributeError as e:
        if logger:
            logger.log("ERROR", f"Error finding element with selector '{selector}': {e}")
    return None

def scrape_weather_data(html_content, logger):
    soup = BeautifulSoup(html_content, 'html.parser')
    forecast_data = []

    try:
        issued_on_str = safe_find(soup, 'div.weather-issued', logger=logger)
        if issued_on_str:
            issued_on_str = issued_on_str.replace('Issued on ', '').split(' by ')[0]
            try:
                issued_on = datetime.strptime(issued_on_str, '%A, %d %B %Y at %I:%M%p')
            except ValueError:
                issued_on = datetime.strptime(issued_on_str, '%A, %d %B %Y at %H:%M')
        else:
            logger.log("ERROR", "Could not find issued_on date")
            return None, []
    except Exception as e:
        logger.log("ERROR", f"Error parsing issued_on date: {e}")
        return None, []

    for day in soup.find_all('h2'):
        try:
            date_str = day.text.strip()
            if date_str == 'Forecast by':
                break

            date = calculate_date(issued_on, date_str, logger)
            weather_detail = day.find_next('div', class_='weather-detailed')

            forecast = {
                'date': date,
                'max_temp': safe_find(weather_detail, 'div.temperature-max', logger=logger),
                'min_temp': safe_find(weather_detail, 'div.temperature-min', logger=logger),
                'wind_speed': safe_find(weather_detail, 'span.wind-speed', logger=logger),
                'wind_direction': safe_find(weather_detail, 'span.wind-speed', 'title', logger),
                'weather_state': safe_find(weather_detail, 'img.weather-state', 'alt', logger),
                'description': safe_find(weather_detail, 'div.weather-value p', logger=logger),
                'wind': safe_find(weather_detail, 'div.weather-detail:nth-of-type(2) div.weather-value p', logger=logger),
                'visibility': safe_find(weather_detail, 'div.weather-detail:nth-of-type(3) div.weather-value p', logger=logger),
                'rainfall': safe_find(weather_detail, 'div.weather-detail:nth-of-type(4) div.weather-value', logger=logger),
                'comments': safe_find(weather_detail, 'div.weather-detail:nth-of-type(5) div.weather-value p', logger=logger)
            }

            # Clean up the data
            if forecast['wind_direction']:
                forecast['wind_direction'] = forecast['wind_direction'].split(':')[1].strip().split()[0]
            if forecast['max_temp']:
                forecast['max_temp'] = forecast['max_temp'].strip('°C')
            if forecast['min_temp']:
                forecast['min_temp'] = forecast['min_temp'].strip('°C')

            forecast_data.append(forecast)
        except Exception as e:
            logger.log("ERROR", f"Error processing forecast for {date_str}: {e}")

    return issued_on, forecast_data

def create_database():
    try:
        conn = sqlite3.connect('data.db', detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = conn.cursor()
        
        # Enable foreign key support
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Create html_content table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS html_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_hash TEXT UNIQUE,
            content TEXT,
            fetched_at datetime
        )
        ''')
        
        # Create forecasts table with foreign key
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS forecasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            html_content_id INTEGER,
            issued_on datetime,
            date date,
            max_temp INTEGER,
            min_temp INTEGER,
            wind_speed INTEGER,
            wind_direction TEXT,
            weather_state TEXT,
            description TEXT,
            wind TEXT,
            visibility TEXT,
            rainfall TEXT,
            comments TEXT,
            FOREIGN KEY (html_content_id) REFERENCES html_content (id)
        )
        ''')
        
        # Create index on html_content_id for faster queries
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_html_content_id ON forecasts (html_content_id)
        ''')
        
        # Create logs table with foreign key to html_content
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS scraper_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp datetime,
            level TEXT,
            message TEXT,
            html_content_id INTEGER,
            FOREIGN KEY (html_content_id) REFERENCES html_content (id)
        )
        ''')
        
        # Create run_status table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS run_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date date UNIQUE,
            status TEXT,
            html_content_id INTEGER,
            FOREIGN KEY (html_content_id) REFERENCES html_content (id)
        )
        ''')
        
        conn.commit()
        return conn, cursor
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        return None, None

def check_content_exists(cursor, content_hash):
    # Calculate the date range: yesterday, today, and tomorrow
    tomorrow = date.today() + timedelta(days=1)
    two_days_ago = tomorrow - timedelta(days=2)
    
    cursor.execute("""
    SELECT id FROM html_content 
    WHERE content_hash = ? AND DATE(fetched_at) BETWEEN ? AND ?
    """, (content_hash, two_days_ago, tomorrow))
    return cursor.fetchone()

def insert_data(conn, cursor, html_content, issued_on, forecast_data, logger):
    if not conn or not cursor:
        logger.log("ERROR", "Database connection not established")
        return None

    try:
        # Insert new HTML content
        content_hash = hashlib.md5(html_content.encode()).hexdigest()
        cursor.execute('''
        INSERT INTO html_content (content_hash, content, fetched_at)
        VALUES (?, ?, ?)
        ''', (content_hash, html_content, datetime.now()))
        html_content_id = cursor.lastrowid
        logger.set_html_content_id(html_content_id)

        # Insert forecast data
        for forecast in forecast_data:
            cursor.execute('''
            INSERT INTO forecasts (html_content_id, issued_on, date, max_temp, min_temp, wind_speed, wind_direction, 
                                   weather_state, description, wind, visibility, rainfall, comments)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (html_content_id, issued_on, forecast['date'], forecast.get('max_temp'), forecast.get('min_temp'), 
                  forecast.get('wind_speed'), forecast.get('wind_direction'), forecast.get('weather_state'), 
                  forecast.get('description'), forecast.get('wind'), forecast.get('visibility'), 
                  forecast.get('rainfall'), forecast.get('comments')))
        
        # Insert or update run status
        cursor.execute('''
        INSERT OR REPLACE INTO run_status (run_date, status, html_content_id)
        VALUES (?, ?, ?)
        ''', (date.today(), 'SUCCESS', html_content_id))
        
        conn.commit()
        logger.log("INFO", f"Successfully inserted {len(forecast_data)} forecasts into the database.")
        return html_content_id
    except sqlite3.Error as e:
        logger.log("ERROR", f"Error inserting data into database: {e}")
        conn.rollback()
        return None

def check_run_status(conn, cursor, check_date):
    try:
        cursor.execute('''
        SELECT status, html_content_id FROM run_status WHERE run_date = ?
        ''', (check_date,))
        result = cursor.fetchone()
        if result:
            status, html_content_id = result
            return status == 'SUCCESS', html_content_id
        return False, None
    except sqlite3.Error as e:
        logging.error(f"Error checking run status: {e}")
        return False, None

def check_run_success(check_date):
    conn, cursor = create_database()
    if conn and cursor:
        success, html_content_id = check_run_status(conn, cursor, check_date)
        conn.close()
        return success, html_content_id
    else:
        print("Failed to establish database connection.")
        return False, None

def main(url):
    conn, cursor = create_database()
    if conn and cursor:
        logger = DatabaseLogger(conn, cursor)
        logger.log("INFO", f"Weather scraper started at {datetime.now()}")
    else:
        print("Failed to establish database connection. Falling back to file logging.")
        return

    html_content = fetch_html_content(url, logger)
    if html_content:
        content_hash = hashlib.md5(html_content.encode()).hexdigest()
        
        # Check if we've already processed this content in the relevant date range
        if check_content_exists(cursor, content_hash):
            logger.log("INFO", f"Content with hash {content_hash} has already been processed in the last 3 days. Skipping.")
            print("This content has already been processed in the last 3 days. No new data to add.")
        else:
            issued_on, forecast_data = scrape_weather_data(html_content, logger)
            if issued_on and forecast_data:
                html_content_id = insert_data(conn, cursor, html_content, issued_on, forecast_data, logger)
                if html_content_id:
                    print("Weather forecast data has been successfully scraped and stored in the database.")
                    logger.log("INFO", "Weather forecast data has been successfully scraped and stored in the database.")
                else:
                    print("Failed to insert data into the database.")
                    # Update run status to FAIL
                    cursor.execute('''
                    INSERT OR REPLACE INTO run_status (run_date, status, html_content_id)
                    VALUES (?, ?, ?)
                    ''', (date.today(), 'FAIL', None))
                    conn.commit()
            else:
                print("Failed to scrape weather data.")
                # Update run status to FAIL
                cursor.execute('''
                INSERT OR REPLACE INTO run_status (run_date, status, html_content_id)
                VALUES (?, ?, ?)
                ''', (date.today(), 'FAIL', None))
                conn.commit()
    else:
        print("Failed to fetch the web page. Please check the URL and try again.")
        # Update run status to FAIL
        cursor.execute('''
        INSERT OR REPLACE INTO run_status (run_date, status, html_content_id)
        VALUES (?, ?, ?)
        ''', (date.today(), 'FAIL', None))
        conn.commit()

    logger.log("INFO", f"Weather scraper finished at {datetime.now()}")
    conn.close()

if __name__ == "__main__":
    url = "https://www.gov.im/weather/5-day-forecast/"
    main(url)