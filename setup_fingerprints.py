import requests
import hashlib
import json
import time
import signal
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

# Base model definition
Base = declarative_base()

class BrowserFingerprintF5(Base):
    """Model for browser_fingerprints_f5 table"""
    __tablename__ = 'browser_fingerprints_f5'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    data = Column(JSONB, nullable=False)
    fingerprint_hash = Column(String(64), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class FingerprintCollector:
    def __init__(self, db_url, api_url, check_interval):
        self.db_url = db_url
        self.api_url = api_url
        self.check_interval = check_interval
        self.engine = None
        self.Session = None
        self.is_running = True
        
        # Signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Signal handler for graceful shutdown"""
        print(f"Received signal {signum}, shutting down...")
        self.is_running = False

    def setup_database(self):
        """Set up database connection and create tables"""
        try:
            # Create engine for PostgreSQL
            self.engine = create_engine(
                self.db_url,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                echo=False
            )
            
            # Create table if it doesn't exist
            Base.metadata.create_all(self.engine)
            
            # Create session factory
            self.Session = sessionmaker(bind=self.engine)
            
            print("Database configured successfully")
            return True
            
        except SQLAlchemyError as e:
            print(f"Database setup error: {e}")
            return False

    def calculate_hash(self, data):
        """Calculate SHA-256 hash for fingerprint data"""
        # Sort keys for consistency
        sorted_data = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(sorted_data.encode('utf-8')).hexdigest()

    def fetch_fingerprint(self):
        """Fetch fingerprint from API"""
        try:
            response = requests.get(
                self.api_url, 
                timeout=10,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
            return None

    def save_fingerprint(self, fingerprint_data):
        """Save fingerprint to database with uniqueness error handling"""
        session = None
        try:
            fingerprint_hash = self.calculate_hash(fingerprint_data)
            
            session = self.Session()
            
            # Create new record
            new_fingerprint = BrowserFingerprintF5(
                data=fingerprint_data,
                fingerprint_hash=fingerprint_hash
            )
            
            session.add(new_fingerprint)
            session.commit()
            
            print(f"Saved new fingerprint ID: {new_fingerprint.id}, Hash: {fingerprint_hash}")
            return True, fingerprint_hash
            
        except IntegrityError:
            # Handle uniqueness error (hash already exists)
            if session:
                session.rollback()
            print(f"Fingerprint already exists in DB: {fingerprint_hash}")
            return False, fingerprint_hash
        except SQLAlchemyError as e:
            if session:
                session.rollback()
            print(f"Database save error: {e}")
            return False, None
        finally:
            if session:
                session.close()

    def run(self):
        """Main fingerprint collection loop"""
        if not self.setup_database():
            return
            
        print(f"Starting fingerprint collection with interval {self.check_interval} seconds")
        
        while self.is_running:
            try:
                # Get fingerprint
                fingerprint_data = self.fetch_fingerprint()
                
                if fingerprint_data:
                    # Save new fingerprint
                    saved, fingerprint_hash = self.save_fingerprint(fingerprint_data)
                    
                    if not saved and fingerprint_hash:
                        print(f"Duplicate fingerprint detected. Shutting down.")
                        break
                
                # Wait before next request
                time.sleep(self.check_interval)
                
            except Exception as e:
                print(f"Unexpected error in main loop: {e}")
                time.sleep(self.check_interval)

    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            print("Database connection closed")

# Configuration
username = ""
password = ""
host = ""
port = ""
database = ""
DB_URL = f"postgresql://{username}:{password}@{host}:{port}/{database}"
API_URL = ""
CHECK_INTERVAL = 1.0  # Interval in seconds

def main():
    collector = FingerprintCollector(DB_URL, API_URL, CHECK_INTERVAL)
    
    try:
        collector.run()
    except KeyboardInterrupt:
        print("Operation interrupted by user")
    except Exception as e:
        print(f"Critical error: {e}")
    finally:
        collector.close()

if __name__ == "__main__":
    main()