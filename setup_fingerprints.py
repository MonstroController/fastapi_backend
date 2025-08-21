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

# Определение базовой модели
Base = declarative_base()

class BrowserFingerprintF5(Base):
    """Модель для таблицы browser_fingerprints_f5"""
    __tablename__ = 'browser_fingerprints_f5'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    data = Column(JSONB, nullable=False)  # JSONB для PostgreSQL
    fingerprint_hash = Column(String(64), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<BrowserFingerprintF5(id={self.id}, hash={self.fingerprint_hash})>"

class FingerprintCollector:
    def __init__(self, db_url, api_url, check_interval):
        self.db_url = db_url
        self.api_url = api_url
        self.check_interval = check_interval
        self.engine = None
        self.Session = None
        self.seen_hashes = set()
        self.is_running = True
        
        # Обработка сигналов для graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Обработчик сигналов для graceful shutdown"""
        print(f"Получен сигнал {signum}, завершаем работу...")
        self.is_running = False

    def setup_database(self):
        """Настройка подключения к базе данных и создание таблиц"""
        try:
            # Создаем engine для PostgreSQL
            self.engine = create_engine(
                self.db_url,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                echo=False  # Установите True для отладки SQL-запросов
            )
            
            # Создаем таблицу если она не существует
            Base.metadata.create_all(self.engine)
            
            # Создаем фабрику сессий
            self.Session = sessionmaker(bind=self.engine)
            
            print("База данных настроена успешно")
            return True
            
        except SQLAlchemyError as e:
            print(f"Ошибка настройки базы данных: {e}")
            return False

    def load_existing_hashes(self):
        """Загрузка существующих хешей из базы данных"""
        try:
            session = self.Session()
            hashes = session.query(BrowserFingerprintF5.fingerprint_hash).all()
            self.seen_hashes = {hash[0] for hash in hashes}
            print(f"Загружено {len(self.seen_hashes)} существующих хешей")
            session.close()
            
        except SQLAlchemyError as e:
            print(f"Ошибка загрузки хешей: {e}")
            self.seen_hashes = set()

    def calculate_hash(self, data):
        """Вычисление хеша SHA-256 для данных отпечатка"""
        # Сортируем ключи для обеспечения консистентности
        sorted_data = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(sorted_data.encode('utf-8')).hexdigest()

    def fetch_fingerprint(self):
        """Получение отпечатка с API"""
        try:
            response = requests.get(
                self.api_url, 
                timeout=10,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе к API: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Ошибка парсинга JSON: {e}")
            return None

    def save_fingerprint(self, fingerprint_data):
        """Сохранение отпечатка в базу данных"""
        session = None
        try:
            fingerprint_hash = self.calculate_hash(fingerprint_data)
            
            # Проверяем, есть ли уже такой отпечаток
            if fingerprint_hash in self.seen_hashes:
                print(f"Отпечаток уже существует: {fingerprint_hash}")
                return False, fingerprint_hash
            
            session = self.Session()
            
            # Создаем новую запись
            new_fingerprint = BrowserFingerprintF5(
                data=fingerprint_data,
                fingerprint_hash=fingerprint_hash
            )
            
            session.add(new_fingerprint)
            session.commit()
            
            self.seen_hashes.add(fingerprint_hash)
            print(f"Сохранен новый отпечаток ID: {new_fingerprint.id}, Hash: {fingerprint_hash}")
            return True, fingerprint_hash
            
        except IntegrityError:
            # На случай race condition
            if session:
                session.rollback()
            print(f"Конфликт уникальности (возможно параллельное выполнение): {fingerprint_hash}")
            return False, fingerprint_hash
        except SQLAlchemyError as e:
            if session:
                session.rollback()
            print(f"Ошибка при сохранении в базу данных: {e}")
            return False, None
        finally:
            if session:
                session.close()

    def run(self):
        """Основной цикл сбора отпечатков"""
        if not self.setup_database():
            return
            
        self.load_existing_hashes()
        print(f"Запуск сбора отпечатков с интервалом {self.check_interval} секунд")
        
        while self.is_running:
            try:
                # Получаем отпечаток
                fingerprint_data = self.fetch_fingerprint()
                
                if fingerprint_data:
                    # Сохраняем новый отпечаток
                    saved, fingerprint_hash = self.save_fingerprint(fingerprint_data)
                    
                    if not saved and fingerprint_hash:
                        print(f"Обнаружен повторяющийся отпечаток. Завершение работы.")
                        break
                
                # Ожидаем перед следующим запросом
                time.sleep(self.check_interval)
                
            except Exception as e:
                print(f"Неожиданная ошибка в основном цикле: {e}")
                time.sleep(self.check_interval)

    def close(self):
        """Закрытие соединения с базой данных"""
        if self.engine:
            self.engine.dispose()
            print("Соединение с базой данных закрыто")

# Конфигурация
username = ""
password = ""
host = ""
port = ""
database = ""
DB_URL = f"postgresql://{username}:{password}@{host}:{port}/{database}"
API_URL = "http://31.129.110.131/generate/f5.php"
CHECK_INTERVAL = 1.0  # Интервал в секундах

def main():
    collector = FingerprintCollector(DB_URL, API_URL, CHECK_INTERVAL)
    
    try:
        collector.run()
    except KeyboardInterrupt:
        print("Работа прервана пользователем")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
    finally:
        collector.close()

if __name__ == "__main__":
    main()