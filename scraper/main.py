import os
import time
import json
import logging
import requests
# pyrefly: ignore [missing-import]
import feedparser
from datetime import datetime
from io import BytesIO
from bs4 import BeautifulSoup
# pyrefly: ignore [missing-import]
from minio import Minio
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] news_fetcher: %(message)s'
)
logger = logging.getLogger("news_fetcher")

class NewsFetcher:
    def __init__(self):
        # Environment settings
        self.kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "message-queue:29092")
        self.minio_host = os.getenv("MINIO_ENDPOINT", "data-lake-storage:9000")
        self.minio_user = os.getenv("STORAGE_USER", "admin_user")
        self.minio_pass = os.getenv("STORAGE_PASSWORD", "simple_password_123")
        self.interval = int(os.getenv("FETCH_INTERVAL", 30))

        # Comprehensive News Sources
        self.feeds = {
            "hespress": "https://www.hespress.com/feed",
            "le360": "https://fr.le360.ma/rss.xml",
            "map_news": "https://www.mapnews.ma/ar/rss.xml",
            "aljazeera": "https://www.aljazeera.net/aljazeerarss/a7c186be-1baa-4bd4-9d80-a84db1012da0/5fb3a770-5807-428a-9f5b-6c68758d9e6e",
            "bbc_arabic": "https://www.bbc.com/arabic/index.xml",
            "reuters_arabic": "https://www.reuters.com/rssFeed/arabicNews"
        }

        self.producer = None
        self.storage = None

    def _get_storage(self):
        """Lazy-init MinIO connection."""
        if not self.storage:
            try:
                self.storage = Minio(
                    self.minio_host,
                    access_key=self.minio_user,
                    secret_key=self.minio_pass,
                    secure=False
                )
            except Exception as e:
                logger.error(f"Failed to connect to Storage: {e}")
        return self.storage

    def _get_producer(self):
        """Lazy-init Kafka connection with short timeout."""
        if not self.producer:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_host,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    request_timeout_ms=2000,
                    retries=1
                )
            except NoBrokersAvailable:
                logger.warning("Message Queue (Kafka) is offline. Skipping streaming for now.")
            except Exception as e:
                logger.error(f"Kafka error: {e}")
        return self.producer

    def extract_text(self, url):
        """Downloads and cleans article text."""
        try:
            headers = {'User-Agent': 'NewsInsightBot/1.0 (Educational Project)'}
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                soup = BeautifulSoup(r.content, 'html.parser')
                # Get paragraphs and filter out short/empty ones
                p_text = [p.get_text().strip() for p in soup.find_all('p')]
                return " ".join([t for t in p_text if len(t) > 30])
        except Exception:
            pass
        return ""

    def run_cycle(self):
        """Scrapes all feeds once."""
        storage = self._get_storage()
        producer = self._get_producer()
        
        if not storage:
            logger.error("Storage not available. Aborting cycle.")
            return

        logger.info("--- Starting Collection Cycle ---")
        now = datetime.now()
        date_path = now.strftime("%Y/%m/%d")
        time_slug = now.strftime("%H%M%S")

        for label, url in self.feeds.items():
            try:
                feed = feedparser.parse(url)
                batch = []
                
                for entry in feed.entries[:15]:
                    article = {
                        "guid": entry.get("id", entry.get("link")),
                        "title": entry.get("title", "Untitled"),
                        "link": entry.get("link"),
                        "source": label,
                        "published": entry.get("published"),
                        "content": self.extract_text(entry.get("link")),
                        "collected_at": now.isoformat()
                    }
                    batch.append(article)
                    
                    if producer:
                        producer.send("raw-news-stream", article)

                if batch:
                    # Save JSON batch to MinIO
                    path = f"{date_path}/{label}_{time_slug}.json"
                    data = json.dumps(batch, ensure_ascii=False).encode('utf-8')
                    storage.put_object(
                        "raw-news-data",
                        path,
                        BytesIO(data),
                        len(data),
                        content_type="application/json"
                    )
                    logger.info(f"✅ Secured {len(batch)} items from {label}")

            except Exception as e:
                logger.error(f"Error scraping {label}: {e}")

        if producer:
            producer.flush()
        logger.info("--- Cycle Finished ---")

    def start(self):
        """Infinite loop."""
        logger.info(f"News Insight Fetcher active (Interval: {self.interval}m)")
        while True:
            try:
                self.run_cycle()
            except Exception as e:
                logger.error(f"Critical error in loop: {e}")
            
            logger.info(f"Zzz... Sleeping for {self.interval} minutes")
            time.sleep(self.interval * 60)

if __name__ == "__main__":
    NewsFetcher().start()
