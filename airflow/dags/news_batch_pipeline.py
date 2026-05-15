"""
NEWS-INSIGHT AUTOMATED PIPELINE
===============================
Orchestration of the News Analysis Flow
"""

import re
import json
import logging
import requests
import feedparser
from datetime import datetime, timedelta
from collections import Counter
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import psycopg2

# Configure Logger
logger = logging.getLogger("airflow.task")

# Default DAG settings
default_args = {
    "owner": "news-insight-engine",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "news_processing_pipeline",
    default_args=default_args,
    description="Refines raw news data and generates analytics",
    schedule_interval="@hourly",
    catchup=False,
    tags=["production", "simple-etl"]
)

# --- Helper: Get MinIO Client ---
def get_storage():
    return Minio(
        "data-lake-storage:9000",
        access_key="admin_user",
        secret_key="simple_password_123",
        secure=False
    )

# --- Task 1: Fetch Raw Data (Integrated Fetcher) ---
def step_fetch_news(**context):
    """Triggers the collection of news from RSS feeds and streams to Kafka."""
    logger.info("🚀 Step 1: Starting news collection cycle...")
    
    storage = get_storage()
    
    # 1. Initialize Kafka Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers="message-queue:29092",
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000
        )
        logger.info("📡 Kafka stream connection established.")
    except Exception as e:
        logger.warning(f"Kafka streaming unavailable: {e}")
        producer = None

    # 2. Comprehensive Sources
    feeds = {
        "hespress": "https://www.hespress.com/feed",
        "le360": "https://fr.le360.ma/rss.xml",
        "map_news": "https://www.mapnews.ma/ar/rss.xml",
        "aljazeera": "https://www.aljazeera.net/aljazeerarss/a7c186be-1baa-4bd4-9d80-a84db1012da0/5fb3a770-5807-428a-9f5b-6c68758d9e6e",
        "bbc_arabic": "https://www.bbc.com/arabic/index.xml",
        "reuters": "https://www.reuters.com/rssFeed/arabicNews"
    }

    now = datetime.now()
    date_path = now.strftime("%Y/%m/%d")
    time_slug = now.strftime("%H%M%S")

    def extract_text(url):
        try:
            headers = {'User-Agent': 'NewsInsight/1.0'}
            r = requests.get(url, headers=headers, timeout=5)
            if r.status_code == 200:
                soup = BeautifulSoup(r.content, 'html.parser')
                return " ".join([p.get_text() for p in soup.find_all('p') if len(p.get_text()) > 30])
        except: pass
        return ""

    # 3. Collection Loop (Optimized with Multithreading)
    def process_entry(entry, label):
        try:
            return {
                "guid": entry.get("id", entry.get("link")),
                "title": entry.get("title", "Untitled"),
                "link": entry.get("link"),
                "source": label,
                "published": entry.get("published"),
                "content": extract_text(entry.get("link")),
                "collected_at": now.isoformat()
            }
        except:
            return None

    for label, url in feeds.items():
        try:
            feed = feedparser.parse(url)
            entries = feed.entries[:12]
            
            # Fetch content in parallel for this feed
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(lambda e: process_entry(e, label), entries))
            
            batch = [r for r in results if r]
            
            for article in batch:
                # Real-time Stream
                if producer:
                    producer.send("news-insight-stream", article)
            
            if batch:
                path = f"{date_path}/{label}_{time_slug}.json"
                data = json.dumps(batch, ensure_ascii=False).encode('utf-8')
                storage.put_object("raw-news-data", path, BytesIO(data), len(data), "application/json")
                logger.info(f"📥 Secured {len(batch)} items from {label} (Parallel)")

        except Exception as e:
            logger.error(f"Error fetching {label}: {e}")

    if producer:
        producer.flush()

# --- Task 2: Clean Data ---
def step_clean_news(**context):
    """Parses raw JSON files and strips HTML."""
    storage = get_storage()
    ds = context["ds"]
    date_prefix = ds.replace("-", "/")
    
    logger.info(f"🧹 Step 2: Cleaning news data for {ds}")
    
    raw_objects = storage.list_objects("raw-news-data", prefix=date_prefix, recursive=True)
    
    cleaned_data = []
    for obj in raw_objects:
        try:
            response = storage.get_object("raw-news-data", obj.object_name)
            batch = json.loads(response.read().decode("utf-8"))
            for article in batch:
                if article.get("content"):
                    soup = BeautifulSoup(article["content"], "html.parser")
                    article["content_clean"] = soup.get_text(separator=" ").strip()
                else:
                    article["content_clean"] = ""
                article["word_count"] = len(article["content_clean"].split())
                cleaned_data.append(article)
        except Exception as e:
            logger.warning(f"Failed to process {obj.object_name}: {e}")

    if cleaned_data:
        output_json = json.dumps(cleaned_data, ensure_ascii=False, indent=2).encode("utf-8")
        storage.put_object("cleaned-news-data", f"{date_prefix}/cleaned_articles.json", BytesIO(output_json), len(output_json), "application/json")
        logger.info(f"✨ Cleaned {len(cleaned_data)} articles.")

# --- Task 3: Analyze Data ---
def step_analyze_news(**context):
    """Extracts keywords and provider stats."""
    storage = get_storage()
    ds = context["ds"]
    path = f"{ds.replace('-', '/')}/cleaned_articles.json"
    
    try:
        response = storage.get_object("cleaned-news-data", path)
        articles = json.loads(response.read().decode("utf-8"))
        
        all_text = " ".join([f"{a.get('title','')} {a.get('content_clean','')}" for a in articles]).lower()
        tokens = re.findall(r'[\w\u0600-\u06FF]{4,}', all_text)
        
        results = {
            "report_date": ds,
            "metrics": {
                "total_articles": len(articles),
                "sources": dict(Counter(a.get("source") for a in articles)),
                "top_keywords": Counter(tokens).most_common(25)
            }
        }
        
        output_json = json.dumps(results, ensure_ascii=False).encode("utf-8")
        storage.put_object("final-analytics-data", f"{ds.replace('-', '/')}/analysis_report.json", BytesIO(output_json), len(output_json), "application/json")
        logger.info("🧠 Step 3: Analysis complete.")
    except:
        logger.error("No data for analysis.")

# --- Helper: Simple Categorizer ---
def categorize_article(text):
    text = text.lower()
    categories = {
        'sport': ['football', 'match', 'joueur', 'championnat', 'caf', 'fifa', 'botola', 'sport', 'كرة', 'لاعب', 'منتخب'],
        'économie': ['dirham', 'bourse', 'banque', 'économie', 'finance', 'entreprise', 'investissement', 'اقتصاد', 'درهم', 'مالية', 'استثمار'],
        'politique': ['gouvernement', 'ministre', 'parlement', 'élection', 'politique', 'parti', 'diplomatie', 'سياسة', 'حكومة', 'برلمان', 'وزير'],
        'santé': ['santé', 'hôpital', 'médecin', 'virus', 'santé', 'médicament', 'صحة', 'مستشفى', 'دواء', 'طبيب'],
        'technologie': ['ia', 'tech', 'smart', 'internet', 'logiciel', 'digital', 'تكنولوجيا', 'رقمي', 'حاسوب']
    }
    for cat, keywords in categories.items():
        if any(kw in text for kw in keywords):
            return cat
    return 'general'

# Language Mapping
SOURCE_LANGS = {
    'hespress': 'ar', 'barlamane': 'ar', 'map_news': 'ar', 
    'aljazeera': 'ar', 'bbc_arabic': 'ar', 'le360': 'fr', 'reuters': 'en'
}

# --- Task 4: Database Archival ---
def step_save_to_db(**context):
    """Loads results and individual articles into PostgreSQL."""
    storage = get_storage()
    ds = context["ds"]
    date_path = ds.replace("-", "/")
    
    try:
        # 1. Load Aggregated Report
        report_path = f"{date_path}/analysis_report.json"
        response_report = storage.get_object("final-analytics-data", report_path)
        report = json.loads(response_report.read().decode("utf-8"))

        # 2. Load Detailed Cleaned Articles
        articles_path = f"{date_path}/cleaned_articles.json"
        response_articles = storage.get_object("cleaned-news-data", articles_path)
        articles = json.loads(response_articles.read().decode("utf-8"))

        conn = psycopg2.connect(
            host="analytics-db",
            dbname="news_analytics",
            user="data_analyst",
            password="data_pass_2024"
        )
        cur = conn.cursor()

        # --- A. Update Aggregate Tables ---
        cur.execute("DELETE FROM analytics_top_keywords WHERE report_date = %s", (ds,))
        for word, freq in report["metrics"]["top_keywords"]:
            cur.execute("INSERT INTO analytics_top_keywords (report_date, keyword, frequency) VALUES (%s, %s, %s)", (ds, word, freq))

        for source, count in report["metrics"]["sources"].items():
            cur.execute("INSERT INTO analytics_articles_per_day (report_date, source_name, total) VALUES (%s, %s, %s) ON CONFLICT (report_date, source_name) DO UPDATE SET total = EXCLUDED.total", (ds, source, count))

        # --- B. Update Fact Table (Articles) ---
        for article in articles:
            try:
                # Get or Create Source ID
                cur.execute("INSERT INTO dim_sources (source_name) VALUES (%s) ON CONFLICT (source_name) DO NOTHING", (article['source'],))
                cur.execute("SELECT id FROM dim_sources WHERE source_name = %s", (article['source'],))
                source_id = cur.fetchone()[0]

                # Get Date ID
                cur.execute("SELECT id FROM dim_dates WHERE full_date = %s", (ds,))
                date_res = cur.fetchone()
                date_id = date_res[0] if date_res else None

                # Clean and Parse Published Date
                pub_date = article.get('published')
                if not pub_date:
                    pub_date = datetime.now()

                # Get or Create Category ID
                cat_name = categorize_article(f"{article['title']} {article.get('content_clean', '')}")
                cur.execute("INSERT INTO dim_categories (category_name) VALUES (%s) ON CONFLICT (category_name) DO NOTHING", (cat_name,))
                cur.execute("SELECT id FROM dim_categories WHERE category_name = %s", (cat_name,))
                cat_id = cur.fetchone()[0]

                # Get Language
                lang = SOURCE_LANGS.get(article['source'], 'ar')

                # Insert Fact
                cur.execute("""
                    INSERT INTO fact_articles (article_uuid, title, url, content, word_count, source_id, date_id, category_id, language, published_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (article_uuid) DO UPDATE SET 
                        category_id = EXCLUDED.category_id,
                        language = EXCLUDED.language
                """, (
                    article['guid'][:36], 
                    article['title'],
                    article['link'],
                    article.get('content_clean', ''),
                    article.get('word_count', 0),
                    source_id,
                    date_id,
                    cat_id,
                    lang,
                    pub_date
                ))
            except Exception as inner_e:
                logger.warning(f"Failed to archive article {article.get('guid')}: {inner_e}")

        conn.commit()
        logger.info(f"💾 Step 4: Database updated with {len(articles)} articles archived.")
    except Exception as e:
        logger.error(f"DB Archival Error: {e}")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

# --- DAG Definition ---
with dag:
    fetch_task = PythonOperator(task_id="collect_news_data", python_callable=step_fetch_news)
    clean_task = PythonOperator(task_id="clean_news_data", python_callable=step_clean_news)
    analyze_task = PythonOperator(task_id="analyze_news_data", python_callable=step_analyze_news)
    store_task = PythonOperator(task_id="save_to_database", python_callable=step_save_to_db)

    fetch_task >> clean_task >> analyze_task >> store_task