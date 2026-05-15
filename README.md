# 🌐 MediaPulse: End-to-End Big Data News Pipeline


## 📖 Overview

**MediaPulse** is a robust, scalable Big Data platform designed to automate the collection, processing, and visualization of global news data. Born from a need to understand media trends in real-time, this project implements a full **Lambda Architecture** using a **Medallion Data Lake** approach.

Whether it's tracking sentiment, detecting fake news, or analyzing regional topics, MediaPulse provides the infrastructure to turn raw web content into actionable insights.

---

## 🏗️ Architecture & Data Flow

The platform follows the **Medallion Architecture** to ensure data quality and lineage:

1.  **🥉 Bronze Layer (Raw):** Scrapers collect raw HTML and JSON from RSS feeds and news sites, pushing them into Kafka. Airflow then ingests these into the `raw-news-data` MinIO bucket.
2.  **🥈 Silver Layer (Cleaned):** A processing spark/python job removes HTML noise, detects language, and standardizes the schema. Results are stored in `cleaned-news-data`.
3.  **🥇 Gold Layer (Analytical):** Data is aggregated and enriched (e.g., sentiment analysis, entity extraction) and loaded into the **PostgreSQL Data Warehouse** for reporting.

![System Architecture](simple_architecture_diagram_1778877353030.png)

---

## 🛠️ Technical Stack

| Category | Technology | Purpose |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow | Managing DAGs and data movement |
| **Streaming** | Apache Kafka | Real-time message queuing for scrapers |
| **Storage** | MinIO (S3 Compatible) | Data Lake for Bronze and Silver layers |
| **Warehouse** | PostgreSQL | Structured storage for final analytical data |
| **Visualization**| Apache Superset | Interactive BI Dashboards & Exploration |
| **Infrastructure**| Docker & Compose | Containerized deployment and scaling |

---

## 🚀 Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
- At least 8GB of RAM allocated to Docker.

### Installation
1. **Clone the repository:**
   ```bash
   git clone https://github.com/YOUR_USERNAME/MediaPulse.git
   cd MediaPulse
   ```

2. **Configure Environment:**
   Create a `.env` file in the root directory (refer to the documentation for required keys).

3. **Launch the platform:**
   ```bash
   docker-compose up -d
   ```

---

## 🔗 Dashboard Access

| Service | URL | Credentials (Default) |
| :--- | :--- | :--- |
| **Airflow UI** | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `admin_user` / `simple_password_123` |
| **Superset BI** | [http://localhost:8088](http://localhost:8088) | `admin` / `admin` |
| **Postgres Port** | `localhost:5433` | See `.env` |

