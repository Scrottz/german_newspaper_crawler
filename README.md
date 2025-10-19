# German Newspaper Crawler

The **German Newspaper Crawler** is a modular and extensible framework designed to collect, parse, and store German-language news articles.  
It connects to MongoDB, structures raw HTML into clean, analyzable data objects, and optionally performs POS tagging via spaCy.  
Each supported newspaper or domain has its own parser module, while the crawler core handles configuration, deduplication, and database persistence.

> **Purpose:** provide a unified pipeline for gathering and normalizing newspaper data into a structured format suitable for linguistic analysis, NLP pipelines, and long-term archiving.

## Features

- **Config-driven architecture:** All domains, database settings, and logging levels are defined in a single YAML file (`configs/config.yaml`).
- **Extensible domain support:** Each newspaper or website is represented by a self-contained parser module located in `lib/domain/`.
- **Automatic deduplication:** Articles are uniquely identified and tracked through SHA-256 content hashes.
- **POS tagging (optional):** Integrates spaCy (`de_core_news_sm`) for part-of-speech tagging; falls back to whitespace tokenization when spaCy is unavailable.
- **MongoDB integration:** Articles are upserted into domain-specific collections with automatic index creation on `url` and `content_hash`.
- **Parallel fetching:** The crawler can download multiple URLs concurrently using a thread pool and progress tracking via `tqdm`.
- **Detailed logging:** Unified logger setup (console and rotating file logs) ensures consistent traceability across modules.

## Project Structure

**DIR: configs/**  
- **FILE:** `config.yaml`  
  → Global configuration file that defines logging, MongoDB settings, scheduler options, and all domains to crawl.

**DIR: lib/common/**  
- **FILE:** `config_handler.py` – Loads and caches YAML configuration files.  
- **FILE:** `logging.py` – Provides a unified logger factory for console and file output.  
- **FILE:** `mongodb.py` – Manages MongoDB connections, upserts, and index creation.  
- **FILE:** `object_model.py` – Defines the central article data model (`ObjectModel`) with serialization and hashing logic.  
- **FILE:** `parallel_fetcher.py` – Enables multi-threaded fetching and parsing of URLs with progress reporting.  
- **FILE:** `pos_tagging.py` – Handles part-of-speech tagging using spaCy, with intelligent fallbacks for missing models.  
- **FILE:** `web_requests.py` – Core crawling and parsing helpers; implements `process_domain_generic(...)` and URL builders.

**DIR: lib/domain/**  
- **FILE:** `taz.py`  
  → Example domain parser for *taz.de*, demonstrating metadata extraction (title, author, category, teaser) and body text parsing via BeautifulSoup.

**ROOT FILES:**  
- **FILE:** `german_newspaper_crawler.py`  
  → The main entry point that loads the config, connects to MongoDB, processes domains, and orchestrates the full crawl.  
- **FILE:** `pyproject.toml`  
  → Build configuration, metadata, and dependency list for setuptools.  
- **FILE:** `requirements.txt`  
  → Flat list of all required packages and version pins.

## Installation

### Requirements
- Python **3.11** or higher  
- A running **MongoDB** instance (local or remote)  
- Optional: spaCy model `de_core_news_sm` for POS tagging

### Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/Scrottz/german_newspaper_crawler.git
   cd german_newspaper_crawler

2. **Create and activate a virtual environment**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
       ```
   
3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
   
4. **(Optional) Install spaCy model for POS tagging**
   ```bash
   python -m spacy download de_core_news_sm
   ```
   
## Configuration
All settings are defined in the main YAML configuration file located at: configs/config.yaml


A minimal example looks like this:

```yaml
logging:
  level: "DEBUG"
  logdir: "logs"
  retentions_day: 14

domains:
  - name: taz
    collection: "taz"
    class_path: "lib.domain.taz:TAZ"
    base_url: "https://taz.de/"

mongodb:
  uri: "mongodb://localhost:27017"
  database_name: "german_news_paper"

scheduler:
  enabled: true
  time: "01:00"
  timezone: "Europe/Berlin"
```
**Key Sections**

logging:
Defines the global log level (DEBUG, INFO, ERROR, etc.) and the output directory for rotating log files.

domains:
Lists all newspaper domains to be crawled.
Each domain includes:

name: logical identifier (also used to locate the module under lib/domain/)

collection: MongoDB collection name

class_path: path to the domain class or parser

base_url: main entry URL for the newspaper

mongodb:
Contains the MongoDB connection URI and target database name.
The crawler will automatically ensure indexes on url and content_hash.

scheduler:
Placeholder configuration for future scheduling integration.
Currently, execution is manual or can be triggered externally (e.g., via cron).

**Config loading**
```python
from lib.common.config_handler import load_config
cfg = load_config()
```

## Usage

Run the crawler from the project root:

```bash
python german_newspaper_crawler.py
```
What Happens

Loads configs/config.yaml and validates domains and MongoDB settings.

Connects to MongoDB and gathers known content hashes to prevent duplicates.

For each domain:

Builds or imports its parser (lib/domain/{name}.py)

Collects article URLs, downloads, and parses articles

Optionally applies POS tagging (spaCy)

Upserts structured data into MongoDB

Closes the MongoDB client and finalizes logs.

Logging

Console and file logging (default: logs/german_newspapaer_crawler.log)

Adjustable verbosity via the logging.level setting in configs/config.yaml

## Architecture Overview

The crawler follows a modular, layered design with clear separation of responsibilities:

### 1. Configuration Layer
- **File:** `configs/config.yaml`  
  Defines all runtime settings such as domains, MongoDB connection, and logging.

### 2. Entry Point
- **File:** `german_newspaper_crawler.py`  
  - Loads configuration  
  - Connects to MongoDB  
  - Collects known hashes  
  - Iterates over all configured domains  

### 3. Domain Processing
- **File:** `lib/common/web_requests.py`  
  - Core logic in `process_domain_generic(...)`  
  - Builds article URLs (`build_article_urls(...)`)  
  - Fetches, parses, and upserts data into MongoDB  

### 4. Domain Parsers
- **Directory:** `lib/domain/`  
  Each domain module (e.g., `taz.py`) provides:
  - `get_article_urls()` to collect article links  
  - `parse_article()` to extract structured article content  

### 5. Database and Tagging Layers
- **File:** `lib/common/mongodb.py` → handles upserts, indexing, and hash collection  
- **File:** `lib/common/pos_tagging.py` → optional POS tagging via spaCy  

This structure ensures that adding a new domain only requires a single new parser file, without modifying the core crawler logic.

## Data Model

All parsed articles are represented as instances of the `ObjectModel` class defined in `lib/common/object_model.py`.  
This unified data structure ensures that each article, regardless of its source, follows the same schema when stored in MongoDB.

### Core Fields

| Field Name      | Type         | Description |
|-----------------|--------------|-------------|
| `_id`           | `int`        | Internal numeric ID assigned automatically |
| `url`           | `str`        | Original article URL |
| `titel`         | `str` or `None` | Article headline (if extracted) |
| `teaser`        | `str` or `None` | Short teaser or description |
| `autor`         | `str` or `None` | Author name, if available |
| `category`      | `str` or `None` | Section or category of the article |
| `published_date`| `datetime`   | Original publication timestamp |
| `parsed_date`   | `datetime`   | Timestamp when the article was parsed |
| `html`          | `str`        | Raw HTML source of the article |
| `text`          | `str`        | Cleaned and concatenated text content |
| `ai_keywords`   | `list[str]`  | Placeholder for AI-generated keywords (optional) |
| `pos_taggs`     | `list[tuple]`| List of POS-tagged tokens `(id, token, lemma, tag, pos)` |
| `content_hash`  | `str`        | SHA-256 hash for deduplication |

### Serialization and Storage

- Each `ObjectModel` instance can be converted to and from a dictionary via `.to_dict()` and `.from_dict()`.  
- The crawler automatically computes a **content hash** based on the URL or article text.  
- The hash is used to prevent duplicate entries in MongoDB.  
- Indexes are maintained on both `url` and `content_hash` to optimize lookups.