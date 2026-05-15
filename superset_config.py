# Superset Configuration File

# Flask-Limiter Configuration (for rate limiting)
# Must be set before any Flask-Limiter initialization
RATELIMIT_STORAGE_URL = 'redis://redis:6379/2'

# Redis Cache Configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_REDIS_URL': 'redis://redis:6379/1',
    'CACHE_DEFAULT_TIMEOUT': 300,
}

# Results Backend Configuration
RESULTS_BACKEND = 'cache'
RESULTS_BACKEND_USE_MSGPACK = True

# Secret key (already set via environment but can be overridden here)
SECRET_KEY = 'news-platform-superset-secret-key-change-in-prod'

# Additional security settings
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Disable development warnings
FLASK_ENV = 'production'
