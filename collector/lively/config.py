import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://lively:lively@localhost:5432/lively")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CONCURRENCY = int(os.getenv("CONCURRENCY", "20"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "25"))
USER_AGENT = os.getenv("USER_AGENT", "LivelyBot/0.1 (job liveness research)")
FAIL_DEACTIVATE_AFTER = int(os.getenv("FAIL_DEACTIVATE_AFTER", "3"))

DOMAIN_RPS = float(os.getenv("DOMAIN_RPS", "4"))
DOMAIN_BURST = int(os.getenv("DOMAIN_BURST", "8"))
MAX_THROTTLE_WAIT = float(os.getenv("MAX_THROTTLE_WAIT", "15"))

BREAKER_THRESHOLD = int(os.getenv("BREAKER_THRESHOLD", "15"))
BREAKER_WINDOW = int(os.getenv("BREAKER_WINDOW", "60"))
BREAKER_OPEN_SECONDS = int(os.getenv("BREAKER_OPEN_SECONDS", "30"))
BREAKER_MAX_OPEN_SECONDS = int(os.getenv("BREAKER_MAX_OPEN_SECONDS", "600"))

ROBOTS_ENFORCE = os.getenv("ROBOTS_ENFORCE", "1") == "1"
ROBOTS_TTL = int(os.getenv("ROBOTS_TTL", "86400"))

LEASE_SECONDS = int(os.getenv("LEASE_SECONDS", "90"))
CLAIM_SIZE = int(os.getenv("CLAIM_SIZE", "40"))

MIN_INTERVAL = int(os.getenv("MIN_INTERVAL", "1800"))
BASE_INTERVAL = int(os.getenv("BASE_INTERVAL", "86400"))
MAX_INTERVAL = int(os.getenv("MAX_INTERVAL", "604800"))
TIME_SCALE = float(os.getenv("TIME_SCALE", "1"))