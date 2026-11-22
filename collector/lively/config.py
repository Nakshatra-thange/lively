import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://lively:lively@localhost:5432/lively")
CONCURRENCY = int(os.getenv("CONCURRENCY", "20"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "25"))
USER_AGENT = os.getenv("USER_AGENT", "LivelyBot/0.1 (job liveness research)")
FAIL_DEACTIVATE_AFTER = int(os.getenv("FAIL_DEACTIVATE_AFTER", "3"))
