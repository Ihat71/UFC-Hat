import os


class Config:
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-change-me")
    DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "data", "ufc-hat.db"))
    LOG_DIR = os.environ.get("LOG_DIR", os.path.join(BASE_DIR, "logs"))
    LOG_FILE = os.environ.get("LOG_FILE", os.path.join(LOG_DIR, "app.log"))
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = "Lax"
    SESSION_PERMANENT = False
    SESSION_TYPE = "filesystem"

