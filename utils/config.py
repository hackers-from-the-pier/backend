from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST=os.environ.get("DB_HOST")
DB_PORT=os.environ.get("DB_PORT")
DB_NAME=os.environ.get("DB_NAME")
DB_USER=os.environ.get("DB_USER")
DB_PASSWORD=os.environ.get("DB_PASSWORD")

# API settings
API_HOST = os.environ.get("API_HOST", "0.0.0.0")
API_PORT = os.environ.get("API_PORT", "8000")
API_VERSION = os.environ.get("API_VERSION", "v1")   
API_RELOAD =    os.environ.get("API_RELOAD", "True").lower() == "true"

# JWT settings
JWT_SECRET = os.environ.get("JWT_SECRET")

# AWS S3 Beget configuration
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.environ.get("AWS_BUCKET_NAME")
AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL")
AWS_REGION = os.environ.get("AWS_REGION")
AWS_PATH_STYLE_URL = f"{AWS_ENDPOINT_URL}/{AWS_BUCKET_NAME}"
AWS_VIRTUAL_HOSTED_URL = f"https://{AWS_BUCKET_NAME}.s3.ru1.storage.beget.cloud"

# FTP Configuration
FTP_HOST = os.getenv('FTP_HOST', 'ftp.ru1.storage.beget.cloud')
FTP_USERNAME = os.getenv('FTP_USERNAME')
FTP_PASSWORD = os.getenv('FTP_PASSWORD')
FTP_BASE_URL = os.getenv('FTP_BASE_URL', 'https://ru1.storage.beget.cloud')

# pochta
SMTP_USER = "info@kilowattt.ru"
SMTP_PASSWORD = "8oXBi2i297R*"

# Account Type – IMAP
# Incoming – imap.beget.com
# Outgoing – smtp.beget.com