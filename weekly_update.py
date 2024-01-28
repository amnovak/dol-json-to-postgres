from download_doj import download_latest
from extract_doj import extract_all
from update_postgres import upsert_json


download_latest()
extract_all()
upsert_json()