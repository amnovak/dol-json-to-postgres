from datetime import datetime, timezone
import sys


# Get current weekday (UTC)
now_utc = datetime.now(timezone.utc)

today = now_utc.weekday()

if today != 0:
    print(f"Today is {now_utc.strftime('%A')} (UTC). This update runs on Monday.")
    sys.exit()

else: 
    print(f"Today is {now_utc.strftime('%A')} (UTC). Starting weekly update...")


from download_doj import download_latest
from extract_doj import extract_all
from update_postgres import upsert_json
from cleanup import cleanup


def weekly_update():
    print("Running cleanup.")
    cleanup()

    print("Running download_latest.")
    download_latest()

    print("Running extract_all.")
    extract_all()
    
    print("Running upsert.")
    upsert_json()


if __name__ == '__main__':
    weekly_update()





