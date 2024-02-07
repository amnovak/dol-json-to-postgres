from download_doj import download_latest
from extract_doj import extract_all
from update_postgres import upsert_json
from apscheduler.schedulers.blocking import BlockingScheduler


def weekly_update():
    download_latest()
    extract_all()
    upsert_json()


if __name__ == '__main__':
    
    # Testing scheduler and clock process 
    sched = BlockingScheduler()
    sched.add_job(weekly_update, 'interval', minutes=10, start_date='2024-02-07 10:20:00', timezone='US/Central')
    sched.start()



