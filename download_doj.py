#!/usr/bin/python3

import datetime
import os
import requests
import time

def download(url, dest, retries=25):

    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36' }
    failed = False
    try:
        r = requests.get(url, headers=headers, timeout=(6,6))
        if r.status_code != 200:
            failed = True
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        failed = True

    if failed:
        if retries:
            print("retrying", url)
            time.sleep(30-retries)
            return download(url, dest, retries-1)
        return False

    with open(dest, 'wb') as outfile:
        outfile.write(r.content)

    return True


def download_all():

    if not os.path.exists("zipcache"):
        os.mkdir("zipcache")


    # To download all available data set to October 9, 2023:  .date(2019,10,9) 
    date = datetime.date(2025, 1, 31)

    print(f"Downloading all data since {date}")

    while True:
        for item in ["jo", "h2a", "h2b"]:
            d = date.isoformat()
            dest = f"zipcache/{d}_{item}.zip"
            url = f"https://api.seasonaljobs.dol.gov/datahub-search/sjCaseData/zip/{item}/{d}"
            if os.path.exists(dest):
                continue

            print(url)
            if not download(url, dest):
                print("download failed")

        if date == datetime.date.today():
            break

        date += datetime.timedelta(days=19)
        if date > datetime.date.today():
            date = datetime.date.today()



def download_latest():

    if not os.path.exists("zipcache"):
        os.mkdir("zipcache")

    print("Downloading just the latest JSON files.")

    date = datetime.date.today()

    for item in ["jo", "h2a", "h2b"]:

        d = date.isoformat()
        dest = f"zipcache/{d}_{item}.zip"
        url = f"https://api.seasonaljobs.dol.gov/datahub-search/sjCaseData/zip/{item}/{d}"
        if os.path.exists(dest):
            continue

        print(url)
        if not download(url, dest):
            print(f"{item} download failed")


if __name__ == '__main__':
    download_latest()
