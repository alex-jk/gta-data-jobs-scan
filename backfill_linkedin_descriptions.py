"""
Fills in the description for rows that have a real LinkedIn URL but ended up
with a blank description (LinkedIn's pane rendering too slowly deep into a
long scraping session -- see linkedin_wait_for_description's retry/timeout
bump in job_scan.py). Visits each job's URL directly and reads the pane the
same way the live scraper does, then writes the result back into
simplyhired_final_cleaned.csv in place, checkpointing every 20 rows.

Run this AFTER copying simplyhired_final_cleaned_with_summaries.csv back over
simplyhired_final_cleaned.csv, and BEFORE re-running summarize_existing.py --
otherwise the next summarizer pass won't see these rows as needing a summary
update in the right file state.
"""
import os
import random
import re
import time

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

import job_scan

CSV = "simplyhired_final_cleaned.csv"
PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".linkedin_profile")
CHECKPOINT_EVERY = 20


def make_driver_with_profile():
    opts = Options()
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(f"--user-data-dir={PROFILE_DIR}")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(45)
    return driver


def run():
    df = pd.read_csv(CSV)

    is_blank_desc = df["description"].isna() | (
        df["description"].astype(str).str.strip().isin(["", "N/A", "nan"])
    )
    is_linkedin = df["url"].astype(str).str.contains(r"linkedin\.com/jobs/view", regex=True)
    targets = df[is_blank_desc & is_linkedin]

    print(f"Backfilling {len(targets)} blank LinkedIn descriptions...")
    if targets.empty:
        return

    driver = make_driver_with_profile()
    filled = 0
    failed = 0
    consecutive_fails = 0
    try:
        for n, (idx, row) in enumerate(targets.iterrows(), start=1):
            url = str(row["url"])
            m = re.search(r"/jobs/view/(\d+)", url)
            if not m:
                failed += 1
                continue
            job_id = m.group(1)

            # A tight, fixed-interval loop of back-to-back page loads reads as
            # bot traffic and LinkedIn appears to soft-throttle the
            # description pane specifically under that pattern (the page,
            # title and URL all load fine -- only the pane content stalls).
            # Randomized pacing plus a cooldown after repeated failures avoids
            # re-triggering that.
            if consecutive_fails >= 3:
                cooldown = 45 + random.uniform(0, 15)
                print(f"   -- {consecutive_fails} failures in a row, cooling down {cooldown:.0f}s --")
                time.sleep(cooldown)
                consecutive_fails = 0

            t0 = time.time()
            desc = ""
            try:
                driver.get(url)
                time.sleep(random.uniform(2.5, 5.0))
                desc = job_scan.linkedin_wait_for_description(driver, job_id, timeout=15.0)
                if not desc:
                    # One retry on the same page before giving up on this row.
                    time.sleep(random.uniform(2.0, 4.0))
                    desc = job_scan.linkedin_wait_for_description(driver, job_id, timeout=10.0)
            except Exception as exc:
                desc = ""
                print(f"  [ERROR] {type(exc).__name__}: {exc}")

            elapsed = time.time() - t0
            title = str(row.get("title", ""))[:60]
            if desc:
                df.at[idx, "description"] = desc
                filled += 1
                consecutive_fails = 0
                print(f"[{n}/{len(targets)}] OK   {elapsed:4.1f}s  len={len(desc):5d}  {title}")
            else:
                failed += 1
                consecutive_fails += 1
                print(f"[{n}/{len(targets)}] FAIL {elapsed:4.1f}s  (still blank)  {title}")

            if n % CHECKPOINT_EVERY == 0:
                df.to_csv(CSV, index=False, encoding="utf-8")
                print(f"   -- checkpoint saved ({n}/{len(targets)}) --")

            time.sleep(random.uniform(1.0, 2.5))

        df.to_csv(CSV, index=False, encoding="utf-8")
    finally:
        driver.quit()

    print(f"\nDone. Filled {filled}, still failed {failed}, out of {len(targets)}.")


if __name__ == "__main__":
    run()
