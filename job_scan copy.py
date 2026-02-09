import time
import random
import pandas as pd
import logging
import os
import re
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# ---------- CONFIG ----------
KEYWORDS = ["data scientist", "data analyst"]
LOCATION = "Toronto, ON"
RADIUS = 50
OUTPUT_FILE = "simplyhired_final_cleaned.csv"
MAX_JOBS_TO_SCRAPE = 8
MAX_PAGES_PER_KEYWORD = 2

# Salary reliability controls
SALARY_RETRIES = 3              # retry salary extraction per job
SALARY_WAIT_SECONDS = 8         # wait per attempt
OPEN_URL_FALLBACK = True        # if pane fails, open job URL in new tab & extract there

# Debug
DEBUG_SALARY = True
MAX_SALARY_DEBUG_PRINTS = 12
_salary_debug_count = 0

BAD_KEYWORDS = [
    "intern", "co-op", "coop", "student", "summer", "placement",
    "manager", "director", "head of", "vp", "president", "chief", "principal", "lead",
    "sales", "customer service", "technician", "support", "clerk", "admin",
    "marketing", "account executive", "driver", "warehouse", "nurse", "bilingual",
    "business analyst", "business systems analyst", "business system analyst"
]

STRONG_KEYWORDS = [
    "data scientist", "data engineer", "machine learning", "ai engineer",
    "computer vision", "nlp", "business intelligence", "deep learning",
    "data analyst", "quantitative researcher"
]

AMBIGUOUS_KEYWORDS = [
    "analyst", "insights", "consultant", "scientist", "researcher",
    "strategist", "specialist", "associate"
]

TECH_KEYWORDS = [
    "sql", "python", " r ", "r-programming", "tableau", "power bi", "powerbi",
    "aws", "azure", "gcp", "snowflake", "etl", "pipeline", "modeling",
    "machine learning", "statistical", "looker", "bigquery", "spark", "hadoop"
]
# ---------------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Robust salary regex (handles –, —, ranges, /yr, per year, CA$, etc.)
SALARY_RE = re.compile(
    r"""
    (?:CA?\$|C\$|\$)\s*[\d]{1,3}(?:[,\s]\d{3})*(?:\.\d+)?\s*[kK]?
    (?:\s*(?:-|–|—|to)\s*
        (?:CA?\$|C\$|\$)?\s*[\d]{1,3}(?:[,\s]\d{3})*(?:\.\d+)?\s*[kK]?
    )?
    (?:\s*(?:/|per\s*)?(?:hour|hr|year|yr|month|mo|week|wk|day|annum))?
    """,
    re.IGNORECASE | re.VERBOSE
)

def make_driver():
    opts = Options()
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    # If you still see flakiness, try commenting this out.
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    )

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(30)
    return driver

def clean_salary_text(text: str) -> str:
    if not text:
        return "N/A"
    t = " ".join(text.split())
    m = SALARY_RE.search(t)
    return m.group(0).strip() if m else "N/A"

def debug_salary(driver, job_title="", job_url=""):
    global _salary_debug_count
    if not DEBUG_SALARY:
        return
    if _salary_debug_count >= MAX_SALARY_DEBUG_PRINTS:
        return
    _salary_debug_count += 1

    print("\n========== SALARY DEBUG ==========")
    if job_title:
        print("JOB:", job_title)
    if job_url:
        print("URL:", job_url)

    # Pane text
    try:
        pane = driver.find_element(By.CSS_SELECTOR, '[data-testid="viewJobBodyContainer"]')
        txt = (pane.get_attribute("innerText") or "")
        print("\n--- PANE innerText (first 700 chars) ---")
        print(txt[:700])
        idx = txt.find("$")
        if idx != -1:
            print("\n--- PANE near first $ ---")
            print(txt[max(0, idx-140): idx+260])
        else:
            print("\n--- No $ found in pane innerText ---")
    except Exception as e:
        print("Could not read viewJobBodyContainer:", repr(e))

    # Compensation block outerHTML
    try:
        comp = driver.find_element(By.CSS_SELECTOR, '[data-testid="viewJobBodyJobCompensation"]')
        print("\n--- COMP textContent ---")
        print((comp.get_attribute("textContent") or "").strip())
        print("\n--- COMP outerHTML (first 500 chars) ---")
        print((comp.get_attribute("outerHTML") or "")[:500])
    except Exception:
        print("\n--- COMP block NOT FOUND ---")

    print("=================================\n")

def _scroll_job_body_once(driver):
    """Force-render lazy bits by scrolling the job body container a bit."""
    try:
        body = driver.find_element(By.CSS_SELECTOR, '[data-testid="viewJobBodyContainer"]')
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + 400;", body)
        time.sleep(0.15)
        driver.execute_script("arguments[0].scrollTop = 0;", body)
        time.sleep(0.1)
    except Exception:
        pass

def _extract_salary_from_current_page(driver, wait_seconds=8) -> str:
    """
    Extract salary from the currently loaded job details page/pane.
    Waits for NON-EMPTY text that matches SALARY_RE.
    """
    wait = WebDriverWait(driver, wait_seconds)

    # Primary: exact node from your screenshot
    selectors = [
        '[data-testid="viewJobBodyJobCompensation"] [data-testid="detailText"]',
        '[data-testid="viewJobBodyJobCompensation"]',
    ]

    # Try a few selectors and require regex match (non-empty)
    for sel in selectors:
        try:
            el = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))

            def has_salary_text(_):
                txt = (el.get_attribute("textContent") or "").strip()
                return bool(SALARY_RE.search(" ".join(txt.split())))

            wait.until(has_salary_text)
            txt = (el.get_attribute("textContent") or "").strip()
            sal = clean_salary_text(txt)
            if sal != "N/A":
                return sal
        except Exception:
            pass

    # Fallback: search entire job body container text for salary pattern
    try:
        body = driver.find_element(By.CSS_SELECTOR, '[data-testid="viewJobBodyContainer"]')
        txt = (body.get_attribute("innerText") or "").strip()
        sal = clean_salary_text(txt)
        if sal != "N/A":
            return sal
    except Exception:
        pass

    return "N/A"

def get_salary_with_retries(driver, job_title="", job_url="") -> str:
    """
    Robust: retries salary extraction because React hydration can be flaky.
    """
    # CRITICAL: Wait for compensation block to exist first
    wait = WebDriverWait(driver, 5)
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="viewJobBodyJobCompensation"]')))
        time.sleep(0.5)  # Extra stabilization
    except TimeoutException:
        # Comp block doesn't exist, skip retries
        debug_salary(driver, job_title=job_title, job_url=job_url)
        return "N/A"
    
    for attempt in range(1, SALARY_RETRIES + 1):
        _scroll_job_body_once(driver)
        sal = _extract_salary_from_current_page(driver, wait_seconds=SALARY_WAIT_SECONDS)
        if sal != "N/A":
            return sal
        # small backoff and try again
        time.sleep(0.25 * attempt)

    # Debug after retries fail
    debug_salary(driver, job_title=job_title, job_url=job_url)
    return "N/A"

def get_salary_by_opening_url_in_new_tab(driver, url: str, job_title="") -> str:
    """
    Last resort: open the job URL in a separate tab, extract salary, close tab.
    This avoids the "right pane didn't actually update" problem completely.
    """
    if not url or url == "N/A":
        return "N/A"

    original = driver.current_window_handle
    existing_tabs = driver.window_handles[:]

    # Open new tab
    driver.execute_script("window.open(arguments[0], '_blank');", url)
    time.sleep(0.3)

    # Switch to newest tab
    new_tabs = driver.window_handles
    new_handle = next((h for h in new_tabs if h not in existing_tabs), None)
    if not new_handle:
        return "N/A"

    driver.switch_to.window(new_handle)

    try:
        WebDriverWait(driver, 12).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="viewJobBodyContainer"]'))
        )
        sal = get_salary_with_retries(driver, job_title=job_title, job_url=url)
    except Exception:
        sal = "N/A"

    # Close tab & return
    driver.close()
    driver.switch_to.window(original)
    return sal

def extract_qualifications(driver) -> list:
    """
    Returns a list of qualification strings from the details pane.
    """
    try:
        items = driver.find_elements(By.CSS_SELECTOR, 'span[data-testid="viewJobQualificationItem"]')
        quals = []
        for el in items:
            t = (el.get_attribute("textContent") or "").strip()
            if t:
                quals.append(t)
        # de-dupe while preserving order
        seen = set()
        out = []
        for q in quals:
            if q not in seen:
                seen.add(q)
                out.append(q)
        return out
    except Exception:
        return []

def parse_job_data(driver, card):
    data = {}

    # --- 1) PRE-CLICK SCRAPE FROM CARD HTML ---
    try:
        card_html = card.get_attribute("outerHTML")
        soup = BeautifulSoup(card_html, "lxml")

        title_tag = soup.find("a", class_=lambda x: x and "jobTitle" in x)
        if not title_tag:
            title_tag = soup.find("a")

        raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
        data["title"] = raw_title

        href = title_tag.get("href", "") if title_tag else ""
        if href and not href.startswith("http"):
            href = "https://www.simplyhired.ca" + href
        data["url"] = href.split("?")[0] if href else "N/A"

        comp_tag = soup.find("span", attrs={"data-testid": "companyName"})
        data["company"] = comp_tag.get_text(strip=True) if comp_tag else "N/A"

        loc_tag = soup.find("span", attrs={"data-testid": "searchSerpJobLocation"})
        data["location"] = loc_tag.get_text(strip=True) if loc_tag else "N/A"

    except Exception:
        return None

    # --- 2) CLICK & SYNC (wait for pane to update) ---
    prev_desc = ""
    try:
        prev_desc = driver.find_element(
            By.CSS_SELECTOR,
            'div[data-testid="viewJobBodyJobFullDescriptionContent"]'
        ).text.strip()
    except Exception:
        prev_desc = ""

    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
    time.sleep(0.2)

    try:
        link_elem = card.find_element(By.TAG_NAME, "a")
        driver.execute_script("arguments[0].click();", link_elem)
    except Exception:
        driver.execute_script("arguments[0].click();", card)

    wait = WebDriverWait(driver, 12)
    try:
        wait.until(lambda d: (
            d.find_element(By.CSS_SELECTOR, 'div[data-testid="viewJobBodyJobFullDescriptionContent"]').text.strip()
            and d.find_element(By.CSS_SELECTOR, 'div[data-testid="viewJobBodyJobFullDescriptionContent"]').text.strip() != prev_desc
        ))
        # Extra wait for everything to hydrate - increased to 3 seconds
        time.sleep(3)
        
        # FORCE RENDER: Scroll down the job body to trigger lazy-loading of all elements
        try:
            body = driver.find_element(By.CSS_SELECTOR, '[data-testid="viewJobBodyContainer"]')
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", body)
            time.sleep(0.5)
            driver.execute_script("arguments[0].scrollTop = 0;", body)
            time.sleep(0.5)
        except Exception:
            pass
            
    except Exception:
        data["description"] = "N/A"
        data["salary"] = "N/A"
        data["date_posted"] = "N/A"
        data["scraped_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return data

    # --- 3) SCRAPE DETAILS PANE ---
    try:
        desc_elem = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="viewJobBodyJobFullDescriptionContent"]')
        data["description"] = desc_elem.text
    except Exception:
        data["description"] = "N/A"

    # Qualifications (try but don't fail if missing - some jobs have no quals listed)
    try:
        WebDriverWait(driver, 3).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'span[data-testid="viewJobQualificationItem"]'))
        )
    except TimeoutException:
        # No qualifications found, that's okay
        pass
    
    quals = extract_qualifications(driver)
    data["qualifications"] = "; ".join(quals) if quals else "N/A"

    # Salary (retry-based with longer wait)
    data["salary"] = get_salary_with_retries(driver, job_title=data["title"], job_url=data["url"])

    # If still N/A and enabled, open URL in new tab and extract there
    if data["salary"] == "N/A" and OPEN_URL_FALLBACK:
        data["salary"] = get_salary_by_opening_url_in_new_tab(driver, data["url"], job_title=data["title"])

    try:
        date_elem = driver.find_element(By.CSS_SELECTOR, 'span[data-testid="viewJobBodyJobPostingTimestamp"]')
        data["date_posted"] = date_elem.text.replace("Posted", "").strip()
    except Exception:
        data["date_posted"] = "N/A"

    data["scraped_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return data

def save_job(job_data):
    if not job_data:
        return
    
    # Ensure consistent column order
    ordered_data = {
        "title": job_data.get("title", "N/A"),
        "company": job_data.get("company", "N/A"),
        "location": job_data.get("location", "N/A"),
        "salary": job_data.get("salary", "N/A"),
        "date_posted": job_data.get("date_posted", "N/A"),
        "qualifications": job_data.get("qualifications", "N/A"),
        "description": job_data.get("description", "N/A"),
        "url": job_data.get("url", "N/A"),
        "scraped_at": job_data.get("scraped_at", "N/A")
    }
    
    df = pd.DataFrame([ordered_data])
    header = not os.path.exists(OUTPUT_FILE)
    try:
        df.to_csv(OUTPUT_FILE, mode="a", header=header, index=False, encoding="utf-8")
    except Exception:
        pass

def run():
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    driver = make_driver()
    total_saved = 0

    try:
        for kw in KEYWORDS:
            logger.info(f"=== SEARCHING: {kw} ===")
            url = (
                f"https://www.simplyhired.ca/search?"
                f"q={kw.replace(' ', '+')}&l={LOCATION.replace(' ', '+')}&w={RADIUS}&so=d"
            )
            driver.get(url)

            page_num = 1
            while True:
                try:
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "job-list")))

                    cards = driver.find_elements(By.CSS_SELECTOR, "div[class*='SerpJob']")
                    if not cards:
                        cards = driver.find_elements(By.CSS_SELECTOR, "#job-list > li")

                    logger.info(f"Page {page_num}: Scanning {len(cards)} cards...")

                    for i in range(len(cards)):
                        if total_saved >= MAX_JOBS_TO_SCRAPE:
                            break

                        try:
                            cards = driver.find_elements(By.CSS_SELECTOR, "div[class*='SerpJob']")
                            if not cards:
                                cards = driver.find_elements(By.CSS_SELECTOR, "#job-list > li")
                            if i >= len(cards):
                                break

                            card = cards[i]

                            try:
                                title_elem = card.find_element(By.CSS_SELECTOR, "a[class*='jobTitle']")
                            except Exception:
                                title_elem = card.find_element(By.TAG_NAME, "a")

                            raw_title = title_elem.text.strip()
                            title_lower = raw_title.lower()

                            if any(bad in title_lower for bad in BAD_KEYWORDS):
                                continue

                            relevance_type = "SKIP"
                            if any(s in title_lower for s in STRONG_KEYWORDS):
                                relevance_type = "KEEP_IMMEDIATE"
                            elif any(a in title_lower for a in AMBIGUOUS_KEYWORDS):
                                relevance_type = "CHECK_DESCRIPTION"

                            if relevance_type == "SKIP":
                                continue

                            job_data = parse_job_data(driver, card)
                            if not job_data:
                                continue

                            should_save = False
                            if relevance_type == "KEEP_IMMEDIATE":
                                should_save = True
                                print(f"   [KEEP STRONG] {raw_title} | Salary: {job_data['salary']}")
                            elif relevance_type == "CHECK_DESCRIPTION":
                                if job_data["description"] != "N/A":
                                    desc_lower = job_data["description"].lower()
                                    if any(t in desc_lower for t in TECH_KEYWORDS):
                                        should_save = True
                                        print(f"   [KEEP VERIFIED] {raw_title} | Salary: {job_data['salary']}")

                            if should_save:
                                save_job(job_data)
                                total_saved += 1

                            time.sleep(random.uniform(0.8, 1.6))

                        except Exception:
                            continue

                    if total_saved >= MAX_JOBS_TO_SCRAPE:
                        break

                    try:
                        next_btn = driver.find_element(By.CSS_SELECTOR, "a[aria-label='Next page']")
                        driver.execute_script("arguments[0].click();", next_btn)
                        page_num += 1
                        time.sleep(2.5)

                        if page_num > MAX_PAGES_PER_KEYWORD:
                            logger.info(f"Reached max pages ({MAX_PAGES_PER_KEYWORD}) for '{kw}'")
                            break

                    except NoSuchElementException:
                        logger.info("End of results.")
                        break

                except Exception as e:
                    logger.error(f"Page Error: {e}")
                    break

            if total_saved >= MAX_JOBS_TO_SCRAPE:
                break

    finally:
        driver.quit()
        logger.info(f"Done. Saved {total_saved} relevant jobs to {OUTPUT_FILE}")

if __name__ == "__main__":
    run()
