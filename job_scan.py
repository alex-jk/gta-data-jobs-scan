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
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

# ---------- CONFIG ----------
KEYWORDS = ["data scientist", "data analyst"]
LOCATION = "Toronto, ON"
RADIUS = 50
OUTPUT_FILE = "simplyhired_final_cleaned.csv"
MAX_JOBS_TO_SCRAPE = 500
MAX_PAGES_PER_KEYWORD = 15

BAD_KEYWORDS = [
    "intern", "co-op", "coop", "student", "summer", "placement", 
    "manager", "director", "head of", "vp", "president", "chief", "principal", "lead",
    "sales", "customer service", "technician", "support", "clerk", "admin", "engineer",
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

def make_driver():
    opts = Options()
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

def clean_salary_text(text):
    if not text: return "N/A"
    # Enhanced regex: capture full salary ranges with proper formatting
    # Matches: $50,000 - $70,000 a year, $40/hour, etc.
    salary_pattern = r'\$\s*[\d,]+(?:\.\d{2})?\s*(?:[-–—]\s*\$?\s*[\d,]+(?:\.\d{2})?)?(?:\s*(?:a|per|/)\s*(?:year|yr|annum|hour|hr|month|mo))?'
    match = re.search(salary_pattern, text, re.IGNORECASE)
    return match.group(0).strip() if match else "N/A"

def parse_job_data(driver, card):
    data = {}
    
    # --- 1. PRE-CLICK SCRAPE (Card Info) ---
    try:
        card_html = card.get_attribute('outerHTML')
        soup = BeautifulSoup(card_html, "lxml")
        
        title_tag = soup.find("a", class_=lambda x: x and "jobTitle" in x)
        if not title_tag: title_tag = soup.find("a")
        raw_title = title_tag.get_text(strip=True) if title_tag else "N/A"
        data['title'] = raw_title
        
        href = title_tag.get("href", "") if title_tag else ""
        if href and not href.startswith("http"): href = "https://www.simplyhired.ca" + href
        data['url'] = href.split("?")[0]

        data['company'] = soup.find("span", attrs={"data-testid": "companyName"}).get_text(strip=True) if soup.find("span", attrs={"data-testid": "companyName"}) else "N/A"
        data['location'] = soup.find("span", attrs={"data-testid": "searchSerpJobLocation"}).get_text(strip=True) if soup.find("span", attrs={"data-testid": "searchSerpJobLocation"}) else "N/A"
        
        card_salary = "N/A"
        sal_tag = soup.find("span", attrs={"data-testid": "searchSerpJobSalary"})
        if sal_tag:
            card_salary = sal_tag.get_text(strip=True)
    except Exception:
        return None

    # --- 2. CLICK & SYNC ---
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
    time.sleep(0.3)
    
    try:
        card.find_element(By.TAG_NAME, "a").click()
    except:
        driver.execute_script("arguments[0].click();", card)

    wait = WebDriverWait(driver, 7)
    try:
        # Confirm pane has switched to the clicked job
        wait.until(lambda d: raw_title[:10] in d.find_element(By.CSS_SELECTOR, "div[data-testid='viewJobBodyContainer']").text)
        
        # --- FIX: LONGER METADATA DELAY ---
        # Increased to 4s to ensure Salary and Qualifications blocks fully hydrate
        time.sleep(4) 
    except:
        data['description'] = "N/A" 
        data['salary'] = clean_salary_text(card_salary)
        data['qualifications'] = "N/A"
        data['date_posted'] = "N/A"
        data['scraped_at'] = time.strftime("%Y-%m-%d %H:%M:%S")
        return data

    # --- 3. SCRAPE DETAILS PANE ---
    try:
        # Description
        desc_elem = driver.find_element(By.CSS_SELECTOR, "div[data-testid='viewJobBodyJobFullDescriptionContent']")
        data['description'] = desc_elem.text
        
        # Salary - with fallback chain
        detail_sal = "N/A"
        try:
            # Try the nested detailText first (cleanest format)
            detail_sal = driver.find_element(By.CSS_SELECTOR, '[data-testid="viewJobBodyJobCompensation"] [data-testid="detailText"]').text
        except:
            try:
                # Fallback to compensation block text
                detail_sal = driver.find_element(By.CSS_SELECTOR, '[data-testid="viewJobBodyJobCompensation"]').text
            except:
                detail_sal = card_salary
        data['salary'] = clean_salary_text(detail_sal)

        # Qualifications
        try:
            quals_elems = driver.find_elements(By.CSS_SELECTOR, '[data-testid="viewJobQualificationItem"]')
            # Map items to their detailText spans for cleaner extraction
            qual_list = []
            for q in quals_elems:
                try:
                    qual_list.append(q.find_element(By.CSS_SELECTOR, '[data-testid="detailText"]').text)
                except:
                    qual_list.append(q.text.strip())
            data['qualifications'] = "; ".join(qual_list) if qual_list else "N/A"
        except:
            data['qualifications'] = "N/A"

        # Date Posted
        try:
            date_elem = driver.find_element(By.CSS_SELECTOR, "span[data-testid='viewJobBodyJobPostingTimestamp']")
            data['date_posted'] = date_elem.text.replace("Posted", "").strip()
        except:
            data['date_posted'] = "N/A"

    except Exception:
        data['description'] = "N/A"

    data['scraped_at'] = time.strftime("%Y-%m-%d %H:%M:%S")
    return data

def save_job(job_data):
    if not job_data: return
    df = pd.DataFrame([job_data])
    header = not os.path.exists(OUTPUT_FILE)
    try:
        df.to_csv(OUTPUT_FILE, mode='a', header=header, index=False, encoding='utf-8')
    except:
        pass

def run():
    if os.path.exists(OUTPUT_FILE): os.remove(OUTPUT_FILE)
    driver = make_driver()
    total_saved = 0

    try:
        for kw in KEYWORDS:
            logger.info(f"=== SEARCHING: {kw} ===")
            url = f"https://www.simplyhired.ca/search?q={kw.replace(' ', '+')}&l={LOCATION.replace(' ', '+')}&w={RADIUS}&so=d"
            driver.get(url)
            
            page_num = 1
            while True:
                try:
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "job-list")))
                    cards = driver.find_elements(By.CSS_SELECTOR, "div[class*='SerpJob']")
                    if not cards: cards = driver.find_elements(By.CSS_SELECTOR, "#job-list > li")
                    
                    logger.info(f"Page {page_num}: Scanning {len(cards)} cards...")
                    
                    for i in range(len(cards)):
                        if total_saved >= MAX_JOBS_TO_SCRAPE: break

                        try:
                            cards = driver.find_elements(By.CSS_SELECTOR, "div[class*='SerpJob']")
                            if not cards: cards = driver.find_elements(By.CSS_SELECTOR, "#job-list > li")
                            if i >= len(cards): break
                            card = cards[i]

                            # 1. TITLE FILTER
                            title_text = card.text.split('\n')[0].lower()
                            if any(bad in title_text for bad in BAD_KEYWORDS): 
                                continue

                            relevance = "SKIP"
                            if any(s in title_text for s in STRONG_KEYWORDS): relevance = "KEEP"
                            elif any(a in title_text for a in AMBIGUOUS_KEYWORDS): relevance = "CHECK"
                            
                            if relevance == "SKIP": continue

                            # 2. CLICK & SYNC
                            job_data = parse_job_data(driver, card)
                            if not job_data: continue

                            # 3. VERIFY & SAVE
                            should_save = False
                            if relevance == "KEEP":
                                should_save = True
                                print(f"   [KEEP] {job_data['title']} | Salary: {job_data['salary']}")
                            elif relevance == "CHECK" and job_data['description'] != "N/A":
                                if any(t in job_data['description'].lower() for t in TECH_KEYWORDS):
                                    should_save = True
                                    print(f"   [VERIFIED] {job_data['title']} | Salary: {job_data['salary']}")

                            if should_save:
                                save_job(job_data)
                                total_saved += 1
                            
                            time.sleep(random.uniform(0.5, 1.0))

                        except Exception:
                            continue

                    if total_saved >= MAX_JOBS_TO_SCRAPE: break

                    # Pagination
                    try:
                        next_btn = driver.find_element(By.CSS_SELECTOR, "a[aria-label='Next page']")
                        driver.execute_script("arguments[0].click();", next_btn)
                        page_num += 1
                        time.sleep(3)
                        if page_num > MAX_PAGES_PER_KEYWORD: break
                    except:
                        break
                
                except Exception as e:
                    logger.error(f"Page Error: {e}")
                    break
    finally:
        driver.quit()
        logger.info(f"Done. Saved {total_saved} relevant jobs to {OUTPUT_FILE}")

if __name__ == "__main__":
    run()