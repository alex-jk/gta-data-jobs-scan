import time
import random
import pandas as pd
import logging
import os
import re
from bs4 import BeautifulSoup

# Check for torch availability for the summarizer
try:
    import torch
except ImportError:
    torch = None

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# ---------- CONFIG ----------
KEYWORDS = ["statistical analysis, python, sql, modeling"]
LOCATION = "Toronto, ON"
RADIUS = 50
OUTPUT_FILE = "simplyhired_final_cleaned.csv"
MAX_JOBS_TO_SCRAPE = 600
MAX_PAGES_PER_KEYWORD = 18

# Salary reliability controls
SALARY_RETRIES = 3              
SALARY_WAIT_SECONDS = 8         
OPEN_URL_FALLBACK = True        

# --- ADVANCED KEYWORD LOGIC (IMPORTED FROM CODE A) ---
BAD_KEYWORDS = [
    "intern", "co-op", "coop", "student", "summer", "placement",
    "manager", "director", "head of", "vp", "president", "chief", "principal", "lead",
    "sales", "customer service", "technician", "support", "clerk", "admin",
    "marketing", "account executive", "driver", "warehouse", "nurse", "bilingual",
    "business analyst", "business systems analyst", "business system analyst"
]

STRONG_KEYWORDS = [
    "data scientist", "data engineer", "machine learning", "ai engineer", "analytics",
    "computer vision", "nlp", "business intelligence", "deep learning",
    "data analyst", "quantitative researcher", "statistical modeling", "statistician"
]

AMBIGUOUS_KEYWORDS = [
    "analyst", "insights", "consultant", "scientist", "researcher",
    "strategist", "specialist", "associate"
]

TECH_KEYWORDS = [
    "sql", "python", " r ", "r-programming", "tableau", "power bi", "powerbi",
    "aws", "azure", "gcp", "snowflake", "etl", "pipeline", "modeling", "models",
    "machine learning", "statistical", "looker", "bigquery", "spark", "hadoop"
]
# ---------------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

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
    # Create Chrome options object to customize browser behavior
    opts = Options()
    # Set a consistent browser window size (avoids responsive/mobile layouts)
    opts.add_argument("--window-size=1600,1000")
    # Reduce Selenium detection by disabling Chrome automation flags
    opts.add_argument("--disable-blink-features=AutomationControlled")
    # Spoof a realistic desktop Chrome user-agent to avoid bot blocking
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
    # Automatically download and configure the correct ChromeDriver version
    service = Service(ChromeDriverManager().install())
    # Launch a Chrome browser instance with the configured options
    driver = webdriver.Chrome(service=service, options=opts)
    # Fail fast if a page takes too long to load (prevents hanging)
    driver.set_page_load_timeout(30)
    # Return the ready-to-use Selenium WebDriver
    return driver

def clean_salary_text(text: str) -> str:
    # Handle missing or empty salary strings
    if not text:
        return "N/A"
    # Normalize whitespace (remove newlines, tabs, extra spaces)
    t = " ".join(text.split())
    # Extract salary pattern using a precompiled regex
    m = SALARY_RE.search(t)
    # Return cleaned salary text if found, otherwise mark as unavailable
    return m.group(0).strip() if m else "N/A"

def parse_job_data(driver, card, prev_desc):
    data = {}
    # A "card" = one job listing block on the search results page
    # (the clickable tile that shows job title, company, etc.)
    # --- 1) CARD INFO ---
    try:
        # Grab the full HTML of this single job card from the page
        card_html = card.get_attribute("outerHTML")
        # Parse the card HTML with BeautifulSoup for easier extraction
        soup = BeautifulSoup(card_html, "lxml")
        # Find the job title link (class usually contains "jobTitle")
        # Fallback to any <a> tag if the class changes
        title_tag = (
            soup.find("a", class_=lambda x: x and "jobTitle" in x)
            or soup.find("a")
        )
        # Extract job title text
        data["title"] = title_tag.get_text(strip=True) if title_tag else "N/A"
        # Extract job link (href) from the title anchor
        href = title_tag.get("href", "") if title_tag else ""
        # Convert relative URLs to absolute SimplyHired URLs
        data["url"] = (
            "https://www.simplyhired.ca" + href.split("?")[0]
            if href and not href.startswith("http")
            else href
        )
        # Extract company name if present in the card
        data["company"] = (
            soup.find("span", attrs={"data-testid": "companyName"})
            .get_text(strip=True)
            if soup.find("span", attrs={"data-testid": "companyName"})
            else "N/A"
        )
    # If anything breaks while parsing this card, skip it
    except:
        return None

    print(f"\n--- Processing: {data['title']} ---")

    # --- 2) CLICK & WAIT ---
    # Scroll job card into view so the click is reliable
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
    time.sleep(0.5)
    # Try normal click first; fall back to JS click if intercepted
    try: card.find_element(By.TAG_NAME, "a").click()
    except: driver.execute_script("arguments[0].click();", card)
    # Prepare an explicit wait for the job description panel to load
    wait = WebDriverWait(driver, 12)
    desc_text = "N/A"
    
    # Retry a few times to wait for a new job description to load
    for _ in range(5):
        try:
            # Wait for the description container to appear in the pane
            elem = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-testid="viewJobBodyJobFullDescriptionContent"]')))
            # Accept only non-empty text that is different from the previous job
            if elem.text.strip() != "" and elem.text.strip() != prev_desc:
                desc_text = elem.text.strip()
                break
        except: pass
        time.sleep(1.5)

    # Save final job description and report status
    data["description"] = desc_text
    print(f"   Description: {'OBTAINED' if desc_text != 'N/A' else 'FAILED (Saved as N/A)'}")

    # --- 3) SCROLL PANE & SCRAPE ---
    # Scroll job details pane to load content that appears on scroll
    try:
        pane = driver.find_element(By.CSS_SELECTOR, '[data-testid="viewJobBodyContainer"]')
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", pane)
        time.sleep(0.5)
    except: pass

    # Extract salary from explicit salary section if available; fallback to full pane text
    try:
        sal_box = driver.find_element(By.CSS_SELECTOR, '[data-testid="viewJobBodyJobCompensation"]')
        data["salary"] = clean_salary_text(sal_box.text)
    except:
        data["salary"] = clean_salary_text(driver.find_element(By.CSS_SELECTOR, '[data-testid="viewJobBodyContainer"]').text)
    print(f"   Salary: {data['salary']}")

    # Extract listed qualification items, if present
    try:
        quals = driver.find_elements(By.CSS_SELECTOR, 'span[data-testid="viewJobQualificationItem"]')
        data["qualifications"] = "; ".join(q.text.strip() for q in quals if q.text) or "N/A"
    except:
        data["qualifications"] = "N/A"
    print(f"   Quals: {data['qualifications'][:50].replace('\n', ' ')}...")

    # Timestamp scrape for traceability
    data["scraped_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return data

# --- NEW FUNCTION: SUMMARIZE ONLY THE NEW BUFFER ---
def summarize_new_jobs_buffer(new_jobs_list):
    """
    Takes the list of newly scraped dictionaries, converts to DataFrame,
    runs summarization on them, and returns the DataFrame ready for appending.
    """
    if not new_jobs_list:
        return pd.DataFrame()

    print(f"\n--- Summarizing {len(new_jobs_list)} NEW jobs ---")

    # Try importing required ML libraries; skip if not available
    try:
        from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
    except ImportError:
        print("Transformers not installed. Skipping summarization.")
        return pd.DataFrame(new_jobs_list)

    # Use GPU if available, otherwise fall back to CPU
    device = "cuda" if torch and torch.cuda.is_available() else "cpu"
    print(f"Running on device: {device.upper()}")

    # Load tokenizer and summarization model
    tokenizer = AutoTokenizer.from_pretrained("google/flan-t5-large")
    model = AutoModelForSeq2SeqLM.from_pretrained("google/flan-t5-large").to(device)

    df = pd.DataFrame(new_jobs_list)
    
    def process_text(text):
        # Skip empty, missing, or very short descriptions
        if not text or text == "N/A" or len(str(text).split()) < 80: return text
        try:
            # Split long text into manageable word chunks
            words = str(text).split()
            chunk_size = 450
            chunks = [" ".join(words[i:i + chunk_size]) for i in range(0, len(words), chunk_size)]
            intermediate = []

            # Summarize each chunk individually
            for chunk in chunks:
                prompt = f"Summarize technical skills and duties in this job text: \n\n{chunk}"
                inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=1024).to(device)
                outputs = model.generate(inputs["input_ids"], max_length=150)
                intermediate.append(tokenizer.decode(outputs[0], skip_special_tokens=True))
            
            # Combine chunk summaries into one polished job summary
            final_prompt = (
                "Write a professional paragraph job summary listing tech stack and responsibilities: \n\n"
                + " ".join(intermediate)
            )
            inputs = tokenizer(final_prompt, return_tensors="pt", truncation=True, max_length=1024).to(device)
            outputs = model.generate(inputs["input_ids"], max_length=300, min_length=100, num_beams=4)
            return tokenizer.decode(outputs[0], skip_special_tokens=True)
        except:
            return text

    # Apply summarization to all new job descriptions
    df["description"] = df["description"].apply(process_text)
    df["salary"] = df["salary"].replace(r"^\s*$", "N/A", regex=True).fillna("N/A")
    
    return df

def run():
    # --- UPDATED: DO NOT REMOVE FILE. LOAD EXISTING URLs ---
    seen_urls = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            old_df = pd.read_csv(OUTPUT_FILE)
            if "url" in old_df.columns:
                seen_urls = set(old_df["url"].dropna().tolist())
            print(f"Found existing file with {len(seen_urls)} jobs. Will append only new ones.")
        except Exception as e:
            print(f"Could not read existing file: {e}. Starting fresh.")
    else:
        print("No existing file found. Starting fresh.")

    # Start a Selenium-controlled browser session
    driver = make_driver()
    
    # --- UPDATED: Buffer to hold ONLY new jobs for this run ---
    new_jobs_buffer = [] 
    total_saved_this_run = 0

    try:
        for kw in KEYWORDS:
            print(f"\n=== SEARCHING: {kw} ===")

            # Load SimplyHired search results for keyword + location + radius
            driver.get(
                f"https://www.simplyhired.ca/search?q={kw.replace(' ', '+')}"
                f"&l={LOCATION.replace(' ', '+')}&w={RADIUS}&so=d"
            )

            page_num = 1
            while page_num <= MAX_PAGES_PER_KEYWORD:
                # Wait until the job results list is present
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "job-list"))
                )

                # Collect all job listing cards on the page
                cards = driver.find_elements(By.CSS_SELECTOR, "div[class*='SerpJob']")
                if not cards:
                    cards = driver.find_elements(By.CSS_SELECTOR, "#job-list > li")

                print(f"Page {page_num}: Found {len(cards)} cards.")
                prev_description = ""

                for i in range(len(cards)):
                    # Stop once the global job limit is reached
                    if total_saved_this_run >= MAX_JOBS_TO_SCRAPE:
                        break
                    try:
                        # Re-fetch cards to avoid stale element references
                        cards = driver.find_elements(By.CSS_SELECTOR, "div[class*='SerpJob']")
                        if not cards:
                            cards = driver.find_elements(By.CSS_SELECTOR, "#job-list > li")

                        card = cards[i]

                        # --- NEW STEP: CHECK URL BEFORE CLICKING ---
                        # We try to grab the href immediately to see if we already have this job.
                        # This saves massive time by not clicking/parsing old jobs.
                        try:
                            temp_soup = BeautifulSoup(card.get_attribute("outerHTML"), "lxml")
                            temp_title_tag = (
                                temp_soup.find("a", class_=lambda x: x and "jobTitle" in x)
                                or temp_soup.find("a")
                            )
                            if temp_title_tag:
                                raw_href = temp_title_tag.get("href", "")
                                check_url = (
                                    "https://www.simplyhired.ca" + raw_href.split("?")[0]
                                    if raw_href and not raw_href.startswith("http")
                                    else raw_href
                                )
                                if check_url in seen_urls:
                                    # Skip silently or log debug
                                    continue
                        except:
                            pass # If check fails, continue to normal processing just in case
                        # -------------------------------------------

                        # --- ADVANCED FILTERING LOGIC START ---
                        # Extract title text safely for checking
                        try:
                            # Try to find the title link text specifically
                            title_elem = card.find_element(By.CSS_SELECTOR, "a[class*='jobTitle']")
                        except:
                            # Fallback to full card text if element missing
                            title_elem = card
                            
                        raw_title = title_elem.text.strip()
                        title_lower = raw_title.lower()

                        # 1. Skip if contains BAD keywords
                        if any(bad in title_lower for bad in BAD_KEYWORDS):
                            continue

                        # 2. Determine Relevance Type
                        relevance_type = "SKIP"
                        if any(s in title_lower for s in STRONG_KEYWORDS):
                            relevance_type = "KEEP_IMMEDIATE"
                        elif any(a in title_lower for a in AMBIGUOUS_KEYWORDS):
                            relevance_type = "CHECK_DESCRIPTION"

                        # 3. If irrelevant, skip before clicking
                        if relevance_type == "SKIP":
                            continue
                        # --- ADVANCED FILTERING LOGIC END ---

                        # Parse data from one job card
                        job_data = parse_job_data(driver, card, prev_description)
                        if job_data:
                            # Final check to ensure we don't duplicate (in case the pre-check failed)
                            if job_data['url'] in seen_urls:
                                print("   [DUPLICATE] Already in CSV.")
                                continue

                            prev_description = job_data["description"]
                            
                            # --- DECIDE TO SAVE BASED ON RELEVANCE ---
                            should_save = False
                            
                            if relevance_type == "KEEP_IMMEDIATE":
                                should_save = True
                                print(f"   [KEEP STRONG] {raw_title}")
                            elif relevance_type == "CHECK_DESCRIPTION":
                                # Only save ambiguous jobs if description contains tech keywords
                                if job_data["description"] != "N/A":
                                    desc_lower = job_data["description"].lower()
                                    if any(t in desc_lower for t in TECH_KEYWORDS):
                                        should_save = True
                                        print(f"   [KEEP VERIFIED] {raw_title}")
                                    else:
                                        print(f"   [SKIP AMBIGUOUS] {raw_title} (No tech keywords found)")
                            
                            if should_save:
                                # --- UPDATED: APPEND TO LIST, DON'T SAVE TO CSV YET ---
                                new_jobs_buffer.append(job_data)
                                seen_urls.add(job_data['url']) # Add to seen so we don't grab it again in this run
                                total_saved_this_run += 1
                                print(f"   >>> Buffered New Job. Total New: {total_saved_this_run}")
                                
                    except:
                        continue

                # Stop pagination if job limit reached
                if total_saved_this_run >= MAX_JOBS_TO_SCRAPE:
                    break

                try:
                    # Move to the next search results page
                    next_btn = driver.find_element(By.CSS_SELECTOR, "a[aria-label='Next page']")
                    driver.execute_script("arguments[0].click();", next_btn)
                    page_num += 1
                    time.sleep(3)
                except:
                    break
    finally:
        # Always close the browser
        driver.quit()
        
        # --- PROCESS NEW JOBS ---
        if new_jobs_buffer:
            print(f"\nScraping complete. Found {len(new_jobs_buffer)} NEW jobs.")
            
            # Summarize ONLY the new jobs
            df_final_new = summarize_new_jobs_buffer(new_jobs_buffer)
            
            # Append to the existing CSV
            # If file doesn't exist, header=True. If it exists, header=False
            header = not os.path.exists(OUTPUT_FILE)
            df_final_new.to_csv(OUTPUT_FILE, mode="a", header=header, index=False, encoding="utf-8")
            print(f"Success: Appended {len(df_final_new)} summarized jobs to {OUTPUT_FILE}.")
        else:
            print("\nScraping complete. No new jobs found to append.")

if __name__ == "__main__":
    run()